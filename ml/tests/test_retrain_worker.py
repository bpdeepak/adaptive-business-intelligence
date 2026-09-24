"""The retrain worker is an action-executing path, so it must enforce the same
governance invariant as the Go execution guard: no training without a prior
approved/auto_approved audit row on a `retrain_model` action for that model.

DB-backed (skips when Postgres is unreachable). The governance tables come from
the shared DDL (api/internal/actions/schema.sql via ml.common), so the test does
not depend on the Go server having run first. Training itself is stubbed.
"""
from __future__ import annotations

import json
import time

import pytest

from ml import common
from ml.monitor import retrain_worker


@pytest.fixture()
def db():
    try:
        common.ensure_governance_tables()
        conn = common.conn()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"postgres unreachable: {exc}")
    created: list[int] = []
    yield conn, created
    conn.rollback()
    for action_id in created:
        conn.execute("delete from gold.retrain_requests where action_id = %s", (action_id,))
        # The audit log is append-only (DB trigger); test cleanup opts in explicitly,
        # for this transaction only.
        conn.execute("set local abi.audit_maintenance = 'on'")
        conn.execute("delete from gold.action_audit_log where action_id = %s", (action_id,))
        conn.execute("delete from gold.action_queue where id = %s", (action_id,))
        conn.commit()
    conn.close()


def make_action(conn, created, *, action="retrain_model", model="churn_risk", approved=None,
                params=None, trigger_model=None):
    """Insert a queue row (+ optional approval audit row); return its id."""
    trigger = {"payload": {"drift": {"model": trigger_model if trigger_model is not None else model}}}
    action_id = conn.execute(
        """
        insert into gold.action_queue (action, entity, risk_tier, status, params, trigger, rule, dedup_key)
        values (%s, %s, 'approval_required', 'executed', %s::jsonb, %s::jsonb, 'test', %s)
        returning id
        """,
        (action, model, json.dumps(params or {}), json.dumps(trigger),
         f"it_worker|{time.time_ns()}"),
    ).fetchone()[0]
    created.append(action_id)
    conn.execute(
        "insert into gold.action_audit_log (action_id, transition, actor) values (%s, 'proposed', 'playbook')",
        (action_id,),
    )
    if approved:
        conn.execute(
            "insert into gold.action_audit_log (action_id, transition, actor, reason) values (%s, %s, 'tester', 'ok')",
            (action_id, approved),
        )
    conn.commit()
    return action_id


def make_request(conn, action_id, model):
    rid = conn.execute(
        "insert into gold.retrain_requests (action_id, model) values (%s, %s) returning id",
        (action_id, model),
    ).fetchone()[0]
    conn.commit()
    return {"id": rid, "action_id": action_id, "model": model}


@pytest.fixture()
def trainer(monkeypatch):
    calls: list[list[str]] = []
    monkeypatch.setattr(retrain_worker.subprocess, "run", lambda cmd, **kw: calls.append(cmd))
    monkeypatch.setattr(retrain_worker, "refresh_sidecar_manifest", lambda: None)
    return calls


def status_of(conn, rid):
    return conn.execute("select status, detail from gold.retrain_requests where id = %s", (rid,)).fetchone()


def test_unapproved_request_is_refused_and_trains_nothing(db, trainer):
    conn, created = db
    request = make_request(conn, make_action(conn, created, approved=None), "churn_risk")
    retrain_worker.process_request(conn, request, dry_run=False)
    assert trainer == [], "a request with no approval row must never reach the trainer"
    status, detail = status_of(conn, request["id"])
    assert status == "failed" and detail["refused"] is True
    outcome = conn.execute(
        "select detail from gold.action_audit_log where action_id = %s and transition = 'outcome'",
        (request["action_id"],),
    ).fetchone()
    assert outcome is not None and outcome[0]["refused"] is True


def test_request_for_missing_action_is_refused_without_fk_error(db, trainer):
    conn, created = db
    missing_id = 10**12 + time.time_ns() % 10**9  # unique per run; no such queue row
    request = make_request(conn, missing_id, "churn_risk")
    created.append(missing_id)  # cleanup removes the orphan request row
    retrain_worker.process_request(conn, request, dry_run=False)
    assert trainer == []
    assert status_of(conn, request["id"])[0] == "failed"


def test_approval_of_a_different_action_type_does_not_authorise(db, trainer):
    conn, created = db
    request = make_request(conn, make_action(conn, created, action="log_event_note", approved="approved"),
                           "churn_risk")
    retrain_worker.process_request(conn, request, dry_run=False)
    assert trainer == []
    assert status_of(conn, request["id"])[0] == "failed"


def test_approval_does_not_authorise_a_different_model(db, trainer):
    conn, created = db
    action_id = make_action(conn, created, model="churn_risk", approved="approved")
    request = make_request(conn, action_id, "fraud_risk")  # tampered target
    retrain_worker.process_request(conn, request, dry_run=False)
    assert trainer == []
    assert status_of(conn, request["id"])[0] == "failed"


@pytest.mark.parametrize("transition", ["approved", "auto_approved"])
def test_approved_request_trains_a_candidate(db, trainer, transition):
    conn, created = db
    request = make_request(conn, make_action(conn, created, approved=transition), "churn_risk")
    retrain_worker.process_request(conn, request, dry_run=False)
    assert len(trainer) == 1 and "--candidate" in trainer[0] and "ml/train_churn.py" in trainer[0]
    assert status_of(conn, request["id"])[0] == "done"


def test_model_binding_falls_back_to_params_like_the_go_executor(db, trainer):
    conn, created = db
    # drift.model empty (a hand-proposed action): the Go executor uses params.model.
    action_id = make_action(conn, created, model="", approved="approved",
                            params={"model": "churn_risk"}, trigger_model="")
    request = make_request(conn, action_id, "churn_risk")
    retrain_worker.process_request(conn, request, dry_run=False)
    assert len(trainer) == 1


def test_a_crashed_run_left_running_is_failed_not_forgotten(db, trainer):
    conn, created = db
    action_id = make_action(conn, created, approved="approved")
    request = make_request(conn, action_id, "churn_risk")
    conn.execute("update gold.retrain_requests set status = 'running', started_at = now() - interval '5 hours' where id = %s",
                 (request["id"],))
    conn.commit()
    assert retrain_worker.reap_stale_running(conn) >= 1
    status, detail = status_of(conn, request["id"])
    assert status == "failed" and detail["interrupted"] is True
    assert trainer == [], "reaping must not re-run the trainer: another run is a governed effect"
    outcome = conn.execute(
        "select detail from gold.action_audit_log where action_id = %s and transition = 'outcome'",
        (action_id,)).fetchone()
    assert outcome is not None and outcome[0]["interrupted"] is True


def test_a_recent_running_request_is_left_alone(db, trainer):
    conn, created = db
    request = make_request(conn, make_action(conn, created, approved="approved"), "churn_risk")
    conn.execute("update gold.retrain_requests set status = 'running', started_at = now() where id = %s", (request["id"],))
    conn.commit()
    retrain_worker.reap_stale_running(conn)
    assert status_of(conn, request["id"])[0] == "running"
