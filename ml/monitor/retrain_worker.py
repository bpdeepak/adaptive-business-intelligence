"""Governed retrain worker (Phase 4).

Consumes gold.retrain_requests rows written by the `retrain_model` action
(proposed when a drift event is critical, executed only after human approval).
The worker is itself an action-executing path, so it enforces the governance
invariant on its own: a request is honoured ONLY when its action_id is a
`retrain_model` action with an `approved`/`auto_approved` audit transition and
the request's model is the one that approved action targets. Anything else is
refused (marked failed, with an audit `outcome` when the action exists) — a row
inserted straight into gold.retrain_requests trains nothing.
For each authorised pending request it re-runs that model's trainer as a subprocess
(script mode, so the trainers' `import common` resolves), pinned to an
auditable version tag ``<now_tag>.retrain<action_id>``, registered as a
candidate — it NEVER supersedes the serving model; promotion to `active` is a
deliberate human flip in gold.model_registry.

After each run it records the audit ``outcome`` transition on the action and
finishes the request, closing the approved -> execute -> outcome loop that the
dashboard approval history traces end-to-end.

Run:  uv run --group ml python -m ml.monitor.retrain_worker [--watch]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import subprocess
import sys
import time

import psycopg.rows

from ml import common

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

TRAINER_BY_MODEL = {
    "fraud_risk": "ml/train_fraud.py",
    "bot_score": "ml/train_bot.py",
    "churn_risk": "ml/train_churn.py",
    "forecast_category_weekly_revenue": "ml/train_forecast.py",
    "forecast_category_weekly_orders": "ml/train_forecast.py",
}


def audit(c, action_id: int, transition: str, detail: dict) -> None:
    c.execute(
        """
        INSERT INTO gold.action_audit_log (action_id, transition, actor, detail)
        VALUES (%s, %s, 'retrain-worker', %s::jsonb)
        """,
        (action_id, transition, json.dumps(detail, default=str)),
    )


def refresh_sidecar_manifest() -> None:
    """Rebuild artifacts/sidecar_models.json from the active registry rows."""
    try:
        ml_dir = str(ROOT / "ml")
        if ml_dir not in sys.path:
            sys.path.insert(0, ml_dir)
        import train_all  # type: ignore[import-not-found]  # noqa: PLC0415

        train_all.write_sidecar_manifest()
    except Exception as exc:  # noqa: BLE001 — manifest refresh must not fail the job
        print(f"  (warn) sidecar manifest refresh failed: {exc}")


def _approved_model(trigger: dict, params: dict) -> str:
    """The model an approved retrain action targets — same precedence as the Go
    `retrain_model` executor: event drift.model, event model, then params.model."""
    payload = (trigger or {}).get("payload") or {}
    drift = payload.get("drift") or {}
    return drift.get("model") or payload.get("model") or (params or {}).get("model") or ""


def authorise(c, request: dict) -> tuple[bool, str]:
    """Governance check for one retrain request. Returns (ok, refusal reason).

    Mirrors the Go execution guard (actions.Service.execute): the action must be
    a retrain_model action and carry an approved/auto_approved audit row.
    """
    row = c.execute(
        "select action, params, trigger from gold.action_queue where id = %s",
        (request["action_id"],),
    ).fetchone()
    if row is None:
        return False, f"action {request['action_id']} does not exist on gold.action_queue"
    action, params, trigger = row
    if action != "retrain_model":
        return False, f"action {request['action_id']} is '{action}', not retrain_model"
    approved = c.execute(
        """
        select 1 from gold.action_audit_log
         where action_id = %s and transition in ('approved', 'auto_approved')
         limit 1
        """,
        (request["action_id"],),
    ).fetchone()
    if approved is None:
        return False, f"action {request['action_id']} has no approved/auto_approved audit row"
    target = _approved_model(trigger, params)
    if target != request["model"]:
        return False, (f"request model '{request['model']}' is not the approved action's "
                       f"target '{target}'")
    return True, ""


STALE_RUNNING = dt.timedelta(hours=2)


def reap_stale_running(c, max_age: dt.timedelta = STALE_RUNNING) -> int:
    """Fail requests a crashed worker left in 'running'.

    The worker marks a request running before it launches the trainer; if the
    process dies mid-training nothing ever finalises it, and the request sat
    'running' forever with no audit outcome. A request running longer than
    ``max_age`` is marked failed (with an audit ``outcome``) so a human sees it. It
    is NOT silently re-queued: another training run is a governed effect, and the
    approval that authorised the first attempt should be looked at before one more.
    """
    stale = c.execute(
        """
        select id, action_id from gold.retrain_requests
         where status = 'running' and started_at is not null
           and started_at < now() - %s
        """,
        (max_age,),
    ).fetchall()
    for rid, action_id in stale:
        detail = {"error": f"worker interrupted: no completion within {max_age} of starting",
                  "interrupted": True}
        c.execute(
            "update gold.retrain_requests set status = 'failed', finished_at = now(), detail = %s::jsonb where id = %s",
            (json.dumps(detail), rid),
        )
        if c.execute("select 1 from gold.action_queue where id = %s", (action_id,)).fetchone():
            audit(c, action_id, "outcome", {"status": "failed", **detail})
    if stale:
        c.commit()
    return len(stale)


def process_request(c, request: dict, dry_run: bool) -> None:
    rid = request["id"]
    action_id = request["action_id"]
    model = request["model"]

    ok, refusal = authorise(c, request)
    if not ok:
        print(f"  REFUSED request {rid} (action {action_id}): {refusal}")
        c.execute(
            """
            UPDATE gold.retrain_requests
               SET status = 'failed', finished_at = now(), detail = %s::jsonb
             WHERE id = %s
            """,
            (json.dumps({"error": f"refused: {refusal}", "refused": True}), rid),
        )
        # An audit outcome can only reference an action that exists (FK).
        if c.execute("select 1 from gold.action_queue where id = %s", (action_id,)).fetchone():
            audit(c, action_id, "outcome", {"status": "failed", "refused": True, "error": refusal})
        c.commit()
        return

    def finalize(status: str, detail: dict) -> None:
        c.execute(
            """
            UPDATE gold.retrain_requests
               SET status = %s, finished_at = now(), detail = %s::jsonb
             WHERE id = %s
            """,
            (status, json.dumps(detail, default=str), rid),
        )
        audit(c, action_id, "outcome", {"status": status, **detail})

    script = TRAINER_BY_MODEL.get(model)
    if script is None:
        print(f"  no trainer mapped for model '{model}'; marking failed")
        finalize("failed", {"error": f"no trainer mapped for model {model}"})
        return

    tag = f"{common.now_tag()}.retrain{action_id}"
    cmd = [sys.executable, script, "--version", tag, "--candidate"]
    print(f"retrain {model} (request {rid}, action {action_id}) -> {tag}")
    if dry_run:
        print(f"  [dry-run] would run: {' '.join(cmd)}")
        return

    c.execute("UPDATE gold.retrain_requests SET status = 'running', started_at = now() WHERE id = %s", (rid,))
    c.commit()

    try:
        subprocess.run(cmd, cwd=str(ROOT), check=True)
    except Exception as exc:  # noqa: BLE001 — surfaced into the audit log
        print(f"  FAILED: {exc}")
        finalize("failed", {"error": str(exc), "new_version": tag})
        c.commit()
        return

    finalize("done", {"new_version": tag, "trained_candidate": True})
    c.commit()
    print(f"  done: {model} candidate {tag} registered; audit 'outcome' written for action {action_id}")
    refresh_sidecar_manifest()


def main() -> int:
    ap = argparse.ArgumentParser(description="Consume gold.retrain_requests and retrain candidate models")
    ap.add_argument("--watch", action="store_true", help="poll every 60s instead of running once")
    ap.add_argument("--dry-run", action="store_true", help="print pending requests, run nothing")
    args = ap.parse_args()

    while True:
        with common.conn() as c:
            reaped = reap_stale_running(c)
            if reaped:
                print(f"marked {reaped} interrupted retrain request(s) failed")
            cur = c.cursor(row_factory=psycopg.rows.dict_row)
            cur.execute(
                "select id, action_id, model from gold.retrain_requests where status = 'pending' order by id"
            )
            pending = cur.fetchall()
            if not pending:
                print(f"[{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}] no pending retrains")
            for request in pending:
                process_request(c, request, args.dry_run)

        if not args.watch:
            return 0
        time.sleep(60)


if __name__ == "__main__":
    sys.exit(main())