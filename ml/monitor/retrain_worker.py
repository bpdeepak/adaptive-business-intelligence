"""Governed retrain worker (Phase 4).

Consumes gold.retrain_requests rows written by the `retrain_model` action
(proposed when a drift event is critical, executed only after human approval).
For each pending request it re-runs that model's trainer as a subprocess
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


def process_request(c, request: dict, dry_run: bool) -> None:
    rid = request["id"]
    action_id = request["action_id"]
    model = request["model"]

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

    c.execute("UPDATE gold.retrain_requests SET status = 'running' WHERE id = %s", (rid,))
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