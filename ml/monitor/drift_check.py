"""Phase 4 drift + decay monitor.

Writes per-feature findings into gold.model_drift (the Go API's model-health
surface). Two drift pipelines plus real-decay checks:

* stream PSI — fraud_risk (order) and bot_score (session): compare the live
  feature vectors the Go score-writer persisted into gold.predictions.features
  against the train-time baseline in gold.model_registry.drift_baseline;
* batch PSI — churn_risk and the two forecast models: compare the current
  feature-mart rows against the same baseline;
* forecast decay — current WAPE over served forecast predictions vs the
  train-time WMAPE recorded in the registry metrics.

Each row is one (model, feature, computed_at) measurement. The Go drift poller
flattens a run by (model, computed_at) into a single event, worst status wins,
so a critical feature proposes ONE retrain.

Run:  uv run --group ml python -m ml.monitor.drift_check [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import numbers
import sys
from typing import Any

import psycopg.rows

from ml import common
from ml.monitor import psi

# model -> grain of the live (stream) feature vectors the Go score-writer keeps
# in gold.predictions.features (metadata.source = 'stream_score').
STREAM_MODELS = {"fraud_risk": "order", "bot_score": "session"}

# model -> (feature mart table, row order column) for batch PSI over current rows.
BATCH_MARTS = {
    "churn_risk": ("gold.feature_customer_churn", "as_of_date"),
    "forecast_category_weekly_revenue": ("gold.feature_forecast_weekly", "week_start"),
    "forecast_category_weekly_orders": ("gold.feature_forecast_weekly", "week_start"),
}

FORECAST_DECAY_MODELS = ("forecast_category_weekly_revenue", "forecast_category_weekly_orders")


def load_active_models(cur) -> list[dict[str, Any]]:
    cur.execute(
        """
        select model_name, model_version, grain, metrics, drift_baseline
        from gold.model_registry
        where status = 'active'
        order by model_name
        """
    )
    out = []
    for r in cur.fetchall():
        baseline = r["drift_baseline"] or {}
        if not baseline:
            continue
        out.append({"name": r["model_name"], "version": r["model_version"],
                    "grain": r["grain"], "metrics": r["metrics"] or {},
                    "baseline": baseline})
    return out


def stream_feature_samples(cur, model: str, grain: str, limit: int) -> dict[str, list[float]]:
    """Collect recent stream feature vectors per feature for one model."""
    cur.execute(
        """
        select features
        from gold.predictions
        where model_name = %s and grain = %s
          and metadata->>'source' = 'stream_score'
          and features <> '[]'::jsonb and jsonb_typeof(features) = 'object'
        order by id desc
        limit %s
        """,
        (model, grain, limit),
    )
    by_feature: dict[str, list[float]] = {}
    # dict_row: rows are dicts keyed by column name, so read row["features"].
    for row in cur.fetchall():
        feats = row["features"]
        if not isinstance(feats, dict):
            continue
        for name, value in feats.items():
            # Accept int/float/Decimal alike: numeric columns round-trip through
            # psycopg as Decimal, integer columns as int, booleans as bool.
            if isinstance(value, numbers.Number):
                by_feature.setdefault(name, []).append(float(value))
    return by_feature


def batch_feature_samples(cur, table: str, order_col: str, baseline: dict, limit: int) -> dict[str, list[float]]:
    """Collect current feature-mart rows (newest first) per feature."""
    cols = ", ".join(baseline.keys())
    cur.execute(f"select {cols} from {table} order by {order_col} desc limit %s", (limit,))
    by_feature: dict[str, list[float]] = {f: [] for f in baseline}
    for r in cur.fetchall():
        for f in baseline:
            v = r.get(f)
            # numeric columns come back as Decimal, int columns as int; accept
            # any Number (bool included: has_prior_week is boolean-typed).
            if isinstance(v, numbers.Number):
                by_feature[f].append(float(v))
    return by_feature


def feature_rows(model: str, version: str, computed_at, baseline: dict,
                 samples: dict[str, list[float]], min_samples: int) -> list[tuple]:
    rows = []
    for feat, ref in baseline.items():
        vals = samples.get(feat) or []
        if len(vals) < min_samples:
            continue
        p = psi.psi_between(ref, vals)
        rows.append((model, version, computed_at, feat, round(p, 6),
                     psi.status_for(p), "psi", json.dumps({"n": len(vals)})))
    return rows


def forecast_decay(cur, plain_rows, model: str, version: str, computed_at,
                   metrics: dict) -> list[tuple]:
    """Current WAPE over served forecast predictions vs train-time WMAPE."""
    cur.execute(
        """
        select prediction, (metadata->>'actual')::float8 as actual
        from gold.predictions
        where model_name = %s and grain = 'category_week'
          and metadata ? 'actual'
        order by id desc
        limit 2000
        """,
        (model,),
    )
    err_sum = 0.0
    abs_actual = 0.0
    n = 0
    # cursor is dict_row here, so rows are dicts keyed by column name.
    for row in cur.fetchall():
        pred, actual = row["prediction"], row["actual"]
        if pred is None or actual is None or pred != pred or actual != actual:
            continue  # skip nulls/NaN
        err_sum += abs(float(pred) - float(actual))
        abs_actual += abs(float(actual))
        n += 1
    if n < 20 or abs_actual <= 0:
        return []
    current = err_sum / abs_actual
    baseline_wmape = float(metrics.get("wmape") or current)
    status = psi.decay_status(current, baseline_wmape)
    plain_rows.append((model, version, computed_at, "wmape", round(current, 6),
                       status, "forecast_decay",
                       json.dumps({"n": int(n), "baseline_wmape": round(baseline_wmape, 6)})))
    return plain_rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Phase 4 drift + decay monitor (writes gold.model_drift)")
    ap.add_argument("--limit", type=int, default=5000, help="max sample rows per model")
    ap.add_argument("--min-samples", type=int, default=50, help="min values for a feature to be measured")
    ap.add_argument("--dry-run", action="store_true", help="print what would be written, write nothing")
    args = ap.parse_args()

    computed_at = dt.datetime.now(dt.timezone.utc)
    rows: list[tuple] = []

    with common.conn() as c:
        cur = c.cursor(row_factory=psycopg.rows.dict_row)
        models = load_active_models(cur)
        if not models:
            print("no active models with a drift_baseline; nothing to measure")
            return 0

        for m in models:
            name = m["name"]
            if name in STREAM_MODELS:
                samples = stream_feature_samples(cur, name, STREAM_MODELS[name], args.limit)
                rows += feature_rows(name, m["version"], computed_at, m["baseline"],
                                     samples, args.min_samples)
            elif name in BATCH_MARTS:
                table, order_col = BATCH_MARTS[name]
                samples = batch_feature_samples(cur, table, order_col, m["baseline"], args.limit)
                rows += feature_rows(name, m["version"], computed_at, m["baseline"],
                                     samples, args.min_samples)
            if name in FORECAST_DECAY_MODELS:
                rows = forecast_decay(cur, rows, name, m["version"], computed_at, m["metrics"])

        if args.dry_run:
            print(f"[dry-run] would write {len(rows)} gold.model_drift rows @ {computed_at.isoformat()}")
            for r in rows[:20]:
                print("  ", r)
            if len(rows) > 20:
                print(f"   ... and {len(rows) - 20} more")
            return 0

        cur.executemany(
            """
            insert into gold.model_drift
                (model_name, model_version, computed_at, feature, psi, status, kind, detail)
            values (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            rows,
        )
        c.commit()

    by_status = {"ok": 0, "warning": 0, "critical": 0}
    for r in rows:
        by_status[r[5]] = by_status.get(r[5], 0) + 1
    print(f"wrote {len(rows)} gold.model_drift rows @ {computed_at.isoformat()}: {by_status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())