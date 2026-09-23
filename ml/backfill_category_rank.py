"""One-off backfill: persist metrics.category_rank onto the ACTIVE regression
rows (fraud_risk, forecast_category_weekly_orders, forecast_category_weekly_revenue)
so the Phase 3 serving layer (Go score-writer, /forecast/{category}) resolves
category names to the exact ordinal codes the trained models saw — without
retraining.

Both ranks are computed by importing the trainers' own ``load_data()``
functions, which now build the code map through ``common.category_rank_from``.
The formula is therefore literally the training-time formula, not a re-implementation
(see the shared-feature-contract note in docs/phase3.md). Future ``make train``
for these models writes the same map into their new registry rows.

Usage: uv run --group ml python ml/backfill_category_rank.py
"""
from __future__ import annotations

import json
import sys

import common
import train_fraud
import train_forecast

# The forecast trainer registers revenue and orders as two rows from the same
# loaded feature frame, so both carry the identical category_rank.
MODELS = [
    ("fraud_risk", "fraud"),
    ("forecast_category_weekly_orders", "forecast"),
    ("forecast_category_weekly_revenue", "forecast"),
]


def apply_rank(model_name: str, rank: dict[str, int]) -> int:
    with common.conn() as c:
        cur = c.execute(
            """
            UPDATE gold.model_registry
               SET metrics = COALESCE(metrics, '{}'::jsonb) || %s::jsonb
             WHERE model_name = %s AND status = 'active'
            """,
            (json.dumps({"category_rank": rank}), model_name),
        )
        return cur.rowcount


def main() -> int:
    _, _, fraud_rank = train_fraud.load_data()
    _, forecast_rank = train_forecast.load_data()
    print(f"fraud rank codes: {len(fraud_rank)} categories")
    print(f"forecast rank codes: {len(forecast_rank)} categories")

    updated = 0
    for model_name, kind in MODELS:
        rank = fraud_rank if kind == "fraud" else forecast_rank
        n = apply_rank(model_name, rank)
        print(f"  {model_name}: updated {n} active row(s) via metrics.category_rank")
        updated += n
    if updated < 3:
        print("warning: expected 3 active registry rows to carry category_rank", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())