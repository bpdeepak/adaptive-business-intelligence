"""Forecast feature contract (Phase 5 audit of the Phase 2 inputs).

* The trainer must not use the target week's own avg_order_value (target leakage:
  it is that week's revenue / orders, unknowable for a future week).
* The training mart and the next-week scoring mart are built by one dbt macro, and
  the scoring mart carries every trained feature except category_code (derived
  from the registry's training-time rank at serving time).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from ml import common
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import train_forecast  # noqa: E402

MARTS = Path(__file__).resolve().parents[2] / "dbt" / "models" / "marts" / "predict"


def test_trainer_has_no_same_week_aov_feature():
    assert "avg_order_value" not in train_forecast.FEATURES
    assert "aov_lag1" in train_forecast.FEATURES


def test_both_forecast_marts_use_the_shared_macro():
    for name in ("feature_forecast_weekly.sql", "feature_forecast_next_week.sql"):
        assert "forecast_feature_rows(" in (MARTS / name).read_text(encoding="utf-8"), name


def _columns(table: str) -> list[str]:
    with common.conn() as c:
        return [r[0] for r in c.execute(
            "select column_name from information_schema.columns "
            "where table_schema='gold' and table_name=%s order by ordinal_position", (table,)).fetchall()]


def test_next_week_mart_carries_every_trained_feature():
    try:
        cols = set(_columns("feature_forecast_next_week"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"database unavailable: {exc}")
    if not cols:
        pytest.skip("gold.feature_forecast_next_week not built (run dbt)")
    missing = [f for f in train_forecast.FEATURES if f != "category_code" and f not in cols]
    assert not missing, f"scoring mart lacks trained features: {missing}"
    assert {"recent_orders_avg", "as_of_week"} <= cols
