"""The churn scoring mart and the training mart share one dbt macro
(dbt/macros/churn_features.sql); this pins that the scoring mart exposes EXACTLY
the trainer's features, in order, plus its two keys (DB-backed; skipped when the
gold layer is unavailable). The Go scorer sends every non-key mart column to the
strict sidecar, so a drift here would be refused at scoring time; this catches it
before that.
"""
from __future__ import annotations

import pytest

from ml import common, train_churn


def test_scoring_mart_columns_are_the_trainer_features():
    try:
        with common.conn() as c:
            cols = [r[0] for r in c.execute(
                "select column_name from information_schema.columns "
                "where table_schema='gold' and table_name='feature_customer_churn_current' "
                "order by ordinal_position").fetchall()]
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"database unavailable: {exc}")
    if not cols:
        pytest.skip("gold.feature_customer_churn_current not built (run dbt)")
    assert cols[:2] == ["customer_unique_id", "as_of_date"]
    assert cols[2:] == train_churn.FEATURES


def test_both_churn_marts_use_the_shared_macro():
    from pathlib import Path

    marts = Path(__file__).resolve().parents[2] / "dbt" / "models" / "marts" / "predict"
    for name in ("feature_customer_churn.sql", "feature_customer_churn_current.sql"):
        sql = (marts / name).read_text(encoding="utf-8")
        for macro in ("churn_order_ctes()", "churn_feature_ctes()", "churn_feature_columns()"):
            assert macro in sql, f"{name} must build its features from {macro}"
