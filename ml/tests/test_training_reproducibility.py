"""A trained model must be a function of the data, not of the database's physical
row order.

Found while bootstrapping the deploy stack: the same Olist data loaded into a fresh
database trained a churn model with recommended threshold 0.905, where the
long-lived dev database gave 0.595 (and fraud AUC 0.828 vs 0.817). The trainers'
queries had no ORDER BY, the time split is stable (ties keep input order), and
XGBoost's row subsampling depends on row order. These tests feed the SAME rows in
two different orders and require identical splits and identical predictions.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import backtest  # noqa: E402
import common  # noqa: E402
import train_churn  # noqa: E402


def _churn_frame(n: int = 600) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    df = pd.DataFrame({f: rng.random(n) * 10 for f in train_churn.FEATURES})
    df["customer_unique_id"] = [f"c{i:04d}" for i in range(n)]
    # Heavy timestamp ties on purpose: that is where input order leaked in.
    df["as_of_date"] = pd.to_datetime("2017-01-01") + pd.to_timedelta(rng.integers(0, 20, n), unit="D")
    df["churned"] = (df[train_churn.FEATURES[0]] + rng.normal(0, 3, n) > 5).astype(int)
    return df


def _train_on(frame: pd.DataFrame, monkeypatch) -> tuple[list[str], np.ndarray]:
    monkeypatch.setattr(train_churn.common, "read_sql", lambda sql: frame.copy())
    df = train_churn.load_data()
    train, test = backtest.time_split(df, time_col="as_of_date", test_frac=0.2)
    model = train_churn.make_model(train)
    return list(test["customer_unique_id"]), model.predict_proba(test[train_churn.FEATURES])[:, 1]


def test_churn_training_is_invariant_to_row_order(monkeypatch):
    base = _churn_frame()
    ids_a, pred_a = _train_on(base, monkeypatch)
    ids_b, pred_b = _train_on(base.sample(frac=1.0, random_state=7), monkeypatch)
    assert ids_a == ids_b, "the train/test boundary must not depend on input order"
    np.testing.assert_allclose(pred_a, pred_b, rtol=0, atol=1e-12)


def test_category_rank_breaks_ties_by_name():
    df = pd.DataFrame({"cat": ["b", "a", "c", "c"], "v": [5.0, 5.0, 1.0, 1.0]})
    shuffled = df.sample(frac=1.0, random_state=3)
    assert common.category_rank_from(df, "cat", "v") == {"a": 0, "b": 1, "c": 2}
    assert common.category_rank_from(shuffled, "cat", "v") == {"a": 0, "b": 1, "c": 2}


def test_every_trainer_orders_its_frame():
    root = Path(__file__).resolve().parents[1]
    for name in ("train_churn.py", "train_fraud.py", "train_bot.py", "train_forecast.py"):
        assert "common.canonical_order(df," in (root / name).read_text(encoding="utf-8"), name
