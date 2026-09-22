"""Unit tests for the Phase 2 ML helpers — no database required, so they run
in CI with just `uv sync --group ml && uv run python -m pytest ml/tests`.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ml import backtest


def _series_df(n: int = 60, seed: int = 1) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    weeks = pd.date_range("2017-01-01", periods=n, freq="W-MON") + np.timedelta64(1, "D")
    return pd.DataFrame(
        {
            "category": "a",
            "week_start": weeks,
            "revenue": np.maximum(0, 50 + rng.normal(0, 8, n).cumsum()),
        }
    )


def test_forecast_metrics_known_values():
    recs = pd.DataFrame(
        {"actual": [10, 20, 30], "prediction": [12, 18, 30]}
    )
    m = backtest.forecast_metrics(recs)
    assert m["mae"] == pytest.approx(4 / 3)
    assert m["wmape"] == pytest.approx(4 / 60)
    assert m["rmse"] == pytest.approx(np.sqrt(8 / 3))


def test_expanding_origin_is_strictly_future():
    df = _series_df(n=40)
    # mini linear model fitted inside the backtest
    def fit(tr: pd.DataFrame):
        x = np.arange(len(tr)).reshape(-1, 1)
        slope, intercept = np.polyfit(x.ravel(), tr["revenue"], 1)
        return slope, intercept

    def predict(model, X):
        slope, intercept = model
        return np.asarray([slope * i + intercept for i in range(len(X))])[:, None]

    df = _series_df(n=40)
    df["t"] = np.arange(len(df))
    out = backtest.expanding_origin_forecast(
        df,
        key_col="category",
        time_col="week_start",
        target_col="revenue",
        feature_cols=["t"],
        fit=fit,
        predict=predict,
        horizon=4,
        min_train=10,
        step=4,
    )
    assert len(out) > 0
    # first row of the FIRST fold must be after 10th training week
    assert out["week_start"].min() > _series_df(n=40)["week_start"].iloc[9]


def test_time_split_preserves_order():
    df = pd.DataFrame({"t": range(100), "v": range(100)})
    train, test = backtest.time_split(df, time_col="t", test_frac=0.2)
    assert len(train) == 80
    assert len(test) == 20
    assert test["t"].min() > train["t"].max()


def test_classification_metrics_perfect_model():
    y = np.array([0, 0, 0, 1, 1, 1, 1, 1, 1, 1])
    p = np.array([0.1, 0.2, 0.3, 0.8, 0.85, 0.9, 0.95, 0.97, 0.98, 0.99])
    m = backtest.classification_metrics(y, p, positive_share=0.5)
    assert m["auc"] > 0.99
    assert m["lift_at_5pct"] >= 2.0  # top 5% (one row) is all positive


def test_classification_metrics_known_auc():
    y = np.array([0, 0, 1, 1])
    p = np.array([0.1, 0.2, 0.8, 0.9])
    m = backtest.classification_metrics(y, p, positive_share=0.5)
    assert m["auc"] == pytest.approx(1.0)

    # a deliberately wrong ordering gives chance-level AUC
    p2 = np.array([0.4, 0.6, 0.1, 0.9])
    m2 = backtest.classification_metrics(y, p2, positive_share=0.5)
    assert m2["auc"] == pytest.approx(0.5)


def test_recommended_threshold_perfect_separator():
    y = np.array([0, 0, 0, 1, 1, 1])
    p = np.array([0.1, 0.2, 0.3, 0.95, 0.97, 0.99])
    # precision is already 1.0 at 0.5 -> the naive cutoff IS the operating point
    assert backtest.recommended_threshold(y, p) == 0.5


def test_recommended_threshold_raises_the_cutoff():
    # precision at 0.5 (0.45) is below the bar -> the helper must raise the
    # cutoff. The last negative sits at 0.56 and the positives at >= 0.97, so
    # the first >= 0.85-precision sweep step lands mid-gap (~0.565): threshold
    # rounding can never flip the outcome.
    y = np.array([0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1])
    p = np.array([0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.97, 0.975, 0.98, 0.985, 0.99])
    t = backtest.recommended_threshold(y, p)
    assert t > 0.5

    from sklearn.metrics import precision_score

    assert precision_score(y, (p >= t).astype(int), zero_division=0) >= 0.85


def test_recommended_threshold_falls_back_to_best_achievable():
    # bar unreachable (min_precision 0.9): fall back to the argmax-precision
    # threshold, never to a silent 0.5 that is worse than achievable
    y = np.array([1, 0, 0])
    p = np.array([0.6, 0.7, 0.8])
    t = backtest.recommended_threshold(y, p, min_precision=0.9)

    from sklearn.metrics import precision_score

    sweep = np.arange(0.5, 0.995, 0.005)
    best = max(precision_score(y, (p >= th).astype(int), zero_division=0) for th in sweep)
    assert precision_score(y, (p >= t).astype(int), zero_division=0) == pytest.approx(best)