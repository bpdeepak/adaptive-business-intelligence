"""Honest, time-based model evaluation shared by all four trainers.

Two primitives:
- expanding-origin backtest for the per-category time series (forecast model);
- `classification_metrics` for the three classifiers with a lift-at-5% cut for
  the "act on the riskiest 5% of cases" story.

No random splits across time: folds always predict strictly future rows.
"""
from __future__ import annotations

from typing import Any, Callable

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Forecast: expanding-origin backtest per series
# ---------------------------------------------------------------------------

def expanding_origin_forecast(
    df: pd.DataFrame,
    *,
    key_col: str,
    time_col: str,
    target_col: str,
    feature_cols: list[str],
    fit: Callable[[pd.DataFrame], Any],
    predict: Callable[[Any, pd.DataFrame], np.ndarray],
    horizon: int = 4,
    min_train: int = 16,
    step: int = 1,
) -> pd.DataFrame:
    """Rolling-origin backtest over one series per `key_col` (e.g. category).

    For each fold: train on rows [0, train_end) of that series, predict the
    next `horizon` rows. Returns records for every predicted (test) row with
    `actual`, `prediction`, series key, time, and fold number.
    """
    records: list[dict[str, Any]] = []
    for key, group in df.groupby(key_col, sort=False):
        g = group.sort_values(time_col, kind="mergesort").reset_index(drop=True)
        n = len(g)
        if n < min_train + horizon:
            continue
        train_end = min_train
        fold = 0
        while train_end + horizon <= n:
            train = g.iloc[:train_end]
            test = g.iloc[train_end : train_end + horizon]
            model = fit(train)
            preds = np.asarray(predict(model, test[feature_cols])).reshape(-1)
            for i, row in test.iterrows():
                records.append(
                    {
                        key_col: key,
                        time_col: row[time_col],
                        "actual": float(row[target_col]),
                        "prediction": float(preds[test.index.get_loc(i)]),
                        "fold": fold,
                    }
                )
            train_end += step
            fold += 1
    return pd.DataFrame(records)


def forecast_metrics(records: pd.DataFrame) -> dict[str, float]:
    """MAE / RMSE / WMAPE / SMAPE, plus MAPE over non-zero actuals only.

    MAPE is undefined/misleading when a category-week has zero revenue (the
    dense spine is zero-filled), so it is computed on non-zero weeks alone;
    WMAPE and SMAPE are the headline scale-aware metrics.
    """
    actuals = records["actual"].to_numpy(dtype=float)
    preds = records["prediction"].to_numpy(dtype=float)
    nonzero = np.abs(actuals) > 1e-9
    mape = (
        float(np.mean(np.abs((preds[nonzero] - actuals[nonzero]) / actuals[nonzero])))
        if nonzero.any()
        else float("nan")
    )
    return {
        "mae": float(np.mean(np.abs(preds - actuals))),
        "rmse": float(np.sqrt(np.mean((preds - actuals) ** 2))),
        "wmape": float(np.sum(np.abs(preds - actuals)) / np.sum(np.abs(actuals))),
        "mape_nonzero": mape,
        "smape": float(2 * np.mean(np.abs(preds - actuals) / (np.abs(actuals) + np.abs(preds) + 1e-9))),
    }


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def time_split(
    df: pd.DataFrame,
    *,
    time_col: str,
    test_frac: float = 0.2,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split on time order only: the latest `test_frac` fraction goes to test."""
    df = df.sort_values(time_col, kind="mergesort").reset_index(drop=True)
    cut = int(len(df) * (1 - test_frac))
    return df.iloc[:cut].copy(), df.iloc[cut:].copy()


def classification_metrics(y_true: np.ndarray, y_prob: np.ndarray, *, positive_share: float | None = None) -> dict[str, float]:
    """Threshold-free + threshold metrics for scoring decisions.

    lift_at_5pct: positive rate inside the top-5%-riskiest rows divided by the
    base rate — how much better than random the model is if you act on the 5%
    it flags. `positive_share` seeds the base-rate denominator consistently
    across folds (use the TRAIN split's rate, not the test's).
    """
    from sklearn.metrics import (
        average_precision_score,
        f1_score,
        precision_recall_curve,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    y_true = np.asarray(y_true, dtype=int)
    y_prob = np.asarray(y_prob, dtype=float)
    base = float(y_true.mean()) if positive_share is None else float(positive_share)

    labels = np.array([0, 1])
    if set(np.unique(y_true)) == {1}:
        labels = np.array([1])
    auc = float(roc_auc_score(y_true, y_prob, labels=labels)) if len(np.unique(y_true)) > 1 else float("nan")
    f1 = float(f1_score(y_true, (y_prob >= 0.5).astype(int), zero_division=0))
    prec = float(precision_score(y_true, (y_prob >= 0.5).astype(int), zero_division=0))
    rec = float(recall_score(y_true, (y_prob >= 0.5).astype(int), zero_division=0))
    pr_auc = float(average_precision_score(y_true, y_prob, average="weighted")) if len(np.unique(y_true)) > 1 else float("nan")

    k = max(1, int(round(0.05 * len(y_prob))))
    idx = np.argsort(-y_prob)[:k]
    lift = float(y_true[idx].mean() / base) if base > 0 else float("nan")
    lifted_rate = float(y_true[idx].mean())

    return {
        "auc": auc,
        "pr_auc": pr_auc,
        "f1": f1,
        "precision": prec,
        "recall": rec,
        "lift_at_5pct": lift,
        "positive_rate_in_top5pct": lifted_rate,
        "positive_rate": base,
    }


def score_at_thresholds(y_true: np.ndarray, y_prob: np.ndarray, thresholds=(0.5, 0.7, 0.9)) -> dict[str, float]:
    """Precision/recall/F1 sweep used to pick serving thresholds per model."""
    from sklearn.metrics import f1_score, precision_score, recall_score

    out: dict[str, float] = {}
    y_true = np.asarray(y_true, dtype=int)
    for t in thresholds:
        y_pred = (y_prob >= t).astype(int)
        out[f"precision@{t}"] = float(precision_score(y_true, y_pred, zero_division=0))
        out[f"recall@{t}"] = float(recall_score(y_true, y_pred, zero_division=0))
        out[f"f1@{t}"] = float(f1_score(y_true, y_pred, zero_division=0))
    return out