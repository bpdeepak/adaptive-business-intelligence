"""Shared pieces for the three classifier trainers (churn, fraud, bot):
time-split evaluation, metric reporting, and persistence of per-row
predictions + SHAP explanations into gold.predictions.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

import backtest
import common
import explain


def evaluate_classifier(
    model: Any,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    train_positive_rate: float,
) -> dict[str, float]:
    probs = model.predict_proba(X_test)[:, 1]
    metrics = backtest.classification_metrics(
        y_test.to_numpy(), probs, positive_share=train_positive_rate
    )
    metrics.update(backtest.score_at_thresholds(y_test.to_numpy(), probs))
    return metrics


def persist_classifier_predictions(
    model: Any,
    model_name: str,
    model_version: str,
    grain: str,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    entity_ids: np.ndarray | pd.Series,
    *,
    proba_col: int = 1,
    max_rows: int = 20000,
    extra_meta: dict[str, Any] | None = None,
    seed: int = 42,
) -> int:
    """Write (up to `max_rows` sampled) test-set predictions into
    gold.predictions with per-row SHAP explanations."""
    probs = model.predict_proba(X_test)[:, proba_col]
    if len(probs) > max_rows:
        idx = np.random.default_rng(seed).choice(len(probs), size=max_rows, replace=False)
        idx.sort()
    else:
        idx = np.arange(len(probs))

    rows: list[dict[str, Any]] = []
    # Build the SHAP explainer ONCE: constructing it per row re-parses the
    # entire tree ensemble each time, which makes persistence O(n_rows × model)
    # and was the first observed bottleneck (17k+ rows × churn model).
    explainer = explain.explainer_for(model)
    for i in idx:
        x_row = X_test.iloc[[int(i)]]
        try:
            contr = explain.feature_contributions(model, x_row, explainer=explainer)
        except Exception:  # shap can be picky with exotic inputs; degrade gracefully
            contr = {}
        rows.append(
            {
                "entity_id": str(entity_ids.iloc[i] if hasattr(entity_ids, "iloc") else entity_ids[i]),
                "prediction": float(probs[i]),
                "confidence": float(max(probs[i], 1 - probs[i])),  # classifier confidence = distance-from-tossup
                "explanation": {"shap": contr, "predicted_probability": float(probs[i])},
                "metadata": {
                    **(extra_meta or {}),
                    "label": int(y_test.iloc[i] if hasattr(y_test, "iloc") else y_test[i]),
                    "model_version": model_version,
                },
            }
        )
    return common.write_predictions(model_name, model_version, grain, rows, batch_tag="test_split")


def class_counter(series: pd.Series) -> str:
    vc = series.value_counts(dropna=False)
    return ", ".join(f"{k}: {v:,}" for k, v in vc.items())