"""SHAP explainability helpers (global + per-row) shared by the trainers.

SHAP is computed against tree models (LightGBM/XGBoost) using TreeExplainer:
- global: mean |SHAP| across a held-out sample (feature ranking for docs);
- per-row: exact feature contributions for the rows we ACT on, persisted in
  gold.predictions.explanation so every served prediction is explainable.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def explainer_for(model: Any):
    """Build the TreeExplainer for `model` once; constructing it per-row is
    prohibitively expensive for thousands of rows (XGBoost/LightGBM re-parse
    the whole tree ensemble)."""
    import shap

    return shap.TreeExplainer(model)


def global_importance(model: Any, X: pd.DataFrame, sample: int = 5000) -> dict[str, float]:
    """Mean |SHAP value| per feature over (at most) `sample` rows."""
    X = X.sample(n=min(sample, len(X)), random_state=42) if len(X) > sample else X
    explainer = explainer_for(model)
    sv = explainer.shap_values(X)
    arr = _positive_class_array(sv, X)
    mean_abs = np.abs(arr).mean(axis=0)
    out = {col: float(v) for col, v in zip(X.columns, mean_abs)}
    return dict(sorted(out.items(), key=lambda kv: -kv[1]))


def feature_contributions(model: Any, X_row: pd.DataFrame, explainer=None) -> dict[str, float]:
    """Per-feature contributions for a single prediction row, most influential
    first. Exactly the payload shape stored in gold.predictions.explanation."""
    if explainer is None:
        explainer = explainer_for(model)
    sv = explainer.shap_values(X_row)
    arr = _positive_class_array(sv, X_row)
    contribs = {col: float(v) for col, v in zip(X_row.columns, arr[0])}
    return dict(sorted(contribs.items(), key=lambda kv: -abs(kv[1])))


def _positive_class_array(sv, X: pd.DataFrame) -> np.ndarray:
    """TreeExplainer returns (n_classes, n, p) for classifiers; pick the
    positive class. Regression returns (n, p) directly."""
    arr = np.asarray(sv)
    if arr.ndim == 3 and arr.shape[0] == 2:
        return arr[1]
    return arr.reshape(len(X), len(X.columns))


def save_global_shap_plot(model: Any, X: pd.DataFrame, out_path: str, *, sample: int = 4000) -> str | None:
    """Render a SHAP summary (beeswarm) PNG. Returns path or None if plotting
    deps are unavailable (kept optional for lean environments)."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import shap
        from matplotlib import pyplot as plt
    except ImportError:
        return None

    X = X.sample(n=min(sample, len(X)), random_state=42) if len(X) > sample else X
    sv = explainer_for(model).shap_values(X)
    arr = _positive_class_array(sv, X)

    fig = plt.figure(figsize=(10, 6))
    shap.summary_plot(arr, X, show=False, max_display=20)
    plt.tight_layout()
    fig.savefig(out_path, dpi=110, bbox_inches="tight")
    plt.close(fig)
    return out_path