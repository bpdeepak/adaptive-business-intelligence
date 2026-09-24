"""PSI drift math — pure functions with no DB dependency (unit-testable).

PSI (Population Stability Index) measures how far a live feature distribution
has drifted from a reference (train-time) distribution:

    PSI = sum((p_i - q_i) * ln(p_i / q_i)) over bins

where p is the reference proportion and q the sample proportion. Reference
distributions come from common.feature_distribution_baseline (stored in
gold.model_registry.drift_baseline by the trainers).

Conventional banding: ok < 0.10, warning 0.10–0.20, critical > 0.20.
Decay (forecast/churn): compare the current error / positive rate against the
train-time baseline recorded in the registry metrics, as a ratio.
"""
from __future__ import annotations

import math
from typing import Any

import numpy as np

# Smoothing term so empty bins never produce log(0)/log(0/0). 1e-4 keeps the
# PSI contribution of an empty sample bin negligible for ordinary n.
_EPS = 1e-4

# Conventional PSI bands (Retail / Scorecard literature, e.g. on 19 bins).
PSI_OK = 0.10
PSI_WARN = 0.20


def psi_between(reference: dict[str, Any], sample_values: list[float]) -> float:
    """PSI of `sample_values` against a stored reference distribution.

    `reference` is one entry of gold.model_registry.drift_baseline:
      {"min": float, "max": float, "bins": [p0..pN], "n_bins": N, "n": int}
    Sample values outside [min, max] clamp into the edge bins by construction
    (np.histogram range), which is the standard PSI convention.
    """
    vals = np.asarray(sample_values, dtype=float)
    if vals.size == 0:
        return 0.0
    ref = np.asarray(reference["bins"], dtype=float)
    ref_sum = ref.sum()
    if ref_sum <= 0:
        return 0.0
    ref = ref / ref_sum
    lo = float(reference["min"])
    hi = float(reference["max"])
    if hi <= lo:
        return 0.0
    counts, _ = np.histogram(vals, bins=len(ref), range=(lo, hi))
    sample = counts / counts.sum() if counts.sum() > 0 else np.zeros(len(ref))
    rs = ref + _EPS
    ss = sample + _EPS
    return float(np.sum((ss - rs) * np.log(ss / rs)))


def status_for(psi: float, *, warn: float = PSI_OK, crit: float = PSI_WARN) -> str:
    """Map a PSI value onto the conventional ok/warning/critical band."""
    if math.isnan(psi):
        return "ok"  # no valid measurement -> skip
    if math.isinf(psi) or psi > crit:
        return "critical"
    if psi < warn:
        return "ok"
    return "warning"


def decay_status(
    current: float,
    baseline: float,
    *,
    warn_ratio: float = 1.25,
    crit_ratio: float = 2.0,
) -> str:
    """Band a real-decay finding (forecast WAPE / churn rate) against its
    train-time baseline. Degradation beyond warn_ratio x is a warning; beyond
    crit_ratio x is critical. Any drop (improvement) stays ok."""
    if baseline <= 0 or current <= baseline:
        return "ok"
    ratio = current / baseline
    if ratio <= warn_ratio:
        return "ok"
    if ratio <= crit_ratio:
        return "warning"
    return "critical"