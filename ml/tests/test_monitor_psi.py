"""Unit tests for the Phase 4 drift monitors (pure math, no DB needed)."""
from __future__ import annotations

import numpy as np
import pytest

from ml import common
from ml.monitor import psi


def _ref(values: list[float], n_bins: int = 10) -> dict:
    vals = np.asarray(values, dtype=float)
    return {
        "min": float(vals.min()),
        "max": float(vals.max()),
        "bins": ([1.0 / n_bins] * n_bins),
        "n_bins": n_bins,
        "n": int(vals.size),
    }


# ---------------------------------------------------------------------------
# psi_between
# ---------------------------------------------------------------------------

def test_psi_identical_distribution_is_zero() -> None:
    values = [float(i) for i in range(100)]
    assert psi.psi_between(_ref(values), values) == 0.0


def test_psi_shifted_distribution_is_large() -> None:
    values = [float(i) for i in range(100)]
    shifted = [float(100 + i) for i in range(100)]
    assert psi.psi_between(_ref(values), shifted) > 1.0


def test_psi_split_distribution_is_critical() -> None:
    values = [float(i) for i in range(100)]
    bimodal = [float(i) for i in range(50)] + [float(300 + i) for i in range(50)]
    assert psi.psi_between(_ref(values), bimodal) > psi.PSI_WARN


def test_psi_empty_sample_is_zero() -> None:
    assert psi.psi_between(_ref([1.0, 2.0, 3.0]), []) == 0.0


def test_psi_bad_reference_is_zero() -> None:
    assert psi.psi_between({"min": 0.0, "max": 1.0, "bins": [0.0] * 10}, [1.0, 2.0]) == 0.0


def test_psi_out_of_range_clamps_into_edge_bins() -> None:
    # Sample far outside the reference range must not produce NaN/inf.
    ref = _ref([0.0, 1.0])
    val = psi.psi_between(ref, [1e9, 1e9, -1e9, -1e9])
    assert np.isfinite(val)


# ---------------------------------------------------------------------------
# status banding
# ---------------------------------------------------------------------------

def test_status_bands() -> None:
    assert psi.status_for(0.0) == "ok"
    assert psi.status_for(0.09) == "ok"
    assert psi.status_for(0.15) == "warning"
    assert psi.status_for(0.20) == "warning"
    assert psi.status_for(0.21) == "critical"
    assert psi.status_for(float("nan")) == "ok"
    assert psi.status_for(float("inf")) == "critical"


def test_decay_status_ratio_bands() -> None:
    assert psi.decay_status(0.10, 0.10) == "ok"
    assert psi.decay_status(0.10, 0.20) == "ok"          # improvement stays ok
    assert psi.decay_status(0.12, 0.10) == "ok"          # within 1.25x
    assert psi.decay_status(0.15, 0.10) == "warning"     # >1.25x
    assert psi.decay_status(0.21, 0.10) == "critical"    # >2x


# ---------------------------------------------------------------------------
# feature_distribution_baseline
# ---------------------------------------------------------------------------

def test_baseline_normal_feature_bins_sum_to_one() -> None:
    rng = np.random.default_rng(42)
    import pandas as pd

    frame = pd.DataFrame({"feature": rng.normal(10.0, 3.0, 1000)})
    base = common.feature_distribution_baseline(frame, ["feature"])
    assert "feature" in base
    entry = base["feature"]
    assert entry["min"] < entry["max"]
    # The robust [q0.01, q0.99] window excludes the rarest extreme values by
    # design, so the counted n is <= the sample size and bins sum to 1 over it.
    assert 0 < entry["n"] <= 1000
    assert sum(entry["bins"]) == pytest.approx(1.0)
    assert entry["n_bins"] == 10


def test_baseline_constant_feature_single_wide_bin() -> None:
    import pandas as pd

    base = common.feature_distribution_baseline(pd.DataFrame({"f": [5.0] * 100}), ["f"])
    assert base["f"]["min"] < base["f"]["max"] or base["f"]["n_bins"] >= 1
    assert sum(base["f"]["bins"]) == pytest.approx(1.0)


def test_baseline_respects_min_samples_threshold() -> None:
    import pandas as pd

    frame = pd.DataFrame({"a": [None] * 100, "b": list(range(100))})
    base = common.feature_distribution_baseline(frame, ["a", "b"], min_samples=50)
    assert "b" in base
    assert "a" not in base  # zero usable samples < min_samples


def test_baseline_null_values_dropped() -> None:
    import pandas as pd

    frame = pd.DataFrame({"f": list(range(100)) + [None] * 40})
    base = common.feature_distribution_baseline(frame, ["f"])
    assert 0 < base["f"]["n"] <= 100
    assert sum(base["f"]["bins"]) == pytest.approx(1.0)