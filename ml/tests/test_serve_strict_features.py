"""The sidecar must refuse an incomplete feature vector instead of silently
defaulting missing features to 0.0 (that turns any Go<->Python feature-name
drift into confidently wrong scores). No database, no HTTP, no real artifact.
"""
from __future__ import annotations

import numpy as np
import pytest

from ml import serve


class StubClassifier:
    def predict_proba(self, df):
        return np.array([[0.25, 0.75]])


def make_store():
    entry = {"name": "m", "version": "v1", "task": "binary_classification", "grain": "order",
             "artifact_path": "unused", "features": ["a", "b", "c"]}
    store = serve.ModelStore({"models": [entry]})
    store.models["m"]["model"] = StubClassifier()  # skip joblib + SHAP loading
    return store


def test_missing_feature_is_refused_with_400_and_named():
    result = make_store().score("m", {"a": 1.0, "b": 2.0})
    assert result["http"] == 400
    assert result["missing_features"] == ["c"]
    assert "c" in result["error"]
    assert "prediction" not in result


def test_empty_vector_names_every_feature():
    result = make_store().score("m", {})
    assert result["missing_features"] == ["a", "b", "c"]


@pytest.mark.parametrize("bad", [None, "abc", float("nan"), float("inf")])
def test_non_numeric_or_non_finite_value_is_refused(bad):
    result = make_store().score("m", {"a": 1.0, "b": bad, "c": 3.0})
    assert result["http"] == 400
    assert result["invalid_features"] == ["b"]


def test_complete_vector_scores_and_extra_features_are_ignored():
    result = make_store().score("m", {"a": 1.0, "b": 2.0, "c": 3.0, "unused_extra": 9.0})
    assert result["status"] == "ok"
    assert result["prediction"] == pytest.approx(0.75)


def test_unknown_model_keeps_404_semantics():
    result = make_store().score("nope", {"a": 1.0})
    assert "error" in result and "http" not in result
