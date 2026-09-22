"""Unit tests for the serving sidecar (ml/serve.py) — no database, no HTTP.

Pins the contract that runtime-only model state must never leak into the
/models descriptors: after warmup and a score, a manifest entry carries the
loaded estimator and the cached TreeExplainer, neither of which is
JSON-serializable (caught live in a 2026 review round — /models crashed with
`LGBMClassifier is not JSON serializable`).
"""
from __future__ import annotations

from ml import serve


def _store_with_model(extra: dict):
    entry = {
        "name": "m1",
        "version": "v1",
        "task": "regression",
        "grain": "category_week",
        "artifact_path": "artifacts/m1/v1.joblib",
        "baseline_wmape": 0.3103,
        **extra,
    }
    return serve.ModelStore({"models": [entry]})


def test_descriptors_strip_runtime_state():
    store = _store_with_model({})
    # simulate what warmup + a scored request leave on the entry
    store.models["m1"]["model"] = object()
    store.models["m1"]["__explainer"] = object()

    desc = store.descriptors()[0]
    assert "artifact_path" not in desc
    assert "model" not in desc
    assert "__explainer" not in desc
    # manifest metadata survives
    assert desc["name"] == "m1" and desc["baseline_wmape"] == 0.3103


def test_descriptors_are_json_serializable_after_scoring():
    import json

    store = _store_with_model({"baseline_wmape_by_code": {"0": 0.2184, "62": 2.8347}})
    store.models["m1"]["model"] = object()  # what warmup does
    store.models["m1"]["__explainer"] = object()  # what a score does
    payload = {"models": store.descriptors()}
    body = json.dumps(payload)  # must not raise
    assert "baseline_wmape_by_code" in body