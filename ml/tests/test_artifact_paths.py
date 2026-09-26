"""Artifact paths must work on another OS. Trainers run on Windows in dev, but the
sidecar runs in a Linux container / on the deploy VM; a registry path recorded
as `artifacts\fraud_risk\v.joblib` does not resolve there.
"""
from __future__ import annotations

import joblib

from ml import common, serve


def test_portable_path_uses_forward_slashes():
    assert common.portable_path(r"artifacts\fraud_risk\20260924.174626.joblib") == \
        "artifacts/fraud_risk/20260924.174626.joblib"
    assert common.portable_path("artifacts/bot_score/v.joblib") == "artifacts/bot_score/v.joblib"


def test_sidecar_loads_a_windows_recorded_path(tmp_path, monkeypatch):
    (tmp_path / "artifacts" / "m").mkdir(parents=True)
    joblib.dump({"model": "stub"}, tmp_path / "artifacts" / "m" / "v1.joblib")
    monkeypatch.chdir(tmp_path)
    store = serve.ModelStore({"models": [{"name": "m", "version": "v1", "features": [],
                                          "artifact_path": r"artifacts\m\v1.joblib"}]})
    assert store._load("m")["model"] == {"model": "stub"}
