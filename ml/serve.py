"""Phase 2 model-serving sidecar (stdlib HTTP + JSON).

Loads the trained artifacts listed in artifacts/sidecar_models.json (written by
ml/train_all.py) and exposes:

    GET  /health   -> {"status":"ok","models":[...]}
    GET  /models   -> descriptors (name, version, task, grain, features)
    POST /score    -> {"model":"churn_risk","features":{...}}
                      => {"status","model","version","prediction","confidence",
                          "explanation":{"shap":{feature: contribution}}}

No web framework, no inference-time DB access: the artifact + manifest on local
disk are the whole serving contract, and the Go API treats this sidecar as an
optional dependency (503 with a clear error when it is down).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

ARTIFACTS_DIR = Path(os.getenv("ABI_ARTIFACTS_DIR", "artifacts"))
ADDR = os.getenv("ABI_SCORE_ADDR", "127.0.0.1:8093")


def load_manifest() -> dict[str, Any]:
    path = ARTIFACTS_DIR / "sidecar_models.json"
    if not path.exists():
        sys.stderr.write(f"no serving manifest at {path} — run ml/train_all.py first\n")
        return {"models": []}
    return json.loads(path.read_text())


class ModelStore:
    def __init__(self, manifest: dict[str, Any]):
        self.models: dict[str, dict[str, Any]] = {}
        for m in manifest.get("models", []):
            self.models[m["name"]] = m

    def descriptors(self) -> list[dict[str, Any]]:
        return [
            {k: v for k, v in m.items() if k != "artifact_path"}
            for m in self.models.values()
        ]

    def _load(self, name: str) -> dict[str, Any]:
        m = self.models[name]
        if "model" not in m:
            # artifact_path is stored repo-root-relative (e.g.
            # artifacts/churn_risk/<version>.joblib), matching the directory
            # layout produced by ml/common.py save_artifact().
            model = joblib.load(m["artifact_path"])
            m["model"] = model
        return m

    def warmup(self) -> None:
        """Eagerly load every registered artifact at startup so the first
        /score request never pays the joblib + SHAP cold-start cost (which can
        exceed the Go client's 5 s timeout). Artifacts here are a few MB each,
        so this blocks boot for well under a second per model."""
        for name in self.models:
            try:
                self._load(name)
            except Exception as exc:  # missing/corrupt artifact: degrade, don't crash boot
                sys.stderr.write(f"warmup failed for {name}: {exc}\n")

    def score(self, name: str, features: dict[str, float]) -> dict[str, Any]:
        if name not in self.models:
            return {"error": f"unknown model {name!r}"}
        m = self._load(name)
        cols = m["features"]
        row = {c: float(features.get(c, 0.0)) for c in cols}
        df = pd.DataFrame([row], columns=cols)
        model = m["model"]
        task = m.get("task", "binary_classification")

        if task == "regression":
            pred = float(model.predict(df)[0])
            wmape = float(m.get("baseline_wmape", 0.10))
            confidence = float(max(0.05, min(0.999, 1 - wmape)))
        else:
            prob = float(model.predict_proba(df)[0][1])
            pred = prob
            confidence = float(max(prob, 1 - prob))

        explanation: dict[str, Any] = {}
        try:
            explanation = _shap_contributions(model, m, df, task)
        except Exception as exc:  # shap unavailable/malformed input: degrade
            explanation = {"error": str(exc)}

        return {
            "status": "ok",
            "model": name,
            "version": m["version"],
            "task": task,
            "grain": m.get("grain", ""),
            "prediction": pred,
            "confidence": confidence,
            "explanation": explanation,
        }


def _shap_contributions(model: Any, m: dict[str, Any], df: pd.DataFrame, task: str) -> dict[str, Any]:
    import shap

    # Cache the TreeExplainer per model (constructed once per process).
    if "__explainer" not in m:
        m["__explainer"] = shap.TreeExplainer(model)
    explainer = m["__explainer"]
    sv = explainer.shap_values(df)
    arr = np.asarray(sv)
    if arr.ndim == 3 and arr.shape[0] == 2:
        arr = arr[1]
    arr = arr.reshape(1, len(df.columns))
    contribs = {col: round(float(v), 6) for col, v in zip(df.columns, arr[0])}
    return {
        "shap": dict(sorted(contribs.items(), key=lambda kv: -abs(kv[1]))[:25]),
    }


def make_handler(store: ModelStore):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # keep console quiet
            pass

        def _json(self, status: int, payload: dict) -> None:
            body = json.dumps(payload).encode()
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except OSError:
                # Client went away mid-response (timeout/abort): nothing to do;
                # a dropped client must never crash a handler or the process.
                pass

        def do_GET(self):  # noqa: N802
            if self.path == "/health":
                self._json(200, {"status": "ok", "models": list(store.models)})
            elif self.path == "/models":
                self._json(200, {"models": store.descriptors()})
            else:
                self._json(404, {"error": f"unknown path {self.path}"})

        def do_POST(self):  # noqa: N802
            if self.path != "/score":
                self._json(404, {"error": "unknown path"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                req = json.loads(self.rfile.read(length).decode())
            except Exception as exc:
                self._json(400, {"error": f"invalid body: {exc}"})
                return
            name = req.get("model", "").strip()
            features = req.get("features", {})
            if not name or not isinstance(features, dict):
                self._json(400, {"error": "payload needs model + features object"})
                return
            try:
                result = store.score(name, features)
            except Exception as exc:
                self._json(500, {"error": f"scoring failed: {exc}"})
                return
            if "error" in result:
                self._json(404, result)
            else:
                self._json(200, result)

    return Handler


def main() -> int:
    ap = argparse.ArgumentParser(description="Phase 2 model-serving sidecar")
    ap.add_argument("--addr", default=ADDR, help="listen address (default %(default)s)")
    args = ap.parse_args()

    manifest = load_manifest()
    store = ModelStore(manifest)
    store.warmup()
    print(f"warmed {len(store.models)} model artifacts")
    host, port = args.addr.rsplit(":", 1)
    srv = ThreadingHTTPServer((host, int(port)), make_handler(store))
    print(f"model sidecar listening on http://{args.addr} ({len(store.models)} models)")
    srv.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())