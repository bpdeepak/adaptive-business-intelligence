"""Shared plumbing for the Phase 2 ML pipeline: DB access, artifact storage,
model-registry mirror, and environment helpers.

Everything here uses psycopg + the stdlib, so the ``ml`` dependency group only
needs the science libraries (scikit-learn, lightgbm, xgboost, shap). The model
registry mirror (gold.model_registry) and prediction output (gold.predictions)
are the durable contracts the Go API serves; the tables are created lazily here
as well as by the Go server so ``make train`` works standalone.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
from typing import Any

import pandas as pd
import psycopg


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

def database_url_from_env() -> str:
    if v := os.getenv("DATABASE_URL"):
        return v
    host = os.getenv("ABI_PG_HOST", "localhost")
    port = os.getenv("ABI_PG_PORT", "5432")
    user = os.getenv("ABI_PG_USER", "abi")
    pw = os.getenv("ABI_PG_PASSWORD", "abi")
    db = os.getenv("ABI_PG_DB", "abi")
    return f"postgres://{user}:{pw}@{host}:{port}/{db}?sslmode=disable"


DATABASE_URL = database_url_from_env()

# Where trained artifacts + evaluation payloads live. The serving sidecar
# (ml/serve.py) reads from here; Go keeps the path in gold.model_registry.
ARTIFACTS_DIR = pathlib.Path(os.getenv("ABI_ARTIFACTS_DIR", "artifacts"))

# Model registry DDL shared with api/internal/predict (keep in sync).
MODEL_REGISTRY_DDL = """
CREATE TABLE IF NOT EXISTS gold.model_registry (
    model_name text NOT NULL,
    model_version text NOT NULL,
    status text NOT NULL DEFAULT 'active',
    framework text NOT NULL,
    task text NOT NULL,
    grain text NOT NULL,
    artifact_path text NOT NULL,
    params jsonb NOT NULL DEFAULT '{}'::jsonb,
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    features jsonb NOT NULL DEFAULT '[]'::jsonb,
    trained_on jsonb NOT NULL DEFAULT '{}'::jsonb,
    trained_window jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (model_name, model_version)
)
"""

PREDICTIONS_DDL = """
CREATE TABLE IF NOT EXISTS gold.predictions (
    id bigserial PRIMARY KEY,
    model_name text NOT NULL,
    model_version text NOT NULL,
    grain text NOT NULL,
    entity_id text NOT NULL,
    predicted_at timestamptz NOT NULL DEFAULT now(),
    prediction double precision NOT NULL,
    confidence double precision NOT NULL DEFAULT 0,
    lower_bound double precision,
    upper_bound double precision,
    explanation jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
)
"""


def conn() -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL)


def read_sql(sql: str) -> pd.DataFrame:
    """Read a query into a DataFrame (psycopg3 DBAPI connection)."""
    import warnings

    with conn() as c:
        # pandas supports DBAPI connections; its "consider SQLAlchemy" warning
        # is noise here because we deliberately stay ORM-free.
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")
            return pd.read_sql(sql, c)


def ensure_serving_tables() -> None:
    """Idempotent: create gold.model_registry + gold.predictions if missing."""
    with conn() as c:
        c.execute(MODEL_REGISTRY_DDL)
        c.execute(PREDICTIONS_DDL)
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_predictions_model_entity "
            "ON gold.predictions (model_name, entity_id, predicted_at DESC)"
        )
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_predictions_created "
            "ON gold.predictions (predicted_at DESC)"
        )


# ---------------------------------------------------------------------------
# Registry mirror
# ---------------------------------------------------------------------------

def now_tag() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d.%H%M%S")


def register_model(
    model_name: str,
    model_version: str,
    *,
    framework: str,
    task: str,
    grain: str,
    artifact_path: str | pathlib.Path,
    params: dict[str, Any],
    metrics: dict[str, Any],
    features: list[str],
    trained_on: dict[str, Any],
    trained_window: dict[str, str] | None = None,
) -> None:
    """Upsert one row into gold.model_registry (contract for the Go API).

    Registering a new version supersedes any other active version of the same
    model, so exactly one version per model is `active` at a time.
    """
    ensure_serving_tables()
    with conn() as c:
        c.execute(
            """
            UPDATE gold.model_registry
               SET status = 'superseded'
             WHERE model_name = %s
               AND model_version <> %s
               AND status = 'active'
            """,
            (model_name, model_version),
        )
        c.execute(
            """
            INSERT INTO gold.model_registry
                (model_name, model_version, status, framework, task, grain,
                 artifact_path, params, metrics, features, trained_on, trained_window)
            VALUES (%s, %s, 'active', %s, %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb)
            ON CONFLICT (model_name, model_version) DO UPDATE SET
                status = EXCLUDED.status,
                artifact_path = EXCLUDED.artifact_path,
                metrics = EXCLUDED.metrics,
                trained_window = EXCLUDED.trained_window
            """,
            (
                model_name,
                model_version,
                framework,
                task,
                grain,
                str(artifact_path),
                json.dumps(params, default=_json_default),
                json.dumps(metrics, default=_json_default),
                json.dumps(features),
                json.dumps(trained_on, default=_json_default),
                json.dumps(trained_window or {}, default=_json_default),
            ),
        )


def _json_default(o: Any) -> Any:
    if hasattr(o, "item"):  # numpy scalars
        return o.item()
    if isinstance(o, (dt.date, dt.datetime, dt.time)):
        return o.isoformat()
    return str(o)


# ---------------------------------------------------------------------------
# Artifacts
# ---------------------------------------------------------------------------

def save_artifact(model_name: str, version: str, model_obj: Any, meta: dict[str, Any]) -> pathlib.Path:
    """Persist the model (joblib) and a human-readable metadata JSON inside
    <ARTIFACTS_DIR>/<model_name>/<version>.*. Returns the model file path."""
    import joblib

    d = ARTIFACTS_DIR / model_name
    d.mkdir(parents=True, exist_ok=True)
    model_path = d / f"{version}.joblib"
    joblib.dump(model_obj, model_path)
    meta_path = d / f"{version}.json"
    meta_path.write_text(json.dumps(meta, indent=2, default=_json_default))
    return model_path


# ---------------------------------------------------------------------------
# Prediction persistence (gold.predictions)
# ---------------------------------------------------------------------------

def write_predictions(
    model_name: str,
    model_version: str,
    grain: str,
    rows: list[dict[str, Any]],
    batch_tag: str | None = None,
) -> int:
    """Insert prediction rows into gold.predictions. `rows` must have:
    entity_id, prediction, confidence, + optional lower_bound/upper_bound,
    explanation (dict), metadata (dict). Returns rows written."""
    if not rows:
        return 0
    ensure_serving_tables()
    with conn() as c:
        with c.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO gold.predictions
                    (model_name, model_version, grain, entity_id, prediction,
                     confidence, lower_bound, upper_bound, explanation, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                """,
                [
                    (
                        model_name,
                        model_version,
                        grain,
                        r["entity_id"],
                        float(r["prediction"]),
                        float(r.get("confidence", 0.0)),
                        r.get("lower_bound"),
                        r.get("upper_bound"),
                        json.dumps(r.get("explanation", {}), default=_json_default),
                        json.dumps(
                            dict(r.get("metadata", {}), **({"batch": batch_tag} if batch_tag else {})),
                            default=_json_default,
                        ),
                    )
                    for r in rows
                ],
            )
    return len(rows)