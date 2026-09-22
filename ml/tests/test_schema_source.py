"""The Phase 2 serving DDL is single-sourced from
api/internal/predict/schema.sql — Go embeds it (//go:embed schema.sql) and the
trainers read it via ml.common.SCHEMA_SQL, so there is NO second copy to
drift. These tests pin the loader wiring and the file's completeness, so an
accidental edit cannot silently drop a table, column, or index.

No database required; runs in CI with `uv run --group ml python -m pytest ml/tests`.
"""
from __future__ import annotations

from pathlib import Path

from ml import common

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "api" / "internal" / "predict" / "schema.sql"


def test_common_loads_the_shared_file():
    # The whole point: ml/common.py and api/internal/predict/schema.go must
    # execute byte-for-byte the same DDL. This guards the Python wiring.
    assert common.SCHEMA_SQL == SCHEMA_PATH.read_text(encoding="utf-8")


def test_schema_defines_both_tables_and_indexes():
    sql = common.SCHEMA_SQL
    assert "CREATE TABLE IF NOT EXISTS gold.model_registry" in sql
    assert "CREATE TABLE IF NOT EXISTS gold.predictions" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_predictions_model_entity" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_predictions_created" in sql


def test_registry_columns_complete():
    registry = common.SCHEMA_SQL.split("CREATE TABLE IF NOT EXISTS gold.model_registry")[1].split(");")[0]
    for col in [
        "model_name", "model_version", "status", "framework", "task", "grain",
        "artifact_path", "params", "metrics", "features", "trained_on",
        "trained_window", "created_at",
        "PRIMARY KEY (model_name, model_version)",
    ]:
        assert col in registry, f"gold.model_registry is missing {col!r}"


def test_predictions_columns_complete():
    preds = common.SCHEMA_SQL.split("CREATE TABLE IF NOT EXISTS gold.predictions")[1].split(");")[0]
    for col in [
        "id", "model_name", "model_version", "grain", "entity_id",
        "predicted_at", "prediction", "confidence", "lower_bound",
        "upper_bound", "explanation", "metadata", "created_at",
    ]:
        assert col in preds, f"gold.predictions is missing {col!r}"