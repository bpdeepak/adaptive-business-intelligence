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

# ---------------------------------------------------------------------------
# Phase 4 additions to the serving DDL, and the governance DDL (audit T3).
# The original tests pinned only the Phase 2 columns; anything added later could be
# dropped from schema.sql without a single failure.
# ---------------------------------------------------------------------------

GOVERNANCE_PATH = Path(__file__).resolve().parents[2] / "api" / "internal" / "actions" / "schema.sql"


def test_serving_schema_pins_the_phase4_additions():
    sql = common.SCHEMA_SQL
    for fragment in [
        "drift_baseline jsonb NOT NULL DEFAULT '{}'::jsonb",     # registry column (both CREATE and ALTER)
        "ADD COLUMN IF NOT EXISTS features jsonb",                 # predictions.features (stream PSI input)
        "CREATE TABLE IF NOT EXISTS gold.model_drift",
        "idx_model_drift_model_computed",
        "idx_model_drift_status",
    ]:
        assert fragment in sql, f"schema.sql lost {fragment!r}"
    drift = sql.split("CREATE TABLE IF NOT EXISTS gold.model_drift")[1].split(");")[0]
    for col in ["id", "model_name", "model_version", "computed_at", "feature", "psi", "status", "kind", "detail"]:
        assert col in drift, f"gold.model_drift is missing {col!r}"


def test_governance_ddl_is_single_sourced_and_complete():
    assert common.GOVERNANCE_SQL == GOVERNANCE_PATH.read_text(encoding="utf-8")
    sql = common.GOVERNANCE_SQL
    for table in ["action_queue", "action_audit_log", "order_flags", "purchase_orders",
                  "retention_actions", "retrain_requests", "event_log"]:
        assert f"CREATE TABLE IF NOT EXISTS gold.{table}" in sql, f"missing gold.{table}"
    # the constraints the governance guarantees rest on
    assert "uq_action_queue_dedup" in sql and "(dedup_key)" in sql
    assert "REFERENCES gold.action_queue (id)" in sql
    assert "action_id    bigint NOT NULL UNIQUE" in sql, "one retrain request per governance action"
    assert "started_at" in sql.split("gold.retrain_requests")[-1]


def test_audit_log_is_append_only_in_the_shared_ddl():
    sql = common.GOVERNANCE_SQL
    assert "trg_action_audit_log_append_only" in sql and "BEFORE UPDATE OR DELETE" in sql
    assert "trg_action_audit_log_no_truncate" in sql and "BEFORE TRUNCATE" in sql
    assert "abi.audit_maintenance" in sql
