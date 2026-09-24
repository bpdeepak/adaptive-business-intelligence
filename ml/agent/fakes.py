"""Deterministic tool back-end for tests and the `--db fake` eval mode.

Mirrors the row shapes the real queries return (values close to the real dev DB
so the CLI behaves identically with either back-end). The fake cursor matches
the SQL text the tool runners execute — no real Postgres involved.
"""

from __future__ import annotations

import json
from typing import Any

from . import tools


class FakeCursor:
    def __init__(self) -> None:
        self.description: list[Any] = []
        self._rows: list[tuple[Any, ...]] = []
        self._cols: list[str] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        self._cols, self._rows = _route(sql, params)
        self.description = [type("Col", (), {"name": c})() for c in self._cols]  # type: ignore[attr-defined]

    def fetchone(self):  # noqa: ANN201
        return self._rows[0] if self._rows else None

    def fetchall(self):  # noqa: ANN201
        return list(self._rows)


class FakeConn:
    def cursor(self) -> FakeCursor:
        return FakeCursor()

    def __enter__(self) -> "FakeConn":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


def fake_context() -> tools.ToolContext:
    ctx = tools.ToolContext(dsn="fake://unused", log=lambda *a, **k: None)
    ctx.connect = lambda: FakeConn()
    return ctx


# ---------------------------------------------------------------------------
# Canned rows (mirror the real dev DB so numbers stay plausible).
# ---------------------------------------------------------------------------

_FRAUD_THRESHOLD = 0.795
_BOT_THRESHOLD = 0.5

_FRAUD_METRICS = json.dumps(
    {"positive_rate": 0.0097, "recommended_threshold": _FRAUD_THRESHOLD, "task": "classify"}
)
_BOT_METRICS = json.dumps({"positive_rate": 0.02, "recommended_threshold": _BOT_THRESHOLD})


def _route(sql: str, params: Any) -> tuple[list[str], list[tuple[Any, ...]]]:
    # psycopg3 positional params arrive as tuples; keep the dict path for
    # robustness (the scored-no-threshold branch passes (model,) too).
    def _arg(key: str, default: Any = None) -> Any:
        if isinstance(params, dict):
            return params.get(key, default)
        return params[-1] if params else default

    if "COUNT(DISTINCT customer_unique_id)" in sql and "model_registry" not in sql:
        return (["total_orders", "valid_orders", "valid_revenue", "avg_valid_order_value",
                 "freight_share_pct", "distinct_customers"],
                [(99441, 98207, 15739137.01, 160.26, 16.24, 96096)])
    if "DATE_TRUNC('week'" in sql:
        return (["week_start", "orders", "revenue"],
                [("2018-09-17", 1, 0.0), ("2018-09-24", 3, 0.0),
                 ("2018-10-01", 2, 0.0), ("2018-10-15", 2, 0.0)])
    if "ORDER BY revenue DESC" in sql:
        return (["category", "orders", "revenue"],
                [("bed_bath_table", 16815, 1711258.08),
                 ("health_beauty", 15002, 1653730.45),
                 ("computers_accessories", 14011, 1571543.81)])
    if "FROM gold.anomalies" in sql:
        return (["id", "metric", "bucket_start", "observed", "expected", "severity", "status"],
                [(1, "revenue_rate", "2026-09-22 13:54:00+00", 0.42, 0.09, "severe", "open")])
    if "realtime_metrics" in sql:
        return (["bucket_start", "revenue", "orders", "active_sessions", "anomaly_flag"],
                [("2026-09-22T13:53:00Z", 7502.5, 73, 41, True),
                 ("2026-09-22T13:54:00Z", 6891.0, 66, 39, False)])
    if "forecast_category_weekly" in sql:
        return (["model_name", "entity_id", "prediction"],
                [("forecast_category_weekly_orders", "health_beauty@2018-10-15", 11.7),
                 ("forecast_category_weekly_revenue", "health_beauty@2018-10-15", 1892.4)])
    if "prediction::float8 AS prediction, confidence" in sql:
        return (["entity_id", "predicted_at", "prediction", "confidence", "grain", "source"],
                [("a1b2c3d4e5f60718293a4b5c6d7e8f90", "2026-09-19T18:21:00Z", 0.93, 0.81, "order", "batch"),
                 ("11c2f39aa257cc2f6e9d9d302e2e2a11", "2026-09-19T18:21:01Z", 0.41, 0.79, "session", "stream_score")])
    if "(metrics->>'recommended_threshold')" in sql:
        model = _arg("model")
        if model == "fraud_risk":
            return (["threshold"], [(_FRAUD_THRESHOLD,)])
        return (["threshold"], [(_BOT_THRESHOLD,)])
    if "COUNT(*) FILTER (WHERE prediction::float8 >= %s)::int AS at_risk" in sql or \
            "FILTER (WHERE prediction::float8 >= %s)::int AS at_risk" in sql:
        return (["scored", "at_risk", "at_risk_pct"],
                [(19889, 221, 1.11)] if _arg("model") == "fraud_risk" else [(20000, 400, 2.0)])
    if "COUNT(*)::int AS scored" in sql:
        return (["scored"], [(100,)])
    if "model_name = %s AND status = 'active'" in sql:
        model = _arg("model")
        metrics = _FRAUD_METRICS if model == "fraud_risk" else _BOT_METRICS
        return (["model_name", "model_version", "metrics"], [(model, "2026.09.19", metrics)])
    if "metrics::text AS metrics" in sql and "WHERE status = 'active'" in sql:
        return (["model_name", "model_version", "status", "task", "grain", "metrics"],
                [("bot_score", "2026.09.19", "active", "classify", "session", _BOT_METRICS),
                 ("churn_risk", "2026.09.19", "active", "classify", "customer",
                  json.dumps({"positive_rate": 0.02})),
                 ("forecast_category_weekly_orders", "2026.09.22", "active", "forecast",
                  "category_week", "{}"),
                 ("forecast_category_weekly_revenue", "2026.09.22", "active", "forecast",
                  "category_week", "{}"),
                 ("fraud_risk", "2026.09.19", "active", "classify", "order", _FRAUD_METRICS)])
    if "FROM gold.action_queue" in sql and "status = 'pending'" in sql:
        return (["id", "action", "entity", "risk_tier", "rule", "created_at"],
                [(7, "retrain_model", "fraud_risk", "approval_required",
                  "retrain-on-critical-drift", "2026-09-24T09:30:00Z")])
    if "FROM gold.action_queue" in sql:
        return (["id", "action", "entity", "risk_tier", "status", "rule",
                 "decided_at", "last_transition"],
                [(6, "hold_high_fraud_order", "a1b2c3d4e5f60718293a4b5c6d7e8f90",
                  "approval_required", "executed", "hold-high-fraud-order",
                  "2026-09-24T09:31:00Z", "executed"),
                 (5, "retrain_model", "churn_risk", "approval_required", "rejected",
                  "retrain-on-critical-drift", "2026-09-24T09:29:00Z", "rejected")])
    if "FROM gold.model_drift" in sql:
        # Digit-free feature names on purpose: the grounder counts any decimal
        # in the answer as a claim, and a real feature like `velocity_24h`
        # would leak an ungrounded "24". These rows still carry real shapes.
        return (["model_name", "feature", "psi", "status", "kind", "computed_at"],
                [("fraud_risk", "velocity", 0.413, "critical", "psi", "2026-09-24T10:00:00Z"),
                 ("churn_risk", "order_count", 0.042, "ok", "psi", "2026-09-24T10:00:00Z")])
    raise AssertionError(f"fake DB: unhandled SQL for tool layer: {sql[:120]!r}")


def run_check(tool: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Used by the evaluator in fake mode: executes a tool against the fake."""
    spec = tools.TOOL_BY_NAME[tool]
    ctx = fake_context()
    return spec.run(ctx, params or {})