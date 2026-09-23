"""Curated, provenance-tagged read-only tools for the Phase 3 NL-BI agent.

Design notes (3C guardrails):

* There is deliberately NO free-form SQL tool. The agent can only call the
  fixed tools below with parameters, so injection and write access are
  structurally impossible and every returned number carries a provenance
  label ("batch" | "live_replay" | "registry" | "predictions").
* "batch" rows come from the gold semantic layer over the whole historical
  dataset. "live_replay" rows come from gold.realtime_metrics (+ anomalies),
  which reflects the *replay* of that history at speed — never additive with
  batch figures. The grounding pass refuses answers that sum them.
* "predictions" rows are the ONLY source of model scores and sketches
  (gold.predictions). The agent must not invent scores, and the grounder
  checks any claimed score/entity pair exists here.
* Every tool is parameterized and SELECT-only with a hard row cap.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable

# Provenance labels every observation must carry.
BATCH = "batch"
LIVE_REPLAY = "live_replay"
REGISTRY = "registry"
PREDICTIONS = "predictions"

# Fixed caps: tools never return more than this many rows.
MAX_ROWS = 50

# Models whose scores may be surfaced through the model_scores tool.
SCORE_MODELS = ("fraud_risk", "bot_score", "churn_risk")

# Forecast families (batch role, read from gold.predictions like any batch
# forecast — they were produced by training, not by the replay).
FORECAST_MODELS = ("forecast_category_weekly_orders", "forecast_category_weekly_revenue")


@dataclass
class ToolSpec:
    name: str
    description: str
    parameters: dict[str, Any]
    label: str  # fixed provenance of everything this tool returns
    run: Callable[["ToolContext", dict[str, Any]], list[dict[str, Any]]]


@dataclass
class ToolObservation:
    """One executed tool call, captured for the LLM AND the grounder."""

    tool: str
    params: dict[str, Any]
    label: str
    rows: list[dict[str, Any]]
    note: str = ""
    error: str = ""
    render: str = ""  # compact text fed back to the model


@dataclass
class ToolContext:
    dsn: str
    log: Callable[..., None] = field(default=lambda *a, **k: None)
    connect: Callable[..., Any] = field(default=None)

    def __post_init__(self) -> None:
        if self.connect is None:
            self.connect = lambda: _pg_connect(self.dsn)


def _pg_connect(dsn: str) -> Any:
    import psycopg

    return psycopg.connect(dsn, connect_timeout=5)


_KNOWN_MODELS_SQL = """
SELECT model_name, model_version, status, task, grain,
       metrics::text AS metrics
FROM gold.model_registry
WHERE status = 'active'
ORDER BY model_name, created_at DESC
"""

_BATCH_OVERVIEW_SQL = """
SELECT COUNT(*)::int                                   AS total_orders,
       SUM(CASE WHEN NOT is_lost THEN 1 ELSE 0 END)::int AS valid_orders,
       ROUND(SUM(CASE WHEN NOT is_lost THEN payment_value_total ELSE 0 END)::numeric, 2) AS valid_revenue,
       ROUND(SUM(payment_value_total)::numeric /
             NULLIF(SUM(CASE WHEN NOT is_lost THEN 1 ELSE 0 END), 0), 2)            AS avg_valid_order_value,
       ROUND(SUM(freight_total)::numeric /
             NULLIF(SUM(items_gross_total + freight_total), 0) * 100, 2)             AS freight_share_pct,
       (SELECT COUNT(DISTINCT customer_unique_id) FROM gold.fct_orders)::int       AS distinct_customers
FROM gold.fct_orders
"""

_TOP_CATEGORIES_SQL = """
SELECT p.product_category                                   AS category,
       COUNT(DISTINCT o.order_id)::int                      AS orders,
       ROUND(SUM(o.payment_value_total)::numeric, 2)        AS revenue
FROM gold.fct_orders o
JOIN gold.fct_order_items i ON i.order_id = o.order_id
JOIN gold.dim_products p   ON p.product_id = i.product_id
WHERE NOT o.is_lost
GROUP BY p.product_category
ORDER BY revenue DESC
LIMIT %s
"""

_WEEKLY_SQL = """
WITH ds AS (SELECT MAX(order_purchase_timestamp) AS max_ts FROM gold.fct_orders)
SELECT to_char(DATE_TRUNC('week', o.order_purchase_timestamp)::timestamp, 'YYYY-MM-DD') AS week_start,
       COUNT(*)::int AS orders,
       ROUND(SUM(CASE WHEN NOT o.is_lost THEN o.payment_value_total ELSE 0 END)::numeric, 2) AS revenue
FROM gold.fct_orders o, ds
WHERE o.order_purchase_timestamp >= ds.max_ts - INTERVAL '%(weeks)s weeks'
GROUP BY 1
ORDER BY 1 DESC
LIMIT %(weeks)s
"""

_LIVE_BUCKETS_SQL = """
WITH latest AS (SELECT MAX(bucket_start) AS b FROM gold.realtime_metrics)
SELECT to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS bucket_start,
       revenue::float8, orders::int, active_sessions::int, anomaly_flag
FROM gold.realtime_metrics, latest
WHERE bucket_start >= latest.b - INTERVAL '%(minutes)s minutes'
ORDER BY bucket_start
LIMIT 500
"""

_LIVE_ANOMALIES_SQL = """
WITH latest AS (SELECT MAX(bucket_start) AS b FROM gold.realtime_metrics)
SELECT id, metric, bucket_start, observed::float8, expected::float8,
       severity, status
FROM gold.anomalies, latest
WHERE bucket_start >= latest.b - INTERVAL '%(minutes)s minutes'
ORDER BY detected_at DESC
LIMIT 20
"""

_FORECAST_SQL = """
SELECT model_name, entity_id, prediction::float8 AS prediction
FROM gold.predictions
WHERE model_name IN ('forecast_category_weekly_orders', 'forecast_category_weekly_revenue')
  AND entity_id LIKE %s
ORDER BY entity_id DESC
LIMIT 16
"""

_MODEL_SCORES_SQL = """
SELECT entity_id, to_char(predicted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS predicted_at,
       prediction::float8 AS prediction, confidence::float8 AS confidence,
       metadata->>'grain' AS grain, metadata->>'source' AS source
FROM gold.predictions
WHERE model_name = %s
ORDER BY predicted_at DESC
LIMIT %s
"""


def _registry(ctx: ToolContext, params: dict[str, Any]) -> list[dict[str, Any]]:
    with ctx.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_KNOWN_MODELS_SQL)
            cols = [d.name for d in cur.description]
            return [_flatten_metrics(dict(zip(cols, row))) for row in cur.fetchall()]


def _flatten_metrics(row: dict[str, Any]) -> dict[str, Any]:
    """Merge a `metrics` JSON string into the row so threshold/baselines become
    first-class keys the LLM (and grounder) can cite directly."""
    raw = row.get("metrics")
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                merged = dict(row)
                merged.pop("metrics", None)
                merged.update({k: v for k, v in parsed.items() if isinstance(v, (int, float))})
                return merged
        except json.JSONDecodeError:
            pass
    return row


def _batch_overview(ctx: ToolContext, params: dict[str, Any]) -> list[dict[str, Any]]:
    with ctx.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_BATCH_OVERVIEW_SQL)
            cols = [d.name for d in cur.description]
            row = cur.fetchone()
            return [dict(zip(cols, row))]


def _top_categories(ctx: ToolContext, params: dict[str, Any]) -> list[dict[str, Any]]:
    n = min(int(params.get("n", 10)), MAX_ROWS)
    with ctx.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_TOP_CATEGORIES_SQL, (n,))
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def _weekly(ctx: ToolContext, params: dict[str, Any]) -> list[dict[str, Any]]:
    weeks = min(max(int(params.get("weeks", 8)), 1), 26)
    with ctx.connect() as conn:
        with conn.cursor() as cur:
            # interval literal via parameter-composed SQL to keep the layer
            # parameterized (weeks is a validated int).
            cur.execute(_WEEKLY_SQL % {"weeks": weeks})
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def _live_realtime(ctx: ToolContext, params: dict[str, Any]) -> list[dict[str, Any]]:
    minutes = min(max(int(params.get("minutes", 30)), 1), 1440)
    out: list[dict[str, Any]] = []
    with ctx.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_LIVE_BUCKETS_SQL % {"minutes": minutes})
            cols = [d.name for d in cur.description]
            out.extend(dict(zip(cols, r)) for r in cur.fetchall())
            cur.execute(_LIVE_ANOMALIES_SQL % {"minutes": minutes})
            acols = [d.name for d in cur.description]
            out.extend(dict(zip(acols, r)) for r in cur.fetchall())
    return out


def _forecast(ctx: ToolContext, params: dict[str, Any]) -> list[dict[str, Any]]:
    category = str(params.get("category", "")).strip()
    if not category:
        raise ValueError("forecast needs a category")
    with ctx.connect() as conn:
        with conn.cursor() as cur:
            # Pattern is built in Python: no literal % reaches the query text.
            cur.execute(_FORECAST_SQL, (f"{category}@%",))
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def _model_scores(ctx: ToolContext, params: dict[str, Any]) -> list[dict[str, Any]]:
    model = str(params.get("model", "")).strip()
    if model not in SCORE_MODELS:
        raise ValueError(f"model_scores only serves {', '.join(SCORE_MODELS)}")
    limit = min(int(params.get("limit", 20)), MAX_ROWS)
    with ctx.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_MODEL_SCORES_SQL, (model, limit))
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def _registry_metric(ctx: ToolContext, params: dict[str, Any]) -> list[dict[str, Any]]:
    model = str(params.get("model", "")).strip()
    if not model:
        raise ValueError("registry_metric needs a model")
    with ctx.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
SELECT model_name, model_version, metrics::text AS metrics
FROM gold.model_registry
WHERE model_name = %s AND status = 'active'
ORDER BY created_at DESC LIMIT 1
""",
                (model,),
            )
            cols = [d.name for d in cur.description]
            row = cur.fetchone()
            return [_flatten_metrics(dict(zip(cols, row)))] if row else []


_THRESHOLD_SQL = """
SELECT (metrics->>'recommended_threshold')::float8 AS threshold
FROM gold.model_registry
WHERE model_name = %s AND status = 'active'
ORDER BY created_at DESC LIMIT 1
"""

_MODEL_RATE_SQL = """
SELECT COUNT(*)::int AS scored,
       COUNT(*) FILTER (WHERE prediction::float8 >= %s)::int AS at_risk,
       ROUND(100.0 * COUNT(*) FILTER (WHERE prediction::float8 >= %s) / COUNT(*), 2) AS at_risk_pct
FROM gold.predictions
WHERE model_name = %s
"""


def _score_stats(ctx: ToolContext, params: dict[str, Any]) -> list[dict[str, Any]]:
    """Persisted-score rate for one model (gold.predictions only).

    The at-risk threshold comes from the active registry row's own
    recommended_threshold — the rate is therefore the *model's* validation
    semantics, not an ad-hoc cut invented by the agent.
    """
    model = str(params.get("model", "")).strip()
    if model not in SCORE_MODELS:
        raise ValueError(f"model_rate only serves {', '.join(SCORE_MODELS)}")
    with ctx.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_THRESHOLD_SQL, (model,))
            row = cur.fetchone()
            threshold = float(row[0]) if row and row[0] is not None else None
            if threshold is None:
                # No validated threshold → surface scored count only, honestly.
                cur.execute(
                    "SELECT COUNT(*)::int AS scored FROM gold.predictions WHERE model_name = %s",
                    (model,),
                )
                cols = [d.name for d in cur.description]
                return [dict(zip(cols, cur.fetchone()))]
            cur.execute(_MODEL_RATE_SQL, (threshold, threshold, model))
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, cur.fetchone()))]


# Tool registry. Order matters only for discoverability in prompts.
TOOL_SPECS: list[ToolSpec] = [
    ToolSpec(
        name="list_models",
        description="List the model families with an active version (name, version, task, grain, key metrics).",
        parameters={},
        label=REGISTRY,
        run=_registry,
    ),
    ToolSpec(
        name="batch_overview",
        description="Headline batch figures over the whole history: total orders, valid revenue, avg order value, freight share.",
        parameters={},
        label=BATCH,
        run=_batch_overview,
    ),
    ToolSpec(
        name="batch_top_categories",
        description="Top product categories by batch revenue (and orders).",
        parameters={"n": {"type": "integer", "description": "number of categories (default 10)"}},
        label=BATCH,
        run=_top_categories,
    ),
    ToolSpec(
        name="batch_revenue_by_week",
        description="Weekly orders + revenue for the last N weeks (batch).",
        parameters={"weeks": {"type": "integer", "description": "weeks to return (default 8)"}},
        label=BATCH,
        run=_weekly,
    ),
    ToolSpec(
        name="forecast",
        description="Latest weekly forecast series for one product category (orders + revenue).",
        parameters={"category": {"type": "string", "description": "product category slug, e.g. health_beauty"}},
        label=BATCH,
        run=_forecast,
    ),
    ToolSpec(
        name="model_scores",
        description="Recent persisted scores ONE model has produced: fraud_risk, bot_score, or churn_risk. The only source of model scores.",
        parameters={
            "model": {"type": "string", "enum": list(SCORE_MODELS)},
            "limit": {"type": "integer"},
        },
        label=PREDICTIONS,
        run=_model_scores,
    ),
    ToolSpec(
        name="model_rate",
        description="At-risk rate of one model over its persisted scores (gold.predictions), using the model's own recommended_threshold from the registry.",
        parameters={"model": {"type": "string", "enum": list(SCORE_MODELS)}},
        label=PREDICTIONS,
        run=_score_stats,
    ),
    ToolSpec(
        name="registry_metric",
        description="Validated metrics for one active model: recommended_threshold, positive_rate, category_rank.",
        parameters={"model": {"type": "string"}},
        label=REGISTRY,
        run=_registry_metric,
    ),
    ToolSpec(
        name="live_realtime",
        description="Recent LIVE REPLAY buckets + anomalies (gold.realtime_metrics), relative to the latest replay bucket — the newest replay activity. These are replay figures — NEVER additive with batch figures.",
        parameters={"minutes": {"type": "integer", "description": "lookback in minutes from the latest replay bucket (default 30)"}},
        label=LIVE_REPLAY,
        run=_live_realtime,
    ),
]

TOOL_BY_NAME = {spec.name: spec for spec in TOOL_SPECS}


def spec_dicts() -> list[dict[str, Any]]:
    """OpenAI-style tool schema for chat completions."""
    out = []
    for spec in TOOL_SPECS:
        props = {k: v for k, v in spec.parameters.items()}
        out.append(
            {
                "type": "function",
                "function": {
                    "name": spec.name,
                    "description": spec.description,
                    "parameters": {
                        "type": "object",
                        "properties": props,
                        "required": list(props),
                    } if props else {"type": "object", "properties": {}},
                },
            }
        )
    return out


def render_observation(obs: ToolObservation) -> str:
    """Compact, number-friendly text for the LLM conversation."""
    head = f"[tool {obs.tool}] label={obs.label}"
    if obs.note:
        head += f" note={obs.note}"
    if obs.error:
        return f"{head} ERROR: {obs.error}"
    return head + "\n" + _fmt_rows(obs.rows)


def _fmt_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "(no rows)"
    # Union of ALL row keys (first-seen order): mixed-shape tools (e.g.
    # live_realtime returning buckets + anomalies) must render every value.
    keys: list[str] = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    lines = [", ".join(f"{k}={r.get(k)}" for k in keys) for r in rows]
    return "\n".join(lines)