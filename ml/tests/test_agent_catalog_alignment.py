"""The agent's headline figures must equal the Phase 0 semantic layer (audit item 4).

The metrics catalog defines revenue / orders / AOV over the paying, non-lost
population and serves them (via gold.daily_revenue) at /api/v1/summary. The agent's
batch_overview once used a slightly different "valid" population, so it could quote
an AOV (160.26) that disagreed with the dashboard (160.27). DB-backed: skipped when
Postgres or the gold layer is unavailable.
"""
from __future__ import annotations

import pytest

from ml import common
from ml.agent import tools


@pytest.fixture()
def gold():
    try:
        conn = common.conn()
        conn.execute("select 1 from gold.daily_revenue limit 1")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"gold layer unavailable: {exc}")
    yield conn
    conn.close()


def test_batch_overview_matches_daily_revenue(gold):
    ref_revenue, ref_orders = gold.execute(
        "select round(sum(revenue)::numeric, 2), sum(order_count) from gold.daily_revenue").fetchone()
    ctx = tools.ToolContext(dsn=common.DATABASE_URL)
    row = tools.TOOL_BY_NAME["batch_overview"].run(ctx, {})[0]

    assert int(row["valid_orders"]) == int(ref_orders)
    assert float(row["valid_revenue"]) == pytest.approx(float(ref_revenue), abs=0.005)
    assert float(row["avg_valid_order_value"]) == pytest.approx(
        round(float(ref_revenue) / float(ref_orders), 2), abs=0.005)


def test_top_category_revenue_matches_the_batch_top_categories_table(gold):
    ctx = tools.ToolContext(dsn=common.DATABASE_URL)
    top = tools.TOOL_BY_NAME["batch_top_categories"].run(ctx, {"n": 1})[0]
    cols = [r[0] for r in gold.execute(
        "select column_name from information_schema.columns "
        "where table_schema = 'gold' and table_name = 'top_categories'").fetchall()]
    name_col = next((c for c in ("product_category", "category") if c in cols), None)
    if name_col is None:
        pytest.skip(f"cannot find the category column in gold.top_categories: {cols}")
    (ref,) = gold.execute(
        f"select round(revenue::numeric, 2) from gold.top_categories where {name_col} = %s",
        (top["category"],)).fetchone()
    assert float(top["revenue"]) == pytest.approx(float(ref), abs=0.005)
