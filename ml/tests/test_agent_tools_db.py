"""DB-backed regression tests for the agent's SQL tools.

`model_scores` selected `to_char(predicted_at ...) AS predicted_at` and ordered by
`predicted_at`; Postgres resolves that ORDER BY to the second-resolution OUTPUT
column, so rows persisted within the same second came back in arbitrary order and
"the latest score" was not the latest (the eval's long-standing q12 flakiness).
Skipped when Postgres / the gold layer is unavailable.
"""
from __future__ import annotations

import time

import pytest

from ml import common
from ml.agent import tools


@pytest.fixture()
def conn():
    try:
        c = common.conn()
        common.ensure_serving_tables()
        c.execute("select 1 from gold.predictions limit 1")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"gold layer unavailable: {exc}")
    yield c
    c.close()


def test_model_scores_returns_the_true_latest_row_within_one_second(conn):
    marker = f"it-agent-{time.time_ns()}"
    # Three rows in the SAME wall-clock second (2099 keeps them newer than any
    # real row without disturbing live data), differing only in microseconds.
    ids = []
    for micro, entity in [(100, f"{marker}-a"), (900, f"{marker}-z"), (500, f"{marker}-m")]:
        row = conn.execute(
            """
            insert into gold.predictions (model_name, model_version, grain, entity_id,
                                          predicted_at, prediction, confidence, metadata)
            values ('bot_score', 'it-agent', 'session', %s,
                    timestamptz '2099-01-01 00:00:00+00' + make_interval(secs => %s / 1e6),
                    0.5, 0.5, '{"source":"it_agent"}'::jsonb)
            returning id
            """,
            (entity, micro),
        ).fetchone()
        ids.append(row[0])
    conn.commit()
    try:
        ctx = tools.ToolContext(dsn=common.DATABASE_URL)
        for _ in range(8):  # the old query returned different rows across identical calls
            latest = tools.TOOL_BY_NAME["model_scores"].run(ctx, {"model": "bot_score", "limit": 1})[0]
            assert latest["entity_id"] == f"{marker}-z", "must be the row with the greatest microsecond timestamp"
    finally:
        conn.execute("delete from gold.predictions where model_version = 'it-agent'")
        conn.commit()
