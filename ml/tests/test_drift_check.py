"""drift_check.backtest_reproduction (audit C11 / T2).

The check used to be published as "forecast decay". It recomputes WAPE from the
registry's own persisted held-out predictions, so it verifies the stored evidence,
it does not measure decay. These tests pin the honest contract: active version
only, distinct kind, warning-capped (never proposes a retrain). No database: a
fake cursor stands in for psycopg.
"""
from __future__ import annotations

import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from ml.monitor import drift_check  # noqa: E402


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed: list[tuple[str, tuple]] = []

    def execute(self, sql, params=()):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows


def held_out(n, predicted, actual):
    return [{"prediction": predicted, "actual": actual} for _ in range(n)]


def run(rows, metrics=None):
    cur = FakeCursor(rows)
    out = drift_check.backtest_reproduction(
        cur, [], "forecast_category_weekly_revenue", "v-active", "2026-01-01T00:00:00Z",
        metrics if metrics is not None else {"wmape": 0.31})
    return cur, out


def test_only_the_active_version_is_read():
    cur, _ = run(held_out(30, 100.0, 100.0))
    sql, params = cur.executed[0]
    assert "model_version = %s" in sql
    assert params == ("forecast_category_weekly_revenue", "v-active"), \
        "rows from superseded versions must not be compared with this version's WMAPE"


def test_row_is_labelled_backtest_repro_not_decay():
    _, rows = run(held_out(30, 100.0, 100.0))
    assert len(rows) == 1
    kind = rows[0][6]
    assert kind == drift_check.REPRO_KIND == "backtest_repro"
    assert "decay" not in kind
    assert "not a live-decay" in rows[0][7]


def test_a_reproducing_backtest_is_ok():
    # WAPE = sum|p-a| / sum|a| = 31/100 = 0.31 == the registry's wmape
    rows = held_out(20, 131.0, 100.0)[:10] + held_out(10, 69.0, 100.0)
    _, out = run(rows, {"wmape": 0.31})
    assert out[0][5] == "ok"


@pytest.mark.parametrize("predicted", [200.0, 5000.0, 1e9])
def test_a_wildly_inconsistent_backtest_is_only_ever_a_warning(predicted):
    _, out = run(held_out(30, predicted, 100.0), {"wmape": 0.31})
    assert out[0][5] == "warning", "must never be critical: it cannot propose a retrain"


def test_too_few_rows_writes_nothing():
    _, out = run(held_out(19, 100.0, 100.0))
    assert out == []


def test_null_and_nan_rows_are_skipped():
    rows = held_out(25, 100.0, 100.0) + [{"prediction": None, "actual": 1.0},
                                         {"prediction": float("nan"), "actual": 1.0}]
    _, out = run(rows)
    assert out and '"n": 25' in out[0][7]


def test_stream_sampling_is_restricted_to_the_active_version():
    cur = FakeCursor([{"features": {"a": 1.0}}])
    drift_check.stream_feature_samples(cur, "fraud_risk", "v-active", "order", 100)
    sql, params = cur.executed[0]
    assert "model_version = %s" in sql
    assert params == ("fraud_risk", "v-active", "order", 100)
