"""Regression tests for the trailing-window fraud features (audit T4).

The batch builder expresses its windows in epoch *seconds*. Under pandas 3
(datetime64[us]) the old ``astype("int64") // 10**9`` produced ~1000-second
units, so the 24 h velocity window covered ~1000 days, the 90 d category
benchmark became the lifetime mean, and account_age_days was always 0 — silently
degenerate features the models were trained on. These tests pin the window
semantics on tiny hand-built frames, for every datetime resolution pandas can hand
us.
"""
from __future__ import annotations

import pathlib
import sys

import numpy as np
import pandas as pd
import pytest

# build_fraud_features is a script (`import common`); put ml/ on the path like
# ml/monitor/retrain_worker.py does.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import build_fraud_features as bff  # noqa: E402

T0 = pd.Timestamp("2017-09-13 08:59:02", tz="UTC")
T0_EPOCH = 1505293142


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_epoch_seconds_is_resolution_independent(unit):
    series = pd.Series([T0.as_unit(unit)])
    assert int(bff.epoch_seconds(series).iloc[0]) == T0_EPOCH


def frame(customer_times, category="c", value=100.0):
    ts = pd.Series(pd.to_datetime(customer_times, utc=True))
    df = pd.DataFrame({
        "customer_unique_id": "cust",
        "category_primary": category,
        "order_value": value,
        "order_purchase_timestamp": ts,
    })
    df["_ts"] = bff.epoch_seconds(df["order_purchase_timestamp"])
    return df


def test_velocity_counts_only_the_trailing_24_hours():
    df = frame(["2017-01-01 00:00", "2017-01-01 23:00", "2017-01-03 12:00"])
    # order 2 is 23 h after order 1 -> 1; order 3 is 37 h after order 2 -> 0
    assert list(bff.trailing_velocity(df, window_hours=24)) == [0, 1, 0]


def test_velocity_boundary_is_inclusive_like_the_go_ring():
    df = frame(["2017-01-01 00:00", "2017-01-02 00:00"])  # exactly 24 h apart
    assert list(bff.trailing_velocity(df, window_hours=24)) == [0, 1]


def test_benchmark_uses_only_the_trailing_90_days():
    values = [10.0, 30.0, 500.0]
    df = frame(["2017-01-01", "2017-02-01", "2017-06-01"])
    df["order_value"] = values
    bench = bff.trailing_benchmark(df)
    assert bench[0] == 10.0                    # no history -> own value
    assert bench[1] == 10.0                    # mean of the one prior order in window
    assert bench[2] == 500.0                   # Feb order is >90 d before June: no history


def test_account_age_is_whole_days_since_first_order():
    df = frame(["2017-01-01 00:00", "2017-01-01 12:00", "2017-01-04 01:00"])
    age = (df["_ts"] - df.groupby("customer_unique_id")["_ts"].transform("min")) // 86400
    assert list(age) == [0, 0, 3]
    assert age.max() > 0, "account_age_days must not collapse to 0 (the pandas-3 unit bug)"


def test_same_timestamp_ties_break_by_order_id_deterministically():
    # Two orders in the same second and category: "strictly before" is positional,
    # so the tie-break must be fixed. (timestamp, order_id) is the order the
    # stream/replay processes ties in, which is what makes batch == stream.
    df = frame(["2017-01-01 10:00:00", "2017-01-01 10:00:00"])
    df["order_id"] = ["b-order", "a-order"]      # deliberately not in id order
    df["order_value"] = [40.0, 10.0]
    bench = bff.trailing_benchmark(df)
    # a-order is first (no history -> own value 10); b-order sees a-order (mean 10).
    assert bench[1] == 10.0 and bench[0] == 10.0
    vel = bff.trailing_velocity(df, window_hours=24)
    assert list(vel) == [1, 0]                   # b-order counts a-order; a-order counts nothing


def test_primary_item_tie_break_is_deterministic_in_the_load_sql():
    # Two equally priced items in different categories must resolve by order_item_id,
    # the order the Go replay loader hands items to the stream assembler.
    assert "order by cat.price desc, cat.order_item_id" in bff.LOAD_SQL
