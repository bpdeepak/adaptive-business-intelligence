"""Build gold.session_features — the durable training corpus for the
bot-vs-human session classifier.

The streaming clickstream that Phase 1 lands in bronze.stream_events is
retained only 12h, so it cannot be the training store by itself. This builder
produces the DURABLE session-feature table by deterministically replicating
the producer's session synthesis (api/internal/stream/simulator.go) at the
session level:

- converting sessions: one per order, funnel walk of 3..12 page views starting
  10..39 min before purchase (humans), or a 3..12 page burst every 30 ms within
  250 ms of purchase (synthetic bots, 2%);
- abandoned sessions: per simulated day, `orders_today * (1 - conv)/conv`
  sessions of 1..3 page views with 1..8 min gaps (humans) or 40 ms bursts
  (bots); a cart is added in ~60% of them;
- ground truth: is_synthetic_bot for every session (the same label the replay
  streams as TrainingLabel events into bronze.training_ground_truth).

This is a distributional replica of the stream (seeded, deterministic), NOT a
byte-identical replay — it exists so training does not depend on a 6.5-hour
replay pass having already emitted labeled sessions (and so retrains are cheap
and reproducible). A streaming consumer that folds real stream labels into this
table is a documented follow-up slice (see docs/phase2.md).

Output table: gold.session_features (durable, per-session features + label).
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

import common

BOT_RATIO_DEFAULT = 0.02
CONVERSION_DEFAULT = 0.03
CLICK_MIN, CLICK_MAX = 3, 12


def _env_float(name: str, default: float) -> float:
    """Same env contract as api/internal/config/config.go (floatenv): invalid
    or non-positive values fall back to the default."""
    try:
        v = float(os.getenv(name, ""))
        return v if v > 0 else default
    except (TypeError, ValueError):
        return default


# Mirrors the Go producer (api/internal/config/config.go: ABI_BOT_RATIO /
# ABI_SESSION_CONVERSION_RATE). Reading the SAME env vars means tuning the
# replay's synthesis (e.g. the conversion rate) automatically applies to the
# offline training corpus — closing the primary drift vector between the live
# stream and the classifier's training data. The session-level timing
# micro-constants (click gaps, burst intervals, funnel walks) are still
# hand-mirrored between simulator.go and this file: flagged tech debt
# (docs/phase2.md §9).
BOT_RATIO = _env_float("ABI_BOT_RATIO", BOT_RATIO_DEFAULT)
CONVERSION = _env_float("ABI_SESSION_CONVERSION_RATE", CONVERSION_DEFAULT)

FUNNEL = ["home", "category", "search", "product", "cart", "checkout"]
BROWSE = ["home", "category", "search", "product", "cart"]


def _funnel_pages(rng: np.random.Generator, n: int) -> list[str]:
    out: list[str] = []
    pos = -1
    for _ in range(n):
        pos += 1 + int(rng.integers(0, 2))
        if pos >= len(FUNNEL):
            pos = len(FUNNEL) - 1
        out.append(FUNNEL[pos])
    return out


def _abandon_pages(rng: np.random.Generator, n: int) -> list[str]:
    out: list[str] = []
    pos = 0
    for _ in range(n):
        pos += 1 + int(rng.integers(0, len(BROWSE) - 1))
        if pos >= len(BROWSE):
            pos = len(BROWSE) - 1
        out.append(BROWSE[pos])
    return out


def _bot_pages(rng: np.random.Generator, n: int) -> list[str]:
    cycle = ["product", "cart", "checkout"]
    return [cycle[i % 3] for i in range(n)]


def _page_flags(pages: list[str]) -> dict[str, int]:
    return {
        "has_search": int("search" in pages),
        "has_product_page": int("product" in pages),
        "has_cart_page": int("cart" in pages),
        "has_checkout_page": int("checkout" in pages),
        "page_types_distinct": len(set(pages)),
    }


def _cv(gaps: list[float]) -> float:
    if len(gaps) < 2:
        return 0.0
    arr = np.asarray(gaps, dtype=float)
    mean = arr.mean()
    if mean <= 0:
        return 0.0
    return float(arr.std())


def _weekend(d: object) -> int:
    return int(d.isoweekday() in (6, 7))


def build_session_row(
    session_id: str,
    first_time: datetime,
    last_time: datetime,
    pages: list[str],
    gaps: list[float],
    converting: int,
    cart_added: int,
    cart_value: float,
    is_bot: int,
) -> dict:
    flags = _page_flags(pages)
    d = first_time.date()
    return {
        "session_id": session_id,
        "loop_id": "l0",
        "source": "offline_replay",
        "session_date": d,
        "is_converting": converting,
        "click_count": len(pages),
        "duration_seconds": round((last_time - first_time).total_seconds(), 3),
        "click_interval_cv": _cv(gaps),
        "has_search": flags["has_search"],
        "has_product_page": flags["has_product_page"],
        "has_cart_page": flags["has_cart_page"],
        "has_checkout_page": flags["has_checkout_page"],
        "page_types_distinct": flags["page_types_distinct"],
        "cart_added": cart_added,
        "cart_value": round(cart_value, 2),
        "hour_of_day": first_time.hour,
        "is_weekend": _weekend(d),
        "is_synthetic_bot": is_bot,
    }


def converting_sessions(orders: pd.DataFrame, rng: np.random.Generator) -> list[dict]:
    rows: list[dict] = []
    for _, o in orders.iterrows():
        order_time = o["order_purchase_timestamp"].to_pydatetime()
        is_bot = rng.random() < BOT_RATIO
        if is_bot:
            n = CLICK_MIN + int(rng.integers(0, CLICK_MAX - CLICK_MIN + 1))
            pages = _bot_pages(rng, n)
            first = order_time - timedelta(milliseconds=250)
            gaps = [30.0] * (n - 1)
            last = first + timedelta(milliseconds=30 * (n - 1))
        else:
            n = CLICK_MIN + int(rng.integers(0, CLICK_MAX - CLICK_MIN + 1))
            pages = _funnel_pages(rng, n)
            first = order_time - timedelta(seconds=600 + int(rng.integers(0, 1741)))
            t = first
            gaps: list[float] = []
            effective: list[str] = []
            for page in pages:
                gap = 45 + float(rng.integers(0, 300))
                t += timedelta(seconds=gap)
                if effective and t >= order_time - timedelta(seconds=5 + int(rng.integers(0, 30))):
                    break
                effective.append(page)
                gaps.append(gap)
            pages = effective
            last = t if effective else first

        rows.append(
            build_session_row(
                f"c-{o['customer_id']}-{o['order_id']}",
                first, last, pages, gaps, converting=1, cart_added=0,
                cart_value=0.0, is_bot=int(is_bot),
            )
        )
    return rows


def abandoned_sessions_day(
    day,
    orders_today: int,
    rng: np.random.Generator,
    day_epoch: float,
) -> list[dict]:
    n_abandon = int(round(orders_today * (1 - CONVERSION) / CONVERSION))
    rows: list[dict] = []
    for i in range(n_abandon):
        t0 = datetime.fromtimestamp(day_epoch + rng.random() * 86400.0, tz=timezone.utc)
        is_bot = rng.random() < BOT_RATIO
        if is_bot:
            n = 1 + int(rng.integers(0, 3))
            pages = _bot_pages(rng, n)
            gaps = [40.0] * (n - 1)
            last = t0 + timedelta(milliseconds=40 * (n - 1))
        else:
            n = 1 + int(rng.integers(0, 3))
            pages = _abandon_pages(rng, n)
            t = t0
            gaps = []
            last = t0
            for _ in range(n):
                gap = 60 + float(rng.integers(0, 421))
                t += timedelta(seconds=gap)
                gaps.append(gap)
                last = t
        cart_added = int(rng.random() < 0.6)
        cart_value = 0.0
        if cart_added:
            cart_value = float(np.round(np.sum(rng.uniform(20, 300, size=1 + int(rng.integers(0, 4)))), 2))
        rows.append(
            build_session_row(
                f"a-{day.isoformat()}-{i:06d}",
                t0, last, pages, gaps, converting=0, cart_added=cart_added,
                cart_value=cart_value, is_bot=int(is_bot),
            )
        )
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Build gold.session_features (offline bot corpus)")
    ap.add_argument("--limit-orders", type=int, default=0, help="cap converting sessions (smoke)")
    ap.add_argument("--max-abandon-per-day", type=int, default=0, help="cap abandoned/day (smoke)")
    args = ap.parse_args()

    orders = common.read_sql(
        """
        select order_id, customer_id, order_purchase_timestamp
        from gold.fct_orders
        order by order_purchase_timestamp
        """
    )
    if args.limit_orders:
        orders = orders.head(args.limit_orders)

    rng = np.random.default_rng(42)

    cols = [
        "session_id", "loop_id", "source", "session_date", "is_converting",
        "click_count", "duration_seconds", "click_interval_cv",
        "has_search", "has_product_page", "has_cart_page", "has_checkout_page",
        "page_types_distinct", "cart_added", "cart_value", "hour_of_day",
        "is_weekend", "is_synthetic_bot",
    ]

    print("building converting sessions …")
    conv = converting_sessions(orders, rng)
    with common.conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.session_features (
                session_id text PRIMARY KEY,
                loop_id text NOT NULL,
                source text NOT NULL,
                session_date date NOT NULL,
                is_converting int NOT NULL,
                click_count int NOT NULL,
                duration_seconds double precision NOT NULL,
                click_interval_cv double precision NOT NULL,
                has_search int NOT NULL,
                has_product_page int NOT NULL,
                has_cart_page int NOT NULL,
                has_checkout_page int NOT NULL,
                page_types_distinct int NOT NULL,
                cart_added int NOT NULL,
                cart_value double precision NOT NULL,
                hour_of_day int NOT NULL,
                is_weekend int NOT NULL,
                is_synthetic_bot int NOT NULL
            )
            """
        )
        c.execute("TRUNCATE gold.session_features")

        def copy_rows(rows: list[dict]) -> None:
            with c.cursor() as cur:
                with cur.copy(
                    "COPY gold.session_features (%s) FROM STDIN" % ", ".join(cols)
                ) as cp:
                    for r in rows:
                        cp.write_row(tuple(r[col] for col in cols))

        copy_rows(conv)
        print(f"  written {len(conv):,} converting sessions")

        days = orders["order_purchase_timestamp"].dt.date.unique()
        day_counts = (
            orders.groupby(orders["order_purchase_timestamp"].dt.date).size().to_dict()
        )
        ab = 0
        for d in sorted(days):
            day_start = (
                datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()
            )
            rows = abandoned_sessions_day(d, day_counts[d], rng, day_start)
            if args.max_abandon_per_day:
                rows = rows[: args.max_abandon_per_day]
            copy_rows(rows)
            ab += len(rows)
        print(f"  written {ab:,} abandoned sessions")

    with common.conn() as c:
        cur = c.cursor()
        cur.execute("select count(*), avg(is_synthetic_bot) from gold.session_features")
        n, bot_rate = cur.fetchone()
    print(f"gold.session_features total: {n:,} sessions, bot rate {float(bot_rate):.2%}")
    return 0


if __name__ == "__main__":
    sys.exit(main())