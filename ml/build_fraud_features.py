"""Build gold.feature_fraud_orders: order-level features + injected fraud labels.

Honesty rules:
- every feature is as-of the order timestamp (trailing windows only, never
  future rows), so a time-split backtest on this table has no leakage;
- labels are injected AFTER features are computed, from four deterministic
  patterns that plausibly correspond to synthetic fraud:
    A) velocity: >=3 orders by the same customer within trailing 24h
    B) price outlier: order value >= 8x the category's trailing 90-day average
    C) excessive installments: >=12 installments on a high-value order
    D) random noise floor (0.4%) so the task isn't trivially decidable.

Replaces the need for a real fraud dataset: the Olist source is clean, so this
is the accepted Phase 2 stand-in, documented as synthetic.
"""
from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd

import common

LOAD_SQL = """
with orders as (
    select
        o.order_id,
        o.customer_unique_id,
        o.order_purchase_timestamp,
        o.order_purchase_date,
        o.payment_value_total,
        o.item_count,
        case when o.items_price_total + o.freight_total > 0
             then o.freight_total::double precision
                  / (o.items_price_total::double precision + o.freight_total::double precision)
             else 0 end as freight_share,
        extract(hour from o.order_purchase_timestamp)::int as order_hour,
        case when extract(isodow from o.order_purchase_date) in (6, 7) then 1 else 0 end as is_weekend,
        case when o.is_lost then 1 else 0 end as is_lost
    from gold.fct_orders o
),
item_cats as (
    select
        cat.order_id,
        coalesce(dp.product_category, 'unknown') as product_category,
        cat.price,
        row_number() over (partition by cat.order_id order by cat.price desc) as rn
    from gold.fct_order_items cat
    left join gold.dim_products dp on dp.product_id = cat.product_id
),
cat_agg as (
    select
        order_id,
        count(distinct product_category)::int as categories_count,
        max(case when rn = 1 then product_category end) as category_primary
    from item_cats
    group by order_id
),
pay_agg as (
    select
        order_id,
        count(*)::int as payment_count,
        max(payment_installments)::int as installments_max
    from silver.stg_order_payments
    group by order_id
),
pay_primary as (
    select distinct on (order_id)
        order_id,
        payment_type as payment_type_primary
    from silver.stg_order_payments
    order by order_id, payment_value desc
)
select
    o.order_id,
    o.customer_unique_id,
    o.order_purchase_timestamp,
    o.payment_value_total as order_value,
    o.item_count,
    o.freight_share,
    o.order_hour,
    o.is_weekend,
    o.is_lost,
    ca.categories_count,
    ca.category_primary,
    pa.payment_count,
    pa.installments_max,
    pp.payment_type_primary
from orders o
left join cat_agg ca on ca.order_id = o.order_id
left join pay_agg pa on pa.order_id = o.order_id
left join pay_primary pp on pp.order_id = o.order_id

"""


def trailing_benchmark(orders: pd.DataFrame, window_days: int = 90) -> np.ndarray:
    """Per-order category benchmark: mean order value of the same category's
    orders in the trailing `window_days` (strictly before this order). When
    there is no trailing history the order's own value is used (ratio 1.0), so
    no future information leaks into the feature."""
    n = len(orders)
    out = np.zeros(n, dtype=float)
    for _, g in orders.groupby("category_primary", sort=False):
        g = g.sort_values("_ts")
        idx = g.index.to_numpy()
        gts = g["_ts"].to_numpy()
        gvals = g["order_value"].to_numpy()
        cum = np.concatenate([[0], np.cumsum(gvals)])
        start = np.searchsorted(gts, gts - window_days * 86400, side="left")
        end = np.arange(len(g))  # rows strictly before self
        counts = end - start
        sums = cum[end] - cum[start]
        out[idx] = np.where(counts > 0, sums / np.maximum(counts, 1), gvals)
    return out


def trailing_velocity(orders: pd.DataFrame, window_hours: int = 24) -> np.ndarray:
    """Number of the customer's orders in the trailing `window_hours` (strictly
    before this order, excluding itself)."""
    n = len(orders)
    out = np.zeros(n, dtype=float)
    for _, g in orders.groupby("customer_unique_id", sort=False):
        g = g.sort_values("_ts")
        idx = g.index.to_numpy()
        gts = g["_ts"].to_numpy()
        start = np.searchsorted(gts, gts - window_hours * 3600, side="left")
        end = np.arange(len(g))
        out[idx] = np.maximum(end - start, 0)
    return out


def inject_fraud(orders: pd.DataFrame, seed: int = 42) -> np.ndarray:
    rng = np.random.default_rng(seed)
    vel = orders["velocity_24h"].to_numpy()
    bench = orders["price_vs_benchmark"].to_numpy()
    inst = orders["installments_max"].to_numpy()
    val = orders["order_value"].to_numpy()
    noise = rng.random(len(orders)) < 0.004
    fraud = (
        (vel >= 3)
        | (bench >= 8)
        | ((inst >= 12) & (val > 2500))
        | noise
    ).astype(int)
    return fraud


def main() -> int:
    ap = argparse.ArgumentParser(description="Build gold.feature_fraud_orders")
    ap.add_argument("--limit", type=int, default=0, help="cap rows (smoke)")
    args = ap.parse_args()

    df = common.read_sql(LOAD_SQL)
    df = df.dropna(subset=["order_id", "customer_unique_id", "order_value"])
    if args.limit:
        df = df.head(args.limit)
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"], utc=True)
    df["_ts"] = df["order_purchase_timestamp"].astype("int64") // 10**9
    df["order_value"] = df["order_value"].astype(float)
    df["installments_max"] = df["installments_max"].fillna(1).astype(int)
    df["payment_count"] = df["payment_count"].fillna(0).astype(int)
    df["item_count"] = df["item_count"].fillna(0).astype(int)
    df["categories_count"] = df["categories_count"].fillna(1).astype(int)
    df["category_primary"] = df["category_primary"].fillna("unknown")
    df["payment_type_primary"] = df["payment_type_primary"].fillna("unknown")

    benchmark = trailing_benchmark(df)
    df["price_vs_benchmark"] = np.where(
        benchmark > 0, df["order_value"].to_numpy() / np.maximum(benchmark, 1e-9), 1.0
    )
    df["velocity_24h"] = trailing_velocity(df, window_hours=24).astype(int)
    df["account_age_days"] = (
        (df["_ts"] - df.groupby("customer_unique_id")["_ts"].transform("min")) // 86400
    ).astype(int)
    for col in ("item_count", "payment_count", "installments_max", "categories_count",
                "order_hour", "is_weekend", "is_lost"):
        df[col] = df[col].astype(int)
    df["is_fraud"] = inject_fraud(df)

    out = df[
        [
            "order_id",
            "customer_unique_id",
            "category_primary",
            "categories_count",
            "order_purchase_timestamp",
            "order_value",
            "item_count",
            "freight_share",
            "payment_type_primary",
            "payment_count",
            "installments_max",
            "price_vs_benchmark",
            "velocity_24h",
            "account_age_days",
            "order_hour",
            "is_weekend",
            "is_lost",
            "is_fraud",
        ]
    ].copy()

    common.ensure_serving_tables()
    cols = [
        "order_id", "customer_unique_id", "category_primary", "categories_count",
        "order_purchase_timestamp", "order_value", "item_count", "freight_share",
        "payment_type_primary", "payment_count", "installments_max",
        "price_vs_benchmark", "velocity_24h", "account_age_days", "order_hour",
        "is_weekend", "is_lost", "is_fraud",
    ]
    with common.conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.feature_fraud_orders (
                order_id text PRIMARY KEY,
                customer_unique_id text NOT NULL,
                category_primary text NOT NULL,
                categories_count int NOT NULL,
                order_purchase_timestamp timestamptz NOT NULL,
                order_value double precision NOT NULL,
                item_count int NOT NULL,
                freight_share double precision NOT NULL,
                payment_type_primary text NOT NULL,
                payment_count int NOT NULL,
                installments_max int NOT NULL,
                price_vs_benchmark double precision NOT NULL,
                velocity_24h int NOT NULL,
                account_age_days bigint NOT NULL,
                order_hour int NOT NULL,
                is_weekend int NOT NULL,
                is_lost int NOT NULL,
                is_fraud int NOT NULL
            )
            """
        )
        c.execute("TRUNCATE gold.feature_fraud_orders")
        with c.cursor() as cur:
            with cur.copy(
                "COPY gold.feature_fraud_orders (%s) FROM STDIN" % ", ".join(cols)
            ) as cp:
                for t in out[cols].itertuples(index=False, name=None):
                    cp.write_row(t)

    rate = float(out["is_fraud"].mean())
    print(f"gold.feature_fraud_orders: {len(out):,} orders, fraud rate {rate:.3%}")
    print("  pattern mix (orders matched):")
    vel = (out.velocity_24h >= 3).sum()
    bench = (out.price_vs_benchmark >= 8).sum()
    inst = ((out.installments_max >= 12) & (out.order_value > 2500)).sum()
    print(f"    A velocity(>=3/24h)      {vel:,}")
    print(f"    B price >= 8x benchmark  {bench:,}")
    print(f"    C installs>=12 & >2500   {inst:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())