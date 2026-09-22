-- Feature store for the customer-churn model (Phase 2, predictive layer).
--
-- Churn definition (aligned with the Phase 2 spec): a customer churned when
-- they made no purchase within 90 days following their PENULTIMATE order, and
-- the label is read from the observed LAST order. The final order is the
-- observed outcome for every row, so the label is always fully resolved (no
-- right-censoring) — a customer's later absence does not re-label them.
--
-- Train row design: one row per REPEAT customer (>= 2 orders), as-of = the
-- penultimate order; all features are computed strictly from orders BEFORE
-- as-of (information available at decision time): order history RFM, review
-- behaviour, delivery experience, and loss rate.
--
-- Two honesty rules baked into the model contract (findings documented in
-- docs/phase2.md):
--   1. Label availability: rows whose as-of falls inside the final 90 days of
--      the dataset are EXCLUDED — their 90-day window is not yet observable,
--      and including them would silently assume they never came back.
--   2. No as_of_year: the calendar year is a proxy for the marketplace's
--      falling base churn rate (67% → 41% → 14%); a model that leans on it
--      cannot extrapolate under a time split. as_of_month is kept as genuine
--      seasonality.
with customer_orders as (
    select
        o.customer_unique_id,
        o.order_id,
        o.order_purchase_date,
        o.order_purchase_timestamp,
        coalesce(o.payment_value_total, 0)::numeric(12, 2) as payment_value_total,
        coalesce(o.item_count, 0)::int as item_count,
        o.review_score_avg,
        coalesce(o.review_count, 0)::int as review_count,
        coalesce(o.is_lost, false) as is_lost,
        case
            when o.order_delivered_customer_date is not null
             and o.order_estimated_delivery_date is not null
            then extract(day from (o.order_delivered_customer_date - o.order_estimated_delivery_date))
            else 0 end as delivery_delay_days
    from {{ ref('fct_orders') }} o
    where o.customer_unique_id is not null
),
order_cats as (
    -- distinct product categories per order (order-level, dedupes line items)
    select
        oi.order_id,
        array_agg(distinct coalesce(dp.product_category, 'unknown')) as cats
    from {{ ref('fct_order_items') }} oi
    left join {{ ref('dim_products') }} dp on dp.product_id = oi.product_id
    group by oi.order_id
),
orders as (
    select
        co.customer_unique_id,
        co.order_id,
        co.order_purchase_date,
        co.order_purchase_timestamp,
        co.payment_value_total,
        co.item_count,
        co.review_score_avg,
        co.review_count,
        co.is_lost,
        co.delivery_delay_days,
        oc.cats
    from customer_orders co
    left join order_cats oc using (order_id)
),
ranked as (
    select o.*,
           row_number() over (
               partition by customer_unique_id
               order by order_purchase_timestamp, order_id
           ) as ord_n,
           count(*) over (partition by customer_unique_id) as n_orders
    from orders o
),
asof as (
    select customer_unique_id, order_id as asof_order_id,
           order_purchase_date as as_of_date,
           order_purchase_timestamp as as_of_ts
    from ranked
    where n_orders >= 2 and ord_n = n_orders - 1
),
labels as (
    select customer_unique_id, max(order_purchase_timestamp) as last_ts
    from ranked
    group by customer_unique_id
),
data_end as (
    select max(order_purchase_timestamp) as end_ts from customer_orders
),
category_counts as (
    -- distinct categories across PRIOR orders, per (customer, as-of)
    select a.customer_unique_id, a.as_of_ts,
           count(distinct c.category) filter (where c.category <> 'unknown') as category_count
    from asof a
    join ranked r using (customer_unique_id)
    left join lateral unnest(r.cats) as c(category) on r.order_purchase_timestamp < a.as_of_ts
    where r.order_purchase_timestamp < a.as_of_ts
    group by a.customer_unique_id, a.as_of_ts
),
prior_agg as (
    select
        a.customer_unique_id,
        a.as_of_ts,
        count(*) as n_prior_orders,
        sum(r.payment_value_total) as total_spend_prior,
        max(r.payment_value_total) as max_order_value_prior,
        avg(r.item_count) as avg_items_prior,
        avg(r.review_score_avg) as avg_review_prior,
        sum(r.review_count) as review_count_prior,
        avg(r.delivery_delay_days) as avg_delivery_delay_days_prior,
        avg(case when r.is_lost then 1 else 0 end) as lost_share_prior,
        count(distinct r.order_purchase_date) as distinct_days_prior,
        count(*) filter (
            where r.order_purchase_timestamp >= a.as_of_ts - interval '90 days'
        ) as orders_last_90d_prior,
        sum(r.payment_value_total) filter (
            where r.order_purchase_timestamp >= a.as_of_ts - interval '90 days'
        ) as spend_last_90d_prior,
        min(r.order_purchase_timestamp) as first_order_ts,
        max(r.order_purchase_timestamp) as prior_last_ts
    from asof a
    join ranked r using (customer_unique_id)
    where r.order_purchase_timestamp < a.as_of_ts
    group by a.customer_unique_id, a.as_of_ts
)
select
    a.customer_unique_id,
    a.as_of_date,
    -- churned = the customer's last order came more than 90 days after as-of
    case when l.last_ts > a.as_of_ts + interval '90 days' then 1 else 0 end::int as churned,
    coalesce(p.n_prior_orders, 0)::int as n_prior_orders,
    coalesce(p.total_spend_prior, 0)::numeric(12, 2) as total_spend_prior,
    coalesce(
        case when p.n_prior_orders > 0
             then (p.total_spend_prior / p.n_prior_orders)::numeric(12, 2)
             else 0 end,
        0
    ) as aov_prior,
    coalesce(p.max_order_value_prior, 0)::numeric(12, 2) as max_order_value_prior,
    coalesce(p.avg_items_prior, 0)::double precision as avg_items_prior,
    coalesce(p.avg_review_prior, 0)::double precision as avg_review_prior,
    coalesce(p.review_count_prior, 0)::int as review_count_prior,
    coalesce(p.avg_delivery_delay_days_prior, 0)::double precision as avg_delivery_delay_days_prior,
    coalesce(p.lost_share_prior, 0)::double precision as lost_share_prior,
    coalesce(p.distinct_days_prior, 0)::int as distinct_days_prior,
    coalesce(p.orders_last_90d_prior, 0)::int as orders_last_90d_prior,
    coalesce(p.spend_last_90d_prior, 0)::numeric(12, 2) as spend_last_90d_prior,
    coalesce(cc.category_count, 0)::int as categories_prior,
    coalesce(greatest((a.as_of_ts::date - p.first_order_ts::date), 0), 0)::int as days_since_first_order,
    coalesce(greatest((a.as_of_ts::date - p.prior_last_ts::date), 0), 0)::int as days_since_prior_order,
    extract(month from a.as_of_ts)::int as as_of_month
from asof a
left join prior_agg p using (customer_unique_id, as_of_ts)
left join category_counts cc using (customer_unique_id, as_of_ts)
join labels l using (customer_unique_id)
cross join data_end de
where a.as_of_ts <= de.end_ts - interval '90 days'
order by a.customer_unique_id, a.as_of_date