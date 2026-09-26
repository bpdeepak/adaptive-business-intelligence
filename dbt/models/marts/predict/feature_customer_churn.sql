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
-- The feature definitions live in dbt/macros/churn_features.sql and are shared
-- with feature_customer_churn_current (the scoring mart), so training and
-- serving compute every feature the same way.
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
with {{ churn_order_ctes() }},
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
{{ churn_feature_ctes() }}
select
    a.customer_unique_id,
    a.as_of_date,
    -- churned = the customer's last order came more than 90 days after as-of
    case when l.last_ts > a.as_of_ts + interval '90 days' then 1 else 0 end::int as churned,
    {{ churn_feature_columns() }}
from asof a
left join prior_agg p using (customer_unique_id, as_of_ts)
left join category_counts cc using (customer_unique_id, as_of_ts)
join labels l using (customer_unique_id)
cross join data_end de
where a.as_of_ts <= de.end_ts - interval '90 days'
order by a.customer_unique_id, a.as_of_date
