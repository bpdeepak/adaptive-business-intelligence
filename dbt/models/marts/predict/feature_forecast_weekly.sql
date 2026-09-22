-- Feature store for the demand-forecast model (Phase 2, predictive layer).
--
-- Grain: one row per (product_category, week_start) over a DENSE weekly spine.
-- Because one order can span several line-item categories, each order's captured
-- payments are allocated across its line items by price share (the same
-- convention the fraud features use). This makes `revenue` per category
-- MUTUALLY EXCLUSIVE and additive — deliberately different from
-- gold.top_categories, which attributes the full order to every category.
--
-- The spine is zero-filled for weeks with no sales so lag/rolling features are
-- true time-lags (N weeks back), never "N observed rows back". Training may
-- re-filter by series support.
with orders as (
    select
        order_id,
        order_purchase_date,
        payment_value_total
    from {{ ref('fct_orders') }}
    where true
        and not is_lost
        and payment_value_total > 0
),

item_lines as (
    select
        foi.order_id,
        dp.product_category,
        foi.price
    from {{ ref('fct_order_items') }} foi
    left join {{ ref('dim_products') }} dp
        on dp.product_id = foi.product_id
),

allocated as (
    select
        o.order_id,
        o.order_purchase_date,
        date_trunc('week', o.order_purchase_date)::date as week_start,
        coalesce(il.product_category, 'unknown') as category,
        o.payment_value_total * il.price
            / nullif(sum(il.price) over (partition by o.order_id), 0) as revenue_share
    from orders o
    join item_lines il on il.order_id = o.order_id
),

weekly as (
    select
        week_start,
        category,
        sum(revenue_share)::numeric(14, 2) as revenue,
        count(distinct order_id)::int as orders,
        avg(revenue_share)::numeric(10, 2) as avg_order_value
    from allocated
    group by week_start, category
),

spine as (
    select
        c.category,
        d.date as week_start
    from (select distinct category from weekly) c
    cross join (
        select generate_series(
            date_trunc('week', (select min(order_purchase_date) from orders)),
            date_trunc('week', (select max(order_purchase_date) from orders)),
            interval '1 week'
        )::date as date
    ) d
),

dense as (
    select
        s.category,
        s.week_start,
        coalesce(w.revenue, 0)::numeric(14, 2) as revenue,
        coalesce(w.orders, 0)::int as orders,
        coalesce(w.avg_order_value, 0)::numeric(10, 2) as avg_order_value
    from spine s
    left join weekly w
        on w.category = s.category and w.week_start = s.week_start
),

featured as (
    select
        d.category,
        d.week_start,
        d.revenue,
        d.orders,
        d.avg_order_value,
        extract(year from d.week_start)::int as year,
        extract(week from d.week_start)::int as week_of_year,
        lag(d.revenue, 1) over category_win as revenue_lag1,
        lag(d.revenue, 2) over category_win as revenue_lag2,
        lag(d.revenue, 4) over category_win as revenue_lag4,
        lag(d.revenue, 8) over category_win as revenue_lag8,
        lag(d.orders, 1) over category_win as orders_lag1,
        lag(d.orders, 2) over category_win as orders_lag2,
        lag(d.orders, 4) over category_win as orders_lag4,
        lag(d.orders, 8) over category_win as orders_lag8,
        avg(d.revenue) over
            (partition by d.category order by d.week_start
             rows between 4 preceding and 1 preceding) as revenue_roll4_mean,
        stddev(d.revenue) over
            (partition by d.category order by d.week_start
             rows between 4 preceding and 1 preceding) as revenue_roll4_std,
        avg(d.orders) over
            (partition by d.category order by d.week_start
             rows between 4 preceding and 1 preceding) as orders_roll4_mean,
        count(*) over
            (partition by d.category order by d.week_start
             rows between 8 preceding and current row) as series_weeks
    from dense d
    window category_win as (partition by d.category order by d.week_start)
)

select
    category,
    week_start,
    revenue,
    orders,
    avg_order_value,
    year,
    week_of_year,
    revenue_lag1,
    revenue_lag2,
    revenue_lag4,
    revenue_lag8,
    orders_lag1,
    orders_lag2,
    orders_lag4,
    orders_lag8,
    revenue_roll4_mean,
    revenue_roll4_std,
    orders_roll4_mean,
    series_weeks,
    case when revenue_lag1 is null then false else true end as has_prior_week
from featured
order by category, week_start