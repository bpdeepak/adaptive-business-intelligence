{#
  Demand-forecast features: the SINGLE source for both the training mart
  (feature_forecast_weekly) and the scoring mart (feature_forecast_next_week).

  Grain: one row per (product_category, week_start) over a DENSE weekly spine.
  Each order's captured payments are allocated across its line items by price
  share, so per-category revenue is mutually exclusive and additive (unlike
  gold.top_categories, which credits the full order to every category).

  Two data-integrity rules (Phase 5 audit of the Phase 2 model inputs):

  1. No same-week information. Every feature is a lag or a trailing window of
     PRIOR weeks. The Phase 2 mart exposed the target week's own
     avg_order_value (revenue / orders of that very week) as a feature: target
     leakage, unknowable for a future week, and exactly 0 whenever revenue was 0.
     It is replaced by aov_lag1 (last week's AOV). avg_order_value stays in the
     training mart as a descriptive column only.

  2. The spine ends at the last COMPLETE week. Olist's extraction tapers off at
     the end (weekly orders ~1,900 -> 1,072 -> 117 -> 1): those weeks are a
     collection cutoff, not a demand collapse, and training on them teaches the
     model a fake crash. A week is complete when its order count is at least
     75% of the median of the 8 weeks before it; the as-of week is the last
     complete week, and weeks after it are dropped.

  extra_weeks = 0 builds the training rows; extra_weeks = 1 appends the week
  after the as-of week (no outcome yet), whose features are pure lags of real
  weeks, i.e. the next-week forecast row.
#}
{% macro forecast_feature_rows(extra_weeks=0) %}
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

week_totals as (
    select
        date_trunc('week', order_purchase_date)::date as week_start,
        count(*) as orders
    from orders
    group by 1
),

scored_weeks as (
    select
        w.week_start,
        w.orders,
        (
            select percentile_cont(0.5) within group (order by p.orders)
            from week_totals p
            where p.week_start < w.week_start
              and p.week_start >= w.week_start - interval '8 weeks'
        ) as prior8_median
    from week_totals w
),

as_of as (
    -- the last complete week: >= 75% of the median of the 8 weeks before it
    select max(week_start) as as_of_week
    from scored_weeks
    where prior8_median is null or orders >= 0.75 * prior8_median
),

spine as (
    select
        c.category,
        d.date as week_start
    from (select distinct category from weekly) c
    cross join (
        select generate_series(
            date_trunc('week', (select min(order_purchase_date) from orders)),
            (select as_of_week from as_of) + interval '{{ extra_weeks }} weeks',
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
       and s.week_start <= (select as_of_week from as_of)
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
        lag(d.avg_order_value, 1) over category_win as aov_lag1,
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
             rows between 8 preceding and current row) as series_weeks,
        (select as_of_week from as_of) as as_of_week
    from dense d
    window category_win as (partition by d.category order by d.week_start)
)
{% endmacro %}
