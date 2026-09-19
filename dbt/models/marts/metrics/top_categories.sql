-- Category-level aggregates and ranks (for the "top categories" dashboard panel).
-- Revenue/counts are order-level: each order's payment_value_total is attributed
-- to each category present in the order's line items.
with category_revenue as (
    select
        coalesce(p.product_category, 'unknown') as product_category,
        count(distinct f.order_id)::int as order_count,
        sum(f.payment_value_total)::numeric(12, 2) as revenue
    from {{ ref('fct_orders') }} f
    left join {{ ref('stg_order_items') }} oi on f.order_id = oi.order_id
    left join {{ ref('dim_products') }} p on oi.product_id = p.product_id
    where f.order_status not in ('canceled', 'unavailable')
      and f.payment_value_total > 0
    group by coalesce(p.product_category, 'unknown')
)
select
    product_category,
    order_count,
    revenue,
    rank() over (order by revenue desc) as revenue_rank,
    rank() over (order by order_count desc) as order_rank
from category_revenue