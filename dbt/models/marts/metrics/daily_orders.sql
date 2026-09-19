-- Order counts per day, attributed to order_purchase_date.
-- total: all orders; delivered: completed; lost: canceled/unavailable.
select
    order_purchase_date as date,
    count(distinct order_id)::int as total_orders,
    count(distinct case when is_delivered then order_id end)::int as delivered_orders,
    count(distinct case when is_lost then order_id end)::int as lost_orders
from {{ ref('fct_orders') }}
group by order_purchase_date