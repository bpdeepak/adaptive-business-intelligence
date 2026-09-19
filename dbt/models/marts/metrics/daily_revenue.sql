-- Revenue per day, attributed to order_purchase_date.
-- Definition: SUM(payment_value) of non-lost orders (i.e. status NOT IN
-- ('canceled', 'unavailable')) with at least one positive payment.
select
    order_purchase_date as date,
    count(distinct order_id)::int as order_count,
    sum(payment_value_total)::numeric(12, 2) as revenue
from {{ ref('fct_orders') }}
where order_status not in ('canceled', 'unavailable')
  and payment_value_total > 0
group by order_purchase_date