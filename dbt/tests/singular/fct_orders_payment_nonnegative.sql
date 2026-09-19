-- Fact payments must never be negative.
select order_id
from {{ ref('fct_orders') }}
where payment_value_total < 0