-- Order item counts must never be negative.
select order_id
from {{ ref('fct_orders') }}
where item_count < 0