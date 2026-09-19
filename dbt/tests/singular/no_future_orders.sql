-- No orders should be recorded in the future.
select order_id
from {{ ref('stg_orders') }}
where order_purchase_timestamp > current_timestamp