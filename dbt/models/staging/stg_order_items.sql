select
    order_id,
    order_item_id::int as order_item_id,
    product_id,
    seller_id,
    nullif(shipping_limit_date, '')::timestamptz as shipping_limit_date,
    price::numeric(10, 2) as price,
    freight_value::numeric(10, 2) as freight_value
from {{ source('bronze', 'order_items') }}