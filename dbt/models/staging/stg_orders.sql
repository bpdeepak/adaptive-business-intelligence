select
    order_id,
    customer_id,
    order_status,
    nullif(order_purchase_timestamp, '')::timestamptz as order_purchase_timestamp,
    nullif(order_approved_at, '')::timestamptz as order_approved_at,
    nullif(order_delivered_carrier_date, '')::timestamptz as order_delivered_carrier_date,
    nullif(order_delivered_customer_date, '')::timestamptz as order_delivered_customer_date,
    nullif(order_estimated_delivery_date, '')::timestamptz as order_estimated_delivery_date,
    nullif(order_purchase_timestamp, '')::timestamptz::date as order_purchase_date
from {{ source('bronze', 'orders') }}