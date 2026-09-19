with orders as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_purchase_date,
        coalesce(payment_value_total, 0) as payment_value_total,
        is_lost
    from {{ ref('fct_orders') }}
),

item_lines as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        (price + freight_value)::numeric(12, 2) as item_total
    from {{ ref('stg_order_items') }}
)

select
    il.order_id,
    il.order_item_id,
    il.product_id,
    il.seller_id,
    il.shipping_limit_date,
    il.price,
    il.freight_value,
    il.item_total,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_purchase_date,
    o.payment_value_total,
    o.is_lost
from item_lines il
left join orders o on il.order_id = o.order_id