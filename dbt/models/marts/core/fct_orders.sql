with order_items_agg as (
    select
        order_id,
        count(*) as item_count,
        sum(price)::numeric(12, 2) as items_price_total,
        sum(freight_value)::numeric(12, 2) as freight_total,
        sum(price + freight_value)::numeric(12, 2) as items_gross_total
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments_agg as (
    select
        order_id,
        count(*) as payment_count,
        sum(payment_value)::numeric(12, 2) as payment_value_total
    from {{ ref('stg_order_payments') }}
    group by order_id
),

reviews_agg as (
    select
        order_id,
        avg(review_score)::numeric(3, 2) as review_score_avg,
        count(*) as review_count
    from {{ ref('stg_order_reviews') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    cu.customer_unique_id,
    o.order_status,
    o.order_purchase_date,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    coalesce(oi.item_count, 0) as item_count,
    coalesce(oi.items_price_total, 0) as items_price_total,
    coalesce(oi.freight_total, 0) as freight_total,
    coalesce(oi.items_gross_total, 0) as items_gross_total,
    coalesce(pa.payment_count, 0) as payment_count,
    coalesce(pa.payment_value_total, 0) as payment_value_total,
    rv.review_score_avg,
    coalesce(rv.review_count, 0) as review_count,
    -- Business status flags
    case when o.order_status = 'delivered' then true else false end as is_delivered,
    case when o.order_status in ('canceled', 'unavailable') then true else false end as is_lost
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} cu on o.customer_id = cu.customer_id
left join order_items_agg oi on o.order_id = oi.order_id
left join payments_agg pa on o.order_id = pa.order_id
left join reviews_agg rv on o.order_id = rv.order_id