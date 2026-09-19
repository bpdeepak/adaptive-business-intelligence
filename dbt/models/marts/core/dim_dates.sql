with spine as (
    select generate_series(
        date_trunc('day', min(order_purchase_timestamp))::date,
        date_trunc('day', max(order_purchase_timestamp))::date,
        interval '1 day'
    )::date as date
    from {{ ref('stg_orders') }}
)
select
    date,
    extract(year from date)::int as year,
    extract(month from date)::int as month,
    extract(day from date)::int as day,
    extract(quarter from date)::int as quarter,
    to_char(date, 'Day') as day_name,
    extract(isodow from date)::int as day_of_week,
    case when extract(isodow from date) in (6, 7) then true else false end as is_weekend
from spine