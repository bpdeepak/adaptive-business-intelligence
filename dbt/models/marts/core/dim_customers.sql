select
    customer_unique_id,
    count(customer_id)::int as order_accounts,
    min(customer_id) as first_customer_id,
    min(customer_city) as customer_city,
    min(customer_state) as customer_state,
    bool_or(is_identified) as is_identified
from {{ ref('stg_customers') }}
group by customer_unique_id