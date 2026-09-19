select
    order_id,
    payment_sequential::int as payment_sequential,
    payment_type,
    payment_installments::int as payment_installments,
    payment_value::numeric(10, 2) as payment_value
from {{ source('bronze', 'order_payments') }}