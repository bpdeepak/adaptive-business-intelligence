select
    seller_id,
    seller_zip_code_prefix::int as seller_zip_code_prefix,
    nullif(seller_city, '') as seller_city,
    nullif(seller_state, '') as seller_state
from {{ source('bronze', 'sellers') }}