select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix::int as customer_zip_code_prefix,
    nullif(customer_city, '') as customer_city,
    nullif(customer_state, '') as customer_state,
    -- The source contains 3 "unidentified" customers with empty city/state.
    case
        when nullif(customer_city, '') is null and nullif(customer_state, '') is null
            then false
        else true
    end as is_identified
from {{ source('bronze', 'customers') }}