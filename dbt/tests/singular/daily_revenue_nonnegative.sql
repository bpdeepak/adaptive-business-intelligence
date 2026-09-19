-- Daily revenue must never be negative.
select date
from {{ ref('daily_revenue') }}
where revenue < 0