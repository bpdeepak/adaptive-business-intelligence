-- The scoring population is exactly the right-censored customers: none of them
-- may have a resolved training label as of the same order.
select c.customer_unique_id
from {{ ref('feature_customer_churn_current') }} c
join {{ ref('feature_customer_churn') }} t
  on t.customer_unique_id = c.customer_unique_id and t.as_of_date = c.as_of_date
