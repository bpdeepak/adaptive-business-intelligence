-- Review scores must fall within the documented 1..5 range.
select review_id
from {{ ref('stg_order_reviews') }}
where review_score not between 1 and 5