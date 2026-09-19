with dedup as (
    select distinct
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    from {{ source('bronze', 'order_reviews') }}
)
select
    review_id,
    order_id,
    review_score::int as review_score,
    review_comment_title,
    review_comment_message,
    nullif(review_creation_date, '')::timestamptz as review_creation_date,
    nullif(review_answer_timestamp, '')::timestamptz as review_answer_timestamp
from dedup