with source as (

    select * from {{ source('olist_dataset', 'olist_order_reviews_dataset') }}

)

select
    -- ids
    review_id,
    order_id,

    -- review details
    safe_cast(review_score as integer) as review_score,
    review_comment_title,
    review_comment_message,
    
    -- timestamps
    cast(review_creation_date as timestamp) as review_creation_at,
    cast(review_answer_timestamp as timestamp) as review_answer_at

from source
