with source as (

    select * from {{ source('olist_dataset', 'olist_sellers_dataset') }}

)

select
    -- id
    seller_id,

    -- location
    seller_zip_code_prefix,
    seller_city,
    seller_state

from source
