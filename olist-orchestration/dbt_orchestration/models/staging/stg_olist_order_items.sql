with source as (

    select * from {{ source('olist_dataset', 'olist_order_items_dataset') }}

)

select
    -- ids
    order_id,
    order_item_id,
    product_id,
    seller_id,

    -- timestamps
    cast(shipping_limit_date as timestamp) as shipping_limit_at,

    -- numerics
    safe_cast(price as numeric) as price,
    safe_cast(freight_value as numeric) as freight_value

from source
