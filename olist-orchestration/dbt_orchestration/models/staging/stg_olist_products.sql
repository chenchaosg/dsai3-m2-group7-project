-- models/staging/stg_olist_products.sql

with source as (

    select * from {{ source('olist_dataset', 'olist_products_dataset') }}

)

select
    -- id
    product_id,

    -- details
    product_category_name,
    
    safe_cast(product_name_lenght as numeric) as product_name_length,
    safe_cast(product_description_lenght as numeric) as product_description_length,
    safe_cast(product_photos_qty as numeric) as product_photos_qty,
    safe_cast(product_weight_g as numeric) as product_weight_g,
    safe_cast(product_length_cm as numeric) as product_length_cm,
    safe_cast(product_height_cm as numeric) as product_height_cm,
    safe_cast(product_width_cm as numeric) as product_width_cm

from source
