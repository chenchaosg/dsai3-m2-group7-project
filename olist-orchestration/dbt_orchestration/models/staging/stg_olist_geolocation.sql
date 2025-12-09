with source as (

    select * from {{ source('olist_dataset', 'olist_geolocation_dataset') }}

)

select
    geolocation_zip_code_prefix,
    safe_cast(geolocation_lat as numeric) as geolocation_lat,
    safe_cast(geolocation_lng as numeric) as geolocation_lng,
    geolocation_city,
    geolocation_state

from source
