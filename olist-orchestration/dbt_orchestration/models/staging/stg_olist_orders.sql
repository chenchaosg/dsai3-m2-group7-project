-- models/staging/stg_olist_orders.sql

with source as (

    select * from {{ source('olist_dataset', 'olist_orders_dataset') }}

)

select
    -- ids
    order_id,
    customer_id,

    safe.parse_timestamp('%F %T', order_purchase_timestamp) as order_purchase_at,
    safe.parse_timestamp('%F %T', order_approved_at) as order_approved_at,
    safe.parse_timestamp('%F %T', order_delivered_carrier_date) as order_delivered_to_carrier_at,
    safe.parse_timestamp('%F %T', order_delivered_customer_date) as order_delivered_to_customer_at,
    safe.parse_timestamp('%F %T', order_estimated_delivery_date) as order_estimated_delivery_at,

    -- status
    order_status

from source
