with source as (

    select * from {{ source('olist_dataset', 'olist_order_payments_dataset') }}

)

select
    -- ids
    order_id,

    -- payment details
    payment_sequential,
    payment_type,
    safe_cast(payment_installments as integer) as payment_installments,
    safe_cast(payment_value as numeric) as payment_value

from source
