-- models/marts/fct_payments.sql
select
    p.order_id,
    p.payment_type,
    p.payment_installments,
    p.payment_value,
    o.order_purchase_at,
    c.customer_state
from {{ ref('stg_olist_order_payments') }} p
left join {{ ref('stg_olist_orders') }} o on p.order_id = o.order_id
left join {{ ref('stg_olist_customers') }} c on o.customer_id = c.customer_id
