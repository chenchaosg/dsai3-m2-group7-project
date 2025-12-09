-- models/marts/fct_orders.sql

with orders as (
    select * from {{ ref('stg_olist_orders') }}
),

order_items as (
    select * from {{ ref('stg_olist_order_items') }}
),

customers as (
    select * from {{ ref('stg_olist_customers') }}
),

products as (
    select * from {{ ref('stg_olist_products') }}
),

sellers as (
    select * from {{ ref('stg_olist_sellers') }}
),

reviews as (
    select * from {{ ref('stg_olist_order_reviews') }}
),

category_translation as (
    select * from {{ ref('stg_product_category_name_translation') }}
),

order_payments as (
    select * from {{ ref('stg_olist_order_payments') }}
),

-- Step 1: Pre-aggregate payment data
order_payments_agg as (
    select
        order_id,
        sum(payment_value) as total_payment_value,
        sum(payment_installments) as total_payment_installments
    from order_payments
    group by 1
),

-- Step 2: Isolate date calculations in a separate CTE
orders_with_metrics as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_at,
        order_delivered_to_customer_at,
        order_estimated_delivery_at,
        
        -- Date calculations using DATETIME_DIFF for robustness
        DATETIME_DIFF(
            cast(order_delivered_to_customer_at as DATETIME),
            cast(order_purchase_at as DATETIME),
            DAY
        ) as delivery_days,
        
        case 
            when order_estimated_delivery_at is not null and order_delivered_to_customer_at is not null
            then DATETIME_DIFF(
                    cast(order_estimated_delivery_at as DATETIME),
                    cast(order_delivered_to_customer_at as DATETIME),
                    DAY
                 )
            else null 
        end as delivery_diff_from_estimated_days,
        
        DATETIME_DIFF(
            cast(order_delivered_to_carrier_at as DATETIME),
            cast(order_approved_at as DATETIME),
            HOUR
        ) as seller_processing_hours,

        DATETIME_DIFF(
            cast(order_delivered_to_customer_at as DATETIME),
            cast(order_delivered_to_carrier_at as DATETIME),
            DAY
        ) as carrier_shipping_days
        
    from orders
)

-- Step 3: Final join of all data sources
select
    -- Primary Key
    oi.order_id || '-' || oi.order_item_id as order_item_sk,

    -- Foreign Keys
    oi.order_id,
    oi.product_id,
    oi.seller_id,
    c.customer_unique_id,

    -- Order Timestamps & Status (from the metrics CTE)
    om.order_purchase_at,
    om.order_delivered_to_customer_at,
    om.order_status,
    
    -- Delivery Metrics (from the metrics CTE)
    om.delivery_days,
    om.delivery_diff_from_estimated_days,

    om.seller_processing_hours,
    om.carrier_shipping_days,

    -- Item Metrics, Customer, Product, Seller, Payment, Review Info
    oi.price, oi.freight_value, c.customer_city, c.customer_state,
    p.product_category_name, ct.product_category_name_english,
    s.seller_city, s.seller_state, pay.total_payment_value,
    pay.total_payment_installments, rev.review_score

from order_items as oi
left join orders_with_metrics as om on oi.order_id = om.order_id 
left join customers as c on om.customer_id = c.customer_id
left join products as p on oi.product_id = p.product_id
left join sellers as s on oi.seller_id = s.seller_id
left join order_payments_agg as pay on oi.order_id = pay.order_id
left join reviews as rev on oi.order_id = rev.order_id
left join category_translation as ct on p.product_category_name = ct.product_category_name

where om.order_status = 'delivered'
