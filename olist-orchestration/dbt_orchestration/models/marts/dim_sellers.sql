-- models/marts/dim_sellers.sql

with fct_orders as (
    select * from {{ ref('fct_orders') }}
),

-- We need to join back to the original orders table to get the approval and carrier delivery timestamps
orders as (
    select * from {{ ref('stg_olist_orders') }}
),

-- Create a CTE that joins the necessary information at the order_item level
seller_order_items as (
    select 
        f.seller_id,
        f.order_id,
        f.price,
        f.review_score,
        o.order_approved_at,
        o.order_delivered_to_carrier_at
    from fct_orders f
    left join orders o on f.order_id = o.order_id
)

-- Final aggregation to the seller level
select
    seller_id,
    
    -- 1. Revenue Metrics
    sum(price) as total_revenue,
    
    -- 2. Order & Item Metrics
    count(distinct order_id) as total_orders,
    count(order_id) as total_items_sold,
    
    -- 3. Customer Satisfaction Metrics
    avg(review_score) as average_review_score,
    
    -- 4. Operational Efficiency Metrics
    avg(
        DATETIME_DIFF(
            cast(order_delivered_to_carrier_at as DATETIME),
            cast(order_approved_at as DATETIME),
            HOUR
        )
    ) as avg_hours_to_ship

from seller_order_items
group by 1
