-- models/marts/dim_customers.sql

with orders as (
    select 
        customer_unique_id,
        order_id,
        order_purchase_at,
        price + freight_value as order_item_value -- Calculate value at the order_item level
    from {{ ref('fct_orders') }}
),

-- First, aggregate item values to get the value for each distinct order
order_level_values as (
    select
        customer_unique_id,
        order_id,
        min(order_purchase_at) as order_purchase_at,
        sum(order_item_value) as total_order_value
    from orders
    group by 1, 2
),

-- Now, aggregate order values to the customer level
customer_aggregated as (
    select
        customer_unique_id,
        count(distinct order_id) as number_of_orders,
        sum(total_order_value) as lifetime_value,
        avg(total_order_value) as average_order_value,
        min(order_purchase_at) as first_order_at,
        max(order_purchase_at) as last_order_at,
        
        -- Use DATETIME_DIFF for all date calculations
        DATETIME_DIFF(
            cast(max(order_purchase_at) as DATETIME),
            cast(min(order_purchase_at) as DATETIME),
            DAY
        ) as customer_lifespan_days
        
    from order_level_values
    group by 1
)

select 
    c.*,
    
    -- Add customer segments for easier analysis in BI tools
    case
        when c.number_of_orders = 1 then 'New Customer'
        when c.number_of_orders > 1 and c.number_of_orders <=3 then 'Returning Customer'
        when c.number_of_orders > 3 then 'Loyal Customer'
        else 'Unknown'
    end as customer_segment,

    case
        when c.lifetime_value > 500 then 'High Value'
        when c.lifetime_value > 100 and c.lifetime_value <= 500 then 'Medium Value'
        when c.lifetime_value <= 100 then 'Low Value'
        else 'Unknown'
    end as ltv_segment

from customer_aggregated c
