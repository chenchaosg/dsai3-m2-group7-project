# Metabase Olist Exploratory Data Visualisation SQL Queries

### CUSTOMERS TAB

#### 1. Total Number of Customers
```sql
SELECT
  COUNT(*)
FROM
  my-project-dsai-3.olist_data.dim_customers;
```

### 2. Total Number of Orders (from 2017 onwards)
```sql
SELECT
  COUNT(*)
FROM
  my-project-dsai-3.olist_data.fct_orders
WHERE
  order_purchase_at >= timestamp "2017-01-01 00:00:00Z";
```

### 3. Number of Orders per Day (from 2017 onwards)
```sql
SELECT
  TIMESTAMP_TRUNC(order_purchase_at, day) AS order_purchase_at,
  COUNT(*) AS count
FROM
  my-project-dsai-3.olist_data.fct_orders
WHERE
  order_purchase_at >= timestamp "2017-01-01 00:00:00Z"
GROUP BY
  order_purchase_at
ORDER BY
  order_purchase_at ASC;
```

### 4. Revenue in each State
```sql
SELECT
  customer_state,
  SUM(total_payment_value) AS sum
FROM
  my-project-dsai-3.olist_data.fct_orders
WHERE
  customer_state IN (
    'AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'
  )
GROUP BY
  customer_state
ORDER BY
  customer_state ASC;
```

### 5. Revenue Distribution in each State
```sql
SELECT
  customer_state,
  SUM(total_payment_value) AS sum
FROM
  my-project-dsai-3.olist_data.fct_orders
WHERE
  order_purchase_at >= timestamp "2017-01-01 00:00:00Z"
GROUP BY
  customer_state
ORDER BY
  customer_state ASC;
```

### 6. Numbers of Customers in each State
```sql
SELECT
  customer_state,
  COUNT(*)
FROM
  my-project-dsai-3.olist_data.fct_orders
WHERE
  (customer_state = 'AC')
  OR (customer_state = 'AL')
  OR (customer_state = 'AM')
  OR (customer_state = 'AP')
  OR (customer_state = 'BA')
  OR (customer_state = 'CE')
  OR (customer_state = 'DF')
  OR (customer_state = 'ES')
  OR (customer_state = 'GO')
  OR (customer_state = 'MA')
  OR (customer_state = 'MG')
  OR (customer_state = 'MS')
  OR (customer_state = 'MT')
  OR (customer_state = 'PA')
  OR (customer_state = 'PB')
  OR (customer_state = 'PE')
  OR (customer_state = 'PI')
  OR (customer_state = 'PR')
  OR (customer_state = 'RJ')
  OR (customer_state = 'RN')
  OR (customer_state = 'RO')
  OR (customer_state = 'RR')
  OR (customer_state = 'RS')
  OR (customer_state = 'SC')
  OR (customer_state = 'SE')
  OR (customer_state = 'SP')
  OR (customer_state = 'TO')
GROUP BY
  customer_state
ORDER BY
  customer_state ASC;
```

### 7. Number of Orders by Hour of the Day
```sql
SELECT
  EXTRACT(
    hour
   
FROM
      first_order_at
  ) AS first_order_at,
COUNT(*)
FROM
  my-project-dsai-3.olist_data.dim_customers
GROUP BY
  first_order_at
ORDER BY
  first_order_at ASC;
```

### 8. Number of Orders by Day of the Week
```sql
SELECT
  EXTRACT(
    dayofweek
   
FROM
      first_order_at
  ) AS first_order_at,
  COUNT(*)
FROM
  my-project-dsai-3.olist_data.dim_customers
GROUP BY
  first_order_at
ORDER BY
  first_order_at ASC;
```

### 9. ARPU - Average Revenue Per User
```sql
SELECT
  `source`.`purchase_month` AS `purchase_month`,
  `source`.`arpu` AS `arpu`
FROM
  (
    SELECT
      purchase_month,
      SAFE_DIVIDE(gmv, mau) AS arpu
    FROM
      (
        SELECT
          DATE_TRUNC(DATE(order_purchase_at), MONTH) AS purchase_month,
          SUM(price) AS gmv,
          COUNT(DISTINCT customer_unique_id) AS mau
        FROM
          my-project-dsai-3.olist_data.fct_orders
       
GROUP BY
          1
      )
   
ORDER BY
      1
  ) AS `source`
WHERE
  `source`.`purchase_month` > date "2016-12-31"
LIMIT
  1048575;
```

### 10. Customer Segment
```sql
SELECT
  customer_segment,
  COUNT(*) AS count
FROM
  my-project-dsai-3.olist_data.dim_customers
WHERE
  (
    customer_segment = 'Loyal Customer'
  )
 
    OR (
    customer_segment = 'New Customer'
  )
  OR (
    customer_segment = 'Returning Customer'
  )
GROUP BY
  customer_segment
ORDER BY
  customer_segment ASC;
```

### 11. Customers per LTV Segment
```sql
SELECT
  ltv_segment,
  COUNT(*)
FROM
  my-project-dsai-3.olist_data.dim_customers
GROUP BY
  ltv_segment
ORDER BY
  ltv_segment ASC;
```

### 12. Payment Type
```sql
SELECT
    payment_type,
    
    AVG(payment_value) AS average_payment_value,
    
    COUNT(DISTINCT order_id) AS number_of_orders
FROM
    my-project-dsai-3.olist_data.fct_payments
GROUP BY
    payment_type
ORDER BY
    number_of_orders DESC;
```

### 13. Review as Indicator of Repeat Purchase
```sql
-- Step 1: For each unique customer, find the review score from their very first order.
WITH first_customer_review AS (
    SELECT
        customer_unique_id,
        review_score,
        -- We use a window function to rank a customer's orders by purchase time.
        ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY order_purchase_at ASC) as order_rank
    FROM
        my-project-dsai-3.olist_data.fct_orders
    WHERE
        review_score IS NOT NULL
),

-- Step 2: For each unique customer, find their total number of orders.
customer_order_counts AS (
    SELECT
        customer_unique_id,
        COUNT(DISTINCT order_id) as number_of_orders
    FROM
        my-project-dsai-3.olist_data.fct_orders
    GROUP BY 1
)

-- Final Step: Join the first review score with the total order count,
-- then group by the first review score to calculate the repurchase rate for each group.
SELECT
    first_review.review_score AS first_experience_score,
    
    -- Count the total customers in each group (our denominator)
    COUNT(first_review.customer_unique_id) AS total_customers,
    
    -- Count how many of those customers made more than one order (our numerator)
    COUNTIF(counts.number_of_orders > 1) AS repeat_customers,
    
    -- Calculate the repurchase rate for each group
    SAFE_DIVIDE(
        COUNTIF(counts.number_of_orders > 1),
        COUNT(first_review.customer_unique_id)
    ) * 100 AS repurchase_rate
FROM
    first_customer_review AS first_review
JOIN
    customer_order_counts AS counts ON first_review.customer_unique_id = counts.customer_unique_id
WHERE
    first_review.order_rank = 1 -- CRITICAL: We only look at the very first order experience.
GROUP BY
    first_experience_score
ORDER BY
    first_experience_score ASC;
```
### 14. Black Friday
```sql
-- This query analyzes the platform's performance on the single day of Black Friday 2017
-- by comparing its volume and late delivery rate against a "normal" baseline period.

-- Step 1: Define and calculate all metrics for the Black Friday 2017 SINGLE DAY.
WITH black_friday_metrics AS (
    SELECT
        SUM(price) AS bf_gmv,
        COUNT(DISTINCT order_id) AS bf_orders,
        COUNTIF(delivery_diff_from_estimated_days < 0) AS bf_late_deliveries,
        COUNTIF(order_status = 'delivered' AND delivery_diff_from_estimated_days IS NOT NULL) AS bf_total_deliveries,
        -- The Black Friday period is now just 1 day.
        1 AS bf_num_days
    FROM
        my-project-dsai-3.olist_data.fct_orders
    WHERE
        -- Define the Black Friday 2017 single day.
        DATE(order_purchase_at) = '2017-11-24'
),

-- Step 2: Define and calculate all metrics for the "normal" baseline period (Unchanged).
normal_period_metrics AS (
    SELECT
        SUM(price) AS normal_gmv,
        COUNT(DISTINCT order_id) AS normal_orders,
        COUNTIF(delivery_diff_from_estimated_days < 0) AS normal_late_deliveries,
        COUNTIF(order_status = 'delivered' AND delivery_diff_from_estimated_days IS NOT NULL) AS normal_total_deliveries,
        -- The normal period is 92 days (Aug 1 to Oct 31).
        92 AS normal_num_days
    FROM
        my-project-dsai-3.olist_data.fct_orders
	 WHERE
        -- Define the normal baseline period (3 months prior to the event).
        DATE(order_purchase_at) BETWEEN '2017-08-01' AND '2017-10-31'
)

-- Final Step: Combine metrics from both periods to calculate the final comparison ratios.
SELECT
    -- Normal Period Metrics
    np.normal_orders / np.normal_num_days AS normal_avg_daily_orders,

    -- Black Friday Period Metrics (now representing a single day)
    bf.bf_orders AS bf_daily_orders, -- No division needed as it's just 1 day.

    -- The "X-Factor" Comparison
    -- This calculates the "Xx" part of the statement for order volume.
    SAFE_DIVIDE(
        bf.bf_orders, -- Black Friday's total orders
        (np.normal_orders / np.normal_num_days) -- Normal average daily orders
    ) AS order_volume_multiple,
	SAFE_DIVIDE(bf.bf_late_deliveries, bf.bf_total_deliveries) AS bf_late_delivery_rate

-- We use a CROSS JOIN as each CTE only returns a single row of aggregated data.
FROM
    black_friday_metrics bf
CROSS JOIN
    normal_period_metrics np;
```

### 15. Repurchase Customer Profile
```sql
-- Step 1: For each customer, find their first order and classify its experience.
WITH first_order_experience AS (
    SELECT
        customer_unique_id,
        CASE
            WHEN delivery_diff_from_estimated_days >= 0 AND review_score = 5 THEN 'Golden Cohort (On-Time & 5-Star)'
            ELSE 'All Others'
        END AS customer_group,
        -- Use a window function to rank orders to find the first one.
        ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY order_purchase_at ASC) as order_rank
    FROM
        my-project-dsai-3.olist_data.fct_orders
    WHERE
        delivery_diff_from_estimated_days IS NOT NULL
        AND review_score IS NOT NULL
),

-- Step 2: For each customer, get their total number of lifetime orders.
customer_order_counts AS (
    SELECT
        customer_unique_id,
        COUNT(DISTINCT order_id) as number_of_orders
    FROM
        my-project-dsai-3.olist_data.fct_orders
    GROUP BY 1
),

-- *** NEW Step 3: Join the two CTEs and apply the filter FIRST to create a clean, final dataset. ***
final_customer_data AS (
    SELECT
        foe.customer_group,
        coc.customer_unique_id,
        coc.number_of_orders
    FROM
        first_order_experience AS FOE
    JOIN
        customer_order_counts AS COC ON foe.customer_unique_id = coc.customer_unique_id
    WHERE
        foe.order_rank = 1  -- The filter is now applied in this intermediate step.
)

-- Final, Simple Aggregation: This step is now much cleaner and error-free.
SELECT
    customer_group,
    
    COUNT(customer_unique_id) AS total_customers,
    
    COUNTIF(number_of_orders > 1) AS repeat_customers,
    
    SAFE_DIVIDE(
        COUNTIF(number_of_orders > 1),
        COUNT(customer_unique_id)
    ) AS repurchase_rate
FROM
    final_customer_data
GROUP BY
    customer_group
ORDER BY
    repurchase_rate DESC;
```

### SELLERS TAB

### 1. Total Number of Sellers
```sql
SELECT
  COUNT(*)
FROM
  my-project-dsai-3.olist_data.dim_sellers;
```

### 2. Total Number of 5* Sellers
```sql
SELECT
  COUNT(*)
FROM
  my-project-dsai-3.olist_data.dim_sellers
WHERE
  average_review_score = 5;
```

### 3. Distribution of Sellers by State
```sql
SELECT seller_state, COUNT(DISTINCT seller_id) as unique_sellers 
FROM `my-project-dsai-3.olist_data.fct_orders` 
GROUP BY seller_state 
ORDER BY unique_sellers DESC;
```

### 4. Top 10 Best Sellers by Revenue
```sql
SELECT
  seller_id,
  total_revenue,
  total_orders,
  total_items_sold,
  average_review_score,
  avg_hours_to_ship
FROM
  my-project-dsai-3.olist_data.dim_sellers
WHERE
  total_revenue IS NOT NULL
 
   AND (total_orders IS NOT NULL)
ORDER BY
  total_revenue DESC
LIMIT
  10;
```

### 5. 5* rating sellers with total order of more than 5
```sql
SELECT
  seller_id,
  total_revenue,
  total_orders,
  total_items_sold,
  average_review_score,
  avg_hours_to_ship
FROM
  my-project-dsai-3.olist_data.dim_sellers
WHERE
  average_review_score = 5
 
   AND (total_orders > 5)
ORDER BY
  average_review_score DESC,
  total_orders DESC;
```

### 6. Top 10 Worst Sellers by Customer Satisfaction (Number of Orders >20)
```sql
SELECT
  seller_id,
  total_revenue,
  total_orders,
  total_items_sold,
  average_review_score,
  avg_hours_to_ship
FROM
  my-project-dsai-3.olist_data.dim_sellers
WHERE
  average_review_score IS NOT NULL
 
   AND (total_orders > 20)
ORDER BY
  average_review_score ASC,
  total_orders ASC;
```

### 7. Fastest Sellers to Ship with >20 # of Orders
```sql
SELECT
  seller_id,
  total_revenue,
  total_orders,
  total_items_sold,
  average_review_score,
  avg_hours_to_ship
FROM
  my-project-dsai-3.olist_data.dim_sellers
WHERE
  avg_hours_to_ship > 0
 
   AND (total_orders > 20)
ORDER BY
  avg_hours_to_ship ASC
LIMIT
  10;
```

### 8. Processing Time vs Shipping Time
```sql
SELECT
  TIMESTAMP_TRUNC(
    order_purchase_at,
    month
  ) AS order_purchase_at,
  AVG(
    seller_processing_hours
  ) / 24 AS average_seller_processing_days,
  AVG(carrier_shipping_days) AS average_carrier_shipping_days
FROM
  my-project-dsai-3.olist_data.fct_orders
WHERE
  order_purchase_at >= timestamp "2017-01-01 00:00:00Z"
GROUP BY
  order_purchase_at
ORDER BY
  order_purchase_at ASC;
```

### 9. Review Score vs Delivery Status
```sql
SELECT
    -- 1. Create the two segments using a CASE statement. This is our dimension.
    CASE
        WHEN delivery_diff_from_estimated_days >= 0 THEN 'On-Time or Early'
        ELSE 'Late'
    END AS delivery_status,

    -- 2. Calculate the average review score for each segment. This is our metric.
    AVG(review_score) AS average_review_score,
    
    -- 3. (Bonus) Count the number of orders in each segment to show statistical significance.
    COUNT(*) AS number_of_orders
FROM
    -- Use our master fact table which contains all the necessary data.
    my-project-dsai-3.olist_data.fct_orders
WHERE
    -- Ensure our calculation is based on clean data by excluding nulls.
    delivery_diff_from_estimated_days IS NOT NULL
    AND review_score IS NOT NULL
GROUP BY
    -- 4. Group the results by the two segments we created.
    delivery_status;
```

### 10. Top Sellers Stats
```sql
-- Step 1: Use the seller dimension table.
WITH seller_performance AS (
    SELECT
        seller_id,
        total_revenue,
        average_review_score
    FROM
        my-project-dsai-3.olist_data.dim_sellers
),

-- Step 2: Calculate the grand totals for the entire company.
company_totals AS (
    SELECT
        COUNT(seller_id) AS total_sellers,
        SUM(total_revenue) AS grand_total_revenue
    FROM
        seller_performance
),

-- Step 3: Rank all sellers by their revenue.
sellers_with_rank AS (
    SELECT
        sp.seller_id,
        sp.total_revenue,
        sp.average_review_score,
        RANK() OVER (ORDER BY sp.total_revenue DESC) as seller_rank
    FROM
        seller_performance sp
),

-- Step 4: Identify the "Top 1%" sellers using the rank.
top_sellers AS (
    SELECT
        sr.seller_id,
        sr.total_revenue,
        sr.average_review_score
    FROM
        sellers_with_rank sr
    WHERE
        sr.seller_rank <= (SELECT CAST(total_sellers * 0.01 AS INT64) FROM company_totals)
)

-- Final Step (Unchanged): Aggregate the metrics for the "Top 1%" group.
SELECT
    (SELECT total_sellers FROM company_totals) AS total_sellers,
    COUNT(ts.seller_id) AS top_sellers_count,
    SAFE_DIVIDE(COUNT(ts.seller_id), (SELECT total_sellers FROM company_totals)) * 100 AS top_sellers_percentage,
    (SELECT grand_total_revenue FROM company_totals) AS grand_total_revenue,
    SUM(ts.total_revenue) AS top_sellers_revenue,
    SAFE_DIVIDE(SUM(ts.total_revenue), (SELECT grand_total_revenue FROM company_totals)) * 100 AS top_sellers_revenue_percentage,

    AVG(ts.average_review_score) AS top_sellers_avg_review_score
FROM
    top_sellers ts;
```

### 11. List of Top Sellers
```sql
WITH seller_performance AS (
    SELECT
        seller_id,
        total_revenue,
        average_review_score
    FROM
        my-project-dsai-3.olist_data.dim_sellers
),

-- Step 2: Calculate the grand totals for the entire company.
company_totals AS (
    SELECT
        COUNT(seller_id) AS total_sellers,
        SUM(total_revenue) AS grand_total_revenue
    FROM
        seller_performance
),

-- Step 3: Rank all sellers by their revenue.
sellers_with_rank AS (
    SELECT
        sp.seller_id,
        sp.total_revenue,
        sp.average_review_score,
        RANK() OVER (ORDER BY sp.total_revenue DESC) as seller_rank
    FROM
        seller_performance sp
),

-- Step 4: Identify the "Top 1%" sellers using the rank.
top_sellers AS (
    SELECT
        sr.seller_id,
        sr.total_revenue,
        sr.average_review_score
    FROM
        sellers_with_rank sr
    WHERE
        sr.seller_rank <= (SELECT CAST(total_sellers * 0.01 AS INT64) FROM company_totals)
)

-- Final Step : List the top 1% sellers.
SELECT
    seller_id,
    total_revenue,
    average_review_score
FROM
    top_sellers;
```

### 12. Average Late Delivery Rate
```sql
SELECT
    (COUNT(CASE WHEN delivery_diff_from_estimated_days >= 0 THEN NULL ELSE 1 END)/
    COUNT(CASE WHEN order_status = 'delivered' THEN 1 ELSE NULL END))
FROM
    my-project-dsai-3.olist_data.fct_orders;
```


### 13. Late Delivery Rate by State
```sql
-- We use a CTE to pre-calculate the metrics at the state level first.
WITH state_performance AS (
    SELECT
        customer_state,

        -- Metric 1: Count the number of late deliveries in each state.
        COUNTIF(delivery_diff_from_estimated_days < 0) AS late_deliveries,

        -- Metric 2: Count the total number of delivered orders in each state.
        COUNTIF(order_status = 'delivered' AND delivery_diff_from_estimated_days IS NOT NULL) AS total_deliveries

    FROM
        my-project-dsai-3.olist_data.fct_orders
    WHERE
        customer_state IS NOT NULL
    GROUP BY
        customer_state
)

-- Final SELECT statement to calculate percentages and filter for the big states.
SELECT
    sp.customer_state,
    -- Calculate the late delivery rate for the state.
    SAFE_DIVIDE(
        sp.late_deliveries,
        sp.total_deliveries
    ) AS late_delivery_percentage
FROM
    state_performance sp
ORDER BY
    late_delivery_percentage DESC;
```

### PRODUCTS TAB

### 1. Total Number of Products
```sql
SELECT
  COUNT(*)
FROM
  my-project-dsai-3.olist_data.dim_products;
```

### 2. Total Number of Products Categories
```sql
SELECT
  COUNT(DISTINCT product_category_name_english)
FROM
  my-project-dsai-3.olist_data.dim_products;
```

### 3. Top 20 Product Categories
```sql
SELECT
    product_category_name_english,
    COUNT(DISTINCT order_id) AS number_of_orders
FROM
    my-project-dsai-3.olist_data.fct_orders
WHERE
    product_category_name_english IS NOT NULL
GROUP BY
    1 -- "1" means "GROUP BY a.product_category_name_english"
ORDER BY
    2 DESC --  "2" means "ORDER BY number_of_orders"
LIMIT 20;
```

### 4. GMV by Product Category
```sql
SELECT
    product_category_name_english,
    SUM(price) AS gmv
FROM
    my-project-dsai-3.olist_data.fct_orders
WHERE
    product_category_name_english IS NOT NULL
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 15;
```

### 5. Revenue vs Volume
```sql
SELECT
  product_category_name_english,
  SUM(total_units_sold) AS sum,
  SUM(total_revenue_generated) AS sum_2
FROM
  my-project-dsai-3.olist_data.dim_products
GROUP BY
  product_category_name_english
ORDER BY
  product_category_name_english ASC;
```

### 6. Top 20 Highest Review Products Category
```sql
SELECT
  product_category_name_english,
  AVG(average_review_score) AS avg,
  SUM(total_revenue_generated) AS sum
FROM
  my-project-dsai-3.olist_data.dim_products
WHERE
  product_category_name_english IS NOT NULL
 
   AND (
    (
      product_category_name_english <> ''
    )
   
    OR (
      product_category_name_english IS NULL
    )
  )
GROUP BY
  product_category_name_english
ORDER BY
  avg DESC,
  product_category_name_english ASC
LIMIT
  20;
```

### 7. Revenue vs Review Score by Product Category
```sql
WITH category_metrics AS (
    SELECT
        product_category_name_english,
        AVG(average_review_score) AS avg_review_score,
        SUM(total_revenue_generated) AS total_revenue
    FROM my-project-dsai-3.olist_data.dim_products
    GROUP BY product_category_name_english
),
benchmarks AS (
    SELECT
        AVG(avg_review_score) AS avg_review_threshold,
        AVG(total_revenue) AS revenue_threshold
    FROM category_metrics
)
SELECT
    c.product_category_name_english,
    c.avg_review_score,
    c.total_revenue,
    CASE
        WHEN c.total_revenue >= b.revenue_threshold
         AND c.avg_review_score >= b.avg_review_threshold
            THEN '⭐ Stars'
        WHEN c.total_revenue >= b.revenue_threshold
         AND c.avg_review_score < b.avg_review_threshold
            THEN '⚠ Fix'
        WHEN c.total_revenue < b.revenue_threshold
         AND c.avg_review_score >= b.avg_review_threshold
            THEN '🚀 Grow'
        ELSE '💤 Deprioritise'
    END AS segment
FROM category_metrics c
CROSS JOIN benchmarks b
WHERE c.avg_review_score BETWEEN 3.5 AND 5
ORDER BY avg_review_score DESC;;
```

### 8. Price Distribution
```sql
WITH product_prices AS (
    SELECT
        product_id,
        SAFE_DIVIDE(total_revenue_generated, total_units_sold) AS unit_price
    FROM
        `my-project-dsai-3.olist_data.dim_products`
    WHERE
        total_units_sold > 0
)
SELECT
    CASE 
        -- EDIT THIS NUMBER: Items under 50 are Low-end
        WHEN unit_price < 50 THEN '1. Low-end (< R$50)'
        
        -- EDIT THIS NUMBER: Items between 50 and 150 are Mid-range
        WHEN unit_price >= 50 AND unit_price < 150 THEN '2. Mid-range (R$50 - R$150)'
        
        -- Everything else is High-end
        ELSE '3. High-end (> R$150)'
    END AS price_tier,
    
    COUNT(product_id) AS product_count
FROM
    product_prices
GROUP BY
    1 -- This groups by the price_tier we defined above
ORDER BY
    1 DESC;
```

### 9. Category Segmentation
```sql
WITH category_metrics AS (
    SELECT
        product_category_name_english,
        AVG(average_review_score) AS avg_review_score,
        SUM(total_revenue_generated) AS total_revenue
    FROM my-project-dsai-3.olist_data.dim_products
    GROUP BY product_category_name_english
),
benchmarks AS (
    SELECT
        AVG(avg_review_score) AS avg_review_threshold,
        AVG(total_revenue) AS revenue_threshold
    FROM category_metrics
)
SELECT
    c.product_category_name_english,
    c.avg_review_score,
    c.total_revenue,
    CASE
        WHEN c.total_revenue >= b.revenue_threshold
         AND c.avg_review_score >= b.avg_review_threshold
            THEN '⭐ Stars'
        WHEN c.total_revenue >= b.revenue_threshold
         AND c.avg_review_score < b.avg_review_threshold
            THEN '⚠ Fix'
        WHEN c.total_revenue < b.revenue_threshold
         AND c.avg_review_score >= b.avg_review_threshold
            THEN '🚀 Grow'
        ELSE '💤 Deprioritise'
    END AS segment
FROM category_metrics c
CROSS JOIN benchmarks b
ORDER BY avg_review_score DESC;
```

### 10. Category Segmentation (Only 'Grow' & 'Stars')
```sql
WITH category_metrics AS (
    SELECT
        product_category_name_english,
        AVG(average_review_score) AS avg_review_score,
        SUM(total_revenue_generated) AS total_revenue
    FROM my-project-dsai-3.olist_data.dim_products
    GROUP BY product_category_name_english
),
benchmarks AS (
    SELECT
        AVG(avg_review_score) AS avg_review_threshold,
        AVG(total_revenue) AS revenue_threshold
    FROM category_metrics
)
SELECT
    c.product_category_name_english,
    c.avg_review_score,
    c.total_revenue,
    CASE
        WHEN c.total_revenue >= b.revenue_threshold
         AND c.avg_review_score >= b.avg_review_threshold
            THEN '⭐ Stars'
        WHEN c.total_revenue >= b.revenue_threshold
         AND c.avg_review_score < b.avg_review_threshold
            THEN '⚠ Fix'
        WHEN c.total_revenue < b.revenue_threshold
         AND c.avg_review_score >= b.avg_review_threshold
            THEN '🚀 Grow'
        ELSE '💤 Deprioritise'
    END AS segment
FROM category_metrics c
CROSS JOIN benchmarks b
WHERE
    (
        c.total_revenue >= b.revenue_threshold
        AND c.avg_review_score >= b.avg_review_threshold
    )
    OR
    (
        c.total_revenue < b.revenue_threshold
        AND c.avg_review_score >= b.avg_review_threshold
    )
ORDER BY c.avg_review_score DESC;
```