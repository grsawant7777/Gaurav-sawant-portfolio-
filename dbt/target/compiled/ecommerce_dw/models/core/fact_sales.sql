

WITH new_orders AS (
    -- PERFORMANCE OPTIMIZATION: Filter source data BEFORE joins
    SELECT * FROM "ecommerce_data"."public_raw_staging"."stg_orders"
    
    
      -- Handle late-arriving data or status updates with a 3-day buffer
      WHERE order_date >= (SELECT MAX(order_date) - interval '3 days' FROM "ecommerce_data"."public_analytics_mart"."fact_sales")
    
),

final AS (
    SELECT
        -- Use your custom macro to generate the integer date key
        
    CAST(REPLACE(CAST(o.order_date AS TEXT), '-', '') AS INT)
 AS date_key,
        o.order_date, 
        c.customer_key,
        p.product_key, 
        o.total_amount AS order_total_amount,
        1 AS quantity_sold, 
        o.order_id,
        o.status,
        o.payment_method
    FROM new_orders o
    -- Joins are only performed on the subset of data defined in new_orders
    JOIN "ecommerce_data"."public_analytics_mart"."dim_customers" c ON o.customer_id = c.customer_id
    JOIN "ecommerce_data"."public_analytics_mart"."dim_products" p ON o.product_id = p.product_id
)

SELECT * FROM final