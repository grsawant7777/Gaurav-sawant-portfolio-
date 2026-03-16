
  
    

  create  table "ecommerce_data"."public_analytics_mart"."daily_regional_sales__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    order_date,
    customer_region,
    COUNT(o.order_id) AS daily_total_orders,
    SUM(o.order_total_amount) AS daily_total_sales 
FROM
    "ecommerce_data"."public_analytics_mart"."fact_sales" o
JOIN
    "ecommerce_data"."public_analytics_mart"."dim_customers" c 
    ON o.customer_key = c.customer_key
GROUP BY 1, 2
ORDER BY 1 DESC, 4 DESC
  );
  