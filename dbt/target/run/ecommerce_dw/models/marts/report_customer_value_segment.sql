
  
    

  create  table "ecommerce_data"."public_public_analytics_mart"."customer_value_segment_sales__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    c.customer_region,
    COUNT(DISTINCT o.customer_key) AS total_customers,
    COUNT(o.order_id) AS total_orders,
    SUM(o.order_total_amount) AS total_sales,
    AVG(o.order_total_amount) AS avg_order_value
FROM
    "ecommerce_data"."public_analytics_mart"."fact_sales" o
JOIN
    "ecommerce_data"."public_analytics_mart"."dim_customers" c 
    ON o.customer_key = c.customer_key
GROUP BY 1
ORDER BY 4 DESC
  );
  