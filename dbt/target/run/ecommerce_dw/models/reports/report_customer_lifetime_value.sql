
  
    

  create  table "ecommerce_data"."public_analytics_mart"."report_customer_lifetime_value__dbt_tmp"
  
  
    as
  
  (
    -- report_customer_lifetime_value.sql


SELECT
    c.customer_region,
    COUNT(DISTINCT c.customer_key) AS total_customers,
    SUM(o.order_total_amount) / COUNT(DISTINCT c.customer_key) AS customer_lifetime_value,
    COUNT(o.order_id) * 1.0 / COUNT(DISTINCT c.customer_key) AS average_orders_per_customer
FROM
    "ecommerce_data"."public_analytics_mart"."fact_sales" o
JOIN
    "ecommerce_data"."public_analytics_mart"."dim_customers" c 
    ON o.customer_key = c.customer_key
GROUP BY 1
ORDER BY 3 DESC
  );
  