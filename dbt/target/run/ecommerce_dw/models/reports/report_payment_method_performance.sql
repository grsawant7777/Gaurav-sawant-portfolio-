
  
    

  create  table "ecommerce_data"."public_analytics_mart"."report_payment_method_performance__dbt_tmp"
  
  
    as
  
  (
    -- report_payment_method_performance.sql


SELECT
    payment_method,
    COUNT(order_id) AS total_orders_count,
    SUM(order_total_amount) AS total_sales_volume,
    AVG(order_total_amount) AS avg_order_value
FROM
    "ecommerce_data"."public_analytics_mart"."fact_sales"
GROUP BY 1
ORDER BY 3 DESC
  );
  