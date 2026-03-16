
  
    

  create  table "ecommerce_data"."public_analytics_mart"."report_order_status_summary__dbt_tmp"
  
  
    as
  
  (
    -- report_order_status_summary.sql


SELECT
    status,
    COUNT(order_id) AS order_count,
    SUM(order_total_amount) AS total_sales_volume
FROM
    "ecommerce_data"."public_analytics_mart"."fact_sales"
GROUP BY 1
ORDER BY 2 DESC
  );
  