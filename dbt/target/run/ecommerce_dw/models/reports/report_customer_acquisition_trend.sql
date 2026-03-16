
  
    

  create  table "ecommerce_data"."public_analytics_mart"."customer_acquisition_trend__dbt_tmp"
  
  
    as
  
  (
    -- report_customer_acquisition_trend.sql


SELECT
    DATE_TRUNC('month', registration_date) AS registration_month,
    customer_region,
    COUNT(customer_key) AS new_customers_count
FROM
    "ecommerce_data"."public_analytics_mart"."dim_customers"
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
  );
  