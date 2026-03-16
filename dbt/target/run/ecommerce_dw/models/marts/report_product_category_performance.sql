
  
    

  create  table "ecommerce_data"."public_public_analytics_mart"."product_category_performance__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    p.main_category,
    p.product_value_segment,
    COUNT(o.order_id) AS total_orders,
    SUM(o.order_total_amount) AS total_sales,
    AVG(o.order_total_amount) AS avg_order_value
FROM
    "ecommerce_data"."public_analytics_mart"."fact_sales" o
JOIN
    "ecommerce_data"."public_analytics_mart"."dim_products" p
    ON o.product_key = p.product_key
GROUP BY 1, 2
ORDER BY 4 DESC
  );
  