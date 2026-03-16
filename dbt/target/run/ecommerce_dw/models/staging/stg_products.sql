
  create view "ecommerce_data"."public_raw_staging"."stg_products__dbt_tmp"
    
    
  as (
    SELECT
    
    TRIM(LOWER(product_id))
 AS product_id,
    product_name,
    category,
    unit_price,
    supplier_id
FROM "ecommerce_data"."raw_staging"."products"
  );