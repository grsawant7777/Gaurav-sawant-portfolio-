SELECT
    
    TRIM(LOWER(product_id))
 AS product_id,
    product_name,
    category,
    unit_price,
    supplier_id
FROM "ecommerce_data"."raw_staging"."products"