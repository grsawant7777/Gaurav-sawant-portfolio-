

SELECT
    
    md5(cast(concat(
        
            coalesce(cast(product_id as varchar), '_null_')
            
        
    ) as varchar))
 AS product_key,
    product_id,
    product_name,
    unit_price,
    supplier_id,
    CASE 
        WHEN category IN ('Electronics', 'Sports') THEN 'High Value'
        WHEN category IN ('Apparel', 'Books') THEN 'Standard Value'
        ELSE 'Other'
    END AS product_value_segment,
    category AS main_category
FROM "ecommerce_data"."public_raw_staging"."stg_products"