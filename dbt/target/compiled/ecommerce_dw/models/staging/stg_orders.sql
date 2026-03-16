SELECT
    order_id,
    
    TRIM(LOWER(customer_id))
 AS customer_id, 
    
    TRIM(LOWER(product_id))
 AS product_id,
    order_date,
    total_amount,
    status,
    payment_method
FROM "ecommerce_data"."raw_staging"."orders"