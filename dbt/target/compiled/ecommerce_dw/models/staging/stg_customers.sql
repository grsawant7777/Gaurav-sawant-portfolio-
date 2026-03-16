SELECT
    
    TRIM(LOWER(customer_id))
 AS customer_id,
    first_name,
    last_name,
    email,
    registration_date,
    country
FROM "ecommerce_data"."raw_staging"."customers"