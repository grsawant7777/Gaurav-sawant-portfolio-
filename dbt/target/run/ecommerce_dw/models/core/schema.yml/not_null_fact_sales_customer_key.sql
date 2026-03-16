select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select customer_key
from "ecommerce_data"."public_analytics_mart"."fact_sales"
where customer_key is null



      
    ) dbt_internal_test