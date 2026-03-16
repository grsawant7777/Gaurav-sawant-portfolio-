
    
    

select
    order_id as unique_field,
    count(*) as n_records

from "ecommerce_data"."public_analytics_mart"."fact_sales"
where order_id is not null
group by order_id
having count(*) > 1


