
    
    

with child as (
    select date_key as from_field
    from "ecommerce_data"."public_analytics_mart"."fact_sales"
    where date_key is not null
),

parent as (
    select date_key as to_field
    from "ecommerce_data"."public_core"."dim_date"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


