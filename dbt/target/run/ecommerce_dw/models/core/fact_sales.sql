
      
        
            delete from "ecommerce_data"."public_analytics_mart"."fact_sales"
            where (
                order_id) in (
                select (order_id)
                from "fact_sales__dbt_tmp181853926097"
            );

        
    

    insert into "ecommerce_data"."public_analytics_mart"."fact_sales" ("date_key", "order_date", "customer_key", "product_key", "order_total_amount", "quantity_sold", "order_id", "status", "payment_method")
    (
        select "date_key", "order_date", "customer_key", "product_key", "order_total_amount", "quantity_sold", "order_id", "status", "payment_method"
        from "fact_sales__dbt_tmp181853926097"
    )
  