
    
    

select
    order_id as unique_field,
    count(*) as n_records

from "my_database"."public"."customer_orders_status"
where order_id is not null
group by order_id
having count(*) > 1


