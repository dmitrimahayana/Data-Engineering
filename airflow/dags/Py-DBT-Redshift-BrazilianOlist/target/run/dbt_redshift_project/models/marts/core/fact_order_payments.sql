
  
    

  create  table
    "dev"."public"."fact_order_payments__dbt_tmp"
    
    
    
  as (
    with orders as (select *
                     from "dev"."public"."stg_eltool__order_payments")
select order_id,
       payment_sequential,
       payment_type,
       payment_installments,
       payment_value
from orders
  );
  