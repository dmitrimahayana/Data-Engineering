
  
    

  create  table
    "dev"."public"."fact_orders__dbt_tmp"
    
    
    
  as (
    with orders as (select *
                     from "dev"."public"."stg_eltool__orders")
select order_id,
       customer_id,
       order_status,
       order_purchase_timestamp,
       order_approved_at,
       order_delivered_carrier_date,
       order_delivered_customer_date,
       order_estimated_delivery_date
from orders
  );
  