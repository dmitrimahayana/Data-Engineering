
  
    

  create  table
    "dev"."public"."stg_eltool__order_items__dbt_tmp"
    
    
    
  as (
    with source as (select *
                from "dev"."public"."order_items"),
     renamed as (select order_id,
                        order_item_id,
                        product_id,
                        seller_id,
                        shipping_limit_date,
                        price,
                        freight_value
                 from source)
select *
from renamed
  );
  