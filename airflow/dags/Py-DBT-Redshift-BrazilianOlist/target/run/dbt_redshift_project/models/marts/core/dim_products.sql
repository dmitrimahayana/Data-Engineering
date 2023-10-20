
  
    

  create  table
    "dev"."public"."dim_products__dbt_tmp"
    
    
    
  as (
    -- noinspection SqlDialectInspectionForFile

with products as (
    select *
    from "dev"."public"."stg_eltool__products"
    )
select *
from products
  );
  