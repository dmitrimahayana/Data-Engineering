
  
    

  create  table
    "dev"."public"."dim_sellers__dbt_tmp"
    
    
    
  as (
    -- noinspection SqlDialectInspectionForFile

with sellers as (
    select *
    from "dev"."public"."stg_eltool__sellers"
    )
select *
from sellers
  );
  