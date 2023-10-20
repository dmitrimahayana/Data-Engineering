
  
    

  create  table
    "dev"."public"."stg_eltool__sellers__dbt_tmp"
    
    
    
  as (
    with source as (select *
                from "dev"."public"."sellers"),
     renamed as (select seller_id,
                        seller_zip_code_prefix,
                        seller_city,
                        seller_state
                 from source)
select *
from renamed
  );
  