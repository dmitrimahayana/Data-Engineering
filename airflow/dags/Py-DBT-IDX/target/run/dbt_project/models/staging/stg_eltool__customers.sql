
  create view "my_database"."public"."stg_eltool__customers__dbt_tmp"
    
    
  as (
    with source as (select *
                from "my_database"."public"."customers"),
     renamed as (select
                     customer_id,
                    'This is Dummy' as dummy
                 from source
                 where customer_id = 1234567890)
select *
from renamed
  );