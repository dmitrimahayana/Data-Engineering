select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select customer_id
from "my_database"."public"."stg_eltool__customers_snapshots"
where customer_id is null



      
    ) dbt_internal_test