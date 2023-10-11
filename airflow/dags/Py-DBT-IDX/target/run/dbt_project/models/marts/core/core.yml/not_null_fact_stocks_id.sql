select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id
from "my_database"."public"."fact_stocks"
where id is null



      
    ) dbt_internal_test