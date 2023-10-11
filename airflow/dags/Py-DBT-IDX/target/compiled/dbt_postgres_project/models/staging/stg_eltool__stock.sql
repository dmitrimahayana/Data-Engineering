with source as (select DISTINCT *
                from "my_database"."public"."ksql-stock-stream"),
     renamed as (select *
                 from source)
select *
from renamed