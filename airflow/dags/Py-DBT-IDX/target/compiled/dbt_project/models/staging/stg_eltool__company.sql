with source as (select *
                from "my_database"."public"."ksql-company-stream"),
     renamed as (select id,
                        ticker,
                        name,
                        logo
                 from source)
select *
from renamed