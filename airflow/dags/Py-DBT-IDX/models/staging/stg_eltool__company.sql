with source as (select *
                from {{ source('public', 'ksql-company-stream') }}),
     renamed as (select id,
                        ticker,
                        name,
                        logo
                 from source)
select *
from renamed