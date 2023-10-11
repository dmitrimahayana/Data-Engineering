with source as (select DISTINCT *
                from {{ source('public', 'ksql-stock-stream') }}),
     renamed as (select *
                 from source)
select *
from renamed