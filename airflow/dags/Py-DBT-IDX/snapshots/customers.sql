{% snapshot customers_snapshot %}

{{
    config(
      target_database='my_database',
      target_schema='snapshots',
      unique_key='customer_id',

      strategy='timestamp',
      updated_at='datetime_updated',
    )
}}

select *
from {{ source('public', 'customers') }}

{% endsnapshot %}