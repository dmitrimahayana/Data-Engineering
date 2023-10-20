-- noinspection SqlDialectInspectionForFile

with products as (
    select *
    from "dev"."public"."stg_eltool__products"
    )
select *
from products