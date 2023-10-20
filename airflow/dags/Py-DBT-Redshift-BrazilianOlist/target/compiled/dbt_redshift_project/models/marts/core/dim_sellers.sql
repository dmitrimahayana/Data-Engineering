-- noinspection SqlDialectInspectionForFile

with sellers as (
    select *
    from "dev"."public"."stg_eltool__sellers"
    )
select *
from sellers