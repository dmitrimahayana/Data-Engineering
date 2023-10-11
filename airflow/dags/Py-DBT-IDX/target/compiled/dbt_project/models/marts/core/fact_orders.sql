with orders as (
    select *
    from "my_database"."public"."stg_eltool__orders"
)
select * from orders