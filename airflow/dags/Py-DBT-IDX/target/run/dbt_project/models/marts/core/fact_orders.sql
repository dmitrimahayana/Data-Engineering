
  create view "my_database"."public"."fact_orders__dbt_tmp"
    
    
  as (
    with orders as (
    select *
    from "my_database"."public"."stg_eltool__orders"
)
select * from orders
  );