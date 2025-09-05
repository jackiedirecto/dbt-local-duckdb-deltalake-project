{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_products_parquet
),
typed as (
  select
    cast(product_id as bigint) as product_id,
    sku as product_sku,
    name as product_name,
    category as product_category,
    subcategory as product_subcategory,
    cast(current_price as double) as current_price,
    currency as currency,
    cast(is_discontinued as boolean) as is_discontinued,
    cast(introduced_dt as date) as introduced_dt,
    cast(discontinued_dt as dt) as discontinued_dt
  from src
)
select * from typed;