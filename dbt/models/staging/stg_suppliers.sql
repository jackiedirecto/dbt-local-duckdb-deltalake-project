{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_suppliers_parquet
),
typed as (
  select
    cast(supplier_id as bigint) as supplier_id,
    supplier_code as supplier_code,
    name as supplier_name,
    country_code as country_code,
    cast(lead_time_days as int) as lead_time_days,
    cast(preferred as boolean) as is_preferred
  from src
)
select * from typed;
