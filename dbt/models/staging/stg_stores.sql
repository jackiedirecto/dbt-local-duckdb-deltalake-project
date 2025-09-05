{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_stores_parquet
),
typed as (
  select
    cast(store_id as bigint) as store_id,
    store_code as store_code,
    name as store_name,
    channel as store_channel,
    region as store_region,
    state as store_state,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(open_dt as date) as open_dt,
    cast(close_dt as date) as close_dt
  from src
)
select * from typed;
