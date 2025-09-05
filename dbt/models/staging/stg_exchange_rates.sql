{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_exchange_rates_parquet
),
typed as (
  select
    cast(date as date) as date,
    trim(currency) as currency,
    cast(rate_to_aud as numeric(18, 8)) as rate_to_aud
  from src
)
select * from typed;
