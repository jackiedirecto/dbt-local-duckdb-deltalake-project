{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_exchange_rates_parquet
),

-- Step 1: Data Cleaning
cleaned as (
  select
    -- Normalize column names and types
    cast(date as date) as exchange_date,
    upper(trim(coalesce(currency, 'UNKNOWN'))) as currency_code,
    cast(coalesce(rate_to_aud, 1.0) as numeric(18, 8)) as rate_to_aud,
  from src
),

-- Step 2: Enrichment
enriched as (
  select
    *,
    -- Example derived metric: inverse rate (AUD to currency)
    case when rate_to_aud != 0 then 1 / rate_to_aud else null end as rate_from_aud
  from cleaned
)

-- Final output
select
  exchange_date,
  currency_code,
  rate_to_aud,
  rate_from_aud
from enriched;
