{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_customers_parquet
),

-- Step 1: Normalize and clean data
cleaned as (
  select
    cast(customer_id as bigint) as customer_id,
    trim(lower(natural_key)) as natural_key,
    trim(coalesce(first_name, 'Unknown')) as first_name,
    trim(coalesce(last_name, 'Unknown')) as last_name,
    lower(trim(email)) as email,
    trim(phone) as phone,
    trim(address_line1) as address_line1,
    trim(address_line2) as address_line2,
    trim(city) as city,
    trim(state_region) as state_region,
    trim(postcode) as postcode,
    upper(trim(country_code)) as country_code,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(birth_date as date) as birth_date,
    -- Ensure timestamp is in UTC
    convert_timezone('Australia/Perth', 'UTC', cast(join_ts as timestamp)) as join_ts_utc,
    cast(is_vip as boolean) as is_vip,
    cast(gdpr_consent as boolean) as gdpr_consent
),

-- Step 2: Enrich data
enriched as (
  select
    *,
    -- Derived age from birth_date
    date_diff(current_date, birth_date) / 365 as age
  from cleaned
),

-- Step 3: Deduplicate customers by natural_key
deduped_customers as (
  select *
  from (
    select *,
      row_number() over (partition by natural_key order by join_ts_utc desc) as row_num
    from enriched
  ) where row_num = 1
)

-- Final output
select
  customer_id,
  natural_key,
  first_name,
  last_name,
  email,
  phone,
  address_line1,
  address_line2,
  city,
  state_region,
  postcode,
  country_code,
  latitude,
  longitude,
  birth_date,
  age,
  join_ts_utc,
  is_vip,
  gdpr_consent
from deduped_customers;
