{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
  select * from bronze_products_parquet
),

-- Step 1: Data Cleaning
cleaned as (
  select
    cast(product_id as bigint) as product_id,
    trim(lower(sku)) as product_sku,
    trim(coalesce(name, 'Unnamed Product')) as product_name,
    trim(category) as product_category,
    trim(subcategory) as product_subcategory,
    cast(coalesce(current_price, 0.0) as double) as current_price,
    upper(trim(currency)) as currency_code,
    cast(is_discontinued as boolean) as is_discontinued,
    convert_timezone('Australia/Perth', 'UTC', cast(introduced_dt as timestamp)) as introduced_ts,
    convert_timezone('Australia/Perth', 'UTC', cast(discontinued_dt as timestamp)) as discontinued_ts,
  from src
),

-- Step 2: Enrichment
enriched as (
  select
    *,
    -- Derived column: product age in days
    date_diff(current_date, introduced_ts) as product_age_days,
    -- Business metric placeholder: margin (assuming cost_price exists)
    case when cost_price is not null and current_price is not null
         then current_price - cost_price
         else null end as profit_margin
  from cleaned
),

-- Step 3: Deduplication
deduped_products as (
  select *
  from (
    select *,
      row_number() over (
        partition by product_sku
        order by introduced_ts desc nulls last
      ) as row_num
    from enriched
  ) where row_num = 1
)

-- Final Output
select
  product_id,
  product_sku,
  product_name,
  product_category,
  product_subcategory,
  current_price,
  currency_code,
  is_discontinued,
  introduced_ts,
  discontinued_ts,
  product_age_days,
  profit_margin
from deduped_products;
