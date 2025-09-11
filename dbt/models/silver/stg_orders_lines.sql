{{
  config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='merge'
  )
}}

with src as (
  select * from {{ source('bronze', 'orders_lines') }}
),

-- Step 1: Data Cleaning
cleaned as (
  select
    cast(order_id as bigint) as order_id,
    cast(line_number as int) as line_number,
    cast(product_id as bigint) as product_id,
    cast(qty as int) as qty,
    cast(unit_price as numeric(12, 4)) as unit_price,
    cast(line_discount_pct as numeric(5, 4)) as line_discount_pct,
    cast(tax_pct as numeric(5, 4)) as tax_pct,
    -- Ensure ingestion_ts is in UTC
    convert_timezone('Asia/Manila', 'UTC', cast(ingestion_ts as timestamp)) as ingestion_ts
  from src
  where order_id is not null
    and product_id is not null
),

-- Step 2: Deduplication Strategy
-- Remove duplicate order lines based on order_id + line_number
deduped as (
  select *
  from (
    select *,
      row_number() over (
        partition by order_id, line_number
        order by ingestion_ts desc
      ) as row_num
    from cleaned
  ) sub
  where row_num = 1
),

-- Step 3: Data Enrichment
enriched as (
  select
    *,
    -- Calculate order line total: qty * unit_price * (1 - discount) * (1 + tax)
    round(
      qty * unit_price * (1 - coalesce(line_discount_pct, 0)) * (1 + coalesce(tax_pct, 0)),
      4
    ) as line_total
  from deduped
)

-- Final Output
select * from enriched

{% if is_incremental() %}
  where ingestion_ts > (select max(ingestion_ts) from {{ this }})
{% endif %}
