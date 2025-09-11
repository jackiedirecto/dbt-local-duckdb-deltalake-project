{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
    select * from bronze_suppliers_parquet
),

-- Step 1: Data Cleaning
cleaned as (
    select
      cast(supplier_id as bigint) as supplier_id,
      trim(lower(supplier_code)) as supplier_code,
      trim(coalesce(name, 'Unknown Supplier')) as supplier_name,
      upper(trim(coalesce(country_code, 'UNK'))) as country_code,
      cast(lead_time_days as int) as lead_time_days,
      cast(preferred as boolean) as is_preferred
    from src
),

-- Step 2: Deduplicate by supplier_id
dedup_by_id as (
    select *
    from (
        select *,
               row_number() over (
                   partition by supplier_id
                   order bssy supplier_name asc
               ) as rn
        from cleaned
    )
    where rn = 1
),

-- Step 3: Deduplicate by supplier_code
dedup_by_code as (
    select *
    from (
        select *,
               row_number() over (
                   partition by supplier_code
                   order by supplier_name asc
               ) as rn
        from dedup_by_id
    )
    where rn = 1
)

-- Final Output
select
    supplier_id,
    supplier_code,
    cleaned_supplier_name as supplier_name,
    cleaned_country_code as country_code,
    lead_time_days,
    is_preferred
from dedup_by_code;
