{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
    -- Source table
    select * from bronze_returns_parquet
),

-- Step 1: Data Cleaning
cleaned as (
    select
        -- Normalize column names & types
        cast(return_id as bigint) as return_id,
        cast(order_id as bigint) as order_id,
        cast(product_id as bigint) as product_id,
        -- Convert timestamp to UTC (assuming source is in Australia/Perth)
        convert_timezone('Australia/Perth', 'UTC', cast(return_ts as timestamp)) as return_ts_utc,
        cast(qty as int) as qty,
        trim(reason) as reason,             -- Trim whitespace from strings
        -- Handle nulls with sensible defaults
        coalesce(qty, 0) as cleaned_qty,
        coalesce(trim(reason), 'Unknown') as cleaned_reason
    from src
),

-- Step 2: Deduplication by return_id
deduped as (
    select *
    from (
        select *,
               row_number() over (
                   partition by return_id
                   order by return_ts_utc desc nulls last
               ) as rn
        from cleaned
    )
    where rn = 1
)

-- Final Output
select
    return_id,
    order_id,
    product_id,
    return_ts_utc,
    cleaned_qty as qty,
    cleaned_reason as reason
from deduped;
