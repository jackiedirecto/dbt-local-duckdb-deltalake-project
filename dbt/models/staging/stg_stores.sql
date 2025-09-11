{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
    select * from bronze_stores_parquet
),

-- Step 1: Data Cleaning
cleaned as (
    select
        cast(store_id as bigint) as store_id,
        trim(lower(store_code)) as store_code,
        trim(coalesce(name, 'Unnamed Store')) as store_name,
        trim(coalesce(channel, 'Unknown')) as store_channel,
        trim(region) as store_region,
        trim(state) as store_state,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        -- Convert to UTC-compatible timestamps (assuming source is local time)
        convert_timezone('Australia/Perth', 'UTC', cast(open_dt as timestamp)) as open_ts_utc,
        convert_timezone('Australia/Perth', 'UTC', cast(close_dt as timestamp)) as close_ts_utc
    from src
),

-- Step 2: Deduplicate by store_id
dedup_by_id as (
    select *
    from (
        select *,
               row_number() over (
                   partition by store_id
                   order by open_ts_utc desc nulls last
               ) as rn
        from cleaned
    )
    where rn = 1
),

-- Step 3: Deduplicate by store_code
dedup_by_code as (
    select *
    from (
        select *,
               row_number() over (
                   partition by store_code
                   order by open_ts_utc desc nulls last
               ) as rn
        from dedup_by_id
    )
    where rn = 1
)

-- Final Output
select
    store_id,
    store_name,
    store_channel,
    store_region,
    store_state,
    latitude,
    longitude,
    open_ts_utc,
    close_ts_utc
from dedup_by_code;
