{{ 
  config(
    materialized='incremental',
    unique_key='sensor_id',  -- or composite key if needed
    on_schema_change='merge',
    partition_by={'field': 'store_id', 'data_type': 'int'},
    cluster_by=['month']
  ) 
}}

with src as (
    select * 
    from {{ source('bronze', 'sensors') }}
),

-- Step 1: Data Cleaning
cleaned as (
    select
        -- Normalize column names & types
        convert_timezone('Australia/Melbourne', 'UTC', cast(sensor_ts as timestamp)) as sensor_ts_utc,
        cast(store_id as bigint) as store_id,
        trim(lower(shelf_id)) as shelf_id,
        cast(temperature_c as numeric(5, 2)) as temperature_c,
        cast(humidity_pct as numeric(5, 2)) as humidity_pct,
        cast(battery_mv as int) as battery_mv,

        -- Handle nulls with sensible defaults
        coalesce(trim(lower(shelf_id)), 'unknown_shelf') as cleaned_shelf_id,
        coalesce(temperature_c, 0.00) as cleaned_temperature_c,
        coalesce(humidity_pct, 0.00) as cleaned_humidity_pct,
        coalesce(battery_mv, 0) as cleaned_battery_mv,

        -- Partitioning helper: month in yyyy-mm format
        to_char(convert_timezone('Australia/Perth', 'UTC', cast(sensor_ts as timestamp)), 'YYYY-MM') as month
    from src
),

-- Step 2: Data Enrichment
enriched as (
    select
        *,
        -- Example derived metric: battery voltage in volts
        cleaned_battery_mv / 1000.0 as battery_v,

        -- Example JSON parsing (if metadata_json exists)
        try_parse_json(metadata_json) as metadata,
        metadata:firmware_version::string as firmware_version,

        -- Example business metric: temperature deviation from ideal (assume 4°C ideal)
        cleaned_temperature_c - 4.00 as temp_deviation_c
    from cleaned
),

-- Step 3: Deduplication
-- For sensor data, deduplication is usually by (store_id, shelf_id, sensor_ts)
deduped as (
    select *
    from (
        select *,
               row_number() over (
                   partition by store_id, shelf_id, sensor_ts_utc
                   order by sensor_ts_utc desc
               ) as rn
        from enriched
    )
    where rn = 1
)

-- Final Output
select
    sensor_ts_utc,
    store_id,
    cleaned_shelf_id as shelf_id,
    cleaned_temperature_c as temperature_c,
    cleaned_humidity_pct as humidity_pct,
    battery_v,
    firmware_version,
    temp_deviation_c,
    month
from deduped

{% if is_incremental() %}
  where sensor_ts_utc > (select max(sensor_ts_utc) from {{ this }})
{% endif %}
