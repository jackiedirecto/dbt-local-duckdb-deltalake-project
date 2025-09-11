{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
    -- Source table
    select * from bronze_shipments_parquet
),

-- Step 1: Data Cleaning
cleaned as (
    select
        -- Normalize column names & types
        cast(shipment_id as bigint) as shipment_id,
        cast(order_id as bigint) as order_id,
        trim(carrier) as carrier,

        -- Convert timestamps to UTC (assuming source is in Australia/Melbourne)
        convert_timezone('Australia/Perth', 'UTC', cast(shipped_at as timestamp))   as shipped_at_utc,
        convert_timezone('Australia/Perth', 'UTC', cast(delivered_at as timestamp)) as delivered_at_utc,

        cast(ship_cost as numeric(12, 2)) as ship_cost,

        -- Handle nulls with sensible defaults
        coalesce(trim(carrier), 'Unknown Carrier') as cleaned_carrier,
        coalesce(ship_cost, 0.00) as cleaned_ship_cost
    from src
),

-- Step 2: Deduplication by shipment_id
deduped as (
    select *
    from (
        select *,
               row_number() over (
                   partition by shipment_id
                   order by shipped_at_utc desc nulls last
               ) as rn
        from cleaned
    )
    where rn = 1
)

-- Final Output
select
    shipment_id,
    order_id,
    cleaned_carrier as carrier,
    shipped_at_utc,
    delivered_at_utc,
    cleaned_ship_cost as ship_cost
from deduped;
