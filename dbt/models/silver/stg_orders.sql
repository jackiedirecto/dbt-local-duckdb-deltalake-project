{{
  config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='merge'
  )
}}

with src as (
  select * from {{ source('bronze', 'orders') }}
),
typed as (
  select
    cast(order_id as bigint) as order_id,
    cast(order_ts as timestamp) as order_ts,
    cast(order_dt_local as date) as order_dt_local,
    cast(customer_id as bigint) as customer_id,
    cast(store_id as bigint) as store_id,
    channel as order_channel,
    payment_method as payment_method,
    coupon_code as coupon_code,
    cast(shipping_fee as numeric(12,2)) as shipping_fee,
    currency as currency,
    ingestion_ts
  from src
)

select * from typed

{% if is_incremental() %}
  where ingestion_ts > (select max(ingestion_ts) from {{ this }})
{% endif %}
