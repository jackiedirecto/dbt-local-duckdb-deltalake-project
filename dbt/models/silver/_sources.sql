-- External views pointing at Bronze Parquet/Delta. Adjust path if needed.
{% set lake_root = '../lake/bronze' %}

-- For orders
create or replace view bronze_orders_parquet as
select * from read_parquet('{{ lake_root }}/parquet/orders/order_dt=*/*.parquet');

create or replace view bronze_orders_delta as
select * from delta_scan('{{ lake_root }}/delta/orders/order_dt=*');

-- For orders_lines
create or replace view bronze_orders_lines_parquet as
select * from read_parquet('{{ lake_root }}/parquet/orders_lines/order_dt=*/*.parquet');

create or replace view bronze_orders_lines_delta as
select * from delta_scan('{{ lake_root }}/delta/orders_lines/order_dt=*');

-- For events
create or replace view bronze_events_parquet as
select * from read_parquet('{{ lake_root }}/parquet/events/event_dt=*/*.parquet');

create or replace view bronze_events_delta as
select * from delta_scan('{{ lake_root }}/delta/events/event_dt=*');

