-- External views pointing at Bronze Parquet/Delta. Adjust path if needed.
{% set lake_root = '../lake/bronze' %}

-- For customers
create or replace view bronze_customers_parquet as
select * from read_parquet('{{ lake_root }}/parquet/customers/*.parquet');

create or replace view bronze_customers_delta as
select * from delta_scan('{{ lake_root }}/delta/customers');

-- For products
create or replace view bronze_products_parquet as
select * from read_parquet('{{ lake_root }}/parquet/products/*.parquet');

create or replace view bronze_products_delta as
select * from delta_scan('{{ lake_root }}/delta/products');

-- For stores
create or replace view bronze_stores_parquet as
select * from read_parquet('{{ lake_root }}/parquet/stores/*.parquet');

create or replace view bronze_stores_delta as
select * from delta_scan('{{ lake_root }}/delta/stores');

-- For suppliers
create or replace view bronze_suppliers_parquet as
select * from read_parquet('{{ lake_root }}/parquet/suppliers/*.parquet');

create or replace view bronze_suppliers_delta as
select * from delta_scan('{{ lake_root }}/delta/suppliers');

-- For exchange_rates
create or replace view bronze_exchange_rates_parquet as
select * from read_parquet('{{ lake_root }}/parquet/exchange_rates/*.parquet');

create or replace view bronze_exchange_rates_delta as
select * from delta_scan('{{ lake_root }}/delta/exchange_rates');

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

-- For returns
create or replace view bronze_returns_parquet as
select * from read_parquet('{{ lake_root }}/parquet/returns/*.parquet');

create or replace view bronze_returns_delta as
select * from delta_scan('{{ lake_root }}/delta/returns');

-- For shipments
create or replace view bronze_shipments_parquet as
select * from read_parquet('{{ lake_root }}/parquet/shipments/*.parquet');

create or replace view bronze_shipments_delta as
select * from delta_scan('{{ lake_root }}/delta/shipments');