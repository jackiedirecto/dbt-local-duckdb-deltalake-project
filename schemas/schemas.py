from __future__ import annotations
import pyarrow as pa

customers_schema = pa.schema([
    pa.field("customer_id", pa.int64()),
    pa.field("natural_key", pa.string()),
    pa.field("first_name", pa.string()),
    pa.field("last_name", pa.string()),
    pa.field("email", pa.string()),
    pa.field("phone", pa.string()),
    pa.field("address_line1", pa.string()),
    pa.field("address_line2", pa.string()),
    pa.field("city", pa.string()),
    pa.field("state_region", pa.string()),
    pa.field("postcode", pa.string()),
    pa.field("country_code", pa.string()),
    pa.field("latitude", pa.float64()),
    pa.field("longitude", pa.float64()),
    pa.field("birth_date", pa.date32()),
    pa.field("join_ts", pa.timestamp("us")),  # normalize TZ downstream
    pa.field("is_vip", pa.bool_()),
    pa.field("gdpr_consent", pa.bool_()),
])

products_schema = pa.schema([
    pa.field("product_id", pa.int64()),
    pa.field("sku", pa.string()),
    pa.field("name", pa.string()),
    pa.field("category", pa.string()),
    pa.field("subcategory", pa.string()),
    pa.field("current_price", pa.decimal128(12, 4)),
    pa.field("currency", pa.string()),
    pa.field("is_discontinued", pa.bool_()),
    pa.field("introduced_dt", pa.date32()),
    pa.field("discontinued_dt", pa.date32()),
])

stores_schema = pa.schema([
    pa.field("store_id", pa.int64()),
    pa.field("store_code", pa.string()),
    pa.field("name", pa.string()),
    pa.field("channel", pa.string()),
    pa.field("region", pa.string()),
    pa.field("state", pa.string()),
    pa.field("latitude", pa.float64()),
    pa.field("longitude", pa.float64()),
    pa.field("open_dt", pa.date32()),
    pa.field("close_dt", pa.date32()),
])

suppliers_schema = pa.schema([
    pa.field("supplier_id", pa.int64()),
    pa.field("supplier_code", pa.string()),
    pa.field("name", pa.string()),
    pa.field("country_code", pa.string()),
    pa.field("lead_time_days", pa.int32()),
    pa.field("preferred", pa.bool_()),
])

orders_header_schema = pa.schema([
    pa.field("order_id", pa.int64()),
    pa.field("order_ts", pa.timestamp("us")),
    pa.field("order_dt_local", pa.date32()),
    pa.field("customer_id", pa.int64()),
    pa.field("store_id", pa.int64()),
    pa.field("channel", pa.string()),
    pa.field("payment_method", pa.string()),
    pa.field("coupon_code", pa.string()),
    pa.field("shipping_fee", pa.decimal128(12, 2)),
    pa.field("currency", pa.string()),
])

orders_lines_schema = pa.schema([
    pa.field("order_id", pa.int64()),
    pa.field("line_number", pa.int32()),
    pa.field("product_id", pa.int64()),
    pa.field("qty", pa.int32()),
    pa.field("unit_price", pa.decimal128(12, 4)),
    pa.field("line_discount_pct", pa.decimal128(5, 4)),
    pa.field("tax_pct", pa.decimal128(5, 4)),
])

events_schema = pa.schema([
    pa.field("json", pa.string()),
])

sensors_schema = pa.schema([
    pa.field("sensor_ts", pa.timestamp("us")),
    pa.field("store_id", pa.int64()),
    pa.field("shelf_id", pa.string()),
    pa.field("temperature_c", pa.decimal128(5, 2)),
    pa.field("humidity_pct", pa.decimal128(5, 2)),
    pa.field("battery_mv", pa.int32()),
])

exchange_rates_schema = pa.schema([
    pa.field("date", pa.date32()),
    pa.field("currency", pa.string()),
    pa.field("rate_to_aud", pa.decimal128(18, 8)),
])

shipments_schema = pa.schema([
    pa.field("shipment_id", pa.int64()),
    pa.field("order_id", pa.int64()),
    pa.field("carrier", pa.string()),
    pa.field("shipped_at", pa.timestamp("us")),
    pa.field("delivered_at", pa.timestamp("us")),
    pa.field("ship_cost", pa.decimal128(12, 2)),
])

returns_day1_schema = pa.schema([
    pa.field("return_id", pa.int64()),
    pa.field("order_id", pa.int64()),
    pa.field("product_id", pa.int64()),
    pa.field("return_ts", pa.timestamp("us")),
    pa.field("qty", pa.int32()),
    pa.field("reason", pa.string()),
])
