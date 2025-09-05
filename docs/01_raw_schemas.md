# Exercise 1: Understanding Data Schemas

## Objective
Familiarize yourself with the data schemas you'll be working with throughout this assessment. These schemas represent a fictional retail company's data ecosystem.

## Your Task
Review and understand the 11 different data sources defined in `/schemas/schemas.py`. You'll need to generate data matching these exact schemas in Exercise 2.

## Schema Overview

### Dimension Tables (Master Data)

#### 1. Customers (CSV format)
- **Target Rows**: ~80,000
- **Primary Key**: `customer_id` (BIGINT)
- **Natural Key**: `natural_key` (pattern: `CUST-[A-Z0-9]{8}`)
- **Key Fields**:
  - Personal: first_name, last_name, email, phone, birth_date
  - Address: address_line1, address_line2, city, state_region, postcode, country_code
  - Location: latitude, longitude
  - Metadata: join_ts, is_vip, gdpr_consent
- **Required Anomalies**: 
  - 0.5-1% malformed emails
  - 0.2% duplicate natural_keys
  - Some null phone/address fields

#### 2. Products (CSV format)
- **Target Rows**: ~25,000
- **Primary Key**: `product_id` (BIGINT)
- **Natural Key**: `sku` (pattern: `SKU-[A-Z0-9]{6}`)
- **Key Fields**:
  - Product info: name, category, subcategory
  - Pricing: current_price (DECIMAL 12,4), currency
  - Lifecycle: introduced_dt, discontinued_dt, is_discontinued
- **Required Anomalies**: 
  - 0.1-0.5% missing or invalid prices
  - Some discontinued products with null discontinued_dt

#### 3. Stores (CSV format)
- **Target Rows**: ~5,000
- **Primary Key**: `store_id` (BIGINT)
- **Key Fields**:
  - Identification: store_code, name
  - Operations: channel (web/pos), region, state
  - Location: latitude, longitude
  - Lifecycle: open_dt, close_dt (nullable for active stores)
- **Required Anomalies**: 
  - Some impossible lat/lon values
  - Occasional duplicate store_codes

#### 4. Suppliers (CSV format)
- **Target Rows**: ~8,000
- **Primary Key**: `supplier_id` (BIGINT)
- **Key Fields**:
  - Identification: supplier_code, name
  - Operations: country_code, lead_time_days
  - Status: preferred (BOOLEAN)

### Fact Tables (Transactional Data)

#### 5. Orders Header (CSV format, partitioned)
- **Target Rows**: ≥1,000,000
- **Primary Key**: `order_id` (BIGINT)
- **Partitioning**: By `order_dt` (e.g., `orders/order_dt=2024-01-01/`)
- **Key Fields**:
  - Temporal: order_ts (TIMESTAMP with TZ), order_dt_local (DATE)
  - References: customer_id, store_id
  - Transaction: channel, payment_method, coupon_code
  - Financial: shipping_fee, currency
- **Required Anomalies**: 
  - ~1% foreign key violations (invalid customer_id/store_id)
  - 0.05% duplicate order_ids across files

#### 6. Orders Lines (CSV format, partitioned)
- **Target Rows**: ~3-4,000,000
- **Composite Key**: (`order_id`, `line_number`)
- **Partitioning**: Match orders_header partitioning
- **Key Fields**:
  - References: order_id, product_id
  - Transaction: qty, unit_price (DECIMAL 12,4)
  - Discounts: line_discount_pct, tax_pct (DECIMAL 5,4)
- **Required Anomalies**: 
  - 1% invalid product_ids
  - Rare negative quantities or zero prices

### Event and IoT Data

#### 7. Events (JSONL format)
- **Target Rows**: ~2,000,000
- **Format**: One JSON object per line
- **Schema**: Flexible JSON with envelope + payload
- **Expected Fields**:
  - Envelope: event_id, event_ts, event_type, user_id, session_id
  - Payload: Variable JSON content
- **Partitioning**: By event date
- **Required Anomalies**: 
  - 0.05% malformed JSON lines
  - Missing required envelope fields

#### 8. Sensors (CSV format)
- **Target Rows**: 5-10,000,000
- **Partitioning**: By store_id and month
- **Key Fields**:
  - Temporal: sensor_ts (TIMESTAMP)
  - References: store_id, shelf_id
  - Measurements: temperature_c, humidity_pct (DECIMAL 5,2)
  - Metadata: battery_mv (INTEGER)
- **Required Anomalies**: 
  - 0.1-0.5% out-of-range temperature/humidity
  - Occasional missing sensor_ts

### Financial and Operational Data

#### 9. Exchange Rates (XLSX format)
- **Target Rows**: ~1,100 (3 years daily)
- **Format**: Excel workbook
- **Key Fields**:
  - date (DATE)
  - currency (STRING)
  - rate_to_aud (DECIMAL 18,8)
- **Note**: Include weekends (same rate or interpolated)

#### 10. Shipments (Parquet format)
- **Target Rows**: ~1,000,000
- **Primary Key**: `shipment_id` (BIGINT)
- **Format**: Columnar Parquet files
- **Key Fields**:
  - References: order_id
  - Operations: carrier, shipped_at, delivered_at (nullable)
  - Financial: ship_cost (DECIMAL 12,2)
- **Required Anomalies**: 
  - Null delivered_at for in-transit shipments
  - Some late deliveries (delivered_at > shipped_at + SLA)

#### 11. Returns (Delta format with schema evolution)
- **Target Rows**: ~100,000
- **Primary Key**: `return_id` (BIGINT)
- **Special Requirements**: 
  - Initial version with base schema
  - Second version adds `return_reason_code` column
  - Demonstrate UPSERT and DELETE operations
- **Key Fields**:
  - References: order_id, product_id
  - Transaction: return_ts, qty, reason
  - Evolution: return_reason_code (added in v2)

## Data Type Specifications

All schemas use PyArrow types as defined in `/schemas/schemas.py`:
- **Integers**: `pa.int32()`, `pa.int64()`
- **Strings**: `pa.string()`
- **Decimals**: `pa.decimal128(precision, scale)`
- **Dates**: `pa.date32()`
- **Timestamps**: `pa.timestamp("us")` - microsecond precision
- **Booleans**: `pa.bool_()`

## Important Constraints

### Temporal Consistency
- All timestamps should be timezone-aware
- Order dates must be before shipment dates
- Shipment dates must be before delivery dates
- Return dates must be after order dates

### Referential Integrity
- Most foreign keys should be valid (99%)
- Include controlled violations for testing (1%)
- Document where violations are injected

### Business Rules
- Prices must be positive (except for anomalies)
- Discounts between 0-100%
- Geographic coordinates within valid ranges
- Email addresses should follow standard format

## File Naming Conventions

Follow these patterns for generated files:
- Dimensions: `{table_name}.csv` (e.g., `customers.csv`)
- Partitioned facts: `{table}/order_dt={date}/part-{num}.csv`
- Events: `events/event_dt={date}/events_{num}.jsonl`
- Sensors: `sensors/store_id={id}/month={YYYY-MM}/sensors.csv`

## Validation Checklist

Before proceeding to Exercise 2, ensure you understand:
- [ ] The purpose of each table in the business context
- [ ] Required data types and precision for each field
- [ ] Partitioning strategies for large tables
- [ ] Expected anomalies and their rates
- [ ] Relationships between tables (foreign keys)
- [ ] File formats and naming conventions

## Notes

- The schemas in `/schemas/schemas.py` are your source of truth
- Do not modify these schema definitions
- Your generated data must be castable to these schemas
- Include both valid data and controlled anomalies
- Document any assumptions you make about the data

This schema understanding forms the foundation for all subsequent exercises in the assessment.