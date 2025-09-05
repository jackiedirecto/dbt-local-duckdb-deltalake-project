# Exercise 4: dbt Silver Layer Transformations

## Objective
Build the Silver layer using dbt to clean, standardize, and enrich Bronze data while implementing data quality tests and incremental processing.

## Your Task
Extend the dbt project in `/dbt` to create staging models, implement tests, and build incremental fact tables.

## Requirements

### 1. Staging Models (`models/staging/`)
Create `stg_*` models for each Bronze table with:
- **Data Cleaning**:
  - Normalize column names (snake_case)
  - Convert timestamps to UTC
  - Trim whitespace from strings
  - Handle null values appropriately

- **Data Enrichment**:
  - Add derived columns (e.g., age from birth_date)
  - Parse JSON fields into structured columns
  - Calculate business metrics (e.g., order_total)

- **Deduplication**:
  - Remove duplicate orders by order_id
  - Handle duplicate customers by natural_key
  - Document your dedup strategy

### 2. Data Quality Tests (`tests/`)
Implement comprehensive testing:

**Built-in Tests:**
- `unique` on all primary keys
- `not_null` on required fields
- `relationships` for foreign keys
- `accepted_values` for enums

**Custom Tests:**
- Discount percentages between 0-100%
- Latitude between -90 and 90
- Longitude between -180 and 180
- Foreign key violation rate < 1.5%
- Email format validation

**Source Freshness:**
```yaml
sources:
  - name: bronze
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
```

### 3. Incremental Models (`models/silver/`)
Build fact tables with incremental logic:

**Example Structure:**
```sql
{{
  config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='merge'
  )
}}

SELECT 
    -- your transformations
FROM {{ source('bronze', 'orders') }}

{% if is_incremental() %}
  WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
{% endif %}
```

### 4. Snapshot Models (`snapshots/`)
Track slowly changing dimensions:
- `products_snapshot`: Track price changes
- `customers_snapshot`: Track address changes
- Use `updated_at` strategy or `check` strategy

## Implementation Structure

```
dbt/
├── models/
│   ├── staging/
│   │   ├── _sources.yml          # Define Bronze sources
│   │   ├── stg_customers.sql     # Clean customers data
│   │   ├── stg_orders.sql        # Clean orders data
│   │   └── stg_*.sql             # Other staging models
│   └── silver/
│       ├── fct_sales.sql         # Incremental sales fact
│       └── intermediate/         # Helper models
├── tests/
│   ├── generic/                  # Custom test definitions
│   └── singular/                  # One-off SQL tests
├── snapshots/
│   └── products_snapshot.sql     # SCD2 for products
└── macros/
    └── get_custom_schema.sql     # Schema management

```

## Key Concepts to Demonstrate

### Incremental Processing
- Use `is_incremental()` macro effectively
- Handle late-arriving data
- Implement merge strategies for updates

### Data Contracts
```yaml
models:
  - name: stg_customers
    config:
      contract:
        enforced: true
    columns:
      - name: customer_id
        data_type: bigint
        constraints:
          - type: not_null
```

### Macros for Reusability
Create custom macros for common transformations:
```sql
{% macro normalize_timestamp(column_name) %}
    CAST({{ column_name }} AS TIMESTAMP) AT TIME ZONE 'UTC'
{% endmacro %}
```

## Testing Your Implementation

```bash
cd dbt

# Install dependencies
dbt deps

# Test connection
dbt debug

# Run all models
dbt run

# Run tests
dbt test

# Run specific model
dbt run --select stg_customers

# Generate documentation
dbt docs generate
dbt docs serve
```

## Evaluation Criteria

1. **Data Quality**: Are anomalies properly handled?
2. **Test Coverage**: Are all critical paths tested?
3. **Incremental Logic**: Does it handle updates efficiently?
4. **Documentation**: Are models well-documented?
5. **Performance**: Do queries run efficiently?
6. **Best Practices**: Proper use of dbt features?

## Common Pitfalls to Avoid
- Don't forget to handle timezone conversions
- Test for both positive and negative cases
- Ensure incremental models are truly incremental
- Don't hardcode database/schema names
- Remember to document model purposes

## What NOT to Do
- Do not bypass failed tests
- Do not create models without documentation
- Do not ignore data quality issues
- Do not create unnecessary intermediate models
