# Exercise 3: Bronze Ingestion Layer

## Objective
Build a robust ingestion pipeline that reads raw files, validates schemas, handles bad data, and writes to both Parquet and Delta Lake formats in the Bronze layer.

## Your Task
Choose one of two approaches to implement Bronze layer ingestion:
- **Option A**: Traditional Python script (`/scripts/load_to_bronze.py`) 
- **Option B**: DLT (Data Load Tool) pipeline (`/scripts/bronze_dlt_pipeline.py`)

## Requirements (Both Options)

### Core Functionality
1. **Read and validate** all file formats:
   - CSV files with schema enforcement
   - Parquet files natively
   - XLSX files (exchange rates)
   - JSONL files (events)
   - Delta files for incremental updates

2. **Schema validation**:
   - Enforce schemas from `/schemas/schemas.py`
   - Separate valid and invalid records
   - Write rejects with reason codes

3. **Dual output formats**:
   - Parquet: `/lake/bronze/parquet/<table>/`
   - Delta: `/lake/bronze/delta/<table>/`
   - Both formats must contain identical data

4. **Add audit columns**:
   - `ingestion_ts`: Timestamp of ingestion (UTC)
   - `src_filename`: Source file name
   - `src_row_hash`: Hash of original row for lineage

---

## Option A: Traditional Python Script

### Implementation Requirements
1. **Command-line interface** with arguments:
   - `--raw`: Input directory with raw files (default: `data_raw`)
   - `--lake`: Output lake directory (default: `lake`)
   - `--manifest`: DuckDB database for tracking (default: `duckdb/warehouse.duckdb`)
   - `--dry-run`: Validate without writing (optional)

2. **Manual implementation of**:
   - File reading and parsing
   - Schema validation using PyArrow
   - Error handling and reject management
   - Idempotency through manifest tracking
   - Partitioning and file optimization

### Advanced Requirements

**Idempotency:**
- Track processed files in DuckDB manifest table
- Support re-running without duplicating data
- Handle partial failures gracefully

**Partitioning Strategy:**
- Orders: Partition by `order_dt`
- Events: Partition by event date
- Sensors: Partition by `store_id` and month
- Other tables: No partitioning needed

**File Management:**
- Coalesce small files to ~100-250MB
- Maintain reasonable number of partitions
- Optimize for query performance

**Special Handling for Returns Table:**
- Implement UPSERT logic for Delta format
- Handle schema evolution (new columns)
- Support soft deletes with tombstone records

## Implementation Hints

**Schema Validation Approach:**
```python
# Example pattern (not complete solution)
try:
    table = csv.read_csv(file_path)
    table = table.cast(target_schema)  # Will fail if incompatible
except:
    # Handle validation errors
    # Write to rejects with reason
```

**Manifest Table Structure:**
```sql
CREATE TABLE IF NOT EXISTS manifest_processed_files (
    src_path TEXT PRIMARY KEY,
    processed_at TIMESTAMP,
    row_count BIGINT,
    reject_count BIGINT,
    status TEXT
)
```

**Delta Lake Operations:**
- Use `write_deltalake` with `mode='append'` for initial loads
- Use `merge` operations for upserts on returns table
- Enable `overwrite_schema=True` for schema evolution

---

## Option B: DLT (Data Load Tool) Pipeline

### Why DLT?
DLT is an open-source Python library that simplifies data pipeline development with:
- Automatic schema inference and evolution
- Built-in incremental loading
- Data type detection and normalization
- Declarative configuration
- Automatic retries and error handling

### Implementation Requirements

1. **Install DLT**:
```bash
pip install dlt[duckdb] dlt[parquet] dlt[filesystem]
```

2. **Create pipeline structure**:
```python
# scripts/bronze_dlt_pipeline.py
import dlt
from dlt.sources.filesystem import filesystem
import pyarrow as pa
from schemas.schemas import *

# Configure destinations
duckdb_dest = dlt.destinations.duckdb(
    credentials="duckdb/warehouse.duckdb"
)

parquet_dest = dlt.destinations.filesystem(
    bucket_url="lake/bronze/parquet",
    file_format="parquet"
)
```

3. **Define sources and resources**:
```python
@dlt.source(name="retail_bronze")
def retail_source(raw_path: str = "data_raw"):
    
    @dlt.resource(
        name="customers",
        write_disposition="replace",
        columns=customers_schema  # Use PyArrow schema
    )
    def load_customers():
        # Read CSV and yield data
        # DLT handles schema validation automatically
        pass
    
    @dlt.resource(
        name="orders",
        write_disposition="append",
        primary_key="order_id",
        merge_key="order_id"
    )
    def load_orders():
        # Incremental loading with automatic dedup
        pass
    
    @dlt.transformer(
        data_from=load_customers,
        write_disposition="replace"
    )
    def add_audit_columns(record):
        # Add ingestion_ts, src_filename, etc.
        return {
            **record,
            "ingestion_ts": datetime.utcnow(),
            "src_filename": dlt.current.source_state().get("file")
        }
    
    return [
        add_audit_columns,
        load_orders,
        # ... other resources
    ]
```

4. **Configure data quality checks**:
```python
# Use DLT's built-in validators
@dlt.resource(
    name="customers",
    columns={
        "email": {"data_type": "text", "nullable": False},
        "customer_id": {"data_type": "bigint", "unique": True},
        "gdpr_consent": {"data_type": "bool"}
    },
    schema_contract_settings={
        "data_type": "evolve",  # Allow schema evolution
        "columns": "complete"   # But require all defined columns
    }
)
```

5. **Handle multiple destinations**:
```python
def run_bronze_pipeline():
    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="retail_bronze",
        destination=duckdb_dest,
        dataset_name="bronze"
    )
    
    # Load to DuckDB
    load_info = pipeline.run(retail_source())
    
    # Also write to Parquet
    pipeline.destination = parquet_dest
    pipeline.run(retail_source())
    
    # Handle Delta format separately
    write_to_delta(pipeline.last_trace.last_extract_info)
```

### Key DLT Features to Demonstrate

1. **Incremental Loading**:
```python
@dlt.resource(
    name="orders",
    write_disposition="merge",
    primary_key="order_id"
)
def orders_incremental(updated_after=dlt.sources.incremental("order_ts")):
    # DLT tracks last loaded timestamp automatically
    for file in get_order_files():
        data = read_file(file)
        yield from data.filter(lambda x: x["order_ts"] > updated_after.last_value)
```

2. **Schema Evolution**:
```python
# DLT handles new columns automatically
@dlt.resource(schema_contract_settings={"columns": "evolve"})
def returns_with_evolution():
    # First batch without return_reason_code
    # Second batch with return_reason_code
    # DLT adapts automatically
```

3. **Error Handling**:
```python
# Configure retry and error policies
@dlt.resource(
    max_retries=3,
    retry_delay=1.0
)
def sensitive_data_source():
    # DLT handles transient failures
    pass

# Access failed records
load_info = pipeline.run(source)
for package in load_info.load_packages:
    if package.jobs["failed_jobs"]:
        # Write to rejects folder
        write_rejects(package.jobs["failed_jobs"])
```

### DLT-Specific Testing

```bash
# Run DLT pipeline
python scripts/bronze_dlt_pipeline.py

# Check DLT state and schema
dlt pipeline retail_bronze info
dlt pipeline retail_bronze trace

# Verify incremental loading
dlt pipeline retail_bronze load-info

# Test schema evolution
python scripts/bronze_dlt_pipeline.py --evolve-schema
```

### Advantages of DLT Approach
- Less boilerplate code
- Automatic schema inference and validation
- Built-in incremental loading and deduplication
- Automatic retries and error handling
- Schema evolution support out of the box
- Pipeline observability and tracing

### When to Choose DLT
Choose Option B if you want to demonstrate:
- Modern data engineering practices
- Declarative pipeline design
- Understanding of ELT patterns
- Ability to work with pipeline frameworks

---

## Testing Your Implementation

### Option A Testing:
```bash
# Dry run first to validate
python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb --dry-run

# Full ingestion
python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb

# Verify outputs
find lake/bronze -name "*.parquet" | wc -l
find lake/_rejects -name "*.csv" | wc -l

# Check manifest in DuckDB
duckdb duckdb/warehouse.duckdb "SELECT * FROM manifest_processed_files LIMIT 5"
```

### Option B Testing:
```bash
# Run DLT pipeline
python scripts/bronze_dlt_pipeline.py

# Check pipeline status
dlt pipeline retail_bronze info

# Verify incremental state
dlt pipeline retail_bronze trace

# Check loaded data
duckdb duckdb/warehouse.duckdb "SELECT COUNT(*) FROM bronze.customers"
```

## Evaluation Criteria

Your ingestion pipeline will be evaluated on:
1. **Correctness**: All valid data is ingested with proper schemas
2. **Error Handling**: Invalid records are properly rejected with reasons
3. **Idempotency**: Re-running doesn't duplicate data
4. **Audit Trail**: Proper tracking of what was processed when
5. **Performance**: Efficient handling of large files
6. **Code Quality**: Modular, maintainable implementation

## Common Pitfalls to Avoid
- Don't load entire files into memory at once
- Remember to handle missing files gracefully
- Ensure timestamp columns maintain timezone info
- Don't forget to create partition directories
- Handle concurrent runs (file locking if needed)

## What NOT to Do
- Do not skip schema validation to "make it work"
- Do not silently drop invalid records
- Do not modify the source schemas to fit your data
- Do not write Bronze data without audit columns

