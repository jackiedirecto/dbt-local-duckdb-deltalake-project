bash
# Dry run first to validate
python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb --dry-run

# Full ingestion
python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb

# Verify outputs
find lake/bronze -name "*.parquet" | wc -l
find lake/_rejects -name "*.csv" | wc -l

# Check manifest in DuckDB
duckdb duckdb/warehouse.duckdb "SELECT * FROM manifest_processed_files LIMIT 5"
