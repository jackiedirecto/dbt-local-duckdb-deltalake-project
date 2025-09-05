# Exercise 7: Reliability & Operations Testing

## Objective
Demonstrate that your pipeline is production-ready by testing incremental processing, handling late data, managing schema evolution, and monitoring data quality.

## Your Task
Complete a series of operational scenarios to prove your pipeline's reliability and maintainability.

## Required Scenarios

### 1. Incremental Data Processing

**Setup:**
- Generate 2 additional days of order data
- Include one file arriving 24 hours late (backdated)

**Demonstrate:**
- Bronze layer processes only new files (idempotency)
- Silver incremental models update correctly
- Gold layer reflects new data accurately
- Late-arriving data is handled properly

**Evidence to Provide:**
```sql
-- Show row counts before and after
SELECT COUNT(*), MAX(order_dt) FROM bronze.orders;
-- Run your incremental load
SELECT COUNT(*), MAX(order_dt) FROM bronze.orders;
```

### 2. Delta Time Travel

**Demonstrate:**
- Query historical versions of returns table
- Show data changes between versions
- Recover from accidental deletes/updates

**Example Commands:**
```sql
-- View version history
DESCRIBE HISTORY delta_scan('lake/bronze/delta/returns');

-- Query specific version
SELECT * FROM delta_scan('lake/bronze/delta/returns', version=2);

-- Compare versions
WITH v1 AS (SELECT * FROM delta_scan('lake/bronze/delta/returns', version=1)),
     v2 AS (SELECT * FROM delta_scan('lake/bronze/delta/returns', version=2))
SELECT * FROM v2 EXCEPT SELECT * FROM v1;
```

### 3. Schema Evolution Handling

**Setup:**
- Add new column `return_reason_code` to returns data
- Generate new batch with this column populated

**Demonstrate:**
- Bronze layer handles schema change gracefully
- dbt models adapt or flag the change appropriately
- Document your schema evolution strategy

### 4. Data Quality Monitoring

**Implement Checks For:**
- Foreign key violation trends
- Null value percentages
- Data freshness SLAs
- Anomaly detection rates

**Create Monitoring Query:**
```sql
-- Example monitoring dashboard query
SELECT 
    table_name,
    check_type,
    check_date,
    pass_rate,
    CASE 
        WHEN pass_rate < 95 THEN 'ALERT'
        WHEN pass_rate < 99 THEN 'WARNING'
        ELSE 'OK'
    END AS status
FROM data_quality_metrics
WHERE check_date >= CURRENT_DATE - 7
ORDER BY pass_rate ASC;
```

### 5. Performance Optimization

**Tasks:**
- Run OPTIMIZE on Delta tables
- Implement VACUUM with retention policy
- Document partition pruning effectiveness
- Compare query performance before/after optimization

**Metrics to Capture:**
```sql
-- Capture query plans
EXPLAIN ANALYZE SELECT ... FROM your_table;

-- Measure partition pruning
SELECT * FROM table_partitions WHERE pruned = true;

-- File compaction stats
OPTIMIZE delta_table;
VACUUM delta_table RETAIN 168 HOURS;
```

## Advanced Challenges (Optional)

### 1. Change Data Capture (CDC) Simulation

Implement a CDC pattern:
- Track row-level changes in source systems
- Apply changes using MERGE operations
- Maintain audit history of all changes

### 2. Disaster Recovery Test

Simulate and recover from:
- Corrupted Bronze files
- Failed dbt run mid-transformation
- Accidental table deletion
- Schema breaking changes

### 3. Cost/Performance Analysis

Document:
- Storage footprint by layer (Bronze/Silver/Gold)
- Query performance benchmarks
- Compression ratios for different formats
- Optimal partition strategies

### 4. Pipeline Documentation

Generate:
- dbt documentation site
- Data lineage diagram (Mermaid/GraphViz)
- Table dependency matrix
- Column-level lineage tracking

## Testing Your Implementation

### Incremental Load Test
```bash
# Generate new data
python scripts/generate_data.py --seed 43 --start-date 2024-04-01 --days 2 --out data_raw_increment

# Run incremental ingestion
python scripts/load_to_bronze.py --raw data_raw_increment --lake lake --manifest duckdb/warehouse.duckdb

# Run dbt incremental models
cd dbt && dbt run --models tag:incremental

# Verify only new data processed
duckdb duckdb/warehouse.duckdb "SELECT date, count(*) FROM gold.fct_sales GROUP BY date ORDER BY date DESC LIMIT 5"
```

### Data Quality Test
```bash
# Run all dbt tests
cd dbt && dbt test

# Generate test results summary
dbt test --store-failures

# Check test history
SELECT * FROM dbt_test_results WHERE status = 'fail';
```

## Evaluation Criteria

1. **Idempotency**: Can safely re-run without duplicating data
2. **Incremental Processing**: Efficiently handles new data only
3. **Error Recovery**: Gracefully handles and recovers from failures
4. **Monitoring**: Comprehensive data quality tracking
5. **Performance**: Optimization techniques properly applied
6. **Documentation**: Clear operational runbooks provided

## Deliverables

Create an operations document including:
1. Screenshots/logs showing successful incremental runs
2. Query results demonstrating time travel capabilities
3. Performance benchmarks before/after optimization
4. Data quality monitoring dashboard/queries
5. Recovery procedures for common failure scenarios
6. Lessons learned and recommendations

## Common Pitfalls to Avoid
- Don't forget to test rollback scenarios
- Remember to document retention policies
- Don't skip performance baseline measurements
- Ensure monitoring covers all critical paths
- Test with realistic data volumes

## What NOT to Do
- Do not manually fix data issues without documenting
- Do not ignore failed tests in production
- Do not skip backup/recovery planning
- Do not overlook security considerations
