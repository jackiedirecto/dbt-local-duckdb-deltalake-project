# Exercise 2: Data Generation

## Objective
Implement a Python script that generates realistic synthetic data for a retail company with controlled anomalies for testing data quality handling.

## Your Task
Complete the implementation of `/scripts/generate_data.py` to generate all required datasets as specified in `/docs/01_raw_schemas.md`.

## Requirements

### Core Functionality
1. **Command-line interface** with arguments:
   - `--seed`: For reproducible data generation
   - `--out`: Output directory (default: `data_raw`)
   - `--scale`: Scaling factor for quick testing (e.g., 0.01 for 1% of target rows)

2. **Generate 11 different data sources** in various formats:
   - CSV: customers, products, stores, suppliers, orders (header/lines), sensors
   - Parquet: shipments
   - XLSX: exchange_rates
   - JSONL: events
   - Delta: returns (with schema evolution)

3. **Match schema definitions** from `/schemas/schemas.py`:
   - Use the PyArrow schemas as your source of truth
   - Do not hardcode column definitions in multiple places

4. **Inject controlled anomalies** for testing:
   - ~1% foreign key violations
   - 0.05% duplicate primary keys
   - 0.5-1% malformed data (emails, dates, etc.)
   - Out-of-range values for sensors
   - Document what anomalies you inject and where

### Data Volume Targets (at scale=1.0)
- Fact tables: ≥1M rows (orders, events, sensors)
- Dimension tables: 5k-100k rows as specified
- See `/docs/01_raw_schemas.md` for exact targets per table

### Implementation Hints

**Recommended Libraries:**
- `faker` or `mimesis`: Generate realistic names, addresses, emails
- `numpy`: Create statistical distributions for prices, quantities
- `rstr`: Generate pattern-based IDs (e.g., `CUST-[A-Z0-9]{8}`)
- `pyarrow`: Fast CSV/Parquet writing
- `xlsxwriter` or `openpyxl`: Excel file generation
- `deltalake`: Delta format for returns table

**Performance Tips:**
- Generate data in batches to manage memory
- Use vectorized operations where possible
- Consider multiprocessing for large fact tables
- Partition output files by date for fact tables

**Data Realism:**
- Use appropriate distributions (normal for measurements, power law for purchases)
- Maintain referential integrity for most records
- Create temporal patterns (weekday vs weekend sales)
- Include edge cases (null values where allowed, boundary values)

## Testing Your Implementation

Start with a small scale to verify correctness:
```bash
# Generate 1% of target data for quick testing
python scripts/generate_data.py --seed 42 --scale 0.01 --out data_raw

# Check output structure
find data_raw -type f -name "*.csv" | head -10
du -sh data_raw/*
```

## Evaluation Criteria

Your data generator will be evaluated on:
1. **Schema Compliance**: Generated data matches PyArrow schemas exactly
2. **Reproducibility**: Same seed produces identical data
3. **Realism**: Data distributions and relationships make business sense
4. **Anomaly Injection**: Controlled bad data is present and documented
5. **Performance**: Can generate target volumes in reasonable time
6. **Code Quality**: Clean, modular, well-commented code

## Common Pitfalls to Avoid
- Don't forget to handle time zones consistently
- Ensure decimal precision matches schema specifications
- Maintain reasonable cardinality for categorical fields
- Don't generate impossible business scenarios (e.g., delivery before shipping)
- Remember to create directory structures for partitioned outputs

## What NOT to Do
- Do not modify the schema definitions in `/schemas/schemas.py`
- Do not generate all data in memory at once
- Do not create unrealistic data just to meet row counts
- Do not skip anomaly injection - it's required for testing

