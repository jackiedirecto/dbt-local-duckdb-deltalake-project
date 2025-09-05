# Mid-Level Data Engineer Assessment (Local Only)

## Overview
This is a hands-on data engineering assessment where you will build a complete data pipeline for a fictional retail company using local tools only. You will demonstrate your ability to generate synthetic data, implement medallion architecture, perform data transformations, and create business intelligence reports.

## Assessment Structure

This bundle contains:
- `/docs` — Step-by-step exercise instructions (start with `00_overview.md`)
- `/schemas` — PyArrow schema definitions for raw tables (do not modify)
- `/scripts` — Starter templates for your implementation
- `/dbt` — dbt-duckdb project skeleton to extend
- `/lake` — Output directory for Bronze/Silver/Gold layers (create if missing)
- `/duckdb` — Database file location (create if missing)
- `/analytics` — Final Power BI report location (create if missing)

## Prerequisites
- Python 3.10+ installed
- Power BI Desktop (for final visualization task)
- 8GB+ available RAM
- ~5GB disk space for generated data

## Getting Started

### 1. Fork and Setup Repository

**Create Your Own Copy:**
1. **Fork this repository** on GitHub:
   - Click the "Fork" button in the top-right corner
   - This creates your own copy where you can make changes
   - Clone your forked repository locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/local_de_assessment_bundle.git
   cd local_de_assessment_bundle
   ```

2. **Configure Git for Regular Commits:**
   ```bash
   # Set up your Git identity (if not already done)
   git config user.name "Your Name"
   git config user.email "your.email@example.com"
   
   # Create a development branch (optional but recommended)
   git checkout -b assessment-solution
   ```

3. **Modify .gitignore for Assessment Submission:**
   
   The current `.gitignore` excludes data directories to keep the template clean. For your assessment submission, you'll need to include sample data and outputs. Create a custom `.gitignore` for your fork:

   ```bash
   # Backup the original
   cp .gitignore .gitignore.original
   
   # Edit .gitignore to allow sample data (but not full datasets)
   # Remove or comment out these lines:
   # data_raw/
   # lake/
   # duckdb/
   # analytics/*.pbix
   ```

   **Instead, add these specific exclusions:**
   ```bash
   # Add to your .gitignore
   
   # Exclude large datasets but keep samples
   data_raw/*
   !data_raw/README.md
   !data_raw/samples/
   
   # Exclude full lake but keep structure and samples  
   lake/bronze/parquet/*
   lake/bronze/delta/*
   !lake/bronze/parquet/.gitkeep
   !lake/bronze/delta/.gitkeep
   !lake/bronze/parquet/customers/sample_*.parquet
   !lake/bronze/delta/customers/sample_*.parquet
   
   # Include your DuckDB database (if reasonable size)
   # Comment out: *.duckdb
   # Or include only if < 50MB
   
   # Include your Power BI file
   # Comment out: analytics/*.pbix
   ```

### 2. Setup Environment

Using uv (recommended - faster):
```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
uv pip install -r requirements.txt

# Create required directories
mkdir -p lake duckdb analytics data_raw
mkdir -p lake/bronze/parquet lake/bronze/delta lake/_rejects
```

Or using traditional pip:
```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create required directories
mkdir -p lake duckdb analytics data_raw
mkdir -p lake/bronze/parquet lake/bronze/delta lake/_rejects
```

### 3. Commit Workflow - IMPORTANT!

**Make regular commits after each exercise to demonstrate your progress:**

```bash
# After completing Exercise 1 (schema review)
git add docs/
git commit -m "Exercise 1: Completed schema analysis and documentation review

- Reviewed all 11 data source schemas
- Understood relationships and constraints
- Documented assumptions in notes"

# After completing Exercise 2 (data generation)
git add scripts/generate_data.py data_raw/samples/
git commit -m "Exercise 2: Implemented synthetic data generation

- Created generate_data.py with all 11 data sources
- Added controlled anomalies (1% FK violations, 0.5% malformed emails)
- Generated sample datasets for testing
- Implemented partitioning for facts tables
- Added CLI with --scale parameter for testing"

# After completing Exercise 3 (bronze ingestion)
git add scripts/load_to_bronze.py lake/bronze/parquet/samples/ duckdb/
git commit -m "Exercise 3: Built Bronze layer ingestion pipeline

- Implemented robust schema validation using PyArrow
- Added dual output formats (Parquet + Delta)
- Built idempotency with DuckDB manifest tracking  
- Handled rejects with reason codes
- Added audit columns (ingestion_ts, src_filename, src_row_hash)"

# Or if using DLT option:
git commit -m "Exercise 3: Built Bronze layer with DLT pipeline

- Implemented DLT-based ingestion pipeline
- Configured automatic schema validation and evolution
- Set up incremental loading with deduplication
- Added multiple destination support (DuckDB + Parquet + Delta)
- Implemented error handling and retries"

# After completing Exercise 4 (Silver layer)
git add dbt/ 
git commit -m "Exercise 4: Implemented dbt Silver layer transformations

- Created staging models for all data sources
- Built comprehensive data quality tests (unique, not_null, relationships)
- Implemented custom tests for business rules
- Added incremental fact models with merge strategy
- Created products snapshot for SCD2 tracking"

# After completing Exercise 5 (Gold layer) 
git add dbt/models/gold/
git commit -m "Exercise 5: Designed dimensional model in Gold layer

- Created star schema with fact and dimension tables
- Implemented GDPR compliant customer masking
- Built SCD2 product dimension from snapshots
- Added comprehensive date dimension
- Created business-friendly views with calculated KPIs"

# After completing Exercise 6 (Power BI)
git add analytics/report.pbix
git commit -m "Exercise 6: Built Power BI business intelligence dashboard

- Connected to Gold layer via DuckDB ODBC
- Created 5 comprehensive dashboard pages
- Implemented all required DAX measures (YoY growth, CLV, return rates)
- Built executive summary with KPI cards and trend analysis
- Added data quality monitoring dashboard"

# After completing Exercise 7 (Operations)
git add scripts/ docs/operations_runbook.md
git commit -m "Exercise 7: Implemented operational testing and monitoring

- Demonstrated incremental processing with late-arriving data
- Tested Delta time travel and schema evolution
- Built data quality monitoring queries
- Implemented performance optimization (VACUUM, OPTIMIZE)
- Created operational runbook with recovery procedures"

# Final submission commit
git add .
git commit -m "Assessment Complete: End-to-end data pipeline implementation

SUMMARY:
- Generated 1M+ fact records across 11 data sources
- Built medallion architecture (Bronze → Silver → Gold)
- Implemented comprehensive data quality testing
- Created interactive Power BI dashboard
- Demonstrated operational resilience and monitoring

Architecture: Python/DLT → Parquet/Delta → dbt → DuckDB → Power BI
Total pipeline runtime: X minutes for full dataset"
```

**Push your work regularly:**
```bash
# Push after each major exercise
git push origin assessment-solution

# Or push to main branch if preferred
git push origin main
```

### 4. Follow the Documentation
Start with `/docs/00_overview.md` and proceed through each numbered document in order. Each document covers one part of the assessment:
- `01_raw_schemas.md` - Data specifications
- `02_data_generation.md` - Implement data generator
- `03_bronze_ingestion.md` - Build Bronze layer ingestion
- `04_dbt_silver.md` - Create Silver transformations
- `05_gold_model.md` - Design dimensional model
- `06_powerbi.md` - Build BI dashboard
- `07_reliability_ops.md` - Operational testing

## What You Will Build

You must implement:
1. **Data Generator** (`scripts/generate_data.py`) - Create realistic synthetic data with controlled anomalies
2. **Bronze Ingestion** (`scripts/load_to_bronze.py`) - Validate and load raw data to lake storage
3. **dbt Models** - Transform data through Silver and Gold layers
4. **Power BI Report** - Interactive business intelligence dashboard
5. **Documentation** - Brief post-mortem explaining your approach and decisions

## Evaluation Criteria

Your submission will be evaluated on:
- **Correctness**: Does the pipeline run end-to-end without errors?
- **Data Quality**: Are schemas enforced? Are anomalies handled appropriately?
- **Architecture**: Is the medallion architecture properly implemented?
- **Performance**: Can the solution handle the target data volumes?
- **Code Quality**: Is the code readable, maintainable, and well-structured?
- **Documentation**: Are your design decisions clearly explained?

## Important Notes

- **Do NOT modify** files in `/schemas/` - these are the canonical schema definitions
- **Do NOT hardcode solutions** - Your code should be parameterizable and reusable
- **Do document** any assumptions or design decisions in your post-mortem
- **Do test** at small scale first (use `--scale` parameters) before full runs
- The provided starter code is minimal - you must extend it significantly

## Submission Requirements

### Your GitHub Repository Must Include:

1. **All implemented code:**
   - `/scripts/generate_data.py` - Data generation implementation
   - `/scripts/load_to_bronze.py` OR `/scripts/bronze_dlt_pipeline.py` - Bronze ingestion
   - `/dbt/` - Complete dbt project with models, tests, snapshots
   - Any additional utilities you created

2. **Sample data for review:**
   - `data_raw/samples/` - Small representative samples from each source
   - `lake/bronze/parquet/samples/` - Sample Bronze outputs
   - `duckdb/warehouse.duckdb` - Your database (if < 50MB)

3. **Final deliverables:**
   - `analytics/report.pbix` - Power BI dashboard
   - `docs/post_mortem.md` - Your reflection document

4. **Clear Git history:**
   - Regular commits after each exercise
   - Descriptive commit messages showing progress
   - Branch/merge strategy if used

### Post-Mortem Document

Create `docs/post_mortem.md` covering:

```markdown
# Data Engineering Assessment - Post Mortem

## Architecture Decisions
- Why you chose Option A/B for Bronze ingestion
- Key design patterns used
- Trade-offs made for performance vs complexity

## Challenges Encountered
- Technical difficulties and how you solved them
- Data quality issues discovered and handled
- Performance bottlenecks and optimizations

## Data Quality Findings  
- Anomalies you injected and detection rates
- Unexpected data patterns discovered
- Test failures and resolutions

## Performance Results
- Pipeline execution times by layer
- Data volumes processed
- Query performance benchmarks

## Production Recommendations
- What you would do differently in production
- Monitoring and alerting strategies
- Scaling considerations

## Time Investment
- Hours spent per exercise
- Most time-consuming aspects
- What you learned

## Tools Assessment
- Experience with new tools (DLT, dbt, Delta Lake)
- What worked well vs challenges
- Alternative approaches considered
```

### Repository Sharing

When ready for evaluation:

```bash
# Ensure your repository is public or invite reviewers
# Push final changes
git add .
git commit -m "Final submission: Assessment complete with post-mortem"
git push origin main

# Share your repository URL: 
# https://github.com/YOUR_USERNAME/local_de_assessment_bundle
```

**Important:** Your commit history is part of the evaluation - it demonstrates your development process, problem-solving approach, and professional Git practices.

Good luck!
