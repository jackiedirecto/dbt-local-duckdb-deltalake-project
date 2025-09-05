import argparse, pathlib, os, hashlib, json, datetime as dt
import sys
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent))
import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as pads
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.json as pajson
import pandas as pd
from datetime import datetime
try:
    from deltalake import write_deltalake
except Exception as e:
    write_deltalake = None
# Define the lakehouse directory
def ensure_dirs(lake_root):
    for sub in ['bronze/parquet','bronze/delta']:
        (lake_root/sub).mkdir(parents=True, exist_ok=True)
    (lake_root/'_rejects').mkdir(parents=True, exist_ok=True)

def init_manifest(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS manifest_processed_files (
            src_path TEXT PRIMARY KEY,
            processed_at TIMESTAMP,
            row_count BIGINT,
            reject_count BIGINT,
            status TEXT
        )
    """)

def already_processed(conn, p):
    print('already processed') 
    return conn.execute("SELECT 1 FROM manifest_processed_files WHERE src_path = ?", [str(p)]).fetchone() is not None

def mark_processed(conn, p, n, r, status): 
    print('mark processed')
    conn.execute("INSERT OR REPLACE INTO manifest_processed_files VALUES (?, ?, ?, ?, ?)", [str(p), dt.datetime.now(dt.UTC), n, r, status])

# Function to write to parquet, add partitioning
def write_parquet_partitioned(table, base_path, partitioning=None):
    pads.write_dataset(table, base_dir=str(base_path), format='parquet', partitioning=partitioning, existing_data_behavior='overwrite_or_ignore')

# Function to load data in deltalake
def write_delta(table: pa.Table, base_path: str | pathlib.Path, mode: str = "append", partition_by: list[str] | str | None = None, merge_schema: bool = True
):
    if write_deltalake is None:
        raise RuntimeError("deltalake not installed")

    if not isinstance(table, pa.Table):
        raise TypeError("Expected a pyarrow.Table for `table`")

    schema_mode = "merge" if merge_schema else None

    write_deltalake(
        table_or_uri = str(base_path),
        data = table,
        partition_by = partition_by,
        mode = mode,
        schema_mode = schema_mode
    )

# Function to handle upsert
def upsert_delta(table, base_path, merge_schema = True):
    if write_deltalake is None:
        raise RuntimeError("deltalake not installed")

    if not isinstance(table, pa.Table):
        raise TypeError("Expected a pyarrow.Table for `table`")

    schema_mode = "merge" if merge_schema else None

    try:
        partition_by = "is_deleted" if "is_deleted" in table.schema.names else None
        write_deltalake(
            table_or_uri = str(base_path),
            data = table,
            mode = "append",
            schema_mode = schema_mode,
            partition_by = partition_by
        )
        print("UPSERT to Delta Lake successful.")
    except Exception as e:
        print(f"Failed to UPSERT to Delta Lake: {e}")
        raise

# Validate Partition folders by order_date
def is_valid_partition_folder(folder_name, prefix="order_dt="):
    if not folder_name.startswith(prefix):
        return False
    date_str = folder_name[len(prefix):]
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False
    
# Validate Partition folders by event_date
def is_valid_partition_folder_event(folder_name, prefix="event_dt="):
    if not folder_name.startswith(prefix):
        return False
    date_str = folder_name[len(prefix):]
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

# Validate Month
def is_valid_month(month_str):
    try:
        datetime.strptime(month_str, "%Y-%m")
        return True
    except ValueError:
        return False


# Adds ingestion_ts in US Time Zone, src_filename and src_row_hash columns 
def add_audit_columns(tbl, src_path):
    now = pa.scalar(dt.datetime.now(dt.UTC), type=pa.timestamp("us"))
    ts_col = pa.array([now.as_py()] * len(tbl), type=pa.timestamp("us"))
    src_col = pa.array([str(src_path.name)] * len(tbl))
    hash_col = pa.array([hashlib.md5(str(tbl.slice(i,1)).encode()).hexdigest() for i in range(len(tbl))])
    tbl = tbl.append_column('ingestion_ts', ts_col)
    tbl = tbl.append_column('src_filename', src_col)
    tbl = tbl.append_column('src_row_hash', hash_col)
    return tbl

# Handles rejected data
def handle_rejects(tbl, reason, lake_root, src_path):
    reject_path = lake_root/'_rejects'/f"{src_path.stem}_rejects.parquet"
    tbl = tbl.append_column('reject_reason', pa.array( [reason] * len(tbl) ))
    pq.write_table(tbl, reject_path)
    print(f"Failed loading {tbl} due to {reason}.")