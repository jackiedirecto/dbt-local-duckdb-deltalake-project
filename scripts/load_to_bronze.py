# Ingest raw files into Bronze (Parquet + Delta), with schema validation, partitioning,
# rejects, and manifest tracking in DuckDB.
# Usage: python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb
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
from schemas.schemas import customers_schema, products_schema, stores_schema, suppliers_schema, orders_header_schema, orders_lines_schema, events_schema, sensors_schema, exchange_rates_schema, shipments_schema, returns_day1_schema
try:
    from deltalake import write_deltalake
except Exception as e:
    write_deltalake = None

# Define arguments when running the pipeline
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--raw', type=str, default='data_raw')
    ap.add_argument('--lake', type=str, default='lake')
    ap.add_argument('--manifest', type=str, default='duckdb/warehouse.duckdb')
    ap.add_argument('--dry-run', action='store_true')
    return ap.parse_args()

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

# Load data to customers table from customers.csv
def load_customers(raw_root, lake_root, conn, dry_run=False):
    src = raw_root/'customers.csv'
    if not src.exists(): return
    if already_processed(conn, src): return
    try:
        tbl = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
        tbl = tbl.cast(customers_schema, safe=False)
        tbl = add_audit_columns(tbl, src)
        if not dry_run:
            pq_base = lake_root/'bronze'/'parquet'/'customers'
            dl_base = lake_root/'bronze'/'delta'/'customers'
            write_parquet_partitioned(tbl, pq_base)
            write_delta(tbl, dl_base, mode='append')
        mark_processed(conn, src, len(tbl), 0, 'success')
        print('customers has been loaded to deltalake.')
    except Exception as e:
        handle_rejects(tbl, str(e), lake_root, src)
        print('error loading customers into deltalake.')
        mark_processed(conn, src, 0, len(tbl), 'failed')

# Load data to stores table from stores.csv
def load_stores(raw_root, lake_root, conn, dry_run=False):
    src = raw_root/'stores.csv'
    if not src.exists(): return
    if already_processed(conn, src): return
    try:
        tbl = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
        tbl = tbl.cast(stores_schema, safe=False)
        tbl = add_audit_columns(tbl, src)
        if not dry_run:
            pq_base = lake_root/'bronze'/'parquet'/'stores'
            dl_base = lake_root/'bronze'/'delta'/'stores'
            write_parquet_partitioned(tbl, pq_base)
            write_delta(tbl, dl_base, mode='append')
        mark_processed(conn, src, len(tbl), 0, 'success')
        print('stores has been loaded to deltalake.')
    except Exception as e:
        handle_rejects(tbl, str(e), lake_root, src)
        mark_processed(conn, src, 0, len(tbl), 'failed')

# Load data to products table from products.csv
def load_products(raw_root, lake_root, conn, dry_run=False):
    src = raw_root/'products.csv'
    if not src.exists(): return
    if already_processed(conn, src): return
    try:
        tbl = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
        tbl = tbl.cast(products_schema, safe=False)
        tbl = add_audit_columns(tbl, src)
        if not dry_run:
            pq_base = lake_root/'bronze'/'parquet'/'products'
            dl_base = lake_root/'bronze'/'delta'/'products'
            write_parquet_partitioned(tbl, pq_base)
            write_delta(tbl, dl_base, mode='append')
        mark_processed(conn, src, len(tbl), 0, 'success')
        print('products has been loaded to deltalake.')
    except Exception as e:
        handle_rejects(tbl, str(e), lake_root, src)
        mark_processed(conn, src, 0, len(tbl), 'failed')

# Load data to suppliers table from suppliers.csv
def load_suppliers(raw_root, lake_root, conn, dry_run=False):
    src = raw_root/'suppliers.csv'
    if not src.exists(): return
    if already_processed(conn, src): return
    try:
        tbl = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
        tbl = tbl.cast(suppliers_schema, safe=False)
        tbl = add_audit_columns(tbl, src)
        if not dry_run:
            pq_base = lake_root/'bronze'/'parquet'/'suppliers'
            dl_base = lake_root/'bronze'/'delta'/'suppliers'
            write_parquet_partitioned(tbl, pq_base)
            write_delta(tbl, dl_base, mode='append')
        mark_processed(conn, src, len(tbl), 0, 'success')
        print('suppliers has been loaded to deltalake.')
    except Exception as e:
        handle_rejects(tbl, str(e), lake_root, src)
        mark_processed(conn, src, 0, len(tbl), 'failed')

# Load orders from order_lines csv
def load_orders(raw_root, lake_root, conn, dry_run=False):
    orders_root = raw_root / 'orders'
    if not orders_root.exists() or not orders_root.is_dir():
        print(f"'orders' folder not found in {raw_root}")
        return
    pq_base = lake_root / 'bronze' / 'parquet' / 'orders'
    dl_base = lake_root / 'bronze' / 'delta' / 'orders'
    for subfolder in orders_root.iterdir():
        if not subfolder.is_dir():
            continue
        folder_name = subfolder.name
        if not is_valid_partition_folder(folder_name):
            print(f"Skipping folder '{folder_name}': invalid partition folder format.")
            continue
        order_dt = folder_name.split('=')[-1]
        for csv_file in subfolder.glob('*.csv'):
            if already_processed(conn, csv_file): 
                continue
            tbl = None
            try:
                tbl = pacsv.read_csv(csv_file, read_options=pacsv.ReadOptions(encoding='utf-8'))
                tbl = tbl.cast(orders_header_schema, safe=False)
                tbl = add_audit_columns(tbl, csv_file)
                # Add partition column
                tbl = tbl.append_column("order_dt", pa.array([order_dt] * len(tbl)))
                if not dry_run:
                    write_parquet_partitioned(tbl, pq_base, ["order_dt"])
                    write_delta(tbl, dl_base, mode='append', partition_by=["order_dt"], merge_schema=True)
                mark_processed(conn, csv_file, len(tbl), 0, 'success')
                print(f"{csv_file.name} has been loaded to deltalake.")
            except Exception as e:
                handle_rejects(tbl, str(e), lake_root, csv_file)
                mark_processed(conn, csv_file, 0, len(tbl) if tbl else 0, 'failed')

# Load orders_lines from order_lines csv
def load_orders_lines(raw_root, lake_root, conn, dry_run=False):
    orders_lines_root = raw_root / 'orders_lines'
    if not orders_lines_root.exists() or not orders_lines_root.is_dir():
        print(f"'orders_lines' folder not found in {raw_root}")
        return
    pq_base = lake_root / 'bronze' / 'parquet' / 'orders_lines'
    dl_base = lake_root / 'bronze' / 'delta' / 'orders_lines'
    for subfolder in orders_lines_root.iterdir():
        if not subfolder.is_dir():
            continue
        folder_name = subfolder.name
        if not is_valid_partition_folder(folder_name):
            print(f"Skipping folder '{folder_name}': invalid partition folder format.")
            continue
        order_dt = folder_name.split('=')[-1]
        for csv_file in subfolder.glob('*.csv'):
            if already_processed(conn, csv_file): 
                continue
            tbl = None
            try:
                tbl = pacsv.read_csv(csv_file, read_options=pacsv.ReadOptions(encoding='utf-8'))
                tbl = tbl.cast(orders_lines_schema, safe=False)
                tbl = add_audit_columns(tbl, csv_file)
                # Add partition column
                tbl = tbl.append_column("order_dt", pa.array([order_dt] * len(tbl)))
                if not dry_run:
                    write_parquet_partitioned(tbl, pq_base, ["order_dt"])
                    write_delta(tbl, dl_base, mode='append', partition_by=["order_dt"], merge_schema=True)
                mark_processed(conn, csv_file, len(tbl), 0, 'success')
                print(f"{csv_file.name} has been loaded to deltalake.")
            except Exception as e:
                handle_rejects(tbl, str(e), lake_root, csv_file)
                mark_processed(conn, csv_file, 0, len(tbl) if tbl else 0, 'failed')

# Load sensors from sensors csv
def load_sensors(raw_root, lake_root, conn, dry_run=False):
    sensors_root = raw_root / 'sensors'
    if not sensors_root.exists() or not sensors_root.is_dir():
        print(f"'sensors' folder not found in {raw_root}")
        return
    pq_base = lake_root / 'bronze' / 'parquet' / 'sensors'
    dl_base = lake_root / 'bronze' / 'delta' / 'sensors'
    for store_folder in sensors_root.iterdir():
        if not store_folder.is_dir() or not store_folder.name.startswith("store_id="):
            continue
        store_id = store_folder.name.split("=")[-1]
        for month_folder in store_folder.iterdir():
            if not month_folder.is_dir() or not month_folder.name.startswith("month="):
                continue
            month = month_folder.name.split("=")[-1]
            if not is_valid_month(month):
                print(f"Skipping folder '{month_folder.name}': invalid month format.")
                continue
            csv_file = month_folder / 'sensors.csv'
            if not csv_file.exists():
                continue
            if already_processed(conn, csv_file): 
                continue
            tbl = None
            try:
                tbl = pacsv.read_csv(csv_file, read_options=pacsv.ReadOptions(encoding='utf-8'))
                tbl = tbl.cast(sensors_schema, safe=False)
                tbl = add_audit_columns(tbl, csv_file)
                # Add partition columns only if not already present
                if "store_id" not in tbl.schema.names:
                    tbl = tbl.append_column("store_id", pa.array([store_id] * len(tbl)))
                else:
                    tbl = tbl.set_column(tbl.schema.get_field_index("store_id"), "store_id", pa.array([store_id] * len(tbl)))
                if "month" not in tbl.schema.names:
                    tbl = tbl.append_column("month", pa.array([month] * len(tbl)))
                else:
                    tbl = tbl.set_column(tbl.schema.get_field_index("month"), "month", pa.array([month] * len(tbl)))
                if not dry_run:
                    pq.write_to_dataset(tbl, root_path=pq_base, partition_cols=["store_id", "month"])
                    write_delta(tbl, dl_base, mode='append', partition_by=["store_id", "month"], merge_schema=True)
                mark_processed(conn, csv_file, len(tbl), 0, 'success')
                print(f"{csv_file.name} has been loaded to deltalake.")
            except Exception as e:
                handle_rejects(tbl, str(e), lake_root, csv_file)
                mark_processed(conn, csv_file, 0, len(tbl) if tbl else 0, 'failed')

# Load returns data from parquet file to deltalake
def load_returns(raw_root, lake_root, conn, dry_run=False):
    dl_base = lake_root / 'bronze' / 'delta' / 'returns'
    pq_base = lake_root / 'bronze' / 'parquet' / 'returns'
    parquet_files = ['returns_day1.parquet']
    for file_name in parquet_files:
        src = raw_root / file_name
        if not src.exists():
            print(f"File {src} does not exist.")
            continue
        if already_processed(conn, src):
            print(f"File {src} already processed.")
            continue
        tbl = None
        try:
            print(f"Reading Parquet file {src}")
            tbl = pq.read_table(src)
            tbl = tbl.cast(returns_day1_schema, safe=False)
            tbl = add_audit_columns(tbl, src)
            if not dry_run:
                upsert_delta(tbl, dl_base, merge_schema=True)
                write_parquet_partitioned(tbl, pq_base)
            mark_processed(conn, src, len(tbl), 0, 'success')
            print(f"{file_name} has been loaded to Delta Lake.")
        except Exception as e:
            print(f"Error processing file {src}: {e}")
            if tbl is not None:
                handle_rejects(tbl, str(e), lake_root, src)
            mark_processed(conn, src, 0, len(tbl) if tbl else 0, 'failed')

# Load exchange_rates data from Excel file
def load_exchange_rates(raw_root, lake_root, conn, dry_run=False):
    src = raw_root / 'exchange_rates.xlsx'
    if not src.exists(): return
    if already_processed(conn, src): return
    try:
        # Read Excel file 
        df = pd.read_excel(src)
        tbl = pa.Table.from_pandas(df)
        tbl = tbl.cast(exchange_rates_schema, safe=False)
        tbl = add_audit_columns(tbl, src)
        if not dry_run:
            pq_base = lake_root / 'bronze' / 'parquet' / 'exchange_rates'
            dl_base = lake_root / 'bronze' / 'delta' / 'exchange_rates'
            write_parquet_partitioned(tbl, pq_base)
            write_delta(tbl, dl_base, mode='append')
        mark_processed(conn, src, len(tbl), 0, 'success')
        print('exchange_rates has been loaded to deltalake.')
    except Exception as e:
        handle_rejects(tbl, str(e), lake_root, src)
        print('error loading exchange_rates into deltalake.')
        mark_processed(conn, src, 0, len(tbl), 'failed')

# Load shipments from shipments.parquet to deltalake
def load_shipments(raw_root, lake_root, conn, dry_run=False):
    src = raw_root / 'shipments.parquet'
    pq_base = lake_root / 'bronze' / 'parquet' / 'shipments'
    dl_base = lake_root / 'bronze' / 'delta' / 'shipments'
    if not src.exists():
        return
    if already_processed(conn, src):
        return
    try:
        tbl = pq.read_table(src)
        tbl = tbl.cast(shipments_schema, safe=False)
        tbl = add_audit_columns(tbl, src)
        if not dry_run:
            write_parquet_partitioned(tbl, pq_base)
            write_delta(tbl, dl_base, mode='append')
        mark_processed(conn, src, len(tbl), 0, 'success')
        print('shipments has been loaded to deltalake.')
    except Exception as e:
        handle_rejects(tbl, str(e), lake_root, src)
        print('error loading shipments into deltalake.')
        mark_processed(conn, src, 0, len(tbl), 'failed')

# Load events.jsonl to deltalake
def load_events(raw_root, lake_root, conn, dry_run=False):
    events_root = raw_root / 'events'

    if not events_root.exists() or not events_root.is_dir():
        print(f"'events' folder not found in {raw_root}")
        return

    pq_base = lake_root / 'bronze' / 'parquet' / 'events'
    dl_base = lake_root / 'bronze' / 'delta' / 'events'

    for subfolder in events_root.iterdir():
        if not subfolder.is_dir():
            continue
        folder_name = subfolder.name
        if not is_valid_partition_folder_event(folder_name):
            print(f"Skipping folder '{folder_name}': invalid partition folder format.")
            continue
        event_dt = folder_name.split('=')[-1]
        for jsonl_file in subfolder.glob('*.jsonl'):
            if already_processed(conn, jsonl_file):
                continue
            tbl = None
            try:
                with jsonl_file.open('r', encoding='utf-8') as f:
                    json_lines = [line.strip() for line in f if line.strip()]
                    tbl = pa.table({'json': json_lines}, schema=events_schema)
                tbl = add_audit_columns(tbl, jsonl_file)
                # Add partition column
                tbl = tbl.append_column("event_dt", pa.array([event_dt] * len(tbl)))
                if not dry_run:
                    write_parquet_partitioned(tbl, pq_base, ["event_dt"])
                    write_delta(tbl, dl_base, mode='append', partition_by=["event_dt"], merge_schema=True)
                mark_processed(conn, jsonl_file, len(tbl), 0, 'success')
                print(f"{jsonl_file.name} has been loaded to deltalake.")

            except Exception as e:
                handle_rejects(tbl, str(e), lake_root, jsonl_file)
                mark_processed(conn, jsonl_file, 0, len(tbl) if tbl else 0, 'failed')

def main():
    args = parse_args()
    raw_root = pathlib.Path(args.raw)
    lake_root = pathlib.Path(args.lake)
    ensure_dirs(lake_root)
    pathlib.Path(args.manifest).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(args.manifest)
    conn.execute("INSTALL delta; LOAD delta;")
    init_manifest(conn)

    load_customers(raw_root, lake_root, conn, dry_run=args.dry_run)
    load_stores(raw_root, lake_root, conn, dry_run=args.dry_run)
    load_products(raw_root, lake_root, conn, dry_run=args.dry_run)
    load_suppliers(raw_root, lake_root, conn, dry_run=args.dry_run)
    load_orders(raw_root, lake_root, conn, dry_run=args.dry_run)
    load_orders_lines(raw_root, lake_root, conn, dry_run=args.dry_run)
    load_sensors(raw_root, lake_root, conn, dry_run=args.dry_run)
    load_returns(raw_root, lake_root, conn, dry_run=args.dry_run)
    load_exchange_rates(raw_root, lake_root, conn, dry_run=args.dry_run)
    load_shipments(raw_root, lake_root, conn, dry_run=args.dry_run)
    load_events(raw_root, lake_root, conn, dry_run=args.dry_run)


    print("✅ Bronze load completed for implemented loaders (extend for all tables).")

if __name__ == '__main__':
    main()
