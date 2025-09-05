# Generate synthetic raw data locally with controlled edge cases.
# Usage: python scripts/generate_data.py --seed 42 --out data_raw
import csv
from decimal import Decimal
import argparse, os, pathlib, random
from datetime import datetime, timedelta, date
import string
import numpy as np
from faker import Faker
try:
    from mimesis import Person, Address, Generic, Business
    use_mimesis = True
except ImportError:
    use_mimesis = False
import rstr
import pyarrow as pa
import pyarrow.parquet as pq
import xlsxwriter, json, pytz
import pandas as pd
# from schemas import shipments_schema

def parse_args():
    ap = argparse.ArgumentParser(description="Generate synthetic raw data with controlled anomalies.")
    ap.add_argument('--seed', type=int, default=42, help='Random seed for reproducibility')
    ap.add_argument('--out', type=str, default='data_raw', help='Output directory')
    ap.add_argument('--scale', type=float, default=1.0, help='Scaling factor for data volume')
    return ap.parse_args()

def ensure_dir(p): 
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)

# Generate customers.csv
def generate_customers(out, scale, seed):
    fake = Faker('en_AU')
    person = Person('en')
    address = Address('en')
    total_rows = int(80000 * scale)
    path = out / 'customers.csv'
    natural_keys = set()
    with path.open('w', encoding='utf-8') as f:
        f.write(','.join([
            "customer_id", "natural_key", "first_name", "last_name", "email", "phone",
            "address_line1", "address_line2", "city", "state_region", "postcode",
            "country_code", "latitude", "longitude", "birth_date", "join_ts",
            "is_vip", "gdpr_consent"
        ]) + '\n')
        for i in range(1, total_rows + 1):
            nk = 'CUST-' + rstr.rstr('A-Z0-9', 8)
            if random.random() < 0.002:
                nk = list(natural_keys)[random.randint(0, len(natural_keys)-1)] if natural_keys else nk
            natural_keys.add(nk)
            email = fake.email() if random.random() > 0.01 else 'bad_email'
            phone = fake.phone_number().replace(',', ' ') if random.random() > 0.01 else ''
            addr1 = fake.street_address().replace(',', ' ') if random.random() > 0.01 else ''
            addr2 = ''
            city = fake.city().replace(',', ' ')
            state = fake.state_abbr()
            postcode = fake.postcode()
            country_code = 'AU'
            lat = -44 + random.random()*10
            lon = 112 + random.random()*40
            birth = date(1960,1,1) + timedelta(days=random.randint(0, 20000))
            join_ts = datetime(2024,1,1) + timedelta(days=random.randint(0, 400), seconds=random.randint(0, 86399))
            is_vip = str(random.random() < 0.15)
            gdpr_consent = str(random.random() > 0.05)
            f.write(f"{i},{nk},{person.first_name()},{person.last_name()},{email},{phone},{addr1},{addr2},{city},{state},{postcode},{country_code},{lat:.6f},{lon:.6f},{birth.isoformat()},{join_ts.isoformat()},{is_vip},{gdpr_consent}\n")
    print(f"customers.csv file has been generated with {total_rows} rows.")

# Generate Products.csv
def generate_products(out, scale, seed):
    total_rows = int(25000 * scale)
    path = out / 'products.csv'
    categories = ['Electronics', 'Clothing', 'Home', 'Books']
    subcategories = {'Electronics': ['Phones', 'Computers'], 'Clothing': ['Men', 'Women'], 'Home': ['Furniture', 'Kitchen'], 'Books': ['Fiction', 'Non-fiction']}
    with path.open('w', encoding='utf-8') as f:
        f.write(f'product_id,sku,name,category,subcategory,current_price,currency,is_discontinued,introduced_dt,discontinued_dt\n')
        for i in range(1, total_rows + 1):
            sku = 'SKU-' + rstr.rstr('A-Z0-9', 6)
            cat = random.choice(categories)
            subcat = random.choice(subcategories[cat])
            price = round(random.uniform(5, 500), 4)
            if random.random() < 0.005:
                price = -1.0
            discontinued = random.random() < 0.2
            introduced = date(2015,1,1) + timedelta(days=random.randint(0, 3000))
            discontinued_dt = date(2023,1,1) + timedelta(days=random.randint(0, 365)) if discontinued and random.random() > 0.1 else ''
            f.write(f"{i},{sku},{cat} {subcat} Item,{cat},{subcat},{price:.4f},AUD,{str(discontinued)},{introduced},{discontinued_dt}\n")
    print(f"products.csv file has been generated with {total_rows} rows.")

# Generate stores.csv
def generate_stores(out, scale, seed):
    total_rows = int(5000 * scale)
    path = out / 'stores.csv'
    store_codes = set()
    channels = ['web', 'pos']
    regions = ['NSW', 'VIC', 'QLD', 'WA']
    with path.open('w', encoding='utf-8') as f:
        f.write('store_id,store_code,name,channel,region,state,latitude,longitude,open_dt,close_dt\n')
        for i in range(1, total_rows + 1):
            code = 'STORE-' + rstr.rstr('A-Z0-9', 6)
            if random.random() < 0.01:
                code = list(store_codes)[-1] if store_codes else code
            store_codes.add(code)
            lat = -44 + random.random() * 10
            lon = 112 + random.random() * 40
            if random.random() < 0.005:
                lat = 999.0
                lon = 999.0
            open_dt = date(2010,1,1) + timedelta(days = random.randint(0, 5000))
            close_dt = date(2023,1,1) + timedelta(days = random.randint(0, 365)) if random.random() < 0.1 else ''
            f.write(f"{i},{code},Store {i},{random.choice(channels)},{random.choice(regions)},{random.choice(regions)},{lat:.6f},{lon:.6f},{open_dt},{close_dt}\n")
    print(f"stores.csv file has been generated with {total_rows} rows.")

# Generate Suppliers.csv
def generate_suppliers(out, scale, seed):
    total_rows = int(8000 * scale)
    # Initialize Faker
    fake = Faker('en_AU')
    # Output path
    path = out / 'suppliers.csv'

    # Write header and generate supplier data
    with path.open('w', encoding='utf-8') as f:
        f.write('supplier_id,supplier_code,name,country_code,lead_time_days,preferred\n')
        for i in range(1, total_rows + 1):
            supplier_code = 'SUP-' + ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))
            name = fake.company().replace(',', '')
            country_code = fake.country_code()
            lead_time_days = random.randint(1, 60)
            preferred = str(random.random() < 0.2)
            f.write(f"{i},{supplier_code},{name},{country_code},{lead_time_days},{preferred}\n")
    print(f"suppliers.csv file has been generated with {total_rows} rows.")

# Generate Orders.csv, Partitioned
def generate_orders(out, scale, seed):
    total_rows = int(1000000 * scale)
    partition_dates = [date(2024,1,1) + timedelta(days=i) for i in range(30)]
    for dt in partition_dates:
        part_dir = out / f"orders/order_dt={dt.isoformat()}"
        ensure_dir(part_dir)
        path = part_dir / f"part-0.csv"
        with path.open('w', encoding='utf-8') as f:
            f.write('order_id,order_ts,order_dt_local,customer_id,store_id,channel,payment_method,coupon_code,shipping_fee,currency\n')
            for i in range(1, int(total_rows / len(partition_dates)) + 1):
                oid = i
                if random.random() < 0.0005:
                    oid = 1
                order_ts = datetime.combine(dt, datetime.min.time()) + timedelta(seconds=random.randint(0, 86399))
                customer_id = random.randint(1, 80000)
                store_id = random.randint(1, 5000)
                if random.random() < 0.01:
                    customer_id = 999999
                    store_id = 999999
                channel = random.choice(['web', 'pos'])
                payment = random.choice(['credit_card', 'paypal', 'gift_card'])
                coupon = 'SAVE10' if random.random() < 0.1 else ''
                fee = round(random.uniform(0, 20), 2)
                f.write(f"{oid},{order_ts.isoformat()},{dt},{customer_id},{store_id},{channel},{payment},{coupon},{fee:.2f},AUD\n")
    print(f"orders.csv file has been generated with {total_rows} rows.")

# Generate Orders_Lines.csv, Partitioned
def generate_order_lines(out, scale, seed):
    total_rows = int(3000000 * scale)
    partition_dates = [date(2024,1,1) + timedelta(days=i) for i in range(30)]
    for dt in partition_dates:
        part_dir = out / f"orders_lines/order_dt={dt.isoformat()}"
        ensure_dir(part_dir)
        path = part_dir / f"part-0.csv"
        with path.open('w', encoding='utf-8') as f:
            f.write('order_id,line_number,product_id,qty,unit_price,line_discount_pct,tax_pct\n')
            for i in range(1, int(total_rows / len(partition_dates)) + 1):
                order_id = i
                line_number = random.randint(1, 5)
                product_id = random.randint(1, 25000)
                if random.random() < 0.01:
                    product_id = 999999
                qty = random.randint(1, 10)
                if random.random() < 0.001:
                    qty = -1
                unit_price = round(random.uniform(5, 500), 4)
                if random.random() < 0.001:
                    unit_price = 0.0
                discount = round(random.uniform(0, 0.5), 4)
                tax = round(random.uniform(0.05, 0.15), 4)
                f.write(f"{order_id},{line_number},{product_id},{qty},{unit_price:.4f},{discount:.4f},{tax:.4f}\n")
    print(f"orders_lines.csv file has been generated with {total_rows} rows.")

# Generate Sensors.csv
def generate_sensors(out, scale):
    # Define Perth timezone
    perth_tz = pytz.timezone('Australia/Perth')

    # Calculate total rows
    total_rows = int(1000000 * scale)

    # Start timestamp in Perth timezone
    start_ts = perth_tz.localize(datetime(2024, 1, 1))

    for i in range(1, total_rows + 1):
        # Generate timestamp
        ts = start_ts + timedelta(minutes=i)
        if random.random() < 0.001:
            ts_str = ''
        else:
            ts_str = ts.isoformat()

        # Generate other sensor data
        store_id = random.randint(1, 5000)
        shelf_id = 'SHELF-' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
        temp = round(random.uniform(15, 30), 2)
        if random.random() < 0.005:
            temp = 100.0
        humidity = round(random.uniform(30, 70), 2)
        if random.random() < 0.005:
            humidity = -10.0
        battery = random.randint(3000, 4200)

        # Determine output path
        month_str = ts.strftime('%Y-%m') if ts_str else 'unknown'
        sensor_path = out / f'sensors/store_id={store_id}/month={month_str}'
        sensor_path.mkdir(parents=True, exist_ok=True)
        file_path = sensor_path / 'sensors.csv'

        # Write header if file is new
        write_header = not file_path.exists()

        # Write data to CSV
        with file_path.open('a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(['sensor_ts', 'store_id', 'shelf_id', 'temperature_c', 'humidity_pct', 'battery_mv'])
            writer.writerow([ts_str, store_id, shelf_id, temp, humidity, battery])

    print(f"sensors.csv has been generated with {total_rows} rows.")


# Generate shipments.parquet
def generate_shipments(out, scale, seed):
    total_rows = int(1000000 * scale)
    shipped_dates = [datetime(2024,1,1) + timedelta(days=i%90) for i in range(total_rows)]
    delivered_dates = []
    for i in range(total_rows):
        if random.random() < 0.01:
            delivered_dates.append(None)
        else:
            delay = random.randint(1, 10)
            delivered_dates.append(shipped_dates[i] + timedelta(days=delay))
    tbl = pa.table({
        'shipment_id': pa.array(range(1, total_rows+1), type=pa.int64()),
        'order_id': pa.array(range(1, total_rows+1), type=pa.int64()),
        'carrier': pa.array(['AUSPOST']*total_rows, type=pa.string()),
        'shipped_at': pa.array(shipped_dates, type=pa.timestamp('us')),
        'delivered_at': pa.array(delivered_dates, type=pa.timestamp('us')),
        'ship_cost': pa.array([pa.scalar(Decimal("1995.00"), type=pa.decimal128(12, 2)) for _ in range(total_rows)], type=pa.decimal128(12, 2)),
    })
    pq.write_table(tbl, out/'shipments.parquet', compression='snappy')
    print(f"shipments.parquet has been generated with {total_rows} rows.")

# Generate returns
def generate_returns(out, scale, seed):
    np.random.seed(seed)
    num_rows = int(100000 * scale)

    # Day 1: Base schema
    return_ids = np.arange(1, num_rows + 1)
    order_ids = np.random.randint(1000, 5000, size=num_rows)
    product_ids = np.random.randint(100, 500, size=num_rows)
    return_ts = [datetime.now() - timedelta(days=np.random.randint(0, 30)) for _ in range(num_rows)]
    qtys = np.random.randint(1, 5, size=num_rows)
    reasons = np.random.choice(['Damaged', 'Incorrect Item', 'Other'], size=num_rows)

    df_day1 = pd.DataFrame({
        "return_id": return_ids,
        "order_id": order_ids,
        "product_id": product_ids,
        "return_ts": return_ts,
        "qty": qtys,
        "reason": reasons
    })

    # Save Day 1 data
    table_day1 = pa.Table.from_pandas(df_day1)
    pq.write_table(table_day1, os.path.join(out, "returns_day1.parquet"))

    # Day 2: Schema evolution
    df_day2 = df_day1.copy()

    # Add new column
    reason_code_map = {
        'Damaged': 'D',
        'Incorrect Item': 'I',
        'Other': 'O'
    }
    df_day2["return_reason_code"] = df_day2["reason"].map(reason_code_map)

    # UPSERT: Update qty for 5,000 existing rows

    sample_size = min(5000, len(df_day2))
    update_indices = np.random.choice(df_day2.index, size=sample_size, replace=False)
    df_day2.loc[update_indices, "qty"] += 1

    # UPSERT: Insert 5,000 new rows
    new_return_ids = np.arange(num_rows + 1, num_rows + 5001)
    new_order_ids = np.random.randint(1000, 5000, size=5000)
    new_product_ids = np.random.randint(100, 500, size=5000)
    new_return_ts = [datetime.now() - timedelta(days=np.random.randint(0, 30)) for _ in range(5000)]
    new_qtys = np.random.randint(1, 5, size=5000)
    new_reasons = np.random.choice(['Damaged', 'Incorrect Item', 'Other'], size=5000)
    new_reason_codes = [reason_code_map[r] for r in new_reasons]

    df_new = pd.DataFrame({
        "return_id": new_return_ids,
        "order_id": new_order_ids,
        "product_id": new_product_ids,
        "return_ts": new_return_ts,
        "qty": new_qtys,
        "reason": new_reasons,
        "return_reason_code": new_reason_codes
    })

    df_day2 = pd.concat([df_day2, df_new], ignore_index=True)

    # DELETE: Remove rows where reason == 'Other'
    df_day2 = df_day2[df_day2["reason"] != "Other"]

    # Save Day 2 data
    table_day2 = pa.Table.from_pandas(df_day2)
    pq.write_table(table_day2, os.path.join(out, "returns_day2.parquet"))
    print(f"returns has been generated with {num_rows} rows.")

# Generate exchange_rates.xlsx
def generate_exchange_rates(out, scale, seed):
    path = out / 'exchange_rates.xlsx'
    workbook = xlsxwriter.Workbook(str(path))
    worksheet = workbook.add_worksheet()
    worksheet.write_row(0, 0, ['date', 'currency', 'rate_to_aud'])
    currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CNY']
    start_date = date(2021, 1, 1)
    for i in range(1100):
        d = start_date + timedelta(days=i)
        for c in currencies:
            rate = round(0.5 + random.random()*1.5, 8)
            worksheet.write_row(i*len(currencies)+1+currencies.index(c), 0, [d.isoformat(), c, rate])
    workbook.close()
    print(f"exchange_rates.xlsx has been generated.")

## Generate events.jsonl
def generate_events(out, scale, seed):
    random.seed(seed)
    total_events = int(2_000_000 * scale)
    malformed_count = int(total_events * 0.0005)
    missing_fields_count = int(total_events * 0.0005)

    out.mkdir(parents=True, exist_ok=True)

    for i in range(total_events):
        ts = datetime(2024, 1, 1) + timedelta(seconds=random.randint(0, 31536000))
        event_date = ts.date().isoformat()
        event_path = out / f"events/event_dt={event_date}"
        event_path.mkdir(parents=True, exist_ok=True)
        file_path = event_path / f"events_{i//10000}.jsonl"

        envelope = {
            "event_id": i + 1,
            "event_ts": ts.isoformat(),
            "event_type": random.choice(["click", "view", "purchase"]),
            "user_id": random.randint(1, 80000),
            "session_id": f"sess_{random.randint(1000, 9999)}"
        }

        payload = {
            "product_id": random.randint(1000, 9999),
            "price": round(random.uniform(10.0, 500.0), 2),
            "currency": "USD"
        }

        # Introduce anomalies
        if i < malformed_count:
            line = '{"event_id": '  # malformed JSON
        elif i < malformed_count + missing_fields_count:
            del envelope["event_id"]  # missing required field
            line = json.dumps({"envelope": envelope, "payload": payload})
        else:
            line = json.dumps({"envelope": envelope, "payload": payload})

        with open(file_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    print(f"events.jsonl has been generated with {total_events} rows.")

## Main function
def main():
    args = parse_args()
    random.seed(args.seed)
    np.random.seed(args.seed)
    out = pathlib.Path(args.out)
    ensure_dir(out)

    generate_exchange_rates(out, args.scale, args.seed)
    generate_customers(out, args.scale, args.seed)
    generate_products(out, args.scale, args.seed)
    generate_stores(out, args.scale, args.seed)
    generate_suppliers(out, args.scale, args.seed)
    generate_orders(out, args.scale, args.seed)
    generate_order_lines(out, args.scale, args.seed)
    generate_sensors(out, args.scale)

    generate_returns(out, args.scale, args.seed)
    generate_events(out, args.scale, args.seed)
    generate_shipments(out, args.scale, args.seed)

    print(f"✅ Sample raw data written to {out}. Expand to full volumes and schemas as needed.")

if __name__ == '__main__':
    main()
