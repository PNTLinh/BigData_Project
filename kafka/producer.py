import os
import time
import json
import random
import argparse
import heapq
import sys
import requests
import re
import polars as pl
from datetime import datetime, timedelta
from confluent_kafka import Producer

# --- Configuration ---
SCRIPT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
FILENAME = "yellow_tripdata_2025-08"
ORIGINAL_PATH = os.path.join(DATA_DIR, f"{FILENAME}.parquet")
SORTED_PATH = os.path.join(DATA_DIR, f"{FILENAME}_sorted.parquet")
DOWNLOAD_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-08.parquet"

# --- Defaults ---
DEFAULT_TOPIC = "taxi-trips"
DEFAULT_SPEEDUP = 3600.0
DEFAULT_KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

def get_args():
    parser = argparse.ArgumentParser(description="Production-Grade Taxi Data Simulator")
    parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC, help="Kafka topic name")
    parser.add_argument("--speedup", type=float, default=DEFAULT_SPEEDUP, help="Speedup factor (e.g. 60.0 means 1 minute of simulation for every second IRL)")
    parser.add_argument("--max_records", type=int, default=0, help="Stop after X records (0=infinite)")
    return parser.parse_args()

def ensure_data_ready():
    """Ensures data exists, is sorted, AND is filtered for the correct month."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    if os.path.exists(SORTED_PATH):
        print(f"Found clean & sorted dataset: {SORTED_PATH}")
        return SORTED_PATH

    # Download if missing
    if not os.path.exists(ORIGINAL_PATH):
        print(f"Dataset missing. Downloading from {DOWNLOAD_URL}...")
        try:
            response = requests.get(DOWNLOAD_URL, stream=True)
            response.raise_for_status()
            with open(ORIGINAL_PATH, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("Download complete.")
        except Exception as e:
            print(f"Error downloading file: {e}")
            exit(1)

    print(f"Processing dataset (Filtering & Sorting)...")
    try:
        # 1. Read Parquet
        df = pl.read_parquet(ORIGINAL_PATH)
        
        # 2. FILTER: Keep only data actually in Date found in filename
        # This removes the 2009 outliers that cause the "97 day sleep" bug
        year, month = map(int, re.findall(r'(\d{4})-(\d{2})', FILENAME)[0])
        start_date = datetime(year, month, 1)
        end_date = datetime(year, month + 1, 1) if month < 12 else datetime(year + 1, 1, 1)
        
        print(f"  - Original Count: {df.height:,}")
        
        df = df.filter(
            (pl.col("tpep_dropoff_datetime") >= start_date) & 
            (pl.col("tpep_dropoff_datetime") < end_date)
        )
        
        print(f"  - Cleaned Count:  {df.height:,} (Removed outliers)")

        # 3. Sort by Dropoff (Simulation Time)
        df = df.sort("tpep_dropoff_datetime")
        
        # 4. Save
        df.write_parquet(SORTED_PATH)
        print(f"Saved clean dataset to: {SORTED_PATH}")
        return SORTED_PATH
    except Exception as e:
        print(f"Error processing data: {e}")
        exit(1)

def delivery_report(err, msg):
    if err is not None:
        pass # Silently ignore full buffer errors to keep UI clean

def print_status_bar(sim_time, rate, total, late, queue_size):
    """Prints a dynamic status line that overwrites itself."""
    # ANSI escape code to clear line is \x1b[2K, carriage return is \r
    bar = "█" * int(rate / 50)  # Simple visualizer for rate
    status = (
        f"\r\x1b[2K"  # Clear line
        f"🚕 Sim Time: \033[1m{sim_time}\033[0m | "
        f"⚡ Speed: {rate:.0f} msg/s | "
        f"📦 Total: {total:,} | "
        f"🐢 Late Injected: {late} | "
        f"Buffers: {queue_size} "
    )
    sys.stdout.write(status)
    sys.stdout.flush()

def simulate_stream(args, data_path):
    producer = Producer({
        'bootstrap.servers': DEFAULT_KAFKA_BOOTSTRAP,
        'queue.buffering.max.messages': 500000,
        'queue.buffering.max.ms': 200,
        'enable.idempotence': True
    })

    print(f"\n--- Starting Simulation (Ctrl+C to stop) ---")
    print(f"Topic: {args.topic} | Speedup: {args.speedup}x")
    
    print("Loading data (Lazy)...")
    # Chỉ lấy 100.000 dòng đầu để test cho nhẹ RAM, hoặc bỏ .head() nếu máy đủ khỏe sau khi tuning
    df = pl.read_parquet(data_path).head(100000)
    iterator = df.iter_rows(named=True)
    
    # Initialize State
    try:
        first_record = next(iterator)
    except StopIteration:
        print("Error: Dataset is empty after filtering!")
        return

    current_sim_time = first_record['tpep_dropoff_datetime']
    late_buffer = [] 
    sent_count = 0
    late_count = 0
    pending_batch = [first_record]
    
    start_real_time = time.time()
    last_ui_update = time.time()

    # sketchy fixes
    tie_breaker = 0
    sleep_debt = 0.0

    try:
        for row in iterator:
            if args.max_records > 0 and sent_count >= args.max_records:
                break
                
            row_time = row['tpep_dropoff_datetime']
            
            # 1. Process Late Buffer
            while late_buffer and late_buffer[0][0] <= row_time:
                due_time, _, late_row = heapq.heappop(late_buffer)
                key = str(late_row.get('VendorID', '')).encode('utf-8')
                val = json.dumps(late_row, default=str).encode('utf-8')
                producer.produce(args.topic, key=key, value=val, on_delivery=delivery_report)
                late_count += 1
                sent_count += 1
            
            # 2. Time Jump
            time_delta = (row_time - current_sim_time).total_seconds()
            
            if time_delta > 0:
                # Calculate theoretical sleep needed
                real_sleep = time_delta / args.speedup
                
                # Add to debt instead of checking immediately
                sleep_debt += real_sleep
                
                # Only sleep if debt is significant (e.g., 10ms)
                if sleep_debt > 0.01:
                    # Flush batch before sleeping
                    for r in pending_batch:
                        key = str(r.get('VendorID', '')).encode('utf-8')
                        val = json.dumps(r, default=str).encode('utf-8')
                        producer.produce(args.topic, key=key, value=val, on_delivery=delivery_report)
                    
                    producer.poll(0)
                    sent_count += len(pending_batch)
                    pending_batch = []
                    
                    # Update UI
                    elapsed = time.time() - start_real_time
                    rate = sent_count / elapsed if elapsed > 0 else 0
                    print_status_bar(current_sim_time, rate, sent_count, late_count, len(late_buffer))
                    
                    # PAY THE DEBT
                    time.sleep(sleep_debt)
                    sleep_debt = 0.0  # Reset debt
                
                current_sim_time = row_time

            # 3. Store & Fwd Logic
            flag = str(row.get('store_and_fwd_flag', 'N')).upper()
            if flag == 'Y':
                delay_seconds = random.randint(120, 1200) 
                due_time = row_time + timedelta(seconds=delay_seconds)
                heapq.heappush(late_buffer, (due_time, tie_breaker, row))
                tie_breaker += 1 # Increment so it's always unique
            else:
                pending_batch.append(row)

            # High frequency UI update check
            if time.time() - last_ui_update > 0.2:
                elapsed = time.time() - start_real_time
                rate = sent_count / elapsed if elapsed > 0 else 0
                print_status_bar(current_sim_time, rate, sent_count, late_count, len(late_buffer))
                last_ui_update = time.time()

        # Final flush
        producer.flush()
        print(f"\n\nSimulation Complete. Total Sent: {sent_count:,}")

    except KeyboardInterrupt:
        print("\n\nStopping...")
        producer.flush()

if __name__ == "__main__":
    args = get_args()
    final_path = ensure_data_ready()
    simulate_stream(args, final_path)