import os
import time
import json
import random
import argparse
import sys
import requests
import re
import polars as pl
import heapq
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
FILENAME = "yellow_tripdata_2025-08"
ORIGINAL_PATH = os.path.join(DATA_DIR, f"{FILENAME}.parquet")
SORTED_PATH = os.path.join(DATA_DIR, f"{FILENAME}_sorted.parquet")
DOWNLOAD_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-08.parquet"

DEFAULT_SPEEDUP = 3600.0
DEFAULT_GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8000")

def get_args():
    parser = argparse.ArgumentParser(description="Traffic Generator")
    parser.add_argument("--gateway", type=str, default=DEFAULT_GATEWAY_URL, help="Ingestion Gateway URL")
    parser.add_argument("--speedup", type=float, default=DEFAULT_SPEEDUP, help="Speedup factor")
    parser.add_argument("--max_records", type=int, default=0, help="Stop after X records")
    return parser.parse_args()

def ensure_data_ready():
    docker_data = "/data"
    if os.path.exists(os.path.join(docker_data, f"{FILENAME}_sorted.parquet")):
        return os.path.join(docker_data, f"{FILENAME}_sorted.parquet")
    
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
    if os.path.exists(SORTED_PATH): return SORTED_PATH

    print(f"Downloading {DOWNLOAD_URL}...")
    try:
        r = requests.get(DOWNLOAD_URL, stream=True)
        r.raise_for_status()
        with open(ORIGINAL_PATH, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
    except Exception as e:
        print(f"DL Error: {e}")
        exit(1)

    print("Sorting data...")
    try:
        df = pl.read_parquet(ORIGINAL_PATH)
        year, month = map(int, re.findall(r'(\d{4})-(\d{2})', FILENAME)[0])
        start, end = datetime(year, month, 1), datetime(year, month+1, 1) if month<12 else datetime(year+1, 1, 1)
        df = df.filter((pl.col("tpep_dropoff_datetime") >= start) & (pl.col("tpep_dropoff_datetime") < end))
        df = df.sort("tpep_dropoff_datetime")
        df.write_parquet(SORTED_PATH)
        return SORTED_PATH
    except Exception as e:
        print(f"Process Error: {e}")
        exit(1)

def print_status_bar(sim_time, rate, total, late, queue_size):
    sys.stdout.write(f"\r\x1b[2K🚕 Sim: \033[1m{sim_time}\033[0m | ⚡ {rate:.0f} req/s | 📦 {total:,} | 🐢 {late} | Queue: {queue_size} ")
    sys.stdout.flush()

def send_batch(session, url, records):
    if not records: return True
    try:
        payload = [
            {k: (v.isoformat() if isinstance(v, (datetime)) else v) for k, v in r.items()}
            for r in records
        ]
        session.post(url, json=payload, timeout=5)
        return True
    except Exception as e:
        return False

def simulate_volume(args, data_path):
    print(f"\n--- Starting Generator ---")
    print(f"Target: {args.gateway}/trips | Speed: {args.speedup}x")

    session = requests.Session()
    
    df = pl.scan_parquet(data_path).collect()
    iterator = df.iter_rows(named=True)
    
    try:
        first_record = next(iterator)
    except StopIteration:
        return

    current_sim_time = first_record['tpep_dropoff_datetime']
    late_buffer = [] 
    sent_count = 0
    late_count = 0
    
    pending_batch = [first_record]
    
    start_real_time = time.time()
    last_ui_update = time.time()
    last_flush_time = time.time()
    
    sleep_debt = 0.0
    tie_breaker = 0

    try:
        for row in iterator:
            if args.max_records > 0 and sent_count >= args.max_records: break
            
            row_time = row['tpep_dropoff_datetime']
            
            while late_buffer and late_buffer[0][0] <= row_time:
                _, _, late_row = heapq.heappop(late_buffer)
                pending_batch.append(late_row)
                late_count += 1
            
            time_delta = (row_time - current_sim_time).total_seconds()
            
            if time_delta > 0:
                real_sleep = time_delta / args.speedup
                sleep_debt += real_sleep
                
                if sleep_debt > 0.01:
                    if pending_batch:
                         if send_batch(session, f"{args.gateway}/trips", pending_batch):
                            sent_count += len(pending_batch)
                         pending_batch = []
                         last_flush_time = time.time()

                    elapsed = time.time() - start_real_time
                    rate = sent_count / elapsed if elapsed > 0 else 0
                    print_status_bar(current_sim_time, rate, sent_count, late_count, len(late_buffer))

                    time.sleep(sleep_debt)
                    sleep_debt = 0.0
                
                current_sim_time = row_time

            flag = str(row.get('store_and_fwd_flag', 'N')).upper()
            if flag == 'Y':
                due = row_time + timedelta(seconds=random.randint(120, 1200))
                heapq.heappush(late_buffer, (due, tie_breaker, row))
                tie_breaker += 1
            else:
                pending_batch.append(row)

            if time.time() - last_flush_time > 1.0 and len(pending_batch) > 0:
                if send_batch(session, f"{args.gateway}/trips", pending_batch):
                    sent_count += len(pending_batch)
                pending_batch = []
                last_flush_time = time.time()

            if time.time() - last_ui_update > 0.2:
                elapsed = time.time() - start_real_time
                rate = sent_count / elapsed if elapsed > 0 else 0
                print_status_bar(current_sim_time, rate, sent_count, late_count, len(late_buffer))
                last_ui_update = time.time()

    except KeyboardInterrupt:
        print("\nStopping...")

    if pending_batch:
        send_batch(session, f"{args.gateway}/trips", pending_batch)
        sent_count += len(pending_batch)
        
    print(f"\nTotal Sent: {sent_count:,}")

if __name__ == "__main__":
    args = get_args()
    final_path = ensure_data_ready()
    simulate_volume(args, final_path)
