import time
import os
import sys
import argparse
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

def check_data(table_name):
    """
    Connects to Cassandra and checks if any data has been written to the 
    specified table. Retries for up to 60 seconds.
    """
    print("Connecting to Cassandra...")
    
    # Retry connection logic
    cluster = None
    session = None
    
    for i in range(10):
        try:
            # When running in Github Actions (or local), we connect to localhost:9042
            cluster = Cluster(['localhost'], port=9042)
            session = cluster.connect('taxi_streaming')
            print("Connected to Cassandra!")
            break
        except Exception as e:
            print(f"Connection failed ({e}), retrying in 5s...")
            time.sleep(5)
            
    if not session:
        print("Could not connect to Cassandra after retries.")
        sys.exit(1)

    print(f"Polling for data in '{table_name}'...")
    
    # Poll for data
    timeout = 600
    start_time = time.time()
    
    query = f"SELECT count(*) as cnt FROM {table_name}"
    
    while time.time() - start_time < timeout:
        try:
            row = session.execute(query).one()
            count = row.cnt
            print(f"Current Row Count: {count}")
            
            if count > 0:
                print("SUCCESS: Data found in Cassandra!")
                sys.exit(0)
                
        except Exception as e:
            print(f"Query Error: {e}")
            
        time.sleep(2)
        
    print(f"TIMEOUT: No data appeared in Cassandra table '{table_name}' within {timeout} seconds.")
    sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", type=str, default="zone_performance_30m", help="Cassandra table to check")
    args = parser.parse_args()
    
    check_data(args.table)
