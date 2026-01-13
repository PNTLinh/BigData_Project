import os
import time
import subprocess
import logging
import sys
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("batch_scheduler")

SPARK_JARS = (
    "/opt/spark-jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,"
    "/opt/spark-jars/kafka-clients-3.4.1.jar,"
    "/opt/spark-jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,"
    "/opt/spark-jars/commons-pool2-2.11.1.jar,"
    "/opt/spark-jars/spark-cassandra-connector-assembly_2.12-3.5.1.jar"
)

SUBMIT_CMD = [
    "/opt/spark/bin/spark-submit",
    "--master", "local[*]",
    "--jars", SPARK_JARS,
    "--conf", "spark.sql.streaming.checkpointLocation=/tmp/spark_checkpoints_batch", 
    "/opt/spark-apps/streaming.py",
    "--mode", "batch"
]

def get_args():
    parser = argparse.ArgumentParser(description="Spark Batch Job Scheduler")
    parser.add_argument("--speedup", type=float, default=3600.0, help="Simulation speedup factor")
    return parser.parse_args()

def run_batch_job():
    logger.info(">>> Triggering Daily Batch Job...")
    start_time = time.time()
    
    try:
        result = subprocess.run(
            SUBMIT_CMD,
            stdout=sys.stdout,
            stderr=sys.stderr,
            text=True
        )
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            logger.info(f"Batch Job Completed Successfully in {duration:.2f}s.")
        else:
            logger.error(f"Batch Job Failed after {duration:.2f}s!")

    except Exception as e:
        logger.error(f"Failed to execute subprocess: {e}")
        return 0.0

    return time.time() - start_time

def main():
    args = get_args()
    
    real_seconds_per_sim_day = 86400.0 / args.speedup
    
    logger.info("--- Spark Batch Scheduler Service Started ---")
    logger.info(f"Simulation Speedup: {args.speedup}x")
    logger.info(f"Schedule: Running every {real_seconds_per_sim_day:.2f} seconds (Simulated Day)")
    
    while True:
        duration = run_batch_job()
        
        sleep_time = max(0, real_seconds_per_sim_day - duration)
        
        if sleep_time > 0:
            logger.info(f"Job took {duration:.2f}s. Waiting {sleep_time:.2f}s until next run...")
            time.sleep(sleep_time)
        else:
            logger.warning(f"Job took {duration:.2f}s, which exceeded interval {real_seconds_per_sim_day:.2f}s. Running next immediately!")

if __name__ == "__main__":
    main()
