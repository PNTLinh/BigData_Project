import os
import json
import logging
from typing import Dict, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "taxi-trips")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gateway")

app = FastAPI(title="Ingestion Gateway")

producer = None

@app.on_event("startup")
def startup_event():
    global producer
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'queue.buffering.max.messages': 100000,
        'linger.ms': 10,  
        'acks': '1',      
    })

@app.on_event("shutdown")
def shutdown_event():
    if producer:
        logger.info("Flushing Kafka producer...")
        producer.flush(timeout=5.0)

class TripEvent(BaseModel):
    data: Dict[str, Any]

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/trip")
async def ingest_trip(event: Dict[str, Any]):
    """
    Receives a single trip record validation-free (for speed) and pushes to Kafka.
    """
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka producer not initialized")

    try:
        key = str(event.get('VendorID', '')).encode('utf-8')
        value = json.dumps(event).encode('utf-8')
        
        producer.produce(KAFKA_TOPIC, key=key, value=value)
        producer.poll(0) 
        
        return {"status": "accepted"}
    except Exception as e:
        logger.error(f"Failed to produce: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/trips")
async def ingest_trips(events: list[Dict[str, Any]]):
    """
    Receives a batch of trip records for high-throughput ingestion.
    Reduces HTTP overhead significantly.
    """
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka producer not initialized")

    try:
        count = 0
        for event in events:
            key = str(event.get('VendorID', '')).encode('utf-8')
            value = json.dumps(event).encode('utf-8')
            producer.produce(KAFKA_TOPIC, key=key, value=value)
            count += 1
            if count % 1000 == 0:
                producer.poll(0) 
        
        producer.poll(0) 
        return {"status": "accepted", "count": count}
    
    except Exception as e:
        logger.error(f"Failed to produce batch: {e}")
        raise HTTPException(status_code=500, detail=str(e))
