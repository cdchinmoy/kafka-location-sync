from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer
import os
import json
import time

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC_LOCATION = os.environ.get("KAFKA_TOPIC_LOCATION", "live_location")

# Kafka producer
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

# FastAPI instance
app = FastAPI()

# Request body schema
class LocationData(BaseModel):
    latitude: float
    longitude: float
    timestamp: float = time.time()  # Optional: Defaults to current time
    user_id: str  # Identifier for the sender of the location data

# Delivery report callback
def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

@app.post("/publish-location")
async def publish_location(data: LocationData):
    """Publish live location data to Kafka topic."""
    try:
        # Serialize location data to JSON
        message = data.dict()
        producer.produce(
            KAFKA_TOPIC_LOCATION,
            json.dumps(message).encode("utf-8"),
            callback=delivery_report,
        )
        producer.flush()  # Ensure the message is sent
        return {"status": "Location data sent successfully"}
    except Exception as e:
        return {"status": "Failed to send location data", "error": str(e)}