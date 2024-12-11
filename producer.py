from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka.admin import AdminClient, NewTopic, KafkaException
from confluent_kafka import Producer
import uvicorn
from dotenv import load_dotenv
import os
load_dotenv()
import json
import time

# Environment variables
KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_PORT = os.getenv("KAFKA_PORT")
KAFKA_BROKER = f"{KAFKA_SERVER}:{KAFKA_PORT}"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
print("KAFKA_TOPIC", KAFKA_TOPIC)

# Kafka producer
producer = Producer({"bootstrap.servers": KAFKA_BROKER})

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



def create_kafka_topic(topic_name: str, broker):
    """Create a Kafka topic if it doesn't exist."""
    admin_client = AdminClient({"bootstrap.servers": broker})
    topic = NewTopic(topic_name, 1, 1)
    try:
        futures = admin_client.create_topics([topic])
        for topic, future in futures.items():
            try:
                future.result()
                print(f"Topic '{topic}' created successfully.")
            except KafkaException as e:
                if e.args[0].code() == 36:  # TOPIC_ALREADY_EXISTS
                    print(f"Topic '{topic}' already exists.")
                else:
                    print(f"Failed to create topic '{topic}': {e}")
    except Exception as e:
        print(f"Error during topic creation: {e}")


@app.on_event("startup")
async def startup_event():
    """Application startup event to create Kafka topic."""
    create_kafka_topic(KAFKA_TOPIC, KAFKA_BROKER)


@app.post("/publish-location")
async def publish_location(data: LocationData):
    """Publish live location data to Kafka topic."""
    try:
        # Serialize location data to JSON
        message = data.dict()
        producer.produce(
            KAFKA_TOPIC,
            json.dumps(message).encode("utf-8"),
            callback=delivery_report,
        )
        producer.flush()  # Ensure the message is sent
        return {"status": "Location data sent successfully"}
    except Exception as e:
        return {"status": "Failed to send location data", "error": str(e)}
    

if __name__ == "__main__":
    
    uvicorn.run("producer:app", host="0.0.0.0", port=8080, reload=True)