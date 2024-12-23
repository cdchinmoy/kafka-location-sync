from confluent_kafka import Consumer
import mysql.connector
import json
import time
from dotenv import load_dotenv
import os
load_dotenv()

# Kafka Consumer Configuration
KAFKA_BROKER = f"{os.getenv("KAFKA_SERVER")}:{os.getenv("KAFKA_PORT")}"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
})

consumer.subscribe([KAFKA_TOPIC])

# MySQL Connection
def create_connection():
    try:
        connection = mysql.connector.connect(
            host = os.getenv("DB_HOST"),
            user = os.getenv("DB_USER"),
            password = os.getenv("DB_PASSWORD"),
            database = os.getenv("DB_NAME"),
        )
        if connection.is_connected():
            print("Connected to MySQL")
            return connection
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

connection = create_connection()

def batch_insert_data(connection, data):
    """
    Insert data into MySQL in batches.
    :param connection: MySQL connection object
    :param data: List of tuples containing location data
    """
    query = """
    INSERT INTO location_data (user_id, latitude, longitude, timestamp)
    VALUES (%s, %s, %s, %s)
    """
    try:
        cursor = connection.cursor()
        cursor.executemany(query, data)
        connection.commit()
        print(f"{cursor.rowcount} rows inserted.")
    except mysql.connector.Error as e:
        print(f"Error during batch insert: {e}")
        connection.rollback()

# Consumer Loop with Batch Size and Timer
BATCH_SIZE = 10
TIMEOUT = 5  # Seconds

buffer = []

try:
    while True:
        msg = consumer.poll(1.0)  # Poll messages with a 1-second timeout
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Decode the Kafka message and add it to the buffer
        message = json.loads(msg.value().decode("utf-8"))
        buffer.append((message['user_id'], message['latitude'], message['longitude'], message['timestamp']))

        # Check if we should flush the buffer
        if len(buffer) >= BATCH_SIZE:
            print(f"Flushing {len(buffer)} records to the database...")
            batch_insert_data(connection, buffer)
            buffer = []  # Clear the buffer

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    # Final flush of any remaining records in the buffer
    if buffer:
        print(f"Flushing {len(buffer)} remaining records to the database...")
        batch_insert_data(connection, buffer)
    consumer.close()
    if connection:
        connection.close()