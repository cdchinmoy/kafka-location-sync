
# kafka-location-sync

A real-time location data processing pipeline that ingests live location updates from a Kafka topic and batches them into a MySQL database. This project utilizes FastAPI to publish location data, Kafka for message streaming, and MySQL for efficient batch data storage.

## Features

- **Publish location data**: Send real-time location updates (latitude, longitude, timestamp, user_id) via a FastAPI endpoint.
- **Kafka streaming**: Publish location data to a Kafka topic and consume the data with a Kafka consumer.
- **Batch insert**: Consume location data from Kafka and insert it into a MySQL database in batches.
- **MySQL integration**: Uses MySQL to store location data efficiently, supporting batch processing for better performance.

## Requirements

- Python 3.x
- Kafka (running locally or on a server)
- MySQL server (running locally or on a server)
- Docker (optional for easy setup)

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Kafka and MySQL Configuration

Make sure you have Kafka and MySQL running. Update the following environment variables:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: `localhost:29092`)
- `KAFKA_TOPIC_LOCATION`: Kafka topic name for location data (default: `live_location`)
- MySQL database connection details:
  - `host`: MySQL server address
  - `user`: MySQL username
  - `password`: MySQL password
  - `database`: MySQL database name

### 3. Running the FastAPI app

To run the FastAPI application that publishes location data to Kafka:

```bash
python producer.py --reload
```

Visit the endpoint at `http://localhost:8080/docs` to interact with the API and send location data.

### 4. Running the Kafka Consumer

Run the Kafka consumer script to start consuming the location data from Kafka and inserting it into MySQL:

```bash
python kafka_consumer.py
```

### 5. Docker (Optional)

For easy setup, you can use Docker to run Kafka and MySQL containers. The project includes a basic `docker-compose.yml` to set up the environment.

```bash
docker-compose up
```

This will start Kafka, Zookeeper, and MySQL containers automatically.

## Endpoints

### `POST /publish-location`

Publish location data to the Kafka topic.

**Request body:**

```json
{
  "latitude": 52.5200,
  "longitude": 13.4050,
  "user_id": "user_123"
}
```

**Response:**

```json
{
  "status": "Location data sent successfully"
}
```
