# Event-Driven ML Feature Store Client

## Overview
This project implements an event-driven machine learning feature store client using Python, Apache Kafka, PostgreSQL, and FastAPI. It demonstrates a robust real-time data ingestion pipeline that processes raw data events instantly, manages stateful feature computation, ensures idempotent data storage, and serves features with low-latency REST APIs.

## Architecture & Technical Choices

### Components Orchestrated via Docker Compose
- **Event Producer (`producer.py`)**: Simulates a high-throughput upstream microservice producing raw user interactions.
- **Apache Kafka Broker**: Buffers massive volumes of events using the `raw-events` topic, providing an asynchronous decoupler between generation and processing.
- **Zookeeper**: Manages Kafka cluster state.
- **PostgreSQL**: Acts as the "online" feature store.
- **Feature Client Service**: A Python container that simultaneously runs the FastAPI ASGI web server and a background Kafka consumer thread.

### Kafka Topic Design
We utilize a single `raw-events` topic. Kafka's consumer group configuration ensures that the service can gracefully process and commit offsets without losing messages, even upon restart. `KAFKA_ADVERTISED_LISTENERS` is mapped precisely in `docker-compose.yml` to support both internal Docker network traffic and external host machine traffic (e.g., from `producer.py`).

### PostgreSQL Schema
The database uses a robust `features` table:
- `entity_id` (Indexed for fast read lookups by FastAPI)
- `feature_name`
- `feature_value`
- `timestamp`

To guarantee idempotency, we defined a **composite `PRIMARY KEY (entity_id, feature_name)`**.

## Resilience & Error Handling

### Idempotency Strategy
Because Kafka guarantees *at-least-once* delivery, duplicates can occur. When the consumer processes an event, the `db_manager.py` executes an `INSERT ... ON CONFLICT (entity_id, feature_name) DO UPDATE` query. This ensures that even if Kafka delivers the exact same message ten times, the feature store's values remain accurate, simply updating the timestamp.

### Resilient Kafka Consumer & Pydantic Validation
The Kafka consumer (`src/consumer.py`) is wrapped in strict `try...except` blocks. Incoming JSON strings are validated instantly against rigid `Pydantic` schemas (`RawEvent`). If an upstream service sends malformed JSON, Pydantic immediately throws a `ValidationError`. The consumer explicitly catches this, logs the failure, and skips the message without crashing the background thread.

### Graceful Orchestration
The FastAPI application leverages the modern `@asynccontextmanager` lifespan event. On startup, it securely spawns the `confluent-kafka` consumer within a daemonized `threading.Thread`. On shutdown (e.g., SIGTERM), a stop flag is sent to the consumer thread, allowing it to close the Kafka connection, cleanly commit offsets, and exit gracefully without resource leaks. Furthermore, a `run_app.sh` script utilizes `nc` (netcat) to guarantee Postgres and Kafka are fully operational before `uvicorn` ever starts.

## Setup Instructions

### 1. Configure Environment
Copy the example environment file:
```bash
cp .env.example .env
```
(No modifications are necessary for default local execution).

### 2. Launch the Infrastructure
Use Docker Compose to build and start the entire pipeline:
```bash
docker-compose up --build -d
```
Wait a few seconds for the `feature-client` container to confirm that Kafka and Postgres are healthy and the API server has started.

## Generating Sample Data

A standalone script is provided to simulate high-throughput real-world traffic. Run this script on your host machine to fire 10,000 raw JSON events into the Kafka broker:

```bash
# Ensure dependencies are installed on host (or run within a virtualenv)
pip install -r requirements.txt
python producer.py
```
*Note: The producer uses a slight sleep interval to simulate ~100 requests/second without overwhelming local buffers.*

## API Documentation

The feature store is exposed via a high-performance REST endpoint on port `8000`.

### Fetch Entity Features
**Endpoint**: `GET /features/{entity_id}`

**Example Request:**
```bash
curl -X GET "http://localhost:8000/features/user_1" -H "accept: application/json"
```

**Successful Response (HTTP 200):**
```json
{
  "entity_id": "user_1",
  "features": {
    "last_action": "click",
    "user_activity_count": "45"
  }
}
```

**Not Found Response (HTTP 404):**
Returned gracefully when an entity ID does not exist in the store.

*A Postman collection (`Event_Driven_ML_API.postman_collection.json`) is also included in the repository for immediate testing.*

## Running Tests Locally

A comprehensive suite of unit tests has been written using `pytest`. These tests validate Pydantic models (success and failure scenarios) and use `TestClient` alongside mocked dependencies to ensure business logic is perfectly isolated.

To run the tests:
```bash
# Locally via your virtualenv
pytest tests/
```
