import os
import threading
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from src.db_manager import PostgreSQLManager
from src.consumer import FeatureConsumer
from src.models import FeatureResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
db_manager = None
consumer = None
consumer_thread = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_manager, consumer, consumer_thread
    logger.info("Starting up FastAPI application...")
    
    # Initialize DB Manager
    db_manager = PostgreSQLManager(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "feature_store"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres")
    )
    
    # Initialize Kafka Consumer
    consumer = FeatureConsumer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.getenv("RAW_EVENTS_TOPIC", "raw-events"),
        db_manager=db_manager
    )
    
    # Start consumer in a background thread
    consumer_thread = threading.Thread(target=consumer.start_consuming, daemon=True)
    consumer_thread.start()
    
    yield # App is running
    
    logger.info("Shutting down FastAPI application...")
    
    if consumer:
        consumer.stop()
    if consumer_thread:
        consumer_thread.join(timeout=5.0)
    if db_manager:
        db_manager.close()

app = FastAPI(lifespan=lifespan, title="Event-Driven ML Feature Store")

@app.get("/features/{entity_id}", response_model=FeatureResponse)
async def get_features(entity_id: str):
    if not db_manager:
        raise HTTPException(status_code=500, detail="Database not initialized")
    
    features = db_manager.get_features(entity_id)
    if not features:
        raise HTTPException(status_code=404, detail="Entity not found or no features available")
    
    return FeatureResponse(entity_id=entity_id, features=features)
