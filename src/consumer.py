import logging
import json
from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError
from src.models import RawEvent
from src.db_manager import PostgreSQLManager

logger = logging.getLogger(__name__)

class FeatureConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, db_manager: PostgreSQLManager):
        self.topic = topic
        self.db_manager = db_manager
        self._stop_event = False
        
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'feature-store-group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([self.topic])
        logger.info(f"Subscribed to topic: {self.topic}")

    def start_consuming(self):
        logger.info("Starting Kafka consumer loop...")
        while not self._stop_event:
            msg = self.consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
            
            try:
                raw_value = msg.value().decode('utf-8')
                event_dict = json.loads(raw_value)
                
                event = RawEvent(**event_dict)
                
                # Update 'last_action'
                self.db_manager.save_feature(event.entity_id, "last_action", event.action_type)
                
                # Update 'user_activity_count'
                current_count_str = self.db_manager.get_feature(event.entity_id, "user_activity_count")
                current_count = int(current_count_str) if current_count_str else 0
                self.db_manager.save_feature(event.entity_id, "user_activity_count", str(current_count + 1))
                
                logger.debug(f"Processed event {event.event_id} for entity {event.entity_id}")
                
            except json.JSONDecodeError:
                logger.warning(f"Malformed JSON received: {msg.value()}")
            except ValidationError as e:
                logger.warning(f"Event validation failed for message: {e.errors()}")
            except Exception as e:
                logger.error(f"Unexpected error processing message: {e}")

    def stop(self):
        logger.info("Shutting down Kafka consumer...")
        self._stop_event = True
        # Let the poll loop complete, then close connection
        self.consumer.close()
