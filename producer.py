import json
import uuid
import time
import random
import os
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        pass # Print skipped to avoid too much noise

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("RAW_EVENTS_TOPIC", "raw-events")
    
    conf = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(conf)
    
    users = [f"user_{i}" for i in range(1, 11)]
    actions = ["click", "view", "purchase", "login", "logout"]
    
    print("Starting high-throughput producer...")
    for i in range(10000): # Fire 10,000 events
        event = {
            "event_id": str(uuid.uuid4()),
            "entity_id": random.choice(users),
            "action_type": random.choice(actions),
            "timestamp": str(time.time()),
            "metadata": {"source": "producer_script"}
        }
        
        producer.produce(
            topic,
            key=event["entity_id"],
            value=json.dumps(event),
            callback=delivery_report
        )
        producer.poll(0)
        
        if i % 1000 == 0:
            print(f"Produced {i} messages...")
            
        time.sleep(0.01) # ~100 msgs/second limit
        
    producer.flush()
    print("Finished producing 10,000 events.")

if __name__ == "__main__":
    main()
