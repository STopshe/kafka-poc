"""Ride event producer for Kafka POC."""
import json, time, random, uuid
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks="all",
    retries=5,
    enable_idempotence=True,
)

STATUSES = ["REQUESTED", "ACCEPTED", "IN_PROGRESS", "COMPLETED", "CANCELLED"]

def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "ride_id": str(uuid.uuid4()),
        "status": random.choice(STATUSES),
        "passenger_id": f"user_{random.randint(1000, 9999)}",
        "driver_id": f"driver_{random.randint(1000, 9999)}" if random.random() > 0.2 else None,
        "pickup_lat": round(random.uniform(40.6, 40.8), 6),
        "pickup_lng": round(random.uniform(-74.05, -73.85), 6),
        "timestamp": datetime.utcnow().isoformat(),
        "fare": round(random.uniform(8.0, 45.0), 2) if random.random() > 0.5 else None,
    }

if __name__ == "__main__":
    print("Producing ride events...")
    try:
        for _ in range(100):
            event = generate_event()
            producer.send("ride-events", key=event["ride_id"], value=event)
            time.sleep(0.5)
        producer.flush()
        print("Done.")
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        producer.close()
