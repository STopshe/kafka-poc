"""Central config for Kafka POC."""
import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

TOPICS = {
    "ride_events": "ride-events",
    "payment_events": "payment-events",
    "driver_location": "driver-location",
    "ride_analytics": "ride-analytics",
}

CONSUMER_GROUPS = {
    "analytics": "analytics-consumer-group",
    "notifications": "notification-consumer-group",
    "audit": "audit-consumer-group",
}
