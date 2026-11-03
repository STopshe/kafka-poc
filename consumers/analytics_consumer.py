"""Analytics consumer for Kafka POC."""
import json
from collections import defaultdict
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "ride-events",
    bootstrap_servers=["localhost:9092"],
    group_id="analytics-consumer-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
)

counts = defaultdict(int)
total = 0

if __name__ == "__main__":
    print("Consuming ride events...")
    try:
        for msg in consumer:
            event = msg.value
            counts[event.get("status", "UNKNOWN")] += 1
            total += 1
            if total % 10 == 0:
                print(f"--- {total} events ---")
                for s, c in sorted(counts.items()):
                    print(f"  {s}: {c}")
            consumer.commit()
    except KeyboardInterrupt:
        print(f"Final: {dict(counts)}")
    finally:
        consumer.close()
