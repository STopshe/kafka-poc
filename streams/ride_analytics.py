"""Stream processor with tumbling windows."""
import json
from collections import defaultdict
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    "ride-events",
    bootstrap_servers=["localhost:9092"],
    group_id="stream-processor",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

WINDOW = 60
window_start = datetime.utcnow()
window_counts = defaultdict(int)

if __name__ == "__main__":
    print("Stream processing: ride-events -> ride-analytics")
    try:
        for msg in consumer:
            event = msg.value
            window_counts[event.get("status", "UNKNOWN")] += 1
            now = datetime.utcnow()
            if (now - window_start).total_seconds() >= WINDOW:
                result = {
                    "window_start": window_start.isoformat(),
                    "counts": dict(window_counts),
                    "total": sum(window_counts.values()),
                }
                producer.send("ride-analytics", value=result)
                print(f"Flushed: {result}")
                window_counts.clear()
                window_start = now
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        consumer.close()
        producer.close()
