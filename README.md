# Kafka Proof of Concept

Learning project exploring event-driven architecture with Apache Kafka.

## What This Covers

- Producers with idempotent delivery and Avro schemas
- Consumer groups, offset management, manual commits
- Stream processing with tumbling window aggregations
- Schema Registry for schema evolution

## Topics

| Topic | Purpose |
|-------|---------|
| ride-events | Ride lifecycle events |
| ride-analytics | Aggregated metrics |
| payment-events | Payment lifecycle |
| driver-location | GPS updates |

## Quick Start

make setup, make produce, make consume (separate terminals).

## Key Learnings

- Idempotent producers prevent duplicate events
- Consumer groups enable parallel processing
- Manual commits give at-least-once guarantees
- Windowed aggregations for real-time metrics
- Schema Registry is essential for evolution

## License

MIT
