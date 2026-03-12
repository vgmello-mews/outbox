# EventHub Outbox Pattern POC

Proof-of-concept for a transactional outbox pattern targeting Azure Event Hubs.

## Root files

- `OutboxPublisher.cs` -- Azure EventHub reference implementation
- `EventHubOutbox.sql` -- SQL schema (tables, indexes, partition buckets)
- `EventHubOutboxSpec.md` -- design spec

## test/

Self-contained Kafka/Redpanda integration test harness with Docker Compose.
Spins up Azure SQL Edge, Redpanda, an event producer, and the outbox publisher.

## Running

```bash
cd test && SA_PASSWORD=YourPassword docker compose up --build
```
