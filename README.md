# Card Status Pipeline — Testcontainers Load Test (Java / Spring Boot)

Demonstrates testing the full pipeline — **external status service call → PostgreSQL write → Kafka publish** —
against **real, disposable infrastructure** (not mocks of Kafka/DB). Only the external card-status
service is faked, using the same WireMock mapping files as [`../mock-service`](../mock-service).

## What's real vs. faked

| Component | In this test |
|---|---|
| PostgreSQL | Real (Testcontainers `postgres:16-alpine`) |
| Kafka | Real (Testcontainers `confluentinc/cp-kafka`) |
| External card-status service | Faked (Testcontainers running the same `wiremock/wiremock` image + mapping files from `mock-service/mappings`) |

Kafka and the DB are never mocked — their real behavior (offsets, transactions, connection pooling)
is exactly what you want under load, so they run as real, throwaway containers instead.

## Requirements

- Docker running locally (Testcontainers needs it, same as `mock-service`)
- JDK 17+, Maven

## Run it

```bash
mvn test
```

This starts 3 containers, runs 500 simulated card-status requests through the real pipeline with
20 concurrent workers, then prints a report like:

```
=== Load test report ===
records: 500
success: 398  failure: 102
wall-clock total: 3120 ms, throughput: 160.3 records/sec
per-record total latency   p50=45ms p95=210ms p99=340ms
external-call latency only p50=38ms p95=195ms p99=310ms
Kafka messages drained by test consumer: 500
partition 0 consumer lag = 0
```

## Using it as a load/stress test

- **Change file size**: edit `recordCount` in `CardStatusPipelineLoadTest` (500 → 5,000 → 50,000) to
  simulate progressively larger uploaded files. Watch p95/p99 and throughput — when they degrade
  *faster* than the record count grows, you've found the bottleneck stage.
- **Change concurrency**: edit `concurrency` to match (or exceed) your real service's connection
  pool / thread pool size for calling the external API, to see where contention starts.
- **Isolate a stage**: `ProcessingResult` carries `externalCallMs`, `dbWriteMs`, and
  `kafkaPublishMs` separately — the report only prints external-call latency and total right now,
  but all three are available if you want to see exactly which stage dominates.
- **Kafka consumer lag**: `assertConsumerLagIsZeroAfterDraining()` shows the actual API
  (`AdminClient.listConsumerGroupOffsets` + `listOffsets`) used to compute lag — the same approach
  a monitoring job or `kafka-consumer-groups.sh --describe` uses. In a real load test, poll this
  periodically *during* the run instead of once at the end, to see lag rise and fall over time.
- **Force specific external-service behavior**: the WireMock mappings still respond to the
  `X-Simulate: success|error|timeout` header if you want a deterministic case mixed into the load
  test, in addition to the default ~20% cyclic failure rate.

## Notes

- This wasn't run end-to-end in the environment that generated it (no Docker/Maven available there) —
  compile and run it locally before trusting the numbers in this README, which are illustrative only.
