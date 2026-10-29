# Changelog

potik follows [Semantic Versioning](https://semver.org). Before 1.0, a minor version can break the API.

## 0.1.0 (2026-10-29)

The first release.

- Async handlers whose outputs commit with the input's offset in one Kafka transaction, so `read_committed` consumers see each output exactly once.
- Concurrent partitions, with records in order within each partition, and handlers that may run for as long as they need.
- Opt-in key concurrency, which runs a topic's records with different keys concurrently and keeps each key's records in order, with exactly-once outputs.
- Typed topics with pydantic, msgspec, and text serdes, and async serdes for formats that need I/O to decode.
- Exception rules that retry handlers, skip records, or write dead letters. A failure without a rule stops only its partition.
- Concurrency limits, a startup check for missing topics, and OpenTelemetry tracing and metrics as middleware.
- Shutdown on SIGTERM, and liveness and readiness checks for Kubernetes probes.
- A `Producer` that sends outside transactions, and a `MockCluster` for tests.
- A `ReplyListener` that awaits replies to requests in the API replica that sent them.
- `potik.core`, the runtime without the typed layer.
