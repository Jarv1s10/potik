# Changelog

potik follows [Semantic Versioning](https://semver.org). Before 1.0, a minor version can break the API.

## Unreleased

- `ExponentialBackoff` rejects NaN in `initial_seconds`, `max_seconds`, and `multiplier` at construction, instead of failing during a retry or silently ignoring a NaN cap.
- `TracingMiddleware` no longer unpacks a single `ProducerRecord` that a middleware returns in place of a list, so the runtime reports the mistake as such instead of as a `str` in the list.
- A topic of `float` values or keys takes an `int` too, and one of `complex` an `int` or a `float`, as type checkers do, instead of failing with `EncodeError`.
- A handler's value and `Key()` parameters read the record that middleware passes to `call_next`, through its own `value()` and `key()`, e.g. a subclass with a decrypted value. They read `confluent_kafka.Message`'s methods before, which ignored the subclass and failed for other record types.
- `potik.partition.stopped` reports every run of a meter provider, also when some `MetricsMiddleware` instances use it as the global provider and others are given it explicitly. The runs of the instances that named it the other way than the first one went unreported.
- A `Depends(functools.partial(fn))` of a function decorated with `@contextmanager` or `@asynccontextmanager` fails registration, as `Depends(fn)` does. The handler got the unentered context manager before, and its teardown never ran.
- `RetryAfter` rejects a NaN delay with `ValueError`, which stops the partition with the handler's failure as its context. A NaN delay, e.g. from a malformed `Retry-After` value, waited forever before.
- `copy.copy()` and `copy.deepcopy()` of a `Topic`, or of an object that holds one, e.g. settings, return the same topic, as for any immutable object. They raised `AttributeError` before.

## 0.2.0 (2026-10-30)

- `Depends()` gives handlers dependencies in the style of FastAPI: sync and async functions, and functions that yield, whose code after `yield` runs after each handler call and sees its exception. A dependency's parameters take parts of the record, the run's context, and other dependencies, and registration checks them against the topic. A call decodes each part of the record once for all the parameters that take it, and a dependency can limit how long a call runs with `anyio.fail_after()` or `anyio.move_on_after()` around `yield`.
- A transaction that fails with an error that requires an abort, e.g. the sequence gap that an aborted attempt leaves after a rebalance, is aborted and repeated, up to three attempts, instead of failing the run.
- Committing and aborting a transaction wait at most `transaction_timeout_ms`, so a broker outage longer than a transaction fails the run instead of hanging it. librdkafka 2.16.0 can still hold a transaction's offset commit until a broker returns ([confluentinc/librdkafka#5456](https://github.com/confluentinc/librdkafka/issues/5456)), and the liveness probe then restarts the process.
- Reading a partition's log bounds when it is assigned, and a reply listener's end offsets on entry, retry a request that loses its broker connection, e.g. right after startup.

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
