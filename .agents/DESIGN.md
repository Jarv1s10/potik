# Design

How potik behaves, in more detail than the user docs in `docs/`. Docstrings in `src/` are the primary reference. Keep this file in sync with them when behavior changes.

## Records

`ConsumerRecord` is the native `confluent_kafka.Message`, so its fields are methods: `record.key()`, `record.value()`, `record.headers()`, `record.offset()`, and so on. The runtime never copies records into wrappers.

`ProducerRecord` is a `NamedTuple` of `(topic, key, value, headers, partition)`. Headers are either a list of `(name, value)` pairs, which keeps order and allows duplicates, or a dict. `partition=-1` leaves the choice to librdkafka's partitioner. For Java-compatible key partitioning, set `producer_options={"partitioner": "murmur2_random"}`.

## Topics

A `Topic` is the contract that a topic's producers and consumers share: its name, the types of its values and keys, and their serdes. Declare it once, e.g. in a module that the services share. The wire format is the topic's, so no service, and no annotation, can encode it differently.

```python
json = PydanticJson()
orders = Topic("orders", Order, serde=json)  # Topic[Order], keyed by str
audit = Topic("orders.audit", AuditEvent, serde=json)
by_number = Topic("orders.by_number", Accepted, key_type=int, serde=json)  # Topic[Accepted, int]
events = Topic("orders.events", Created | Deleted, serde=json)  # Topic[Created | Deleted]
```

Type checkers infer a topic's types from the given ones, whatever they are, e.g. unions, and check the values and keys of its outputs against them.

A serde (`potik.serde.Serde`) returns a decoder and an encoder for each type it supports, and `None` for the others, typed by the type asked for. Each call also gets a `SerdeContext`: the topic's name, and whether the type is its key or its value, so a serde can depend on the topic, e.g. to name a registry subject (ADR 0017). A topic's `serde` must support its value type, and its `key_serde` its `key_type` (`str` by default), or the declaration fails. Both serdes default to `Text`, which takes `bytes` as they are, `str` as UTF-8, and `int` and `UUID` as their text. Subclasses count as their base, e.g. a `StrEnum` as its value, and a `NewType` as its supertype. `Text` rejects `bool` and constrained types, e.g. `Annotated[str, Field(min_length=3)]`, rather than ignoring the constraint. `PydanticJson` (extra `pydantic`) and `MsgspecJson` (extra `msgspec`) take any type their library supports.

A topic gets its decoders and encoders from its serdes once, when it is declared, usually at import, so a serde does no I/O then. Decoders and encoders are synchronous and run on the event loop for every record, so they must not do I/O either. Every decoder and encoder failure is permanent: `retry()` rules leave the `DecodeError` or `EncodeError` to the next rule.

Decoding that needs I/O, e.g. Avro whose records name their writer schema in a registry, takes an `AsyncSerde` (ADR 0009). It returns a resolver in place of a decoder: an async function that does one record's I/O, e.g. fetching and caching the schema, and returns that record's decoder. The handler call awaits the resolver, then decodes. A resolver's failures propagate as they are, so `retry()` rules retry e.g. a registry's timeout. A resolver raises a `PermanentError` for a failure that would repeat, e.g. an unknown schema ID. The decoder that it returns fails like any other, with a permanent `DecodeError`. A handler's async parts resolve one after another, in parameter order, and handlers without them await nothing more. Headers' decoders and all encoders stay synchronous. An encoder that needs I/O, e.g. its writer schema's registry ID, gets it before the run: the serde learns every topic part when it is declared, so a method of its own, which the service's `main` awaits, registers their schemas. A topic declared during the run, e.g. per tenant, must have its schema registered by then too. A client that the serde needs, e.g. a registry's, belongs to whoever creates the serde, e.g. the service's `main`, as a `Producer` does (ADR 0001). potik gives serdes no lifespan.

Decoding validates, as far as the serde does. Encoding only serializes: potik checks that a value or key is an instance of its topic's class, because pydantic would serialize any object with the model's attributes, and `PydanticJson` fails on values that pydantic would warn about. Neither applies the schema's constraints. `await topic.decode(data)` decodes a value, and `await topic.decode_key(data)` a key, outside a handler, e.g. in middleware, and raises the serde's own exceptions. A topic is immutable.

## Typed handlers

Each handler parameter takes one part of the input record, independently of the others. The decorator returns the handler unchanged, so tests can call it with plain objects.

```python
from typing import Annotated

from potik import ConsumerRecord, Context, Depends, Header, Key


@router.handler(orders)
async def audit_order(
    order: Order,                                         # the one unmarked parameter: the value
    record: ConsumerRecord,                               # the record itself
    customer: Annotated[str, Key()],                      # the topic's key type
    attempt: Annotated[int | None, Header("x-attempt")],  # the header's last value
    tenant: Annotated[str, Header("x-tenant")] = "none",  # missing or null: the default
    sent: Annotated[datetime, Header("sent-at", decoder=parse_time)],
    services: Annotated[Services, Context()],             # the run's context
    customer: Annotated[Customer, Depends(customer)],     # what a dependency returns or yields
) -> None: ...
```

The value parameter takes the topic's value type, and a `Key()` parameter its key type, optionally `| None` for null ones, or `bytes` as they are. Anything else fails the registration, so a handler cannot disagree with its topic. A plain topic name, e.g. for a raw handler taking `ConsumerRecord`, is a topic of `bytes` values and `str` keys. `Text` decodes headers, unless the `Header` has a `decoder`. `record.headers()` has every header.

A null key, value, or header takes the parameter's default, else `None` if the type allows it. Anything else that a parameter cannot take raises `DecodeError`, which names the `parameter`, from the handler call, so middleware handles it like any handler failure, e.g. `on(DecodeError, dead_letter(...))`. Everything is checked when the handler is registered.

### Dependencies

A `Depends(fn)` parameter takes what `fn` returns, or what it yields (ADR 0010). A dependency's parameters take the same markers as a handler's, including the value and other dependencies, and registration checks them against the topic. `_compile_handler` compiles the handler and its dependencies together, once per handler, so a dependency that two handlers take compiles once for each topic. The compiler's map of dependencies holds None for a dependency while it compiles that dependency's parameters, so a cycle fails registration. A function decorated with `@contextmanager` or `@asynccontextmanager` fails registration too: it would otherwise pass as a sync dependency that returns a context manager, which potik would never enter. Registration recognizes it, or a callable object whose `__call__` is one, by the code of contextlib's wrapper, so other decorators around generators still pass. A parameter whose default is `Depends(...)`, FastAPI's other form, fails registration rather than taking the value. A marker wins over a `ConsumerRecord` annotation, so `Annotated[ConsumerRecord, Depends(fn)]` takes the dependency. The map of dependencies is keyed by the function's hash and equality, so the same bound method in two parameters resolves once, and an unhashable callable object fails registration. A call decodes each part of the record at most once. Compiling interns each part that a parameter takes in a `_Part`, keyed by what the part is (the value, the key, or a header by name) and how it decodes: its decoder and resolver, or for `Text`'s header decoders, which it makes anew for subclasses, the type they decode. Parameters that take the same `_Part` share its decoded object, so a mutation in a dependency is visible to the handler, as FastAPI shares a request's body. A decode costs more than the rest of injection, e.g. about 8µs for a 1KB pydantic value, and an `AsyncSerde`'s resolver may do I/O. Only the null handling stays with each parameter: its default, `None`, or a `DecodeError` naming it. A failed decode names the parameter that decoded the part first, in resolution order.

Whether a part is shared is known only once the whole graph has compiled, so `_bind` returns each part as `_PENDING`, and `_finish` then gives each parameter its extractor. A part that one parameter takes, or that a `bytes` parameter takes as it is, keeps the extractor of a function of the record alone, so a handler that shares no part pays nothing for this. A shared part's extractor also takes the call's values, and stores the decoded part there, beside the dependencies' values.

Every handler call takes one path, `_resolve`, whether it has dependencies or not. It walks the arguments in parameter order, and calls a dependency when it first meets it, after the dependency's own arguments. A dict keyed by the compiled dependency holds the call's values, so each dependency runs once per call. Sync dependencies run on the event loop. Dependencies that yield enter through `contextlib`'s factories, and their exits go on a list. A handler without dependencies costs about 0.07µs more per record than before dependencies existed, and a dependency that yields about 1.3µs.

Resolution is sequential, in the call's task, on purpose. Concurrent resolution, as in Litestar, costs about 50µs for a task group, more than a whole record's handling at full throughput, and potik cannot tell from a signature which dependencies do I/O. A dependency started in a child task and resumed in the call's task also breaks: a `ContextVar` that it sets is invisible to the handler, a cancel scope or task group held across `yield` fails to exit, and resetting a `ContextVar` token, e.g. OpenTelemetry's detach, raises `ValueError`. Two concurrent failures would also raise an `ExceptionGroup` of two, which exception rules see as a group (ADR 0015). Since the call runs in one task, middleware hands per-record values to dependencies in `ContextVar`s.

After the call, `_exit` resumes the dependencies that yield, last entered first. Each one sees the exception that the call, or a dependency resumed before it, raised. `_exit` ignores what an exit returns, as `_enter_lifespan` does, so a dependency that catches an exception without raising it again suppresses it neither for the other dependencies nor for the call. Otherwise a dependency could hide a failure from the exception rules, and the runtime would commit the record as handled. A cancellation is the one exception: a cancel scope that a dependency holds across `yield`, e.g. `anyio.move_on_after()`, cancels the handler and then catches its own cancellation. Raising it again would leave a `CancelledError` with no cancellation behind it, which `Partition._handle` does not catch, so the partition would stall and the shutdown hang. So the call ends without outputs, as a handler that runs `move_on_after()` itself would, and its record commits. The scope's deadline may also cut off the teardown of a dependency entered after it, e.g. a transaction's commit, so `_exit` reports that the call ended, and `handle` drops the outputs even when the handler returned. Since only cancellations may be suppressed, `_exit` keeps the last exception that is not a cancellation, and gives it back once a cancellation is suppressed, so a deadline that cuts off a rollback does not hide the handler's failure. A dependency that swallows an outer cancellation, e.g. a revocation's, looks the same to `_exit`, so after any suppressed cancellation it checks whether an enclosing scope is still cancelled, with `checkpoint_if_cancelled()`, and then raises it. Otherwise the record of a revoked call would commit without outputs, and never run again. `handle` raises the exception that `_exit` returns outside its `except` clause, which would replace the exception's context with the call's own, and drop the exceptions between them from the traceback. An exit that raises fails the call, so the record fails before its outputs commit. The exits run inside the call, and so inside each attempt of the exception rules: each retry resolves its own dependencies, and holds none of them during its backoff.

potik does not shield the exits. A revocation or shutdown cancels the call, so the code after `yield` runs in the cancelled scope, and a dependency shields its own awaits, with a timeout. A shield without a timeout could hold a revocation past `rebalance_timeout_ms`, and potik cannot pick a timeout that fits every dependency. The exits run before the transaction commits the outputs, so a write that a dependency commits after `yield` is at-least-once.

Tests have no registry of overrides. A unit test calls the handler, or a dependency, with plain objects. An app test passes fakes as the run's context, the one place where dependencies reach resources.

## Outputs

Calling a topic makes a `ProducerRecord`. Type checkers check the value and the key against the topic's types, and the call encodes them right away, so an `EncodeError` points at the line that made it. Every call names its key: `key=None` makes a null key, and `None` as the value a tombstone. To keep the input's key, take it with a `Key()` parameter and pass it on, which keeps each key's records in one partition downstream, like Kafka Streams' `mapValues`, as long as both topics use the same partitioner and their partition counts do not change. A topic chosen at runtime is a `Topic` too: `Topic(f"tenant.{tenant}", Event, serde=json)(event, key=tenant)`.

Middleware sees the raw input record, e.g. `context.record` in an exception rule, so a dead letter can copy its key and value bytes as they are, even when they do not decode. `dead_letter` does.

A handler returns `None`, a `ProducerRecord`, or a list or tuple of them, possibly empty. Anything else raises `InvalidOutputError` from the handler's call, inside the middleware, as the core does (see Core runtime). The decorator returns the handler unchanged, so tests can call it as a plain function.

```python
@router.handler(orders)
async def route(order: Order, customer: Annotated[str, Key()]) -> list[ProducerRecord]:
    return [audit(AuditEvent.of(order), key=customer), by_number(Accepted(...), key=order.number)]
```

`output_topics` names the topics that a handler returns records to, as `Topic`s or plain names of `bytes` topics, and `Router(output_topics=...)` names them for all of a router's handlers. A run checks that they exist before it starts, with the handled topics (see [Configuration](#configuration)), so a missing one fails the deployment rather than a transaction after its handlers ran. They do not restrict what a handler returns. With `output_topics`, type checkers also check the handler's annotated return type: records that its outputs make, e.g. `ProducerRecord[Accepted]` from calling `by_number`. A record made directly is a `ProducerRecord[Any]`, which they let through, and so is an unannotated `ProducerRecord`. The check compares value types, so it cannot tell apart two outputs of the same type.

```python
@router.handler(orders, output_topics=(audit, by_number))
async def route(
    order: Order, customer: Annotated[str, Key()]
) -> list[ProducerRecord[AuditEvent] | ProducerRecord[Accepted]]:
    return [audit(AuditEvent.of(order), key=customer), by_number(Accepted(...), key=order.number)]
```

## Sends

A `Producer` sends records right away, outside any transaction, with or without a runtime. Use it in a service that only produces, e.g. an API, with the same `Topic`s that its consumers declare, or in a handler that must publish before its transaction commits. Its owner enters it once, e.g. in a web framework's lifespan, which shares it through the framework's lifespan state.

```python
from potik import Producer
from potik.middleware.tracing import TracingMiddleware


@asynccontextmanager
async def lifespan(api: FastAPI) -> AsyncIterator[dict[str, Producer]]:
    async with Producer("localhost:9092", middleware=[TracingMiddleware()]) as producer:
        yield {"producer": producer}


@api.post("/orders")
async def place(order: Order, request: Request) -> None:
    await request.state.producer.send(orders(order, key=order.customer))
```

`send` takes a `ProducerRecord`, e.g. made by calling a topic, and returns its `Delivery`, the partition and offset, once the broker has acknowledged it. It raises `KafkaException` if the record was not delivered within librdkafka's `message.timeout.ms`. The producer is idempotent and waits for all in-sync replicas, and its `options` (librdkafka's) cannot change either. If librdkafka's queue is full, `send` waits for room. A cancelled send may still be delivered. Leaving `async with` waits up to `close_timeout` (10s) for every send to be delivered or fail, then purges the rest, so an outage does not hold the exit for `message.timeout.ms` (ADR 0016). It raises the failure of a send that nobody awaited, e.g. a cancelled one. A fatal error, e.g. a broken idempotent sequence, fails every later send, and `producer.failure` exposes it, so that a liveness probe restarts the process. A callback in `options` that raises, e.g. an `error_cb`, stops the thread that serves delivery reports. The producer then fails the same way, along with every send that awaits a report, so leaving `async with` never waits for a report that cannot come.

A `TopicCheckMiddleware([orders])` checks, when the producer is entered, that the topics it sends to exist, and raises `TopicCheckError` otherwise, so a service fails at startup rather than on its first send. The topics do not restrict what it sends.

Next to a runtime, the service's `main` owns the producer and passes it to handlers in the run's context. `Producer.from_config(config)` reuses the runtime's `options` and `producer_options`. The topics that handlers send to belong to the producer's `TopicCheckMiddleware`, not to their `output_topics`. Producers of one topic should use the same `partitioner`, or a key's records end up in different partitions.

```python
@router.handler(payments)
async def charge(payment: Payment, producer: Annotated[Producer, Context()]) -> ProducerRecord:
    await producer.send(audit(Charged.of(payment), key=payment.id))  # visible now
    return receipts(Receipt.of(payment), key=payment.id)  # visible once the input commits


async def main() -> None:
    async with Producer.from_config(config) as producer:
        await potik.build_runtime(config, [router], context=producer).run()
```

A send is not an output. It is visible before the handler returns, whether or not its input ever commits, and it is at-least-once: a replayed or retried handler sends again, so the consumers of its topic must tolerate duplicates.

## Requests and replies

`ReplyListener` (`potik.core.replies`) awaits the reply to a request in the process that sent it, e.g. one of many API replicas (ADR 0018). It reads every partition of one reply topic with a consumer that belongs to no group and commits nothing. A consumer group would hand each partition to one replica, so a reply would reach the replica that awaits it only by chance. Every replica reads every reply instead, and drops those that it does not await, which suits replies at API rates rather than at stream rates.

```python
async with (
    Producer(BOOTSTRAP) as producer,
    ReplyListener(statuses.name, bootstrap_servers=BOOTSTRAP, correlate=request_id) as listener,
):
    with listener.expect() as pending, anyio.fail_after(10):
        await producer.send(updates(update, key=record_id, headers={"request-id": pending.correlation_id}))
        reply = await pending.wait()
```

The listener neither sends requests nor sets a correlation convention. The app puts `pending.correlation_id` wherever its responder reads it, and `correlate` reads it back from the reply. A responder is a plain handler that returns its reply as an output, so the reply commits with the request's offset, and `read_committed` readers see it only then. `expect()` registers the ID before the send, so a fast reply cannot arrive first, and keeps a reply that arrives before `wait()`. Leaving the block forgets the ID.

Entering assigns each partition at its end offset, read from the broker, before the owner takes requests. A reply follows its request, so it follows that offset, and no reply from an earlier process is ever read. Every 5 seconds the thread assigns partitions that the topic gained, from their start, because all of their records came after the entry. A non-fatal consume error is logged. A reply whose `correlate` raises is logged and dropped, so one malformed record cannot fail every request. A fatal error, or any other exception on the thread, fails every awaited reply and sets `failure`, for the owner's liveness probe. Exiting fails the replies that are still awaited, and `expect()` raises once the listener has exited.

A reply that nothing awaits is dropped, e.g. one that came after its request timed out. A request that timed out may still be handled, and a replayed responder replies again, so only the first reply counts and responders should be idempotent. If the replica dies while it waits, its HTTP caller loses the connection too, so the lost reply harms nothing.

One thread owns the consumer, as in `ConsumerThread`. It runs `correlate` on each record, checks the ID against the awaited ones, and hands each poll's matches to the event loop at once. The event loop checks the ID again, because a block may have exited in the meantime.

## Testing

The decorator returns the handler unchanged, so tests can call it as a plain function. `potik.testing.MockCluster` runs a whole app without Kafka, on librdkafka's mock cluster (ADR 0005). The mock cluster runs inside the process and listens on localhost ports, so the app's own clients connect to it, and its `Runtime`, middleware, and topic check run unchanged. potik has no seam of its own for it. A service exposes a function that builds its runtime from a `Config` and its resources, which both its `main` and its tests call, and a test passes the cluster's config.

confluent-kafka doesn't wrap the mock cluster's C API, so `MockCluster` calls it through `ctypes`. It loads the `confluent_kafka.cimpl` extension module, whose handle also finds the symbols of the librdkafka that it links. So it needs no path to the library, which each wheel bundles under its own name, e.g. `librdkafka-<hash>.so.1` on Linux. The confluent-kafka 2.16.0 wheels for macOS and for manylinux on x86_64 and aarch64 export every symbol it uses. A build that does not fails the `MockCluster` constructor with a `RuntimeError`. Windows looks symbols up only in the module itself, so `MockCluster` refuses to run there. It creates the cluster with `rd_kafka_mock_cluster_new()` on a handle of its own, never through `test.mock.num.brokers`, whose cluster is freed twice on destroy (librdkafka
#5644).

`MockCluster.config()` changes two settings of the app under test:

- The `resume_or_earliest` offset policy, so the app also handles the inputs that a test sends before the app's partitions are assigned.
- A 10ms `fetch.wait.max.ms`. The mock holds a fetch without records for the whole wait, even when a record arrives sooner. With the 500ms default, every hop between a test and its app takes up to half a second. `wait_records()` reads with the same wait.

The mock cluster of librdkafka 2.16.0 has these limits:

- `TxnOffsetCommit` returns success but stores nothing, and it never checks the member's generation or epoch. So a transaction's offsets never commit, a restarted app handles its input again, an app with the `resume_or_fail` offset policy never starts, and no stale member is fenced (librdkafka #5652).
- A signal that interrupts the mock cluster's poll shuts it down (librdkafka
  #4727), e.g. a Ctrl-C during a test run.
- The first send of an idempotent or transactional producer waits about 500ms for its producer ID, so an app takes about half a second to start.

So the runtime's guarantees, e.g. exactly-once commits, rebalances, fencing, and outages, are tested only against a real broker, in `tests/integration`. There, `tests/integration/app.py` injects a crash mid-transaction by replacing `confluent_kafka.Producer` before it imports potik. It relies on potik creating its clients from `confluent_kafka` directly, and the test fails if the process no longer crashes.

## Core runtime

potik has three layers, and each imports only the ones below it (ADR 0006):

| Layer | Modules | Holds |
| --- | --- | --- |
| Core | `potik.core` | `Runtime`, `Producer`, `Config`, records, offset policies, status, `health`, the middleware protocol |
| Built-in middleware | `potik.middleware` | Exception rules, topic check, concurrency limits, tracing, metrics |
| Typed layer | `potik` root: `build`, `router`, `topic`, `handler`, `serde` | `build_runtime()`, `Router`, `Topic`, parameter markers, serdes |

Nested `ruff.toml` files in `src/potik/core/` and `src/potik/middleware/` ban imports of the layers above (TID251). A new typed name on the root `potik` needs an entry in both. The root re-exports the core names that an app writes, e.g. `Config`, `Runtime`, and `RuntimeStatus`. Names for extending the runtime, e.g. `BaseMiddleware` and `RunScope`, come only from `potik.core`. The integration tests run the runtime's guarantees, including chaos, outage, and static membership, on a bare `Runtime`, and test `potik.build_runtime()` only for what it adds.

`potik.core` runs without the typed layer, like Starlette under FastAPI. `Runtime(config, topics, handler, middleware=...)` is one run: it subscribes to `topics` and calls one handler for every record, a function of a `ConsumerRecord` that returns a sequence of `ProducerRecord`s. A service with several topics dispatches on `record.topic()` itself. The runtime checks the handler's result where the handler returns, inside the middleware: anything but a list or tuple of `ProducerRecord`s raises `InvalidOutputError`, a `PermanentError`, so exception rules handle it like the handler's own failure, whichever layer built the handler. The worker checks the result again after the middleware, because middleware may replace the outputs. Its middleware are instances, so their state lasts that run, and another run needs a new `Runtime` with new instances. The core also has the `Producer`, which is entered once and takes instances too, and `potik.middleware` depends only on the core. `_enter_lifespans` records each instance it enters by identity, in a weak registry, and raises if an instance is entered again, by the same owner or another, or appears twice in one list (ADR 0011), because a lifespan keeps state about its owner, e.g. tracing's client ids. The core knows nothing of declared topics: they belong to the typed layer and to whoever builds a `TopicCheckMiddleware`.

`potik.build_runtime(config, routers, ...)` builds one `Runtime` from routers (ADR 0011). There is no App object: a service creates its resources, e.g. a database pool, in its own `main` with `async with`, then builds the runtime, passing handlers' resources as `context` and middleware's as their `__init__` arguments. It composes each topic's pipeline and hands the runtime one handler: a `ConcurrencyMiddleware` built from the `max_concurrency` of handlers and of the runtime, then the bound handler. It puts an `ExceptionMiddleware` of its `exception_rules` after the given middleware, if it has any, and the runtime wraps them around the handler. So the rules run inside every middleware, e.g. tracing and metrics, which see one call per record and its recovered outcome, and outside the concurrency limits. It rejects an `ExceptionMiddleware` among the given middleware, so that the rules have one place and one position (ADR 0008). Routers have no middleware: per-topic rules take `on(..., topics=...)`, and other per-topic behavior checks `record.topic()`. It copies the routers' handlers, rejects two routers that handle one topic, and binds each handler to `context`, failing for a `Context()` parameter without one. Unless `check_topics=False`, it puts a `TopicCheckMiddleware` first, with the handled topics and every declared output topic: handlers' and routers' `output_topics`, and those of any middleware that is `DeclaresOutputs`, e.g. the exception rules' dead letters. A core service that limits concurrency puts `ConcurrencyMiddleware` last in its middleware, so that skipped records and retry backoff hold no slot, and one that wants the topic check puts `TopicCheckMiddleware` first with the topics to check.

## Runtime model

- **One consumer and one transactional producer per run.** The consumer uses the KIP-848 group protocol, so the broker assigns partitions and rebalances are incremental. Each transaction carries the consumer's group metadata (KIP-447), and the coordinator uses it to fence commits from stale or zombie members. No producer per partition is needed.
- **Sequential per partition, concurrent across partitions, or concurrent by key.** Each assigned partition has its own task and record buffer. A topic in the runtime's `key_concurrency` runs by key instead (ADR 0020): the partition's task dispatches records in offset order, each to the task of its key if the key is in progress, or else to a new task once it gets one of the topic's slots, which its partitions share. A key's task runs its records one at a time and gives up the slot when none waits, so the dispatcher runs past busy keys, and a waiting key holds no slot. The dispatcher does wait at a record of a new key until it gets a slot, and the records behind it wait too, even those of keys in progress, whose tasks then run out of records and give up their slots. So while every slot is taken, a partition dispatches nothing new, and keys in progress make no progress past that record. Dispatching around it would need a per-key lookahead in the buffer, which the byte budget would also have to cover. All null keys are one key. A record that finishes before an earlier one of its partition is held with its outputs, and one task at a time hands the finished records at the head of the partition to the committer, in offset order, so a transaction never commits past an unfinished record. A failure stops the partition at the lowest failed record at once, through `Partitions.stop_failed`, so its status and pause do not wait for the records below it: the dispatcher stops, running records above it are cancelled, and records below it, running or waiting for their key, still run, so everything before the failed record commits. `Partition.run()` then raises the failure, also if a revocation or shutdown cancelled it meanwhile. With `stop_failed_partitions=False`, the first failure cancels every record of the partition at once and fails the run. Whether a record may start is checked in `Partition._handle`, right before its call, because a failure or `finish()` may come while the record waits for a slot or for its task's first step. While other keys wait for a slot, a key's task gives up its slot after each record and waits its turn in the semaphore's FIFO queue, so a key with a backlog, e.g. the only key of a keyless topic's partition, does not starve the topic's other partitions. A revocation or shutdown starts no record, and commits the finished records up to the first unfinished one. The slots count keys in progress, including retries' backoff, so a handler's `max_concurrency` still limits handler calls. A handler's `max_concurrency` limits how many of its handler's calls run at once. `potik.build_runtime(max_concurrency=...)` limits handler calls across all topics, e.g. to a database pool's size. Middleware runs outside these limits. A middleware that skips a record (returns `()` without calling `call_next`) never waits for a slot, and retry backoff does not hold one. A topic's slot is taken before a shared one, so waiting for it holds no shared slot. Both limits are one `ConcurrencyMiddleware`, which `potik.build_runtime()` puts inside the exception rules.
- **Polling never stops.** A single poller fetches in batches and appends records to partition buffers. It pauses only the partitions whose handlers cannot keep up. Handlers may take arbitrarily long without exceeding `max.poll.interval.ms`.
- **Adaptive transactions.** Completed records from all partitions accumulate while the previous transaction is in flight, so batches grow with throughput and stay small when traffic is light. A transaction starts once the previous one finishes and either its batch is full (`commit_batch_size`) or `commit_interval_ms` has passed since the previous start.
- **Rebalances.** On assignment, the offset policy picks each partition's start position. If it picks a position other than the committed one, the runtime commits it before handling any record. librdkafka keeps a partition's pause after its revocation, so the runtime resumes each assigned partition, or a partition stopped in an earlier assignment would never fetch again. The consumer reads committed offsets as stable (`read_committed`), so a new owner waits for the previous owner's pending transaction. On revocation, the runtime cancels in-flight handlers, optionally after letting them finish for `revocation_grace_ms`, and commits completed work before releasing the partitions. A handler that returned counts as completed, even if the cancellation reaches it afterwards, e.g. while it waits for room in a full batch, or if it swallowed the cancellation. Buffered records never start. Lost partitions stop the run without committing more. During a shutdown nothing polls, so the runtime learns of the loss only when its final commit fails. librdkafka keeps a fenced member out of the group until a poll serves the loss, so the coordinator rejects that commit, even after another member took the partitions.
- **Failures.** A handler exception stops its partition. The runtime pauses the partition at the failed record and commits nothing past it. The record is retried after the partition's next reassignment or restart, and other partitions keep running. A fetch error of one partition, e.g. a committed offset that retention deleted, stops it the same way (see Configuration). `stop_failed_partitions=False` stops the whole run instead. Handle recoverable errors with exception rules, the `exception_rules` of `potik.build_runtime()` or a `potik.middleware.ExceptionMiddleware`. It asks them in order, like `except` clauses, whether to retry the handler or to commit other outputs (e.g. a dead letter) in place of the failed record. The rules see the single exception of an `ExceptionGroup` that holds only one, e.g. from a task group in which one task failed (ADR 0015). potik adds no rules by default. Stopping the partition is the default handler of every exception, including potik's own `PermanentError`s.

  ```python
  def transient(exc: Exception) -> bool:
      match exc:
          case HTTPStatusError(response=r):
              return r.status_code in (408, 429) or r.status_code >= 500
          case TransportError() | TimeoutError():
              return True
      return False


  potik.build_runtime(
      config,
      [router],
      exception_rules=[
          on(transient, retry(max_attempts=15)),
          on(Exception, dead_letter(dead_letters)),  # also once retries are spent
      ],
  )
  ```

  `retry()` rules leave a `PermanentError` to the next rule, because the same record would fail the same way again. `DecodeError` and `EncodeError` are permanent, and so is any other subclass of `PermanentError`, e.g. the `InvalidOutputError` of a handler result that is not output records. `PermanentError` is a marker that apps subclass, so it is not a `PotikError`, the base of the errors that potik raises. potik's own permanent errors are both. `dead_letter(topic)` commits a copy of the failed record to `topic`: its key, value, and headers as bytes, plus the headers that its `headers` function returns. The default, `dead_letter_headers`, returns `potik.dlq.*` headers with the record's `source.topic`, `source.partition`, and `source.offset`, the exception's qualified class name as `exception.type`, its `exception.message` (`str()`, which may contain data from the record, cut to 1000 characters so that a long validation error keeps the dead letter within the broker's size limit), and the `attempts`. A function of your own picks names that existing dead letter consumers expect, or leaves out the message, and keeps the rule's topic declaration. The partitioner picks the copy's partition. A run checks that `topic` exists before it starts (see Configuration). For any other dead letter, write a rule of your own, and declare its topic in `Router(output_topics=...)`. `on(match, rule)` matches a type, a tuple of types, or a predicate, and `topics=` limits it to some topics' records. A rule is any function of the `ExceptionContext`. It returns `RetryAfter(seconds)`, outputs, `Reraise()` to let the exception propagate without asking later rules, or `None` to pass the exception to the next rule. `reraise()` returns `Reraise()`, so `on(InvalidOutputError, reraise())` before a catch-all rule stops the partition on a handler bug instead of dead-lettering every record. For example, a rule can return the delay that a throttling error asks for. `retry_forever()` on its own retries every failed record in place until it succeeds, so a partition keeps working through an outage instead of waiting for its reassignment.
- **Middleware.** A runtime's middleware has three stages of the runtime: each run's `lifespan`, each record (`handle`), and each transaction (`commit`, which sees its offsets and outputs). Code after `call_next` in `commit` runs once the transaction is committed, and a revocation or graceful shutdown waits for it to finish. A run failure cancels it, even after a transaction that committed, so it may not run. Use it, e.g., to delete the outbox rows of the committed offsets. Middleware subclasses `BaseMiddleware` and is passed as instances, which the service creates after the resources they need, e.g. `OutboxMiddleware(pool)`. Each instance serves one runtime or producer, once, so its state lasts as long as the run. A fourth stage, `send`, wraps each send through a `Producer`, which calls its middleware's `lifespan` and `send` stages. With the `otel` extra, `potik.middleware.tracing` uses all four stages to trace each record's handling, its outputs to the transaction that commits them, and each send (see `examples/tracing`). `potik.middleware.metrics` records the messaging semantic conventions' metrics for handler calls, transactions, and sends. Pass instances of both to the runtime and to each producer. Both emit only the Kafka semantic conventions, with names from `opentelemetry-semantic-conventions` (ADR 0007). The one exception is `potik.partition.stopped`, an observable up-down counter of each run's stopped partitions, which the conventions do not cover (ADR 0013). Its callback reads `RunScope.status`. The SDK keeps the callbacks of an instrument's first registration only, so one module-level callback per meter provider observes every run of every instance that uses it. Every Required attribute, and every Conditionally Required one whose condition holds, is always set. An `optional_attributes` argument picks the Recommended and Opt-In ones that potik can fill: partition, offset, and `client.id` (when configured) by default, key and body size on request. A transaction's span is a `send` (client), named for its output topic if it has only one, or a `commit` settle span if it has no outputs. It keeps the consumer group, because it also commits the group's offsets, and links to the `create` span of each output. Records get no `commit` span of their own (ADR 0021). `transaction_sample_ratio` picks the transactions that get a span up front, at random. Every failed one gets a span too: if it was not picked, the span starts once it fails, backdated to the transaction's start. Users enrich the `process`, transaction, and `send` spans through `trace.get_current_span()`, and metrics through SDK views. `ExceptionMiddleware` records each exception that it retries or handles as a plain `exception` event on the current span.
- **Lifespans.** A lifespan is an async context manager around a run, or around the time a `Producer` is open: each middleware's `lifespan()`. They enter outermost first, then the clients start, and a run is ready once all of them have entered. They exit in reverse, after the clients close, and see the run's failure or the cancellation that stopped it. One helper, `_enter_lifespan`, enters each of them, so they all follow the same rules. The exit is shielded, so a cancelled run still lets a lifespan await while it cleans up, and a stuck exit holds up shutdown until the supervisor kills the process. The helper ignores what `__aexit__` returns, so a lifespan cannot suppress the error. The service's own resources live in its `main`, around the runtime. On SIGTERM `run()` returns, and they close as usual. A cancelled `run()` raises the cancellation once it has shut down, so their cleanup runs in a cancelled scope, and must shield its awaits.
- **Lifespan scope.** A middleware's lifespan takes what it surrounds: a `RunScope` (the run's `config` and `status`) or a `ProducerScope`. Both have `client_options`, which connect a client of the middleware's own to the same cluster, e.g. the topic check's `AdminClient`. The scope is read-only: a lifespan observes the run, and can fail it only by raising before it yields. A `Runtime` is one run, so `status` describes that run from its start. Middleware that needs the run's facts, e.g. the consumer group that tracing and metrics record, reads them from the scope instead of taking them as arguments.
- **Fail-stop.** A failed transaction, a client error, or lost partitions stop the run, and `run()` raises the error. Uncommitted work replays on the next run. There is one exception. If a transaction fails with an error that librdkafka marks as requiring an abort, the runtime aborts it and repeats it with fresh group metadata, up to three attempts. The abort leaves nothing committed, so the repeat is safe. The usual cause is a stale group epoch: under KIP-848 any assignment change bumps the member epoch, and that can race with a transaction in preparation. The abort of such a transaction purges its outputs, which can leave a gap in the producer's sequence numbers, so the repeat can fail with `OUT_OF_ORDER_SEQUENCE_NUMBER`. That error also requires an abort, which bumps the producer epoch, and the next attempt succeeds. Errors that do not require an abort, e.g. a fatal one or a timeout, still fail the run.
- **Graceful shutdown.** Cancelling `run()` stops polling and cancels in-flight handlers, optionally after letting them finish for `shutdown_grace_ms`. Then it commits completed work and leaves the group. The whole shutdown, its final commit included, takes at most `shutdown_grace_ms` plus `rebalance_timeout_ms`, which Config keeps below `max_poll_interval_ms`, or it fails. `run()` does the same on the first SIGTERM or SIGINT and then returns. Pass `handle_signals=False` to opt out, e.g. off the main thread. Only one run in a process may handle signals at a time, and another one that tries raises `RuntimeError` before it installs its receiver. anyio's asyncio backend, like `loop.add_signal_handler`, keeps one handler per signal, so the receiver installed last gets every signal, and the first run would never shut down. A process that runs several runtimes at once passes `handle_signals=False` to each and cancels them on a signal itself. If the shutdown fails, e.g. in its final commit, `run()` raises, so the process does not exit cleanly. librdkafka's blocking calls run on dedicated worker threads, and the runtime joins them instead of abandoning them. The supervisor (e.g. Kubernetes' termination grace period) must enforce a hard deadline.

Exactly-once semantics cover Kafka outputs and Kafka input offsets. They do not cover sends, or other external side effects of handlers.

## Configuration

`Config` holds the connection, the runtime's limits, and librdkafka options.

| Field | Default | Meaning |
| --- | ---: | --- |
| `offset_policy` | `resume_or_latest` | Start position of an assigned partition: `resume_or_latest`, `resume_or_earliest`, `resume_or_fail`, or a custom policy |
| `max_buffered_bytes` | 16MB | Fetched records not yet with the committer, across all partitions, as key and value bytes plus 350 per record (at least 16KB per partition) |
| `stop_failed_partitions` | `True` | A handler exception or a partition's fetch error pauses only that partition; `False` stops the run |
| `commit_batch_size` | 1000 | Completed records per transaction; workers wait when the next batch is full |
| `commit_interval_ms` | 100 | Minimum time between transaction starts unless a batch is full; 0 commits as fast as possible |
| `transaction_timeout_ms` | 10000 | Transaction timeout; also bounds how long a new owner waits for a crashed owner's transaction |
| `rebalance_timeout_ms` | 60000 | Time limit for an assignment or a revocation; must be below `max_poll_interval_ms` |
| `revocation_grace_ms` | 0 | How long revoked partitions' in-flight handlers may finish before cancellation; fetching pauses meanwhile |
| `shutdown_grace_ms` | 0 | How long in-flight handlers may finish on shutdown before cancellation; plus `rebalance_timeout_ms`, it limits the shutdown and must be below `max_poll_interval_ms` |
| `max_poll_interval_ms` | 300000 | librdkafka `max.poll.interval.ms` |

`options` apply to both clients (TLS, SASL, OAuth/OIDC, `client.id`, logging). `consumer_options` and `producer_options` apply to one client. All three use librdkafka's option names:

```python
from potik import Config, resume_or_earliest

config = Config(
    bootstrap_servers="localhost:9092",
    group_id="transformer",
    offset_policy=resume_or_earliest,
    options={"security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN"},
    consumer_options={"group.remote.assignor": "uniform", "client.rack": "eu-west-1a"},
    producer_options={"compression.type": "zstd", "linger.ms": 5},
)
```

The broker assigns partitions (`group.remote.assignor`), so classic client-side assignors do not apply. Options the runtime depends on, such as the group protocol, isolation level, auto-commit, idempotence, and transactional ID, cannot be overridden.

Before a run of `potik.build_runtime()` starts, its `TopicCheckMiddleware` checks that every handled topic exists, and fails with `TopicCheckError` if one is missing or its credentials may not describe it. A misspelled topic would otherwise idle silently. The check also covers the declared output topics: handlers' and routers' `output_topics`, `dead_letter` topics, and the `output_topics` of other middleware. It runs in its lifespan, as one request through an `AdminClient` of its own that never creates topics. A cancelled start, e.g. on SIGTERM, stops waiting for the request, and the client closes when the check ends. `potik.build_runtime()` puts it first, so it fails before other lifespans open resources. `check_topics=False` skips it, e.g. to start consumers before their topics are created. Neither client asks the broker to create a missing topic, so a record to one fails when it is produced, as a delivery timeout. `producer_options={"allow.auto.create.topics": True}` restores librdkafka's producer default, with which a broker that allows it creates the topic with its default partition count.

The group metadata in each transaction is what fences stale members, so each run gets a fresh transactional ID, `{prefix}-{uuid4()}`. With `group.instance.id` set, the ID is `{prefix}-{len(group_id)}-{group_id}-{group.instance.id}` instead. The length keeps the IDs of different groups apart. A restarted instance then aborts the transaction its crashed predecessor left open right away, and `read_committed` readers don't have to wait out `transaction_timeout_ms`. The prefix is `Config.transactional_id_prefix`, `potik` by default, so that operators can grant each app's producer an ACL on its own prefixed `TransactionalId`. The runtime owns the rest of the ID, because two live producers with one ID fence each other.

An offset policy receives an `OffsetContext` with the `committed` offset (or `None`) and the `earliest` and `latest` log bounds, which it fetches only when asked. A policy may rewind or skip on purpose, but the runtime rejects positions outside the log. A committed offset that has fallen out of the log stops its partition instead of silently skipping records.

librdkafka retries transient errors itself, such as leader changes, an unknown or deleted topic, and broker outages, and never hands them to the application. What reaches the runtime, as a fetched record that carries an error, needs an operator: a topic the credentials may not read, a position out of range or truncated by an unclean leader election, a record that does not parse, or a fatal error. An error of an assigned partition stops it, like a handler failure (ADR 0014). Its records fetched before the error are still handled and committed, and those after it are dropped. An error of no partition, e.g. a fatal one, or any error with `stop_failed_partitions=False`, fails the run.

## Health

`Runtime.status()` returns a `RuntimeStatus` snapshot for probes and metrics, with `time.monotonic()` timestamps:

- `running`, and the `failure` that stopped the run, if any;
- `polled_at`: when polling started or the poller last returned. It returns at least every 200ms, except during a rebalance callback (up to `rebalance_timeout_ms`) or shutdown;
- `oldest_uncommitted_at`: completed work normally commits within `commit_interval_ms` plus one transaction;
- per partition: the records that have not started, the running handler calls, whether it is paused, when its oldest running call started, and the failure that stopped it.

`potik.core.health` turns a status into probe results. `liveness()` fails only for what a restart fixes: a failed run, no poll returned for longer than `rebalance_timeout_ms`, or handled records uncommitted for longer than `commit_interval_ms` plus two transactions, each with `slack` (30s) on top. A broker outage or a long handler call does not fail it, and a run that is starting or stopping is alive. `readiness()` passes while the runtime runs, with or without partitions. Neither fails for stopped partitions (ADR 0013). A restart retries the record that stopped a partition, which usually fails again, so failing liveness would turn one stuck partition into a crash loop that stops every partition. Operators alert on stopped partitions instead, through `potik.partition.stopped`, `PartitionStatus.failure`, or the error log. Each check returns the reason it fails, or `None`, so any transport can serve them. `Runtime.liveness()` and `Runtime.readiness()` apply them to the runtime's own status and config.

potik serves no HTTP (ADR 0012). A service serves the probes from its own HTTP framework, with its own checks, from a server task next to `run()` in its `main`. The runtime then keeps its signal handling, and a failed run ends the task group, which stops the server and exits the process. uvicorn's `Server.serve()` captures SIGTERM and SIGINT itself, then restores the earlier handlers and raises the signals again once it stops, so they still reach the runtime's handler. Whichever handler was installed last gets a signal first. `run()` installs its handler before its first checkpoint, so uvicorn's usually wins, and the server stops before the runtime drains. A run that ends removes its handler, so after a restart the next run's handler wins instead, and the server stops after the drain. Both orders stop cleanly, and Kubernetes stops liveness probes of a terminating pod, so probes refused during the drain are harmless. The tracing example runs this shape, and its test probes it.

## Memory

Byte budgets bound the runtime's buffers and librdkafka's prefetch, and `commit_batch_size` bounds the transactions' batches. `Partitions` (`potik.core.partitions`) owns the buffer rules below and makes no client calls: the runtime applies its decisions, and its unit tests run without a broker.

- **Partition buffers.** All buffers together hold about `max_buffered_bytes` (ADR 0019), whatever the number of assigned partitions. A record counts from its fetch until it goes to the committer, so a partition's running records, and in a topic that runs by key its held ones, count too. A slow record of such a topic then pauses its partition once the finished records behind it fill its share (ADR 0020). The poller's estimate of how fast a full buffer drains divides the mean handler time by the number of running calls. A record counts as its key and value plus 350 bytes: about 250 for its Python objects, and about 100 for a `traceparent` header, which stays in C unless something asks for it. Each partition's share is the budget divided among them, at least 16KB, and recomputed on every rebalance. A fetch takes about half a share: as many records as fit at the mean size of the previous fetch's records, up to librdkafka's limit of 1,000,000 per call, so a fetch of larger records exceeds it, by at most librdkafka's prefetch queue. Handler outputs do not count, including those of finished records held behind an unfinished one. A partition that holds more than the other half is paused, unless its handler can drain the excess within 100ms (typically when the committer was busy). It resumes at a quarter. Pausing drops librdkafka's prefetch for that partition and resuming waits for a new fetch, so brief pressure never pauses. With consumer lag the buffers stay full, so the budget is steady memory. Handlers that do I/O need only a few buffered records each, so the 16MB default suits hundreds of partitions. librdkafka queues the records of all partitions in one queue, a fetched record batch at a time, and a broker's response carries a batch larger than `max.partition.fetch.bytes` only for its first partition (KIP-74). So each fetch usually feeds one partition, and a share smaller than the producers' record batches pauses the partition in the middle of a batch. librdkafka then fetches the rest of the batch again. This matters only for handlers that take about a millisecond or less: with 1MB input batches, 32 partitions, and a 1ms handler, a 16MB budget commits about 14k records/s, and 32MB about 20k, against 27k with 16KB batches (`benchmarks/README.md`).
- **Transactions.** At most two batches of `commit_batch_size` completed records wait at once, one committing and one accumulating, along with their outputs.
- **librdkafka prefetch.** librdkafka's defaults prefetch up to 100k records per partition and 64MB in total. The runtime lowers them with `queued.min.messages=2000`, `queued.max.messages.kbytes=16384`, `max.partition.fetch.bytes=131072`, `fetch.max.bytes=8388608` and `fetch.queue.backoff.ms=20`. All of these can be overridden through `consumer_options`.

## Threads

The runtime drives librdkafka's synchronous clients from its own worker threads instead of using `confluent_kafka.aio`. The async wrappers offload each call to a thread pool anyway, and their producer adds a Python batching layer that does not support headers. With its own threads, the runtime can join every blocking call on cancellation. The cost is one thread hop per fetch and per transaction.

Since confluent-kafka 2.16, a `Consumer` blocks calls from other threads while one thread polls. So one thread owns the consumer (`ConsumerThread`), and the event loop only passes it functions to run. Rebalance callbacks hand their work to the event loop and keep running those functions until the work finishes, so that work can call the consumer too.

A `Producer` does not block other threads while one polls. So `send` produces on the event loop, and one thread of the producer's own serves the delivery reports, handing each poll's reports to the event loop at once. It is a thread rather than a task, so the producer holds no task group open in its owner's task, and its exit joins it.
