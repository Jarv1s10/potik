# Proposal: handlers take batches of a partition's records

Status: proposed, not implemented. Once implemented, this becomes ADR 0023, and `.agents/DESIGN.md`, `.agents/GLOSSARY.md`, the docstrings, and `docs/` describe the behavior. Until then, they describe potik without batches.

## Motivation

A handler that writes each record to a database does one statement per record. Many services can replace those with one bulk statement for many records, e.g. an upsert of every account in the batch, or one write per account that collapses its updates. potik calls a handler for each record, so a service cannot do that today.

The feature should serve that case without a second processing mode, keep exactly-once outputs, and keep the core a minimal wrapper around `confluent-kafka`.

## Decision

The batch becomes the core's unit of work, and a single record is a batch of one.

- **Core handler.** `Callable[[Sequence[ConsumerRecord]], Awaitable[Sequence[ProducerRecord]]]`. The records are a non-empty run of consecutive records of one partition, in offset order. Middleware's `handle` stage takes the same sequence.
- **Formation.** `Runtime(..., max_batch={"orders": 200})`. When a partition starts its next call, it takes the records buffered at that moment, up to the topic's `max_batch`. It never waits for more. Under lag the batches grow to `max_batch`, and with no lag they hold one record, so a quiet topic adds no latency. A topic without `max_batch` takes one record per call, as today.
- **Commits.** A batch is consecutive, so its outputs and next offset go to the committer in one `add`, and commit in one transaction. A crash before that transaction replays the whole batch. Nothing else in the committer, `Transaction`, offset policies, or rebalances changes.
- **Key concurrency.** A topic in both `max_batch` and `key_concurrency` is rejected. Records of one key already share a partition, so a batch of consecutive records, grouped by key, holds all of a key's records in it (see the typed layer).
- **Failures.** A batch's handler call returns all of its outputs or raises. An exception that propagates stops the partition at the batch's first record, as a single record's does today.

### Exception rules

`ExceptionMiddleware` asks its rules as today. For a batch of more than one record:

- A `RetryAfter` decision calls the handler again with the whole batch, after the delay. Attempts count calls of the batch.
- Any other decision, including none, falls back to the records one by one. Each runs alone through the rules, from attempt 1, in offset order, and the batch's outputs are theirs, concatenated. A record whose exception no rule handles propagates it, and the partition stops at the batch's first record.

So after a failure, a batched topic behaves like a topic without batches, except that transient failures retry the whole batch. `dead_letter()` and `skip()` only ever act on one record, so a poison record never takes the good records of its batch with it. The fallback costs one call per record, only when a batch fails. A rule that is not a retry runs on the batch's context first, and its decision is discarded, so a rule that logs or counts sees that failure as well as the record's own.

`ExceptionContext` gets `records`, the batch. `record` stays as its first record, which every rule but a retry sees alone anyway.

### Other middleware

- `ConcurrencyMiddleware` limits handler calls, which now matches its main use, a database pool's size, better than records did.
- `MetricsMiddleware` counts each batch's records as consumed messages, and records one process duration per call.
- `TracingMiddleware` gives a batched topic one `process` span per call, with a link to each record's context, as the messaging semantic conventions do for batches. A topic without `max_batch` keeps the record's context as the span's parent. The span shape follows the configuration, not the batch's size, so it does not change with load. The middleware learns which topics batch from its `RunScope`. The OpenTelemetry SDK keeps 128 links per span by default, so a larger `max_batch` loses links unless the app raises the limit.
- A middleware that hands per-record values to dependencies through a `ContextVar`, e.g. a tenant (ADR 0010), mis-scopes every batch of more than one record. `BaseMiddleware.handle` documents it.

### Typed layer

```python
@router.handler(account_events, max_batch=200)
async def apply(batch: Batch[AccountEvent], db: Annotated[Db, Context()]) -> None:
    async with db.transaction() as tx:
        for account, items in batch.by_key():
            await tx.apply_many(account, [item.value for item in items])
```

- `Batch[V, K]` is a `Sequence[Item[V, K]]` in offset order. `Item` is a `NamedTuple` of the decoded `value`, the decoded `key`, and the `record`. `Batch` also has `values`, `records`, and `by_key()`, which returns each key's items in offset order, keys in the order of their first record.
- A handler with `max_batch` takes the batch through one `Batch[V, K]` parameter, whose types must match the topic's. It rejects value, `Key()`, `Header()`, and `ConsumerRecord` parameters, and takes `Context()` and `Depends()`. A dependency resolves once per call and may take the `Batch` too, so one database transaction per batch needs no extra code.
- Every item decodes before the call. A `DecodeError` fails the call, and the exception rules' fallback then dead-letters exactly the record that does not decode.
- Handlers without `max_batch` do not change.
- `potik.testing.handle(runtime, *records)` replaces `handle_record(runtime, record)`, and returns the outputs of one call.

### Naming

"Batch" now means a handler call's records. `Config.commit_batch_size` becomes `max_transaction_records`, since the glossary already calls the committer's unit a Transaction. A Kafka record batch, the producer's unit on the wire, keeps its full name.

## Implementation notes

- `Partition._run_in_order` takes up to `max_batch` buffered records, calls the handler once, adds the batch to the committer with one `add`, and releases the summed sizes.
- `Committer.add` takes the number of records, so `max_transaction_records` keeps counting records. A batch larger than the room left in the transaction overflows it, so that a batch never splits across transactions.
- `Partition.handler_seconds` becomes the mean time per record (a call's time divided by its records). `Partitions._drain_seconds` multiplies it by records, so a mean per call would overestimate the drain time by about the batch size, and pause partitions that would have drained in time.
- `Partition.running` is keyed by the batch's first offset, and `PartitionStatus.running_handlers` counts calls.
- The effective batch size is at most what a partition's share of `max_buffered_bytes` holds, which `Partitions` fetches about half of at a time. With 16MB over 300 partitions, a share holds about 40 typical records. The docs say to raise `max_buffered_bytes` for larger batches.
- `revocation_grace_ms=0` cancels whole batches, so each incremental rebalance replays up to a batch per revoked partition. The docs suggest a grace period for batched topics.
- A batch's composition differs between replays, so its database writes must be idempotent, as every external effect already must be.

## Alternatives considered

**A separate batch handler and middleware stage beside the per-record ones**, as Spring Kafka, FastStream, and Confluent's Parallel Consumer have. Every built-in middleware would implement both stages, `Partition` would have four run modes, and features drift apart: Spring keeps a record and a batch variant of each error handler, and its retry topics do not support batch listeners.

**Batches within key lanes**, with linger. Batches of one key have offsets interleaved with other keys', e.g. key A at 1, 3, 5 and key B at 2, 4, 6, and a batch's outputs must commit with all of its offsets, so no transaction may end inside either span. Under load every key has records queued, these spans chain, and the committed offset only advances when the partition fills its share and pauses. A failure inside a straddling batch also stops the partition below the failed record, which breaks ADR 0020's promise that every earlier record commits. And they would stay small: while every slot is taken, the dispatcher waits at a new key, so the queues of keys in progress stop growing exactly under load.

**Linger**, waiting up to a time for `max_batch` records. A share often holds fewer records than `max_batch`, so most lingers would run to their timeout. A lingering batch holds back its partition's committed offset, and in a key lane, it either holds a slot idle or churns through the slots' queue. Batches that grow with lag already bound the latency, and at low traffic there is little database load to save. It needs a benchmark that shows real savings before it is designed.

**Bisecting a failed batch** until each failure is one record. With inherited attempts, a database outage retries the batch, then bisects without backoff in about twice as many calls as records, and dead-letters every record at once. Without inherited attempts, each half retries from scratch and multiplies the retries.

**Index-based partial failure**, Spring's `BatchListenerFailedException`, which commits the records before the failed one. potik's outputs belong to the whole call, so the outputs of the prefix are unknown.

**A coalescer on a per-record core**, in the style of DataLoader: concurrent calls of a topic that runs by key await one shared bulk write. It needs no core change, but records of one key run one at a time, so it never combines them. It may still become a docs recipe for bulk writes across keys.

## Prior art

- Karafka is batch-first: a consumer gets one partition's records. Its virtual partitions group records by key, but then it commits their offsets after the transaction, so they lose exactly-once. Its dead letter queue moves good records of a failing batch first ("flickering").
- Spring Kafka batches one poll across partitions, with one transaction per batch, and keeps separate batch error handlers.
- Confluent's Parallel Consumer has a batch size, mutually exclusive with single records, and no linger ("self balancing"). Under key ordering a batch holds each key once, and its issue #266 for batches of one key stays open. A poison record re-forms the same batch forever (#887). The project is no longer maintained.
- Kafka Streams, Pekko Connectors Kafka, and Quix Streams commit transactions per interval, decoupled from processing, as potik's committer does. Kafka Streams batches per key with windows and a state store.
- FS2 Kafka's transactional example commits one transaction per `groupWithin` chunk, and needs a producer per partition.
- FastStream and Faust batch one poll across partitions, and have no partial failure handling. FastStream's `batch_timeout_ms` waits only while nothing is buffered.

## Later, on demand

**Key-grouped chunks**, for topics that want both batching and concurrency by key. A partition takes a chunk of up to `max_batch` consecutive records, groups them by key, runs each group as one call under the topic's key slots, and commits the chunk with one `add` once every group finishes. Groups never straddle chunks, so every chunk boundary is a cut point, and exactly-once outputs hold. It is Karafka's virtual partitions with exactly-once kept. A slow group holds up the next chunk, unless chunks later overlap with order kept per key.

## Open questions

1. Should `max_batch` also cap a batch's bytes? Databases limit a statement's size, e.g. Postgres takes at most 65,535 bind parameters.
2. Do users want one rule that dead-letters a whole batch, instead of the fallback to single records?
3. Should a dead letter's `attempts` header count the batch's attempts as well as the record's own?
4. Is one call per partition at a time enough concurrency for batched topics, or do key-grouped chunks need to come sooner?
