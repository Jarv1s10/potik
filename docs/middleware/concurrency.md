# Concurrency limits

Each partition runs one handler call at a time, so an instance runs up to one call per assigned partition (see [Concurrency](../runtime/index.md#ordering-and-concurrency)). To run fewer, for example no more than a database pool's size, set a limit.

`potik.build_runtime(..., max_concurrency=20)` limits handler calls across all topics. A handler's `max_concurrency=` limits the calls of its own topic:

```python
@router.handler(orders, max_concurrency=5)
async def place(order: Order) -> None: ...


runtime = potik.build_runtime(config, [router], max_concurrency=20)
```

A call waits for its topic's slot first, then for a shared one. So a call that waits for its topic's slot holds no shared slot.

The limits cover only the handler. A record that an exception rule skips never waits for a slot, and a retry's backoff holds none.

## Key concurrency

Most topics need order only per key. A handler's `key_concurrency=` lets records with different keys run at the same time, while records with the same key still run one at a time, in the order they were written:

```python
@router.handler(orders, key_concurrency=64, max_concurrency=20)
async def place(order: Order, customer: Annotated[str, Key()]) -> None: ...
```

`key_concurrency` is the most keys of the topic whose records run at once in an instance, across all its partitions. All records without a key count as one key, so a topic without keys gains nothing from it. Set it to at least the number of partitions that an instance may get, or fewer records run at once than without it. Outputs still commit exactly once: a record that finishes before an earlier record of its partition waits for it before it commits.

Three settings balance throughput, memory, and the limits of the systems that handlers call:

- `key_concurrency` sets how many records can make progress at once. Set it above `max_concurrency`, so that records whose key is busy, or that wait for a retry, leave work for the free slots.
- `max_concurrency` limits the handler calls, e.g. to a database pool's size or an API's rate limit. Keys in a retry's backoff hold no slot.
- `Config.max_buffered_bytes` limits the memory of fetched records, including those that run or wait for an earlier record. A slow record lets the records behind it finish until they fill its partition's share, and then the partition pauses until the slow record finishes. A larger budget lets other keys get further ahead of a slow record.

When a handler fails without an exception rule, its partition stops at the failed record. The records after it are cancelled, and the records before it finish and commit. With `Config(stop_failed_partitions=False)`, every record of the partition is cancelled, and the app stops.

## Core runtime

In a [core runtime](../runtime/core.md), pass a `ConcurrencyMiddleware` last in the middleware list:

```python
ConcurrencyMiddleware(max_concurrency=20, per_topic={"orders": 5})
```

Pass key concurrency to the runtime itself:

```python
Runtime(config, ["orders"], handle, middleware=[...], key_concurrency={"orders": 64})
```
