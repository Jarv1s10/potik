# potik

potik is an async Python framework for Kafka services that read records, process them, and write new ones. A handler returns its output records, and potik commits them in one Kafka transaction with the input's offset. Consumers downstream see each output exactly once, even after a crash or a rebalance.

```python
@router.handler(orders)
async def accept(order: Order, customer: Annotated[str, Key()]) -> ProducerRecord:
    return accepted(Accepted(sku=order.sku, total=order.amount * 1.2), key=customer)
```

!!! warning "Alpha"

    The API can change between minor versions until 1.0.

potik needs Python 3.12+ and Kafka 4.0+. It runs on [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) and [AnyIO](https://anyio.readthedocs.io).

## How potik is built

potik has three layers. Each layer uses only the layers below it, and the docs follow the same order.

| Layer | Package | What it does |
| --- | --- | --- |
| [Runtime](runtime/index.md) | `potik.core` | Reads the assigned partitions, calls a handler for each record, and commits the outputs with the record's offset. It also handles rebalances, shutdown, and health checks. |
| [Middleware](middleware/index.md) | `potik.middleware` | Wraps handler calls, transactions, and sends. The built-in middleware retries failed handlers, writes dead letters, limits concurrency, checks topics, and reports traces and metrics. |
| [Typed API](typed/index.md) | `potik` | Declares topics with typed values and keys, passes each handler the decoded parts of its record and its dependencies, and builds a runtime from the handlers. |

Most services use the typed API. It builds a core `Runtime`, so the runtime's rules for delivery, failures, and shutdown apply to every service.

Two clients run outside a runtime, for example in an HTTP API:

- A [`Producer`](producer.md) sends a record at once, without a transaction.
- A [`ReplyListener`](request-reply.md) implements an RPC pattern, where the process awaits a service's reply to a request that it sent.

Start with the [quickstart](quickstart.md). It builds and tests a service against a local broker in a few minutes. The [API reference](reference.md) lists every public class and function.

## Not supported yet

potik doesn't have these features yet. Some have a workaround.

- Batch handlers. A handler takes one record.
- Schema registry serdes, such as Avro and Protobuf. Write an [`AsyncSerde`](typed/topics.md#serdes-that-need-io).
- Subscriptions by topic pattern.
- At-least-once delivery without transactions.

## Not planned

potik leaves these out on purpose. Each has a way to get the same result.

- Sync handlers. A thread cannot be cancelled, so a blocking call hides the waits that a revocation or shutdown depends on. Run blocking code from an `async def` handler with `anyio.to_thread.run_sync()`.
- Several handlers for one topic. They would share one offset, so a retry would repeat the handlers that succeeded. Call the other functions from one handler, or handle the topic in another consumer group.
- Startup and shutdown hooks in `potik.build_runtime()`. Open resources around `run()`, or in a middleware's `lifespan`.
- A command line interface. Start the app with `anyio.run()`.
