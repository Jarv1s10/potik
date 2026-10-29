# Core API

`potik.core` runs a service without the typed API. It has no decorators, serdes, or parameter injection. A handler takes a raw `ConsumerRecord` and returns `ProducerRecord`s, and you pass middleware instances yourself. The runtime is the same one that `potik.build_runtime()` builds, with the same guarantees, shutdown, and health checks.

Use the core when you want full control over each record's bytes, or to build an API of your own on top of it.

## A service

```python
from collections.abc import Sequence

import anyio

from potik.core import Config, ConsumerRecord, ProducerRecord, Runtime, resume_or_earliest
from potik.middleware import (
    ConcurrencyMiddleware,
    ExceptionMiddleware,
    TopicCheckMiddleware,
    on,
    dead_letter,
    retry,
)


async def accept(record: ConsumerRecord) -> list[ProducerRecord]:
    return [ProducerRecord("orders.accepted", record.key(), record.value())]


async def audit(record: ConsumerRecord) -> list[ProducerRecord]:
    return []


handlers = {"orders": accept, "payments": audit}


async def handle(record: ConsumerRecord) -> Sequence[ProducerRecord]:
    return await handlers[record.topic()](record)


exceptions = ExceptionMiddleware(
    on(TimeoutError, retry(max_attempts=5)),
    on(Exception, dead_letter("orders.dlq")),
)
runtime = Runtime(
    Config(bootstrap_servers="localhost:9092", group_id="acceptor", offset_policy=resume_or_earliest),
    handlers,
    handle,
    middleware=[
        TopicCheckMiddleware([*handlers, "orders.accepted", *exceptions.output_topics]),
        exceptions,
        ConcurrencyMiddleware(max_concurrency=20),
    ],
)
anyio.run(runtime.run)
```

A runtime subscribes to the topics that you pass and calls one handler for every record. A service with several topics dispatches on `record.topic()` itself.

The handler returns a list or tuple of records, which may be empty. Any other result raises `InvalidOutputError` from the handler's call, so exception rules handle it like any other handler failure.

## Middleware

A core runtime runs only the middleware that you pass, outermost first. [Order in a core runtime](../middleware/index.md#order-in-a-core-runtime) lists the order that the built-in middleware needs.

## Resources

Open the resources that the handler shares around the run, and pass them in:

```python
async with httpx.AsyncClient() as client:
    await Runtime(config, ["orders"], partial(enrich, client)).run()
```
