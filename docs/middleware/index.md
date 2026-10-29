# Middleware

Middleware wraps the stages of a runtime or a `Producer`. A runtime runs each middleware around its startup and shutdown, each handler call, and each transaction. A producer runs it around its startup and each send.

potik ships five middleware classes:

| Middleware | Effect | How `potik.build_runtime()` adds it |
| --- | --- | --- |
| [`ExceptionMiddleware`](exceptions.md) | Retries failed handlers and writes dead letters | `exception_rules=` |
| [`ConcurrencyMiddleware`](concurrency.md) | Limits concurrent handler calls | `max_concurrency=` |
| [`TopicCheckMiddleware`](topic-check.md) | Fails the start if a topic is missing | On by default |
| [`TracingMiddleware`](tracing.md) | Traces records with OpenTelemetry | `middleware=` |
| [`MetricsMiddleware`](metrics.md) | Reports OpenTelemetry metrics | `middleware=` |

The examples in this section build on the [quickstart](../quickstart.md)'s `config`, `router`, and topics.

## Adding middleware

`potik.build_runtime()` builds the first three from its arguments. Pass any other middleware as `middleware=`, outermost first:

```python
from potik.middleware import on, dead_letter
from potik.middleware.metrics import MetricsMiddleware
from potik.middleware.tracing import TracingMiddleware

runtime = potik.build_runtime(
    config,
    [router],
    middleware=[TracingMiddleware(), MetricsMiddleware()],
    exception_rules=[on(Exception, dead_letter(dead_letters))],
    max_concurrency=20,
)
```

The topic check runs first, at startup. Each record then passes through `middleware`, the exception rules, and the concurrency limits, in that order, before it reaches its handler.

A `Producer` takes middleware too:

```python
producer = Producer("broker:9092", middleware=[TracingMiddleware()])
```

A middleware instance serves one runtime or one producer, once. To use the same middleware in several places, create it in a function:

```python
def observability() -> list[BaseMiddleware]:
    return [TracingMiddleware(), MetricsMiddleware()]


async def main() -> None:
    async with Producer("broker:9092", middleware=observability()) as producer:
        await potik.build_runtime(config, [router], context=producer, middleware=observability()).run()
```

## Order in a core runtime

A [core `Runtime`](../runtime/core.md) runs only the middleware that you pass. Pass the built-in middleware in this order, which `potik.build_runtime()` also uses:

1. `TopicCheckMiddleware`, so a missing topic fails the start before other middleware open resources.
2. `TracingMiddleware` and `MetricsMiddleware`, so their spans and durations include retries.
3. `ExceptionMiddleware`.
4. `ConcurrencyMiddleware`, so a skipped record never waits for a slot, and a retry's backoff holds none.

## Writing middleware

Subclass `BaseMiddleware` and override the stages you need:

| Method | Wraps |
| --- | --- |
| `lifespan(scope)` | One run, or the time a `Producer` is open |
| `handle(record, call_next)` | One handler call |
| `commit(transaction, call_next)` | One transaction |
| `send(record, call_next)` | One send through a `Producer` |

Each method except `lifespan` calls `call_next` to run the rest of the chain. This middleware logs slow handler calls:

```python
import logging
import time
from collections.abc import Sequence

from potik.core import BaseMiddleware, ConsumerRecord, ProducerRecord, RecordHandler

log = logging.getLogger(__name__)


class SlowCallLog(BaseMiddleware):
    def __init__(self, threshold: float) -> None:
        self._threshold = threshold

    async def handle(self, record: ConsumerRecord, call_next: RecordHandler) -> Sequence[ProducerRecord]:
        start = time.monotonic()
        try:
            return await call_next(record)
        finally:
            if (elapsed := time.monotonic() - start) > self._threshold:
                log.warning("%s offset %d took %.1fs", record.topic(), record.offset(), elapsed)
```

Middleware sees raw records, so it works with any handler. Pass a middleware its resources, such as a database pool, in `__init__`. Only `handle` may recover from a failure, by retrying, skipping the record, or replacing its outputs. The [API reference](../reference.md#potik.core.BaseMiddleware) describes each stage.
