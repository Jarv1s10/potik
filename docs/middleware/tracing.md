# Tracing

`TracingMiddleware` reports spans to OpenTelemetry. It needs the `otel` extra:

```sh
uv add "potik[otel]"
```

Pass one instance to the runtime and one to each producer. Put it first in `middleware=`, so its spans cover the other middleware and the retries:

```python
import potik
from potik import Producer
from potik.middleware.tracing import TracingMiddleware


async def main() -> None:
    async with Producer("broker:9092", middleware=[TracingMiddleware()]) as producer:
        await potik.build_runtime(config, [router], context=producer, middleware=[TracingMiddleware()]).run()
```

It creates a span for each handler call, each output, each transaction, and each send. Record headers carry the trace context, so one trace follows a request through every service. The [API reference](../reference.md#potik.middleware.tracing.TracingMiddleware) lists every span.

## Attributes

Spans follow the OpenTelemetry semantic conventions for Kafka, and always have the attributes that the conventions require. The `optional_attributes` argument picks the optional ones. Record keys are off by default, because keys often identify people. To add them:

```python
tracing = TracingMiddleware(optional_attributes=TracingMiddleware.DEFAULT_OPTIONAL_ATTRIBUTES | {"messaging.kafka.message.key"})
```

To add attributes of your own, or to rename a span, change the current span. In a handler, the current span is the record's `process` span:

```python
from opentelemetry import trace


@router.handler(orders)
async def place(order: Order) -> None:
    trace.get_current_span().set_attribute("shop.tenant", order.tenant)
```

## Sampling

The tracer provider's sampler decides which records' traces to keep. To keep every failed trace and a share of the rest, sample on the outcome with the `tail_sampling` processor of the OpenTelemetry Collector.

Each transaction's span starts a trace of its own. `transaction_sample_ratio` gives that share of the transactions a span, plus every failed one. The tracer provider's sampler still decides whether to keep each span:

```python
tracing = TracingMiddleware(transaction_sample_ratio=0.1)
```

The [tracing example](https://github.com/Jarv1s10/potik/tree/dev/examples/tracing) runs three services with Jaeger.
