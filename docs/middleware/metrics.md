# Metrics

`MetricsMiddleware` reports metrics to OpenTelemetry. It needs the `otel` extra:

```sh
uv add "potik[otel]"
```

Pass one instance to the runtime and one to each producer. Put it first in `middleware=`, beside `TracingMiddleware`, so a handler call's duration includes its retries:

```python
import potik
from potik import Producer
from potik.middleware.metrics import MetricsMiddleware


async def main() -> None:
    async with Producer("broker:9092", middleware=[MetricsMiddleware()]) as producer:
        await potik.build_runtime(config, [router], context=producer, middleware=[MetricsMiddleware()]).run()
```

It records these metrics:

| Metric | Measures |
| --- | --- |
| `messaging.client.consumed.messages` | Records that reach the handlers |
| `messaging.process.duration` | Each handler call, with its retries |
| `messaging.client.operation.duration` | Each transaction, and each send through a `Producer` |
| `messaging.client.sent.messages` | Each transaction's outputs, and each send through a `Producer` |
| `potik.partition.stopped` | Partitions that a handler failure or a fetch error stopped |

The health checks ignore stopped partitions, so alert when `potik.partition.stopped` stays above zero (see [Health checks](../runtime/production.md#health-checks)).

## Attributes

Metrics follow the OpenTelemetry semantic conventions for messaging, and always have the attributes that the conventions require. The `optional_attributes` argument picks the optional ones. `potik.partition.stopped` is the one metric that the conventions don't define.

To rename a metric, drop its attributes, or change its buckets, configure a `View` in the SDK's `MeterProvider`.
