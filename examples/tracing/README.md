# Tracing across apps

Three services handle each order, and Jaeger shows its trace end to end:

```
checkout (potik Producer)    -> orders
fulfillment (potik)           orders -> orders.confirmed | orders.dlq
notifier (potik)              orders.confirmed -> notifications, orders.dlq -> (helpdesk ticket)
```

`TracingMiddleware` gives each order a tree like this, across services:

```
POST /checkout                        checkout
└─ send orders                        checkout
   └─ process orders                  fulfillment   one per handler call, retries included
      ├─ UPDATE stock, POST /charges  fulfillment   the handler's own spans
      └─ create orders.confirmed      fulfillment   one per output, propagated in its headers
         └─ process orders.confirmed  notifier
            └─ create notifications   notifier
```

Each transaction gets a trace of its own, rooted at its `send` span. The `send` span links to the `create` span of each output in the transaction, and Jaeger lists these links under the span's "References".

- `fulfillment` retries random payment timeouts. The handler's spans from every attempt are children of one `process orders` span. That span has an `exception` event for each failed attempt.
- `fulfillment` dead-letters every 7th order. Its `process orders` span succeeds and records the `OutOfStockError` as an `exception` event. Its trace continues in `notifier`'s `process orders.dlq`.
- All three services add `messaging.kafka.message.key` to the tracing middleware's `attributes`, so spans carry each order's customer id.
- The 10th order crashes `fulfillment`'s run once. The restarted run replays the orders that it handled but had not committed, so their traces show a second `process orders` span next to the first one. Only the second one's outputs commit, so only its `create` span has a link from a transaction and a `process` span in `notifier`.

```sh
uv sync --extra otel --extra pydantic
podman compose -f examples/tracing/compose.yml up -d --wait   # KAFKA_PORT=19092 if port 9092 is taken

# KAFKA_BOOTSTRAP=localhost:19092 for a moved broker
uv run python examples/tracing/fulfillment.py &
uv run python examples/tracing/notifier.py &
uv run python examples/tracing/checkout.py 21 0.3   # prints each order's trace id

open http://localhost:16686   # service "checkout", or /trace/<trace id>
# OTEL_TRACES_EXPORTER=none to run the services without exporting spans
kill %1 %2
podman compose -f examples/tracing/compose.yml down
```

## Health probes

`fulfillment` and `notifier` serve `/livez` and `/readyz` with uvicorn, on ports 8081 and 8082, or `HEALTH_PORT`. Each runs the server as a task next to its runtime, as [Health checks](../../docs/runtime/production.md#health-checks) shows:

```sh
curl localhost:8081/readyz   # ok
```

## Logs and baggage

A handler logs within its `process` span's context. The committer logs within its transaction's span, e.g. when it repeats a transaction for a stale group epoch. An OpenTelemetry logging handler, such as the SDK's `LoggingHandler`, adds the trace and span ids to these log records, so the backend shows them with the trace.

Baggage set upstream, such as a tenant id, arrives with the trace context. The handler sees it, and the headers of every output propagate it. To add it to the `process` and `create` spans as attributes, add `BaggageSpanProcessor` from `opentelemetry-processor-baggage` to the tracer provider. A transaction starts its own trace and holds the outputs of many records, so its spans have no baggage.

## Metrics

`potik.middleware.metrics.MetricsMiddleware` records `messaging.client.consumed.messages` and `messaging.process.duration` per topic and partition. It records `messaging.client.operation.duration` and `messaging.client.sent.messages` per transaction. Jaeger stores no metrics, so this example leaves it out. To use it, put it first in the middleware list next to `TracingMiddleware`, and give it a `MeterProvider` that exports to your backend.
