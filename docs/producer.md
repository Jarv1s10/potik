# Producer

A `Producer` sends records at once, outside any transaction. Use it where there is no input record to commit with, such as in an HTTP API. A handler can use one too, for records that must be visible before the handler finishes.

`send()` returns once the broker has stored the record, and raises `KafkaException` if the broker didn't store it. The producer is idempotent and waits for all in-sync replicas, so librdkafka's internal retries of a send don't write duplicates.

## In an API

Open the producer once, in the framework's lifespan, and send from the request handlers:

```python
from potik import Producer
from potik.middleware import TopicCheckMiddleware


@asynccontextmanager
async def lifespan(api: FastAPI) -> AsyncGenerator[dict[str, Producer]]:
    async with Producer("localhost:9092", middleware=[TopicCheckMiddleware([orders])]) as producer:
        yield {"producer": producer}


def get_producer(request: Request) -> Producer:
    return request.state.producer


@api.post("/orders")
async def place(order: Order, producer: Annotated[Producer, Depends(get_producer)]) -> None:
    await producer.send(orders(order, key=order.customer))
```

The [topic check](middleware/topic-check.md) fails the lifespan if a topic is missing. A producer opens only once, so each test client's lifespan opens a new one.

`Producer.from_config(config)` opens a producer with a runtime's `Config`. It uses the config's `bootstrap_servers`, `options`, and `producer_options`.

## In a handler

Pass the producer to handlers through the run's [context](typed/index.md#context):

```python
@router.handler(payments)
async def charge(payment: Payment, producer: Annotated[Producer, Context()]) -> ProducerRecord:
    await producer.send(audit_log(Charged.of(payment), key=payment.id))  # visible now
    return receipts(Receipt.of(payment), key=payment.id)                 # visible once the input commits


async def main() -> None:
    async with Producer.from_config(config) as producer:
        await potik.build_runtime(config, [router], context=producer).run()
```

Prefer returning outputs. A send is visible before the handler finishes, even if the input never commits, and a retried or replayed handler sends again. Consumers of a topic that receives sends must tolerate duplicates.

## Shutdown and health

Leaving the `async with` block waits up to `close_timeout` for pending sends, 10 seconds by default, then fails the rest. Keep it, plus the rest of the shutdown, below the pod's termination grace period.

After a fatal error, every later send fails, and `producer.failure` holds the error. Check it in the liveness probe, so that Kubernetes restarts the process.
