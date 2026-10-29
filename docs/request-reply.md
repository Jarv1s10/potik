# Request-reply

An API often sends a request to Kafka and answers its HTTP caller with a service's reply. With several API replicas, the reply must reach the replica that sent the request. A `ReplyListener` reads every partition of the reply topic in every replica, without a consumer group, and hands each reply to the request in its own process that awaits it.

## The requester

Open a `ReplyListener` and a `Producer` in the API's lifespan, before it takes requests. For each request, enter `expect()`, send the request with the block's `correlation_id`, and wait for the reply:

```python
from contextlib import asynccontextmanager

import anyio
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse

from potik import ConsumerRecord, Producer, ReplyListener


def request_id(reply: ConsumerRecord) -> str | None:
    headers = dict(reply.headers() or ())
    return headers["request-id"].decode() if headers.get("request-id") else None


@asynccontextmanager
async def lifespan(api: Starlette):
    async with (
        Producer(BOOTSTRAP) as producer,
        ReplyListener("crm.updates.status", bootstrap_servers=BOOTSTRAP, correlate=request_id) as listener,
    ):
        yield {"producer": producer, "listener": listener}


async def update(request: Request) -> JSONResponse:
    record_id = request.path_params["record_id"]
    with request.state.listener.expect() as pending:
        record = updates(Update(**await request.json()), key=record_id, headers={"request-id": pending.correlation_id})
        try:
            with anyio.fail_after(10):
                await request.state.producer.send(record)
                reply = await pending.wait()
        except TimeoutError:
            return JSONResponse({"error": "no reply yet"}, 504)
    status = await statuses.decode(reply.value() or b"")
    return JSONResponse(status.model_dump())
```

Enter `expect()` before you send, so that a fast reply can't arrive first. potik sets no convention for the correlation ID. Send it however your responder reads it, such as a header or a field of the value, and pass a `correlate` function that reads it from a reply.

## The responder

The responder is a plain [handler](typed/handlers.md). It copies the request's ID to its reply, and returns the reply as an output, which commits with the request's offset:

```python
@router.handler(updates, output_topics=[statuses])
async def apply(
    update: Update, record_id: Annotated[str, Key()], request_id: Annotated[str | None, Header("request-id")]
) -> ProducerRecord[Status]:
    status = await crm.apply(update)
    return statuses(status, key=record_id, headers={"request-id": request_id})
```

## Timeouts and duplicates

A request that times out may still be handled later. Its reply then arrives while nothing awaits it, and the listener drops it. A replayed responder replies again, and only the first reply counts. Make the responder idempotent.

A listener reads only replies that arrive after it opens, so a restarted replica never sees the replies to its previous run's requests. Every replica reads every reply, which suits replies at API rates rather than at stream rates.

Check `listener.failure` in the liveness probe, next to `producer.failure`. The listener sets it when a fatal error stops it from reading.

The [rpc example](https://github.com/Jarv1s10/potik/tree/dev/examples/rpc) runs an API with several replicas and its responder.
