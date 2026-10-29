"""The requester: a Starlette app that sends each update to Kafka and answers with the updater's reply.

Run as many replicas as you like. Each one reads every reply, and keeps only the replies to its own requests.

Usage: uv run uvicorn api:app --app-dir examples/rpc --port 8000
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import anyio
from common import BOOTSTRAP, CORRELATION_ID, Update, setup, statuses, updates
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

from potik import ConsumerRecord, Producer, ReplyListener

TIMEOUT = 10.0


def correlation_id(reply: ConsumerRecord) -> str | None:
    """Return the reply's last `correlation-id` header, which the updater copied from the request."""
    found = dict(reply.headers() or ()).get(CORRELATION_ID)
    return found.decode() if isinstance(found, bytes) else found


@asynccontextmanager
async def lifespan(api: Starlette) -> AsyncGenerator[dict[str, Producer | ReplyListener]]:
    setup("api")
    # The listener opens before the app takes requests, so it reads the reply to each of them.
    async with (
        Producer(BOOTSTRAP) as producer,
        ReplyListener(statuses.name, bootstrap_servers=BOOTSTRAP, correlate=correlation_id) as listener,
    ):
        yield {"producer": producer, "listener": listener}


async def update(request: Request) -> JSONResponse:
    record_id = request.path_params["record_id"]
    update = Update(record_id=record_id, fields=await request.json())
    listener: ReplyListener = request.state.listener
    # Await the reply before sending, so that a fast reply cannot arrive first.
    with listener.expect() as pending:
        record = updates(update, key=record_id, headers={CORRELATION_ID: pending.correlation_id})
        try:
            with anyio.fail_after(TIMEOUT):
                await request.state.producer.send(record)
                reply = await pending.wait()
        except TimeoutError:
            return JSONResponse({"error": "no reply yet, and the update may still apply"}, 504)
    status = await statuses.decode(reply.value() or b"")
    return JSONResponse(status.model_dump(), 200 if status.ok else 422)


async def livez(request: Request) -> PlainTextResponse:
    """Fail once the producer or the listener stops, so that Kubernetes restarts the replica."""
    failure = request.state.producer.failure or request.state.listener.failure
    return PlainTextResponse("ok\n") if failure is None else PlainTextResponse(f"{failure}\n", 503)


app = Starlette(
    routes=[Route("/records/{record_id}", update, methods=["POST"]), Route("/livez", livez)], lifespan=lifespan
)
