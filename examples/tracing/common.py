"""Shared setup of the tracing example: topics, their values, and the OpenTelemetry SDK."""

import logging
import os
import socket
from collections.abc import Callable

import uvicorn
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from potik import Runtime, Topic
from potik.middleware.tracing import TracingMiddleware
from potik.serde import PydanticJson

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
# Each service's health probes listen on HEALTH_PORT, or on a port of its own.
HEALTH_PORT = os.environ.get("HEALTH_PORT")
# The tracing middleware's attributes, plus each record's key, the order's customer id.
KEYED = TracingMiddleware.DEFAULT_OPTIONAL_ATTRIBUTES | {"messaging.kafka.message.key"}


class Order(BaseModel):
    id: str
    customer: str
    sku: str
    amount: float
    status: str = "placed"


class Notification(BaseModel):
    channel: str
    order: str


# The contracts that the services share, which pin the values' format. Every record is keyed by its customer.
JSON = PydanticJson()
orders = Topic("orders", Order, serde=JSON)
confirmed = Topic("orders.confirmed", Order, serde=JSON)
dead_letters = Topic("orders.dlq", Order, serde=JSON)
notifications = Topic("notifications", Notification, serde=JSON)
# Partitions per topic.
TOPICS = {orders.name: 3, confirmed.name: 3, dead_letters.name: 1, notifications.name: 3}


def setup(service: str) -> None:
    """Configure logging, create missing topics, and export spans.

    Spans go over OTLP (`OTEL_EXPORTER_OTLP_*`), or nowhere if `OTEL_TRACES_EXPORTER=none`.
    """
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s {service}[{os.getpid()}] %(levelname)s %(message)s")
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = admin.list_topics(timeout=10).topics
    missing = [NewTopic(name, partitions) for name, partitions in TOPICS.items() if name not in existing]
    for future in admin.create_topics(missing).values() if missing else ():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:  # Another service created it meanwhile.
                raise
    resource = Resource.create(
        {"service.name": service, "service.instance.id": f"{socket.gethostname()}-{os.getpid()}"}
    )
    provider = TracerProvider(resource=resource)
    if os.environ.get("OTEL_TRACES_EXPORTER") != "none":
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)


def probe_server(current: Callable[[], Runtime], port: int) -> uvicorn.Server:
    """Return a server of the current runtime's `/livez` and `/readyz`, on `HEALTH_PORT` or `port`.

    Run it as a task next to the runtime, so that the runtime handles SIGTERM.
    """

    async def livez(request: Request) -> PlainTextResponse:
        return probe(current().liveness())

    async def readyz(request: Request) -> PlainTextResponse:
        return probe(current().readiness())

    app = Starlette(routes=[Route("/livez", livez), Route("/readyz", readyz)])
    config = uvicorn.Config(app, host="0.0.0.0", port=int(HEALTH_PORT or port), log_level="warning", lifespan="off")
    return uvicorn.Server(config)


def probe(reason: str | None) -> PlainTextResponse:
    """Answer a probe with 200, or with 503 and the reason it fails."""
    return PlainTextResponse("ok\n") if reason is None else PlainTextResponse(f"{reason}\n", 503)
