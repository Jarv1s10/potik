"""The attributes of TracingMiddleware's spans and MetricsMiddleware's metrics, driven through their stages."""

import time
from collections.abc import Collection, Sequence

import pytest
from confluent_kafka import Message

pytest.importorskip("opentelemetry.sdk")

import functools
import operator

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.semconv._incubating.attributes import messaging_attributes
from opentelemetry.semconv.attributes import error_attributes
from opentelemetry.trace import SpanKind, StatusCode

from potik import Config, ConsumerRecord, Delivery, ProducerRecord, RuntimeStatus
from potik.core import BaseMiddleware, PartitionStatus, ProducerScope, RunScope
from potik.core.types import Transaction
from potik.middleware.metrics import MetricsMiddleware
from potik.middleware.tracing import TracingMiddleware

pytestmark = pytest.mark.anyio

# The attribute names that the semantic conventions define, for messaging and errors.
REGISTRY = {
    value
    for module in (messaging_attributes, error_attributes)
    for name, value in vars(module).items()
    if name.isupper() and isinstance(value, str)
}
KEY = "messaging.kafka.message.key"
EVERY_OPTIONAL = TracingMiddleware.DEFAULT_OPTIONAL_ATTRIBUTES | {KEY, "messaging.message.body.size"}
RUN = RunScope(
    config=Config(bootstrap_servers="kafka:9092", group_id="shop", options={"client.id": "svc"}),
    status=RuntimeStatus,
    client_options={},
)
OUTPUTS = [ProducerRecord("confirmed", b"customer-1", b"ok"), ProducerRecord("audit", None, None, partition=0)]


async def drive(middleware: BaseMiddleware) -> None:
    """Handle and commit a record, commit no outputs, handle another record, and send through a producer."""

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        return OUTPUTS if record.offset() == 5 else []

    async def commit(transaction: Transaction) -> None:
        pass

    async def deliver(record: ProducerRecord) -> Delivery:
        return Delivery(2, 9)

    async with middleware.lifespan(RUN):
        outputs = await middleware.handle(Message(topic="orders", partition=1, offset=5, key=b"customer-1"), handler)
        await middleware.commit(Transaction({("orders", 1): 6}, outputs), commit)
        await middleware.commit(Transaction({("orders", 1): 6}, ()), commit)
        await middleware.handle(Message(topic="orders", partition=1, offset=6, key=b"customer-1", value=b"{}"), handler)
    async with middleware.lifespan(ProducerScope(client_options={"client.id": "api"})):
        await middleware.send(ProducerRecord("orders", b"customer-2", b"{}"), deliver)


async def traced(**options: object) -> dict[str, list[ReadableSpan]]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    await drive(TracingMiddleware(tracer_provider=provider, **options))  # ty: ignore[invalid-argument-type]
    spans: dict[str, list[ReadableSpan]] = {}
    for span in exporter.get_finished_spans():
        spans.setdefault(span.name, []).append(span)
    return spans


async def test_spans_have_only_semantic_convention_attributes() -> None:
    spans = await traced(optional_attributes=EVERY_OPTIONAL)
    names = {name for span in functools.reduce(operator.iadd, spans.values(), []) for name in span.attributes or {}}
    assert names - REGISTRY == set()


async def test_transactions_are_root_client_spans_that_link_to_their_outputs() -> None:
    """A transaction with several output topics is a `send`, one without outputs is a `commit` of its topic."""
    spans = await traced()
    [send] = spans["send"]
    assert (send.kind, send.parent, send.attributes["messaging.batch.message_count"]) == (SpanKind.CLIENT, None, 2)
    assert "messaging.destination.name" not in send.attributes
    creates = [*spans["create confirmed"], *spans["create audit"]]
    assert [link.context.span_id for link in send.links] == [create.context.span_id for create in creates]
    [commit] = spans["commit orders"]
    assert (commit.kind, commit.parent, commit.links) == (SpanKind.CLIENT, None, ())
    assert commit.attributes["messaging.operation.type"] == "settle"
    assert "messaging.batch.message_count" not in commit.attributes


async def test_defaults_add_partitions_offsets_and_client_ids() -> None:
    spans = await traced()
    [process, _] = spans["process orders"]
    assert (process.attributes["messaging.destination.partition.id"], process.attributes["messaging.kafka.offset"]) == (
        "1",
        5,
    )
    assert process.attributes["messaging.client.id"] == "svc"
    assert KEY not in process.attributes
    [audit] = spans["create audit"]
    assert (
        audit.attributes["messaging.destination.partition.id"],
        audit.attributes["messaging.kafka.message.tombstone"],
    ) == (
        "0",
        True,
    )
    [sent] = spans["send orders"]
    assert (sent.attributes["messaging.client.id"], sent.attributes["messaging.kafka.offset"]) == ("api", 9)


async def test_attributes_pick_the_optional_attributes() -> None:
    spans = await traced(optional_attributes={KEY}, key_text=lambda key: key.decode().upper())
    optional = {
        span.name: (span.attributes or {}).keys() & EVERY_OPTIONAL
        for span in functools.reduce(operator.iadd, spans.values(), [])
    }
    assert optional == {
        "process orders": {KEY},
        "create confirmed": {KEY},
        "create audit": set(),
        "send": set(),
        "commit orders": set(),
        "send orders": {KEY},
    }
    assert spans["process orders"][0].attributes[KEY] == "CUSTOMER-1"


@pytest.mark.parametrize("name", ["messaging.message.id", "messaging.system"])
def test_attributes_reject_names_it_does_not_add(name: str) -> None:
    with pytest.raises(ValueError, match="attributes must be among"):
        TracingMiddleware(optional_attributes={name})
    with pytest.raises(ValueError, match="attributes must be among"):
        MetricsMiddleware(optional_attributes={name})


@pytest.mark.parametrize("attributes", [{"messaging.destination.partition.id"}, set()])
async def test_metrics_have_only_semantic_convention_attributes(attributes: Collection[str]) -> None:
    reader = InMemoryMetricReader()
    await drive(
        MetricsMiddleware(optional_attributes=attributes, meter_provider=MeterProvider(metric_readers=[reader]))
    )
    data = reader.get_metrics_data()
    assert data is not None
    points = [
        point
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        for point in metric.data.data_points
    ]
    names = {name for point in points for name in point.attributes or {}}
    assert names - REGISTRY == set()
    assert ("messaging.destination.partition.id" in names) == bool(attributes)
    operations = {(point.attributes or {}).get("messaging.operation.name") for point in points}
    assert operations == {"process", "send", "commit"}


async def test_metrics_count_the_stopped_partitions_of_every_run() -> None:
    """Each run's partitions report 1 if stopped, else 0, also when several middleware share a meter provider."""
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])

    def run(group: str, *failures: Exception | None) -> RunScope:
        partitions = tuple(
            PartitionStatus(
                topic="orders",
                partition=partition,
                buffered_records=0,
                running_handlers=0,
                paused=failure is not None,
                handler_started_at=None,
                failure=failure,
            )
            for partition, failure in enumerate(failures)
        )
        return RunScope(
            config=Config(bootstrap_servers="kafka:9092", group_id=group),
            status=lambda: RuntimeStatus(partitions=partitions),
            client_options={},
        )

    def stopped() -> dict[tuple[object, object], object]:
        """Return the reported count of each group's partition."""
        if (data := reader.get_metrics_data()) is None:
            return {}  # Nothing to report once the runs end.
        points = [
            point
            for resource in data.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
            if metric.name == "potik.partition.stopped"
            for point in metric.data.data_points
            if isinstance(point, NumberDataPoint)
        ]
        group, partition = "messaging.consumer.group.name", "messaging.destination.partition.id"
        return {((p.attributes or {})[group], (p.attributes or {})[partition]): p.value for p in points}

    shop, billing = MetricsMiddleware(meter_provider=provider), MetricsMiddleware(meter_provider=provider)
    async with shop.lifespan(run("shop", ValueError("poison"), None)), billing.lifespan(run("billing", None)):
        assert stopped() == {("shop", "0"): 1, ("shop", "1"): 0, ("billing", "0"): 0}
    assert stopped() == {}


async def test_a_zero_transaction_sample_ratio_traces_only_failed_transactions() -> None:
    """A failed transaction gets a span once it fails, backdated to its start."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    middleware = TracingMiddleware(transaction_sample_ratio=0, tracer_provider=provider)
    failed_at: list[int] = []

    async def commit(transaction: Transaction) -> None:
        pass

    async def fail(transaction: Transaction) -> None:
        failed_at.append(time.time_ns())
        raise RuntimeError("fenced")

    with pytest.raises(RuntimeError, match="fenced"):
        async with middleware.lifespan(RUN):
            await middleware.commit(Transaction({("orders", 0): 1}, ()), commit)
            await middleware.commit(Transaction({("orders", 0): 2}, ()), fail)

    [transaction] = [span for span in exporter.get_finished_spans() if span.name == "commit orders"]
    assert (transaction.status.status_code, transaction.attributes["error.type"]) == (
        StatusCode.ERROR,
        "builtins.RuntimeError",
    )
    assert transaction.start_time <= failed_at[0]  # It starts when the transaction did, not when it failed.


def test_transaction_sample_ratio_is_a_share() -> None:
    with pytest.raises(ValueError, match="transaction_sample_ratio must be between 0 and 1"):
        TracingMiddleware(transaction_sample_ratio=1.5)
