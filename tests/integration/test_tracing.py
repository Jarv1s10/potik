"""TracingMiddleware's span tree across a failed run and its replay, and MetricsMiddleware's metrics."""

from collections import defaultdict
from typing import Annotated

import anyio
import pytest

import potik
from potik import Config, ConsumerRecord, Context, Producer, ProducerRecord, Router, resume_or_earliest

pytest.importorskip("opentelemetry.sdk")

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import SpanLimits, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from potik.middleware import ExceptionContext, on, retry
from potik.middleware.metrics import MetricsMiddleware
from potik.middleware.tracing import TracingMiddleware

pytestmark = pytest.mark.anyio

KEYED = TracingMiddleware.DEFAULT_OPTIONAL_ATTRIBUTES | {"messaging.kafka.message.key"}


async def test_only_committed_handlings_link_to_a_transaction(workload) -> None:
    """A crash leaves handled records uncommitted, and only the outputs of their replays link to a transaction."""
    work = workload(2, 200)
    exporter = InMemorySpanExporter()
    provider = TracerProvider(span_limits=SpanLimits(max_links=work.records))  # One transaction may send them all.
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    crashed = False

    async def crash(record: ConsumerRecord) -> None:
        nonlocal crashed
        await anyio.sleep(0.002)
        if (record.partition(), record.offset()) == (0, 50) and not crashed:
            crashed = True
            raise ValueError("crash")

    def run():
        # The next commit is due after 5s, so only the start position is committed before the crash.
        middleware = [TracingMiddleware(tracer_provider=provider)]
        return work.typed(crash, middleware=middleware, stop_failed_partitions=False, commit_interval_ms=5000)

    with pytest.raises(ValueError, match="crash"):
        await run().run(handle_signals=False)
    await work.run_until_committed(run())
    work.assert_exactly_once()

    spans = exporter.get_finished_spans()
    creates = defaultdict(list)  # process span id -> its create spans
    processes = defaultdict(list)  # (partition, offset) -> its process spans, in handling order
    for span in spans:
        if span.kind is SpanKind.PRODUCER and span.parent is not None:
            creates[span.parent.span_id].append(span.context.span_id)
        if span.kind is SpanKind.CONSUMER:
            attributes = span.attributes or {}
            key = (attributes["messaging.destination.partition.id"], attributes["messaging.kafka.offset"])
            processes[key].append(span)
    assert len(processes) == work.records
    committed = {
        link.context.span_id
        for span in spans
        if span.kind is SpanKind.CLIENT and span.parent is None and span.status.status_code is StatusCode.UNSET
        for link in span.links
    }

    replayed = 0
    for key, handlings in processes.items():
        assert all(process.parent is None for process in handlings)  # The inputs carry no trace context.
        *earlier, last = handlings
        assert [create in committed for create in creates[last.context.span_id]] == [True]
        if key == ("0", 50):
            assert [p.status.status_code for p in handlings] == [StatusCode.ERROR, StatusCode.UNSET]
        elif earlier:
            (process,) = earlier
            assert process.status.status_code is StatusCode.UNSET
            if uncommitted := creates[process.context.span_id]:
                replayed += 1
                assert [create in committed for create in uncommitted] == [False]
            else:  # In flight when the run failed.
                assert [event.name for event in process.events] == ["cancelled"]
    assert replayed >= 50, "the crash left nothing handled but uncommitted"


class TransientError(Exception):
    pass


async def test_exceptions_are_recorded_and_handling_is_measured(workload) -> None:
    """Retried and dead-lettered exceptions are events of a successful process span, and metrics count every record.

    Records that keep failing spend their retries, then fall through to the dead letter rule.
    """
    work = workload(2, 40)
    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])
    failed: set[bytes] = set()

    async def fail(record: ConsumerRecord) -> None:
        key = record.key() or b""
        if int(key) % 4 == 1 and key not in failed:
            failed.add(key)
            raise TransientError("try again")
        if int(key) % 4 == 3:
            raise TransientError("never")

    def to_input_partition(context: ExceptionContext[Exception]) -> list[ProducerRecord]:
        """Dead-letter the record to the input's partition, which the harness checks."""
        record = context.record
        return [ProducerRecord(work.target, record.key(), b"dead", partition=record.partition())]

    runtime = work.typed(
        fail,
        middleware=[
            MetricsMiddleware(meter_provider=meter_provider),
            TracingMiddleware(optional_attributes=KEYED, tracer_provider=tracer_provider),
        ],
        exception_rules=[
            on(TransientError, retry(max_attempts=2, backoff=lambda _: 0)),
            on(lambda exc: str(exc) == "never", to_input_partition),
        ],
    )
    await work.run_until_committed(runtime)
    work.assert_exactly_once()

    processes = [span for span in exporter.get_finished_spans() if span.kind is SpanKind.CONSUMER]
    assert len(processes) == work.records
    for span in processes:
        attributes = span.attributes or {}
        key = int(str(attributes["messaging.kafka.message.key"]))
        assert span.status.status_code is StatusCode.UNSET
        events = [(event.name, dict(event.attributes or {})) for event in span.events]
        if key % 4 == 1:
            [(name, event)] = events
            assert name == "exception"
            assert event["exception.type"].endswith("TransientError")
        elif key % 4 == 3:
            assert [(name, event["exception.message"]) for name, event in events] == [("exception", "never")] * 2
        else:
            assert events == []

    data = reader.get_metrics_data()
    assert data is not None
    points = {
        metric.name: list(metric.data.data_points)
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }
    process = points["messaging.process.duration"]
    assert {point.attributes["messaging.destination.partition.id"] for point in process} == {"0", "1"}
    assert sum(point.count for point in process) == work.records
    assert all("error.type" not in point.attributes for point in process)
    assert sum(point.value for point in points["messaging.client.consumed.messages"]) == work.records
    [sent] = points["messaging.client.sent.messages"]
    assert (sent.attributes["messaging.destination.name"], sent.value) == (work.target, work.records)
    assert sum(point.count for point in points["messaging.client.operation.duration"]) >= 1


async def test_a_handlers_sends_continue_its_trace(workload) -> None:
    """Each send from a handler is a child of the handler's process span, and its record carries the send's context."""
    work = workload(2, 20)
    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])
    router = Router()

    @router.handler(work.source)
    async def forward(record: ConsumerRecord, producer: Annotated[Producer, Context()]) -> None:
        await producer.send(ProducerRecord(work.target, record.key(), record.value()))

    def observability():
        return [MetricsMiddleware(meter_provider=meter_provider), TracingMiddleware(tracer_provider=tracer_provider)]

    config = Config(bootstrap_servers=work.bootstrap, group_id=work.group, offset_policy=resume_or_earliest)
    async with Producer.from_config(config, middleware=observability()) as producer:
        await work.run_until_committed(
            potik.build_runtime(config, [router], context=producer, middleware=observability())
        )

    spans = exporter.get_finished_spans()
    processes = {span.context.span_id for span in spans if span.kind is SpanKind.CONSUMER}
    sends = [span for span in spans if span.name == f"send {work.target}"]
    assert len(processes) == len(sends) == work.records
    assert all(send.kind is SpanKind.PRODUCER and send.parent.span_id in processes for send in sends)
    carried = {dict(record.headers() or [])["traceparent"].decode().split("-")[2] for record in work.outputs()}
    assert carried == {f"{send.context.span_id:016x}" for send in sends}
    assert {send.attributes["messaging.destination.partition.id"] for send in sends} == {"0", "1"}

    data = reader.get_metrics_data()
    assert data is not None
    [sent] = [
        m for r in data.resource_metrics for s in r.scope_metrics for m in s.metrics if m.name.endswith("sent.messages")
    ]
    assert sum(point.value for point in sent.data.data_points) == work.records
