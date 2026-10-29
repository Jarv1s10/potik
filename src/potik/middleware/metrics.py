"""Measure record handling, transactions, and sends through a `Producer` with OpenTelemetry metrics.

Needs the `otel` extra.
"""

import time
from collections import Counter
from collections.abc import AsyncGenerator, Awaitable, Callable, Collection, Iterable, Sequence
from contextlib import asynccontextmanager
from functools import partial
from typing import ClassVar, cast

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, MeterProvider, Observation
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_CONSUMER_GROUP_NAME,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_DESTINATION_PARTITION_ID,
    MESSAGING_OPERATION_NAME,
)
from opentelemetry.semconv._incubating.metrics.messaging_metrics import (
    MESSAGING_CLIENT_CONSUMED_MESSAGES,
    MESSAGING_CLIENT_OPERATION_DURATION,
    MESSAGING_CLIENT_SENT_MESSAGES,
    MESSAGING_PROCESS_DURATION,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.util.types import AttributeValue

from potik.core.middleware import BaseMiddleware, ProducerScope, RunScope
from potik.core.types import (
    ConsumerRecord,
    Delivery,
    ProducerRecord,
    RecordHandler,
    RecordSender,
    RuntimeStatus,
    Transaction,
)
from potik.middleware.semconv import _KAFKA, _SEND, _SETTLE, _Attributes, _destination, _error_type

__all__ = [
    "MetricsMiddleware",
]

# Bucket boundaries in seconds, as the semantic conventions advise for messaging durations.
_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10)

# The semantic conventions define no metric for it (ADR 0013).
_STOPPED_PARTITIONS = "potik.partition.stopped"

# Each metric has the attributes that its conventions list. Only operation durations, with `_SEND` and `_SETTLE`, have `messaging.operation.type`.
_PROCESS: _Attributes = {**_KAFKA, MESSAGING_OPERATION_NAME: "process"}
_SENT: _Attributes = {**_KAFKA, MESSAGING_OPERATION_NAME: "send"}


class MetricsMiddleware(BaseMiddleware):
    """Measures handler calls, transactions, and `Producer` sends with the OpenTelemetry messaging metrics.

    - `messaging.client.consumed.messages` counts the records that the run passes to its handler, per topic. Its `messaging.operation.name` is `process`, the operation that delivers the records to the application.
    - `messaging.process.duration` measures each handler call, per topic, with `error.type` if it raised.
    - `messaging.client.operation.duration` measures each transaction, including its retries after a rebalance, with `error.type` if it failed. Its operation is `send`, or `commit` if it has no outputs.
    - `messaging.client.sent.messages` counts each transaction's outputs, per topic, with `error.type` if the transaction failed.
    - For each `Producer.send()`, `messaging.client.operation.duration` measures the whole call, including any wait for queue room, and `messaging.client.sent.messages` adds one, per topic.
    - `potik.partition.stopped` counts the stopped partitions, per topic (see `Config.stop_failed_partitions`). The probes ignore them (see `potik.core.health`), so alert when it stays above zero. The semantic conventions do not define this metric.

    Each metric has the attributes that the conventions require for it, e.g. `messaging.consumer.group.name` on a run's durations. `optional_attributes` picks the optional ones. To rename a metric, drop an attribute, or change the buckets, configure a `View` in the SDK's `MeterProvider`.

    Use one instance for each runtime and each producer. A handler call's duration includes its retries, because `potik.build_runtime()` runs the exception rules inside its middleware. In a core `Runtime`, put it before `ExceptionMiddleware` for the same duration. Cancelled handler calls, transactions, and sends record no duration and no sent count, but a cancelled handler call still counts as consumed.

    Args:
        optional_attributes: The optional attributes to add, where they apply. The default is `MetricsMiddleware.DEFAULT_OPTIONAL_ATTRIBUTES`: `messaging.destination.partition.id`.
        meter_provider: Defaults to the global meter provider.

    Raises:
        ValueError: If `optional_attributes` names an attribute that this middleware does not add.
    """

    DEFAULT_OPTIONAL_ATTRIBUTES: ClassVar[frozenset[str]] = frozenset({MESSAGING_DESTINATION_PARTITION_ID})
    """The optional attributes that it adds by default."""

    def __init__(
        self,
        *,
        optional_attributes: Collection[str] = DEFAULT_OPTIONAL_ATTRIBUTES,
        meter_provider: MeterProvider | None = None,
    ) -> None:
        if unknown := set(optional_attributes) - self.DEFAULT_OPTIONAL_ATTRIBUTES:
            raise ValueError(
                f"optional_attributes must be among {sorted(self.DEFAULT_OPTIONAL_ATTRIBUTES)}, got {sorted(unknown)}"
            )
        self._partition = MESSAGING_DESTINATION_PARTITION_ID in optional_attributes
        self._group: _Attributes = {}  # the run's consumer group, set by the lifespan
        self._meter_provider = meter_provider
        self._status: Callable[[], RuntimeStatus] = RuntimeStatus  # the run's, set by the lifespan
        meter = metrics.get_meter("potik", meter_provider=meter_provider)
        self._process = meter.create_histogram(
            MESSAGING_PROCESS_DURATION,
            "s",
            "Duration of processing operation.",
            explicit_bucket_boundaries_advisory=_BUCKETS,
        )
        self._consumed = meter.create_counter(
            MESSAGING_CLIENT_CONSUMED_MESSAGES,
            "{message}",
            "Number of messages that were delivered to the application.",
        )
        self._operation = meter.create_histogram(
            MESSAGING_CLIENT_OPERATION_DURATION,
            "s",
            "Duration of messaging operation initiated by a producer or consumer client.",
            explicit_bucket_boundaries_advisory=_BUCKETS,
        )
        self._sent = meter.create_counter(
            MESSAGING_CLIENT_SENT_MESSAGES,
            "{message}",
            "Number of messages producer attempted to send to the broker.",
        )
        meter.create_observable_up_down_counter(
            _STOPPED_PARTITIONS,
            [partial(_observe_stopped, meter_provider)],
            "{partition}",
            "Number of assigned partitions that a handler failure or a fetch error stopped.",
        )

    @asynccontextmanager
    async def lifespan(self, scope: RunScope | ProducerScope) -> AsyncGenerator[None]:
        if not isinstance(scope, RunScope):
            yield
            return
        self._group = {MESSAGING_CONSUMER_GROUP_NAME: scope.config.group_id}
        self._status = scope.status
        _running[self] = None
        try:
            yield
        finally:
            del _running[self]

    async def handle(self, record: ConsumerRecord, call_next: RecordHandler) -> Sequence[ProducerRecord]:
        source = {
            MESSAGING_DESTINATION_NAME: cast(str, record.topic()),
            **self._group,
            **self._partition_of(cast(int, record.partition())),
        }
        attributes = {**_PROCESS, **source}
        self._consumed.add(1, attributes)
        start = time.perf_counter()
        try:
            outputs = await call_next(record)
        except Exception as exc:
            self._process.record(time.perf_counter() - start, {**attributes, ERROR_TYPE: _error_type(exc)})
            raise
        self._process.record(time.perf_counter() - start, attributes)
        return outputs

    async def commit(self, transaction: Transaction, call_next: Callable[[Transaction], Awaitable[None]]) -> None:
        outputs = transaction.outputs
        operation = {**(_SEND if outputs else _SETTLE), **self._group}
        if (topic := _destination(transaction)) is not None:
            operation[MESSAGING_DESTINATION_NAME] = topic
        start = time.perf_counter()
        try:
            await call_next(transaction)
        except Exception as exc:
            self._committed(transaction, start, operation, {ERROR_TYPE: _error_type(exc)})
            raise
        self._committed(transaction, start, operation, {})

    async def send(self, record: ProducerRecord, call_next: RecordSender) -> Delivery:
        destination: _Attributes = {MESSAGING_DESTINATION_NAME: record.topic}
        start = time.perf_counter()
        try:
            delivery = await call_next(record)
        except Exception as exc:
            self._sent_record(start, destination, {ERROR_TYPE: _error_type(exc)})
            raise
        self._sent_record(start, destination, self._partition_of(delivery.partition))
        return delivery

    def _partition_of(self, partition: int) -> _Attributes:
        return {MESSAGING_DESTINATION_PARTITION_ID: str(partition)} if self._partition else {}

    def _sent_record(self, start: float, destination: _Attributes, outcome: _Attributes) -> None:
        self._operation.record(time.perf_counter() - start, {**_SEND, **destination, **outcome})
        self._sent.add(1, {**_SENT, **destination, **outcome})

    def _committed(self, transaction: Transaction, start: float, operation: _Attributes, error: _Attributes) -> None:
        self._operation.record(time.perf_counter() - start, {**operation, **error})
        for topic, count in Counter(output.topic for output in transaction.outputs).items():
            self._sent.add(count, {**_SENT, MESSAGING_DESTINATION_NAME: topic, **error})


# The instances in a run's lifespan. The SDK keeps the callbacks of an instrument's first registration only, so each meter provider's callback observes the runs of every instance that uses the provider.
_running: dict[MetricsMiddleware, None] = {}


def _observe_stopped(meter_provider: MeterProvider | None, options: CallbackOptions) -> Iterable[Observation]:
    """Count the stopped partitions of each run whose middleware uses `meter_provider`. Runs on the exporter's thread."""
    counts: Counter[tuple[tuple[str, AttributeValue], ...]] = Counter()
    for middleware in _running.copy():  # dict.copy() is atomic, while the loop changes the dict.
        if middleware._meter_provider is not meter_provider:
            continue
        for partition in middleware._status().partitions:
            attributes = {
                **_KAFKA,
                **middleware._group,
                MESSAGING_DESTINATION_NAME: partition.topic,
                **middleware._partition_of(partition.partition),
            }
            counts[tuple(attributes.items())] += partition.failure is not None
    return [Observation(count, dict(attributes)) for attributes, count in counts.items()]
