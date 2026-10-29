"""Trace records with OpenTelemetry, from their handling to the transaction that commits their outputs, and sends through a `Producer`.

Needs the `otel` extra.
"""

import random
import time
from collections.abc import AsyncGenerator, Awaitable, Callable, Collection, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import ClassVar, cast

import anyio
from opentelemetry import context, propagate, trace
from opentelemetry.context import Context
from opentelemetry.propagators.textmap import Getter
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_BATCH_MESSAGE_COUNT,
    MESSAGING_CLIENT_ID,
    MESSAGING_CONSUMER_GROUP_NAME,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_DESTINATION_PARTITION_ID,
    MESSAGING_KAFKA_MESSAGE_KEY,
    MESSAGING_KAFKA_MESSAGE_TOMBSTONE,
    MESSAGING_KAFKA_OFFSET,
    MESSAGING_MESSAGE_BODY_SIZE,
    MessagingOperationTypeValues,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.trace import Link, Span, SpanKind, StatusCode, TracerProvider

from potik.core.middleware import BaseMiddleware, ProducerScope, RunScope
from potik.core.types import (
    ConsumerRecord,
    Delivery,
    Headers,
    ProducerRecord,
    RecordHandler,
    RecordSender,
    Transaction,
)
from potik.middleware.semconv import _SEND, _SETTLE, _Attributes, _destination, _error_type, _operation

__all__ = [
    "TracingMiddleware",
]

_PROCESS = _operation("process", MessagingOperationTypeValues.PROCESS)
_CREATE = _operation("create", MessagingOperationTypeValues.CREATE)


def _utf8(key: bytes) -> str | None:
    try:
        return key.decode()
    except UnicodeDecodeError:  # The semantic conventions omit keys without a canonical text form.
        return None


class TracingMiddleware(BaseMiddleware):
    """Traces each record's handling, its outputs to the transaction that commits them, and each `Producer` send.

    It creates these spans:

    - `process {topic}` (consumer), one per handler call. It continues the trace in the record's headers. A record that runs again, e.g. after a crash, gets a second `process` span in the same trace.
    - `create {topic}` (producer), one per output, a child of its record's `process` span. The output's headers carry its context. It ends at once, because backends count producer spans as dependency latency, and the transaction's span covers the produce.
    - `send {topic}` (client), one per transaction, as the root of its own trace. A transaction without outputs is named `commit {topic}`, and the topic is left out of the name when there are several. It links to the sampled `create` span of each output, up to the SDK's link limit. These links connect a record's trace to the transaction that commits its outputs. Only a committed handling's outputs reach downstream consumers, so when a record runs again, its committed `process` span is the one whose `create` spans have a link and downstream children. A record without outputs has no link to its transaction.
    - `send {topic}` (producer), one per `Producer.send()`, a child of the current span, e.g. a handler's `process` span or an API's request span. The record's headers carry its context. It lasts until the broker acknowledges the record.

    Each span has the attributes that the OpenTelemetry messaging semantic conventions require for it, e.g. `messaging.consumer.group.name` on a run's spans. `optional_attributes` picks the optional ones. To add attributes of your own, or to rename a span, use `trace.get_current_span()` in a handler or in middleware after this one. The current span there is the `process` span, the transaction's span, or the `send` span.

    Use one instance for each runtime and each producer. Put it first in the runtime's middleware, so that its spans cover the other middleware. `potik.build_runtime()` runs the exception rules inside its middleware, so one `process` span covers a record's retries and the outputs of exception rules, such as dead letters. In a core `Runtime`, put it before `ExceptionMiddleware` for the same span. A cancellation, e.g. by a revocation, ends the `process` span with a `cancelled` event instead of an error.

    A transaction's span is the root of its own trace, so the tracer provider's sampler decides on it apart from the records in it. To sample transactions, use `transaction_sample_ratio` with a sampler that keeps root spans.

    Args:
        optional_attributes: The optional attributes to add, where they apply. The default, `TracingMiddleware.DEFAULT_OPTIONAL_ATTRIBUTES`, is `messaging.destination.partition.id`, `messaging.kafka.offset`, and `messaging.client.id` if the client's `client.id` is set. `messaging.kafka.message.key` is off by default, because keys often identify people, and so is `messaging.message.body.size`, which the conventions make opt-in.
        key_text: Returns the text of a key for `messaging.kafka.message.key`, or None to leave the key out. The default decodes UTF-8 and leaves out keys that are not valid UTF-8.
        transaction_sample_ratio: The share of transactions that get a span, from 0 to 1. A failed transaction always gets one, but if it was not picked, its span is not the current span while the transaction runs. The tracer provider's sampler still decides whether to keep each span.
        tracer_provider: Defaults to the global tracer provider.

    Raises:
        ValueError: If `optional_attributes` names an attribute that this middleware does not add, or `transaction_sample_ratio` is not between 0 and 1.
    """

    DEFAULT_OPTIONAL_ATTRIBUTES: ClassVar[frozenset[str]] = frozenset(
        {MESSAGING_DESTINATION_PARTITION_ID, MESSAGING_KAFKA_OFFSET, MESSAGING_CLIENT_ID}
    )
    """The optional attributes that it adds by default."""

    def __init__(
        self,
        *,
        optional_attributes: Collection[str] = DEFAULT_OPTIONAL_ATTRIBUTES,
        key_text: Callable[[bytes], str | None] = _utf8,
        transaction_sample_ratio: float = 1.0,
        tracer_provider: TracerProvider | None = None,
    ) -> None:
        if unknown := set(optional_attributes) - _OPTIONAL:
            raise ValueError(f"optional_attributes must be among {sorted(_OPTIONAL)}, got {sorted(unknown)}")
        if not 0 <= transaction_sample_ratio <= 1:
            raise ValueError(f"transaction_sample_ratio must be between 0 and 1, got {transaction_sample_ratio!r}")
        self._tracer = trace.get_tracer("potik", tracer_provider=tracer_provider)
        self._attributes = frozenset(optional_attributes)
        self._key_text = key_text
        self._transaction_sample_ratio = transaction_sample_ratio
        # Set by the lifespan: the run's consumer group, and each client's id.
        self._group: _Attributes = {}
        self._consumer_id: _Attributes = {}
        self._producer_id: _Attributes = {}

    @asynccontextmanager
    async def lifespan(self, scope: RunScope | ProducerScope) -> AsyncGenerator[None]:
        if isinstance(scope, RunScope):
            config = scope.config
            self._group = {MESSAGING_CONSUMER_GROUP_NAME: config.group_id}
            self._consumer_id = self._client_id({**config.options, **config.consumer_options})
            self._producer_id = self._client_id({**config.client_config(), **config.producer_options})
        else:
            self._producer_id = self._client_id(scope.client_options)
        yield

    async def handle(self, record: ConsumerRecord, call_next: RecordHandler) -> Sequence[ProducerRecord]:
        topic, partition, offset = cast(str, record.topic()), cast(int, record.partition()), cast(int, record.offset())
        attributes = {
            **_PROCESS,
            MESSAGING_DESTINATION_NAME: topic,
            **self._group,
            **self._message(record.key(), record.value()),
            **self._select({MESSAGING_DESTINATION_PARTITION_ID: str(partition), MESSAGING_KAFKA_OFFSET: offset}),
            **self._consumer_id,
        }
        # Extract into an empty context, so the record's context, including its baggage, replaces the ambient one. Runtime tasks inherit whatever context was current when they started, which is unrelated to the record.
        token = context.attach(propagate.extract(record.headers() or [], Context(), getter=_HEADERS))
        try:
            with self._tracer.start_as_current_span(
                f"process {topic}",
                kind=SpanKind.CONSUMER,
                attributes=attributes,
                record_exception=False,
                set_status_on_exception=False,
            ) as span:
                try:
                    outputs = await call_next(record)
                except anyio.get_cancelled_exc_class():
                    span.add_event("cancelled")
                    raise
                except Exception as exc:
                    _set_error(span, exc)
                    raise
                if isinstance(outputs, (list, tuple)):  # The runtime's own check rejects invalid outputs.
                    outputs = [
                        self._create(output) if isinstance(output, ProducerRecord) else output for output in outputs
                    ]
        finally:
            context.detach(token)
        return outputs

    async def commit(self, transaction: Transaction, call_next: Callable[[Transaction], Awaitable[None]]) -> None:
        outputs = transaction.outputs
        operation, name = (_SEND, "send") if outputs else (_SETTLE, "commit")
        attributes = {**operation, **self._group, **self._producer_id}
        if outputs:
            attributes[MESSAGING_BATCH_MESSAGE_COUNT] = len(outputs)
        if (topic := _destination(transaction)) is not None:
            attributes[MESSAGING_DESTINATION_NAME] = topic
            name = f"{name} {topic}"

        def start(start_time: int | None = None) -> Span:
            links = _creation_links(outputs)
            return self._tracer.start_span(name, Context(), SpanKind.CLIENT, attributes, links, start_time)

        started_ns = time.time_ns()
        # random() is below 1.0, and never below 0.0, so the bounds trace every transaction or none.
        span = start() if random.random() < self._transaction_sample_ratio else None
        try:
            if span is None:
                await call_next(transaction)
            else:
                with trace.use_span(span, record_exception=False, set_status_on_exception=False):
                    await call_next(transaction)
        except Exception as exc:
            span = span or start(started_ns)  # A failed transaction is always traced.
            _set_error(span, exc)
            raise
        finally:
            if span is not None:
                span.end()

    async def send(self, record: ProducerRecord, call_next: RecordSender) -> Delivery:
        attributes = {
            **_SEND,
            MESSAGING_DESTINATION_NAME: record.topic,
            **self._message(record.key, record.value),
            **self._producer_id,
        }
        with self._tracer.start_as_current_span(
            f"send {record.topic}",
            kind=SpanKind.PRODUCER,
            attributes=attributes,
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            carrier: dict[str, str] = {}
            propagate.inject(carrier)
            try:
                delivery = await call_next(_with_headers(record, carrier) if carrier else record)
            except anyio.get_cancelled_exc_class():
                span.add_event("cancelled")
                raise
            except Exception as exc:
                _set_error(span, exc)
                raise
            span.set_attributes(
                self._select(
                    {
                        MESSAGING_DESTINATION_PARTITION_ID: str(delivery.partition),
                        MESSAGING_KAFKA_OFFSET: delivery.offset,
                    }
                )
            )
        return delivery

    def _create(self, output: ProducerRecord) -> ProducerRecord:
        attributes = {**_CREATE, MESSAGING_DESTINATION_NAME: output.topic, **self._message(output.key, output.value)}
        if output.partition >= 0:  # Otherwise the partitioner picks it later.
            attributes |= self._select({MESSAGING_DESTINATION_PARTITION_ID: str(output.partition)})
        span = self._tracer.start_span(f"create {output.topic}", kind=SpanKind.PRODUCER, attributes=attributes)
        carrier: dict[str, str] = {}
        propagate.inject(carrier, trace.set_span_in_context(span))
        span.end()
        return _with_headers(output, carrier) if carrier else output

    def _select(self, optional: _Attributes) -> _Attributes:
        return {name: value for name, value in optional.items() if name in self._attributes}

    def _client_id(self, options: Mapping[str, object]) -> _Attributes:
        # librdkafka's default client.id, "rdkafka", identifies nothing.
        client_id = options.get("client.id")
        return self._select({MESSAGING_CLIENT_ID: str(client_id)}) if client_id else {}

    def _message(self, key: bytes | None, value: bytes | None) -> _Attributes:
        attributes: _Attributes = {MESSAGING_KAFKA_MESSAGE_TOMBSTONE: True} if value is None else {}
        if MESSAGING_MESSAGE_BODY_SIZE in self._attributes:
            attributes[MESSAGING_MESSAGE_BODY_SIZE] = len(value or b"")
        selected = MESSAGING_KAFKA_MESSAGE_KEY in self._attributes
        if selected and key is not None and (text := self._key_text(key)) is not None:
            attributes[MESSAGING_KAFKA_MESSAGE_KEY] = text
        return attributes


_OPTIONAL = TracingMiddleware.DEFAULT_OPTIONAL_ATTRIBUTES | {MESSAGING_KAFKA_MESSAGE_KEY, MESSAGING_MESSAGE_BODY_SIZE}


def _creation_links(outputs: Sequence[ProducerRecord]) -> list[Link]:
    links = []
    for output in outputs:
        context = trace.get_current_span(propagate.extract(output.headers or [], Context(), getter=_HEADERS))
        creation = context.get_span_context()
        if creation.trace_flags.sampled:  # An unsampled create span was never exported.
            links.append(Link(creation))
    return links


def _set_error(span: Span, exc: Exception) -> None:
    span.record_exception(exc)
    span.set_status(StatusCode.ERROR, str(exc))
    span.set_attribute(ERROR_TYPE, _error_type(exc))


class _HeaderGetter(Getter[Headers]):
    def get(self, carrier: Headers, key: str) -> list[str] | None:
        items = carrier.items() if isinstance(carrier, dict) else carrier
        values = [_text(value) for name, value in items if name == key and value is not None]
        return values or None

    def keys(self, carrier: Headers) -> list[str]:
        return list(carrier) if isinstance(carrier, dict) else [name for name, _ in carrier]


_HEADERS = _HeaderGetter()


def _text(value: str | bytes) -> str:
    return value if isinstance(value, str) else value.decode("utf-8", errors="replace")


def _with_headers(output: ProducerRecord, carrier: dict[str, str]) -> ProducerRecord:
    headers = output.headers
    if isinstance(headers, dict):
        return output._replace(headers={**headers, **carrier})
    # Keep other headers as they are, including duplicate names and null values.
    kept = [(name, value) for name, value in headers or () if name not in carrier]
    return output._replace(headers=[*kept, *carrier.items()])
