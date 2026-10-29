"""The messaging semantic conventions' attributes that tracing and metrics share.

Requires the `otel` extra.
"""

from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_OPERATION_NAME,
    MESSAGING_OPERATION_TYPE,
    MESSAGING_SYSTEM,
    MessagingOperationTypeValues,
    MessagingSystemValues,
)
from opentelemetry.util.types import AttributeValue

from potik.core.types import Transaction

__all__: list[str] = []

type _Attributes = dict[str, AttributeValue]

_KAFKA: _Attributes = {MESSAGING_SYSTEM: MessagingSystemValues.KAFKA.value}


def _operation(name: str, kind: MessagingOperationTypeValues) -> _Attributes:
    return {**_KAFKA, MESSAGING_OPERATION_NAME: name, MESSAGING_OPERATION_TYPE: kind.value}


# A transaction with outputs sends them. One without outputs only commits its offsets.
_SEND = _operation("send", MessagingOperationTypeValues.SEND)
_SETTLE = _operation("commit", MessagingOperationTypeValues.SETTLE)


def _error_type(exc: BaseException) -> str:
    return f"{type(exc).__module__}.{type(exc).__qualname__}"


def _destination(transaction: Transaction) -> str | None:
    """Return the one topic of a transaction's outputs, or of its offsets if it has none, or None if there are several."""
    outputs = transaction.outputs
    topics = {output.topic for output in outputs} if outputs else {topic for topic, _ in transaction.offsets}
    return next(iter(topics)) if len(topics) == 1 else None
