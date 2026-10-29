"""The core runtime, a minimal wrapper around `confluent-kafka`.

It works without the typed layer: a handler takes a raw `ConsumerRecord` and returns a list or tuple of `ProducerRecord`s.
"""

from potik.core.config import Config
from potik.core.errors import InvalidOffsetError, InvalidOutputError, PartitionsLostError, PermanentError, PotikError
from potik.core.middleware import BaseMiddleware, ProducerScope, RunScope
from potik.core.offsets import (
    OffsetContext,
    OffsetPolicy,
    resume_or_earliest,
    resume_or_fail,
    resume_or_latest,
)
from potik.core.producer import Producer
from potik.core.replies import PendingReply, ReplyListener
from potik.core.runtime import Runtime
from potik.core.types import (
    ConsumerRecord,
    Delivery,
    Headers,
    PartitionStatus,
    ProducerRecord,
    RecordHandler,
    RecordSender,
    RuntimeStatus,
    Transaction,
)

__all__ = [
    "BaseMiddleware",
    "Config",
    "ConsumerRecord",
    "Delivery",
    "Headers",
    "InvalidOffsetError",
    "InvalidOutputError",
    "OffsetContext",
    "OffsetPolicy",
    "PartitionStatus",
    "PartitionsLostError",
    "PendingReply",
    "PermanentError",
    "PotikError",
    "Producer",
    "ProducerRecord",
    "ProducerScope",
    "RecordHandler",
    "RecordSender",
    "ReplyListener",
    "RunScope",
    "Runtime",
    "RuntimeStatus",
    "Transaction",
    "resume_or_earliest",
    "resume_or_fail",
    "resume_or_latest",
]
