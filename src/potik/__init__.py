"""Typed handlers for Kafka topics.

Declare each `Topic` with the types and serdes of its keys and values, register handlers on a `Router`, and build a core `Runtime` from the routers with `build_runtime()`. The package also re-exports the core names that services use.
"""

from potik.build import build_runtime
from potik.core.config import Config
from potik.core.errors import InvalidOutputError, PermanentError, PotikError
from potik.core.offsets import OffsetContext, OffsetPolicy, resume_or_earliest, resume_or_fail, resume_or_latest
from potik.core.producer import Producer
from potik.core.replies import PendingReply, ReplyListener
from potik.core.runtime import Runtime
from potik.core.types import (
    ConsumerRecord,
    Delivery,
    ProducerRecord,
    RuntimeStatus,
)
from potik.handler import Context, DecodeError, HandlerResult, Header, Key
from potik.middleware.topics import TopicCheckError
from potik.router import RegistrationError, Router
from potik.topic import EncodeError, OutputTopic, Topic

__all__ = [
    "Config",
    "ConsumerRecord",
    "Context",
    "DecodeError",
    "Delivery",
    "EncodeError",
    "HandlerResult",
    "Header",
    "InvalidOutputError",
    "Key",
    "OffsetContext",
    "OffsetPolicy",
    "OutputTopic",
    "PendingReply",
    "PermanentError",
    "PotikError",
    "Producer",
    "ProducerRecord",
    "RegistrationError",
    "ReplyListener",
    "Router",
    "Runtime",
    "RuntimeStatus",
    "Topic",
    "TopicCheckError",
    "build_runtime",
    "resume_or_earliest",
    "resume_or_fail",
    "resume_or_latest",
]
