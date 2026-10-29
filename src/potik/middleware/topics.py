"""Check at startup that topics exist, so that a misspelled topic fails the start instead of idling."""

import concurrent.futures
from collections.abc import AsyncGenerator, Iterable, Sequence
from contextlib import asynccontextmanager
from functools import partial
from typing import Any, Protocol, cast, runtime_checkable

import anyio
import anyio.from_thread
from confluent_kafka import KafkaError, KafkaException, TopicCollection
from confluent_kafka.admin import AdminClient

from potik.core.errors import PotikError
from potik.core.middleware import BaseMiddleware, ProducerScope, RunScope
from potik.core.topics import _validate_topics

__all__ = [
    "DeclaresOutputs",
    "NamedTopic",
    "TopicCheckError",
    "TopicCheckMiddleware",
]

# How often the check looks for its task's cancellation while it waits for the broker, in seconds.
_CANCEL_CHECK = 0.1


@runtime_checkable
class NamedTopic(Protocol):
    """Any object with a topic's `name`, e.g. a `Topic`. APIs that take topics take one in place of a name."""

    @property
    def name(self) -> str:
        """The topic's name."""
        ...


def _topic_names(topics: Iterable[NamedTopic | str]) -> tuple[str, ...]:
    if isinstance(topics, (str, NamedTopic)):
        raise TypeError(f"pass a collection of topics, e.g. a tuple, not {topics!r}")
    return _validate_topics(topic if isinstance(topic, str) else topic.name for topic in topics)


@runtime_checkable
class DeclaresOutputs(Protocol):
    """A middleware or exception rule with `output_topics`, the topics that it writes records to.

    `dead_letter()` rules declare their dead letter topic this way, for example. `potik.build_runtime()` adds the declared topics of its middleware and exception rules to its topic check. They do not restrict where records go.
    """

    @property
    def output_topics(self) -> Sequence[str]:
        """The names of the topics that it writes records to."""
        ...


def _declared_topics(item: object) -> tuple[str, ...]:
    """Return the valid topic names that `item` declares, or none if it declares nothing."""
    return _validate_topics(item.output_topics) if isinstance(item, DeclaresOutputs) else ()


class TopicCheckError(PotikError):
    """Raised by `TopicCheckMiddleware` when topics are missing, or the client is not authorized to describe them.

    Attributes:
        missing: The missing topics.
        unauthorized: The topics that the client is not authorized to describe.
    """

    def __init__(self, missing: tuple[str, ...], unauthorized: tuple[str, ...]) -> None:
        reasons = [
            f"{label} {', '.join(topics)}"
            for label, topics in (("missing topics", missing), ("not authorized to describe", unauthorized))
            if topics
        ]
        super().__init__(f"topic check failed: {'; '.join(reasons)}")
        self.missing = missing
        self.unauthorized = unauthorized


class TopicCheckMiddleware(BaseMiddleware):
    """Checks that topics exist before a run or a `Producer` starts, and fails the start otherwise.

    Put it first, so that a missing topic fails the start before other lifespans open resources. `potik.build_runtime()` puts one first, with its handled topics and every output topic that its handlers, routers, middleware, and exception rules declare, unless `check_topics=False`.

    The client needs permission to describe each topic. A missing or denied topic fails the start with `TopicCheckError`, and other broker errors, e.g. a timeout, with `KafkaException`. Cancelling the start, e.g. on SIGTERM, stops the check. It never creates a missing topic.

    Args:
        topics: The topics to check, as `Topic`s or names, e.g. the topics that a run subscribes to and returns outputs to, or that a `Producer` sends to.
        timeout: How long the metadata request may take, including finding a broker, in seconds.

    Raises:
        ValueError: If a topic name is invalid.
    """

    def __init__(self, topics: Iterable[NamedTopic | str], *, timeout: float = 10.0) -> None:
        self._topics = _topic_names(topics)
        self._timeout = timeout

    @asynccontextmanager
    async def lifespan(self, scope: RunScope | ProducerScope) -> AsyncGenerator[None]:
        options = {**scope.client_options, "allow.auto.create.topics": False}
        await anyio.to_thread.run_sync(partial(_check, options, sorted(set(self._topics)), self._timeout))
        yield


def _check(options: dict[str, Any], topics: list[str], timeout: float) -> None:
    """Raise `TopicCheckError` if a topic is missing, or `KafkaException` if no broker answers within `timeout`.

    It runs on a worker thread, and stops waiting once its task is cancelled.
    """
    if not topics:
        return  # describe_topics() never returns for none (confluent-kafka 2.16.0).
    client = AdminClient(options)
    try:
        futures = client.describe_topics(TopicCollection(topics), request_timeout=timeout)
        pending = set(futures.values())
        while pending:
            anyio.from_thread.check_cancelled()
            _, pending = concurrent.futures.wait(pending, _CANCEL_CHECK)
    finally:
        del client  # Destroys the client here, not on the event loop, since it has no close().
    missing: list[str] = []
    unauthorized: list[str] = []
    for topic, future in futures.items():
        if (failure := future.exception()) is None:
            continue
        error = cast(KafkaError, failure.args[0])
        if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            missing.append(topic)
        elif error.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
            unauthorized.append(topic)
        else:
            raise KafkaException(error)
    if missing or unauthorized:
        raise TopicCheckError(tuple(missing), tuple(unauthorized))
