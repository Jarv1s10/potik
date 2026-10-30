"""Await the reply to a request, in whichever process sent it, e.g. one of many API replicas."""

import logging
import threading
import time
from collections.abc import Callable, Generator, Iterable, Mapping
from contextlib import contextmanager
from types import TracebackType
from typing import Any, Self
from uuid import uuid4

import anyio
import anyio.lowlevel
from anyio.lowlevel import EventLoopToken
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition

from potik.core.config import _client_config
from potik.core.consumer import _watermarks
from potik.core.types import ConsumerRecord

__all__ = [
    "PendingReply",
    "ReplyListener",
]

logger = logging.getLogger(__name__)

# Client options that reading every partition from the end, without a group, depends on.
_LISTENER_OWNED = frozenset(
    {
        "bootstrap.servers",
        "enable.auto.commit",
        "enable.auto.offset.store",
        "enable.partition.eof",
        "isolation.level",
        "auto.offset.reset",
    }
)
_POLL_TIMEOUT = 0.1
_BATCH_SIZE = 500
# How often the listener's thread looks for new partitions of the reply topic. Their replies arrive this late at most.
_REFRESH_INTERVAL = 5.0
_METADATA_TIMEOUT = 10.0
# The thread exits only between calls, so a refresh during an outage delays the exit this long at most. The next refresh tries again.
_REFRESH_TIMEOUT = 1.0


class PendingReply:
    """The reply that a `ReplyListener.expect()` block awaits."""

    __slots__ = ("_arrived", "_failure", "_record", "correlation_id")

    def __init__(self, correlation_id: str) -> None:
        self.correlation_id = correlation_id
        self._arrived = anyio.Event()
        self._record: ConsumerRecord | None = None
        self._failure: Exception | None = None

    async def wait(self) -> ConsumerRecord:
        """Return the first reply with this correlation ID, which may have arrived before the call.

        Bound the wait with e.g. `anyio.fail_after()`.

        Raises:
            KafkaException: If the listener failed.
            RuntimeError: If the listener closed before the reply arrived.
        """
        await self._arrived.wait()
        if self._failure is not None:
            raise self._failure
        assert self._record is not None
        return self._record

    def _settle(self, record: ConsumerRecord | None, failure: Exception | None = None) -> None:
        """Keep the first reply or failure, and wake the waiter."""
        if not self._arrived.is_set():
            self._record, self._failure = record, failure
            self._arrived.set()


class ReplyListener:
    """Hands each reply from a reply topic to the request in this process that awaits it.

    Every process that sends requests, e.g. each replica of an API, reads every partition of the reply topic itself. The consumer belongs to no group and commits nothing. It reads from the end of each partition, as it is when the listener opens, so it never reads a reply to a request from an earlier process. It reads with `read_committed`, so a transactional responder's reply arrives once its request's offset commits.

    The listener does not send requests, and potik sets no convention for correlation IDs. The caller puts each request's ID wherever its responder reads it, e.g. a header or a field of the value, and the responder copies it to its reply, where `correlate` reads it.

    A reply that arrives while nothing awaits it is dropped, e.g. after its request timed out, or one to another process. A request that times out may still be handled. A responder that runs it again sends another reply, and only the first one counts. A reply whose correlation ID `correlate` cannot read is logged and dropped.

    Open it once with `async with`, e.g. in an API's lifespan, before the API takes requests. Once it opens, it reads every reply to a request that the process sends. Exiting fails the requests that still await a reply. After a fatal consumer error, every awaited and later request fails. Check `failure` in a liveness probe to restart the process then.

    Args:
        reply_topic: The topic that the responders reply to.
        bootstrap_servers: The cluster's bootstrap servers.
        correlate: Returns a reply's correlation ID, or None if it has none. It runs on the listener's thread for every record of the reply topic, so it must be thread-safe and fast.
        options: librdkafka consumer options, e.g. TLS or SASL. `group.id` defaults to `potik-reply-listener`. The consumer never joins the group or commits its offsets, but the client requires one, and its ACL may need to allow it.

    Raises:
        ValueError: If `options` override a setting that the listener owns.
    """

    def __init__(
        self,
        reply_topic: str,
        *,
        bootstrap_servers: str,
        correlate: Callable[[ConsumerRecord], str | None],
        options: Mapping[str, Any] | None = None,
    ) -> None:
        options = dict(options or {})
        if conflicts := _LISTENER_OWNED.intersection(options):
            raise ValueError(f"options cannot include settings that the reply listener manages: {sorted(conflicts)}")
        self._topic = reply_topic
        self._config = {
            "group.id": "potik-reply-listener",
            **_client_config(bootstrap_servers, options),
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "enable.partition.eof": False,
            "isolation.level": "read_committed",
            "auto.offset.reset": "latest",
        }
        self._correlate = correlate
        self._waiting: dict[str, PendingReply] = {}
        self._failure: KafkaException | None = None
        self._entered = False
        self._open = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def failure(self) -> KafkaException | None:
        """The error that stopped reading replies, if any. Check it in a liveness probe."""
        return self._failure

    @contextmanager
    def expect(self, correlation_id: str | None = None) -> Generator[PendingReply]:
        """Await the reply with `correlation_id` from entering the block until leaving it.

        Enter the block before sending the request, so that a fast reply cannot arrive first. Send `PendingReply.correlation_id` with the request in any way the responder reads it, e.g. a header, a field of its value, or an HTTP call that makes the responder reply to Kafka.

        Args:
            correlation_id: The request's ID, unique among this process's requests. Defaults to a new UUID.

        Raises:
            RuntimeError: If the listener is not open.
            KafkaException: If the listener failed.
            ValueError: If another block in this process awaits the same ID.
        """
        if not self._open:
            raise RuntimeError("the reply listener is not open, so enter it with async with first")
        if self._failure is not None:
            raise KafkaException(self._failure.args[0])
        correlation_id = uuid4().hex if correlation_id is None else correlation_id
        if correlation_id in self._waiting:
            raise ValueError(f"a reply with correlation ID {correlation_id!r} is already awaited")
        self._waiting[correlation_id] = pending = PendingReply(correlation_id)
        try:
            yield pending
        finally:
            del self._waiting[correlation_id]

    async def __aenter__(self) -> Self:
        if self._entered:
            raise RuntimeError("a reply listener opens only once, so make a new one to open again")
        self._entered = True
        consumer = Consumer(self._config)
        try:
            await anyio.to_thread.run_sync(self._assign_at_end, consumer)
        except BaseException:
            with anyio.CancelScope(shield=True):
                await anyio.to_thread.run_sync(consumer.close)
            raise
        self._thread = threading.Thread(
            target=self._read,
            args=(consumer, anyio.lowlevel.current_token()),
            name="potik-reply-listener",
            daemon=True,  # Only for an owner that never exits: the exit joins it.
        )
        self._thread.start()
        self._open = True
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Stop reading replies, close the consumer, and fail the requests that still await a reply."""
        assert self._thread is not None
        self._open = False
        self._stop.set()
        with anyio.CancelScope(shield=True):
            await anyio.to_thread.run_sync(self._thread.join)
        for pending in self._waiting.values():
            pending._settle(None, RuntimeError("the reply listener closed before the reply arrived"))

    def _assign_at_end(self, consumer: Consumer) -> None:
        """Assign every partition of the reply topic at its end offset, which a reply to a later request follows."""
        assigned = []
        for partition in self._partitions(consumer, _METADATA_TIMEOUT):
            deadline = time.monotonic() + _METADATA_TIMEOUT
            _, end = _watermarks(consumer, TopicPartition(self._topic, partition), deadline)
            assigned.append(TopicPartition(self._topic, partition, end))
        consumer.assign(assigned)

    def _partitions(self, consumer: Consumer, timeout: float) -> set[int]:
        metadata = consumer.list_topics(self._topic, timeout=timeout).topics[self._topic]
        if metadata.error is not None:
            raise KafkaException(metadata.error)
        return set(metadata.partitions)

    def _read(self, consumer: Consumer, loop: EventLoopToken) -> None:
        """Read replies on the listener's thread, and hand each poll's awaited ones to the event loop at once."""
        try:
            refreshed = time.monotonic()
            while not self._stop.is_set():
                arrived = []
                # consume() waits for a full batch, so block only for the first record.
                if records := consumer.consume(1, _POLL_TIMEOUT):
                    records += consumer.consume(_BATCH_SIZE - 1, 0)
                for record in records:
                    if (error := record.error()) is not None:
                        if error.fatal():
                            raise KafkaException(error)
                        logger.warning("Reading replies from %s: %s", self._topic, error)
                    # The event loop checks again, because a block may exit meanwhile.
                    elif (
                        correlation_id := self._correlation_id(record)
                    ) is not None and correlation_id in self._waiting:
                        arrived.append((correlation_id, record))
                if arrived:
                    anyio.from_thread.run_sync(self._settle, arrived, token=loop)
                if time.monotonic() - refreshed > _REFRESH_INTERVAL:
                    refreshed = time.monotonic()
                    try:
                        self._assign_new(consumer)
                    except KafkaException as exc:  # e.g. an outage, so the next refresh tries again
                        logger.warning("Could not look for new partitions of %s: %s", self._topic, exc)
        except Exception as exc:
            logger.error("Stopped reading replies from %s", self._topic, exc_info=exc)
            if isinstance(exc, KafkaException):
                failure = exc
            else:
                failure = KafkaException(KafkaError(KafkaError._FATAL, f"reading replies failed: {exc!r}", fatal=True))
            anyio.from_thread.run_sync(self._fail, failure, token=loop)
        finally:
            consumer.close()

    def _correlation_id(self, record: ConsumerRecord) -> str | None:
        """Return the record's correlation ID, or None if `correlate` finds none or fails."""
        try:
            return self._correlate(record)
        except Exception:
            logger.warning(
                "Dropped a reply from %s [%d] at offset %d, whose correlation ID could not be read",
                self._topic,
                record.partition(),
                record.offset(),
                exc_info=True,
            )
            return None

    def _assign_new(self, consumer: Consumer) -> None:
        """Assign partitions that the reply topic gained since the listener opened, from their start."""
        assigned = {position.partition for position in consumer.assignment()}
        if new := self._partitions(consumer, _REFRESH_TIMEOUT) - assigned:
            consumer.incremental_assign([TopicPartition(self._topic, p, OFFSET_BEGINNING) for p in sorted(new)])

    def _settle(self, arrived: Iterable[tuple[str, ConsumerRecord]]) -> None:
        for correlation_id, record in arrived:
            if (pending := self._waiting.get(correlation_id)) is not None:
                pending._settle(record)

    def _fail(self, failure: KafkaException) -> None:
        self._failure = failure
        for pending in self._waiting.values():
            pending._settle(None, KafkaException(failure.args[0]))
