"""Send records at once, outside any transaction, e.g. from an API service or from inside a handler."""

import logging
import threading
from collections.abc import AsyncGenerator, Iterable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from functools import partial
from types import MappingProxyType, TracebackType
from typing import Any, Self, cast

import anyio
import anyio.lowlevel
import confluent_kafka
from anyio.lowlevel import EventLoopToken
from confluent_kafka import KafkaError, KafkaException, Message

from potik.core.config import _PRODUCER_OWNED, Config, _client_config, _idempotent
from potik.core.middleware import BaseMiddleware, ProducerScope, _chain, _enter_lifespans, _middleware_tuple
from potik.core.types import Delivery, ProducerRecord, RecordSender

__all__ = [
    "Producer",
]

logger = logging.getLogger(__name__)

# The longest a poll for delivery reports blocks, which bounds how long closing waits for the polling thread.
_POLL_TIMEOUT = 0.1
_CLOSE_TIMEOUT = 10.0


class _Pending:
    """A send produced to librdkafka and awaiting its delivery report."""

    __slots__ = ("abandoned", "delivery", "done", "error")

    def __init__(self) -> None:
        self.done = anyio.Event()
        self.error: KafkaError | None = None
        self.delivery: Delivery | None = None
        self.abandoned = False  # its sender stopped waiting, e.g. when cancelled


class Producer:
    """A Kafka producer that sends records at once, outside any transaction.

    Use it where there is no input record to commit with, e.g. in an API service, or from a handler. Open it once with `async with`, e.g. in an API service's lifespan, or around a run, and pass it to handlers through `potik.build_runtime(context=...)`. Exiting waits up to `close_timeout` for every send to be delivered or fail, then fails the rest. On a normal exit, it then raises the first failure of a send that nobody awaited, e.g. a cancelled one. A producer opens only once.

    A send from a handler is at-least-once, unlike the handler's outputs. A retried or replayed handler sends again, and readers see the record before the input record commits.

    The producer is idempotent and waits for all in-sync replicas (`acks=all`). After a fatal error, e.g. a broken idempotent sequence, every later send fails. A callback in `options` that raises does the same, and also fails the sends that wait for a delivery report. Check `failure` in a liveness probe to restart the process then.

    Args:
        bootstrap_servers: The cluster's bootstrap servers.
        options: librdkafka options, e.g. TLS, SASL, `linger.ms`, or `message.timeout.ms`, which bounds how long a send may take. Keep the `partitioner` the same as other producers of the same topics, so that each key stays in one partition.
        middleware: Middleware instances, outermost first. A producer runs their `lifespan` and `send` stages (see `BaseMiddleware`). Add a `TopicCheckMiddleware` to check at the start that the topics exist. The producer never asks the broker to create a missing topic, unless `allow.auto.create.topics` is set back to true.
        close_timeout: How many seconds exiting waits for sends to be delivered or fail. Then it fails the rest with a `_PURGE_QUEUE` or `_PURGE_INFLIGHT` error, although a purged send that was already in flight may still be delivered. Keep it, plus the time that the rest of the shutdown takes, below the process supervisor's deadline, e.g. Kubernetes' termination grace period of 30 seconds.

    Raises:
        ValueError: If `options` override a setting that the producer owns, or `close_timeout` is negative.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        options: Mapping[str, Any] | None = None,
        middleware: Iterable[BaseMiddleware] = (),
        close_timeout: float = _CLOSE_TIMEOUT,
    ) -> None:
        if not bootstrap_servers:
            raise ValueError("bootstrap_servers must not be empty")
        if not close_timeout >= 0:
            raise ValueError(f"close_timeout must not be negative, got {close_timeout!r}")
        self._close_timeout = close_timeout
        options = dict(options or {})
        if conflicts := _PRODUCER_OWNED.intersection(options):
            raise ValueError(f"options cannot include settings that the producer manages: {sorted(conflicts)}")
        self._scope = ProducerScope(client_options=MappingProxyType(_client_config(bootstrap_servers, options)))
        self._config = _idempotent(self._scope.client_options)
        self._middleware = _middleware_tuple(middleware)
        self._client: confluent_kafka.Producer | None = None
        self._send: RecordSender | None = None
        self._open: AbstractAsyncContextManager[None] | None = None  # the open lifetime, entered by __aenter__
        self._entered = False
        self._pending: set[_Pending] = set()
        self._reported: list[_Pending] = []  # the current poll's delivery reports, appended on the polling thread
        self._room = anyio.Event()  # delivery reports freed room in librdkafka's queue
        self._drained = anyio.Event()  # no send awaits its delivery report
        self._failure: KafkaException | None = None
        self._unobserved: KafkaError | None = None  # the first failure of a send that nobody awaited

    @classmethod
    def from_config(
        cls, config: Config, *, middleware: Iterable[BaseMiddleware] = (), close_timeout: float = _CLOSE_TIMEOUT
    ) -> Self:
        """Make a producer for a runtime's cluster, with the config's `options` and `producer_options`."""
        return cls(
            config.bootstrap_servers,
            options={**config.options, **config.producer_options},
            middleware=middleware,
            close_timeout=close_timeout,
        )

    @property
    def failure(self) -> KafkaException | None:
        """The fatal error that stopped this producer, if any.

        Only a new producer, e.g. in a restarted process, can send again.
        """
        return self._failure

    async def send(self, record: ProducerRecord) -> Delivery:
        """Send `record` at once, and return where the broker appended it.

        It returns once the broker acknowledges the record. If librdkafka's queue is full, it first waits for room. A cancelled send may still be delivered.

        Raises:
            KafkaException: If the record was not delivered, or the producer has failed.
            RuntimeError: If the producer is not entered.
        """
        if self._send is None:
            raise RuntimeError("enter the producer with async with before sending")
        return await self._send(record)

    async def __aenter__(self) -> Self:
        if self._entered:
            raise RuntimeError("a producer opens only once, so make a new one to open again")
        self._entered = True
        self._open = opened = self._lifetime()
        try:
            await opened.__aenter__()
        except BaseException:
            self._open = None
            raise
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Wait up to `close_timeout` for every send to be delivered or fail, fail the rest, then close.

        On a normal exit, raise the first failure of a send that nobody awaited.

        The middleware's lifespans exit last, and see that failure or the owner's error.
        """
        assert self._open is not None
        opened, self._open = self._open, None
        await opened.__aexit__(exc_type, exc, traceback)

    @asynccontextmanager
    async def _lifetime(self) -> AsyncGenerator[None]:
        """Open the client inside the middleware's lifespans, and close it once every send has a delivery report."""
        async with _enter_lifespans(self._middleware, self._scope):
            self._client = client = confluent_kafka.Producer(self._config)
            try:
                # A thread rather than a task, so that the producer holds no task group open in its owner's task, whose cancel scopes would then have to nest around it.
                stop = threading.Event()
                poller = threading.Thread(
                    target=self._poll_reports,
                    args=(client, stop, anyio.lowlevel.current_token()),
                    name="potik-producer",
                    daemon=True,  # Only for an owner that never exits: the exit joins it.
                )
                poller.start()
                self._send = _chain(self._middleware, "send", self._deliver)
                try:
                    yield
                finally:
                    self._send = None
                    # Shielded, so that a cancelled owner still delivers what it sent, within close_timeout.
                    with anyio.CancelScope(shield=True):
                        try:
                            with anyio.move_on_after(self._close_timeout):
                                await self._wait_drained()
                            if self._pending:
                                # Purged sends get their reports from the next poll.
                                await anyio.to_thread.run_sync(client.purge)
                                await self._wait_drained()
                        finally:
                            stop.set()
                            await anyio.to_thread.run_sync(poller.join)
            finally:
                with anyio.CancelScope(shield=True):
                    await anyio.to_thread.run_sync(client.close)
                self._client = None
            if self._unobserved is not None:
                raise KafkaException(self._unobserved)

    async def _wait_drained(self) -> None:
        while self._pending:
            self._drained = anyio.Event()
            await self._drained.wait()

    async def _deliver(self, record: ProducerRecord) -> Delivery:
        if (client := self._client) is None:
            raise RuntimeError("the producer is closed")
        topic, key, value, headers, partition = record
        pending = _Pending()
        while True:
            if self._failure is not None:
                raise KafkaException(self._failure.args[0])
            if self._send is None:
                raise RuntimeError("the producer closed before sending the record")
            try:
                client.produce(
                    topic, value, key, partition, on_delivery=partial(self._report, pending), headers=headers
                )
                break
            except BufferError:
                await self._room.wait()
            except KafkaException as exc:
                if exc.args[0].fatal() and self._failure is None:
                    self._failure = exc
                raise
        self._pending.add(pending)
        try:
            await pending.done.wait()
        except BaseException:
            pending.abandoned = True
            if pending.done.is_set():
                self._observe(pending)
            raise
        if pending.error is not None:
            raise KafkaException(pending.error)
        assert pending.delivery is not None
        return pending.delivery

    def _poll_reports(self, client: confluent_kafka.Producer, stop: threading.Event, loop: EventLoopToken) -> None:
        """Serve delivery reports on the polling thread, and settle each poll's sends on the event loop at once.

        A poll raises what a callback in the options raised. No report comes after that, so the producer fails.
        """
        try:
            while not stop.is_set():
                self._reported = []
                client.poll(_POLL_TIMEOUT)
                if self._reported:
                    anyio.from_thread.run_sync(self._settle, self._reported, token=loop)
        except Exception as exc:
            logger.error("The producer stopped polling for delivery reports", exc_info=exc)
            error = KafkaError(KafkaError._FATAL, f"the producer stopped polling: {exc!r}", fatal=True)
            anyio.from_thread.run_sync(self._stop_reports, self._reported, error, token=loop)

    def _settle(self, reported: list[_Pending]) -> None:
        for pending in reported:
            self._pending.discard(pending)
            if (error := pending.error) is not None and error.fatal() and self._failure is None:
                self._failure = KafkaException(error)
            if pending.abandoned:
                self._observe(pending)
            pending.done.set()
        self._room.set()
        self._room = anyio.Event()
        if not self._pending:
            self._drained.set()

    def _stop_reports(self, reported: list[_Pending], error: KafkaError) -> None:
        """Settle the reports that came, and fail the producer and every other send that awaits one."""
        self._settle(reported)
        if self._failure is None:
            self._failure = KafkaException(error)
        for pending in self._pending:
            pending.error = error
        self._settle(list(self._pending))

    def _report(self, pending: _Pending, error: KafkaError | None, message: Message) -> None:
        """Record librdkafka's delivery report. librdkafka calls it on the polling thread."""
        if error is None:
            pending.delivery = Delivery(cast(int, message.partition()), cast(int, message.offset()))
        else:
            pending.error = error
        self._reported.append(pending)

    def _observe(self, pending: _Pending) -> None:
        """Keep the first failure of a send whose sender stopped waiting, for the exit to raise."""
        if pending.error is not None and self._unobserved is None:
            self._unobserved = pending.error
