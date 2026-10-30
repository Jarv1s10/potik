"""The runtime, which consumes records, calls a handler for each, and commits the results in transactions."""

import logging
import signal
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Generator, Iterable, Mapping
from contextlib import contextmanager
from functools import partial
from types import MappingProxyType
from typing import Any, cast

import anyio
from anyio.abc import TaskGroup, TaskStatus
from confluent_kafka import Consumer, KafkaException, Message, TopicPartition

from potik.core import health
from potik.core.committer import Committer
from potik.core.config import Config
from potik.core.consumer import ConsumerThread, _watermarks
from potik.core.errors import PartitionsLostError
from potik.core.middleware import (
    BaseMiddleware,
    RunScope,
    _chain,
    _checked,
    _enter_lifespans,
    _middleware_tuple,
)
from potik.core.offsets import OffsetContext, _resolve_offset
from potik.core.partitions import Fetch, Partition, Partitions
from potik.core.topics import _validate_topics
from potik.core.types import RecordHandler, RuntimeStatus

__all__ = [
    "Runtime",
]

logger = logging.getLogger(__name__)

# The longest a queued consumer call waits for a poll to return.
_POLL_SLICE = 0.01
# Whether a run handles signals. Only the receiver installed last gets them, so a second one would leave the first run running.
_handling_signals = False


class Runtime:
    """Consumes topics, calls a handler for each record, and commits its outputs with the record's offset.

    Each transaction produces a batch of outputs and commits their input records' offsets atomically, so each record's outputs appear exactly once to `read_committed` readers. A run uses one consumer, with the `consumer` group protocol of Kafka 4.0+, and one transactional producer. Each assigned partition handles its records one at a time, in offset order, unless its topic runs by key (see `key_concurrency`), and partitions run concurrently. The consumer keeps polling while handlers run, so a slow handler does not make it leave the group.

    On a revocation or shutdown, the runtime lets in-flight handlers finish within a grace period, then cancels them, and commits the handled records before it gives up the partitions. A failure of one partition stops it (see `Config.stop_failed_partitions`). Any other failure, e.g. a failed transaction or lost partitions, stops the run without committing more, and the uncommitted records run again on the next run.

    Cancelling `run()` requests a graceful shutdown and waits for it. If the shutdown fails, `run()` raises its error instead of the cancellation. The runtime never abandons a blocking client call, so the process supervisor must enforce a hard deadline.

    A runtime runs once. Make a new one, with new middleware, for another run. `potik.build_runtime()` builds one from routers of typed handlers.

    Args:
        config: The runtime limits and client options.
        topics: The names of the topics to subscribe to.
        handler: Returns the outputs of one record of any of `topics`, as a list or tuple of `ProducerRecord`s. Any other result raises `InvalidOutputError` from the handler call, so middleware handles it like any other handler exception.
        middleware: Middleware instances, outermost first (see `BaseMiddleware`). Put a `TopicCheckMiddleware` first to check that the topics exist before the run starts, and a `ConcurrencyMiddleware` last to limit concurrent handler calls.
        key_concurrency: The topics whose records run concurrently by key, each with the maximum number of its keys whose records run at once, across its partitions. While keys wait for their turn, a key with more records takes turns with them, so that every partition makes progress. Records with the same key run one at a time, in offset order, and records with other keys run concurrently. All records with a null key count as one key. A key is in progress while its records run, including their retries' backoff, so the limit is not a pool of handler calls: limit those with a `ConcurrencyMiddleware`. A record that finishes before an earlier record of its partition waits for it, with its outputs, so that the committed offset never skips an unfinished record. Records count toward `Config.max_buffered_bytes` until they go to a transaction, so a slow record pauses its partition once the finished records behind it fill the partition's share. A handler failure stops the partition at the lowest failed record, cancels the running records above it, and lets the records below it run, unless `Config.stop_failed_partitions` is off, in which case it cancels them all and fails the run. The records of other topics run one at a time per partition.

    Raises:
        ValueError: If `topics` is empty, a topic name is invalid, or `key_concurrency` names a topic outside `topics` or a limit that is not a positive integer.
        TypeError: If `topics` is a string, or a middleware is not a `BaseMiddleware` instance.
    """

    def __init__(
        self,
        config: Config,
        topics: Iterable[str],
        handler: RecordHandler,
        *,
        middleware: Iterable[BaseMiddleware] = (),
        key_concurrency: Mapping[str, int] | None = None,
    ) -> None:
        self._config = config
        self._topics = tuple(dict.fromkeys(_validate_topics(topics)))
        if not self._topics:
            raise ValueError("a runtime needs at least one topic")
        key_concurrency = dict(key_concurrency or {})
        for topic, limit in key_concurrency.items():
            if topic not in self._topics:
                raise ValueError(f"key_concurrency names topic {topic!r}, which is not among the runtime's topics")
            if type(limit) is not int or limit < 1:
                raise ValueError(f"the key concurrency of topic {topic!r} must be a positive integer, got {limit!r}")
        self._middleware = _middleware_tuple(middleware)
        self._handler = cast(RecordHandler, _chain(self._middleware, "handle", _checked(handler)))
        self._started = False
        self._rebalance_timeout = config.rebalance_timeout_ms / 1000
        self._consumer: ConsumerThread | None = None
        self._committer: Committer | None = None
        self._tasks: TaskGroup | None = None
        self._partitions = Partitions(
            config.max_buffered_bytes,
            stop_failed_partitions=config.stop_failed_partitions,
            key_concurrency=key_concurrency,
        )
        self._finishing: set[Partition] = set()  # revoked or draining partitions whose handlers may still run
        self._stopping = False  # no new work: stop polling and drain
        self._clients_closing = False  # clients are closing: revocations need no draining
        self._stop_requested = anyio.Event()
        self._poll_exited = anyio.Event()
        self._ready = anyio.Event()
        self._stopped = anyio.Event()
        self._failure: Exception | None = None
        self._polled_at: float | None = None

    def status(self) -> RuntimeStatus:
        """Return a snapshot of the run.

        Any thread may call it, e.g. a metrics exporter's. Off the event loop, the fields may come from slightly different instants.
        """
        committer = self._committer
        return RuntimeStatus(
            running=self._ready.is_set() and not self._stopping and not self._stopped.is_set(),
            polled_at=self._polled_at,
            oldest_uncommitted_at=None if committer is None else committer.oldest_uncommitted_at,
            failure=self._failure,
            partitions=self._partitions.status(),
        )

    def liveness(self, *, slack: float = 30.0) -> str | None:
        """Check for failures that a restart fixes, with `potik.core.health.liveness`.

        Serve it as the liveness probe. Any thread may call it.

        Args:
            slack: Seconds on top of each limit.

        Returns:
            Why the probe fails, or None if it passes.
        """
        return health.liveness(self.status(), self._config, slack=slack)

    def readiness(self) -> str | None:
        """Check that the run is running, with `potik.core.health.readiness`.

        Serve it as the readiness probe. Any thread may call it.

        Returns:
            Why the probe fails, or None if it passes.
        """
        return health.readiness(self.status())

    async def run(
        self, *, task_status: TaskStatus[None] = anyio.TASK_STATUS_IGNORED, handle_signals: bool = True
    ) -> None:
        """Run until stopped or failed, and raise the run's failure, if any.

        Cancelling it shuts the run down gracefully.

        Args:
            task_status: Lets `await task_group.start(runtime.run)` return once the run is ready.
            handle_signals: Whether the first SIGTERM or SIGINT shuts the run down gracefully, after which `run()` returns. Later signals are ignored, so a stuck shutdown needs SIGKILL, which Kubernetes sends after the termination grace period. It requires the main thread on Unix, and only one run in the process may handle signals at a time. Turn it off in tests, to handle signals yourself, or to run several runtimes at once, which you then cancel on a signal.

        Raises:
            RuntimeError: If the runtime has already run, or it handles signals outside the main thread or while another run in the process handles them.
        """
        if self._started:
            raise RuntimeError("a runtime runs only once, so make a new one for another run")
        # Checked before installing a receiver, which would replace the other run's, and before this run counts as started.
        if handle_signals and _handling_signals:
            raise RuntimeError(
                "another run already handles SIGTERM and SIGINT, and only one can receive them; "
                "pass handle_signals=False to each run and cancel them on a signal yourself"
            )
        if handle_signals:
            await _run_until_signal(self._run, task_status)
        else:
            await self._run(task_status)

    async def _run(self, task_status: TaskStatus[None]) -> None:
        # Set here, after the signal receiver is installed, so a run that fails to install one can run again without it.
        self._started = True
        async with _enter_lifespans(
            self._middleware,
            RunScope(
                config=self._config,
                status=self.status,
                client_options=MappingProxyType(self._config.client_config()),
            ),
        ):
            try:
                async with anyio.create_task_group() as tasks:
                    tasks.start_soon(self._serve)
                    try:
                        await self._ready.wait()
                        if self._failure is None:
                            task_status.started()
                            logger.info("Runtime started for consumer group %s", self._config.group_id)
                        await self._stopped.wait()
                    finally:
                        self._stop_requested.set()
            finally:
                # This also replaces the cancellation that requested the stop, so a failed shutdown, such as a failed final commit, does not look clean.
                if self._failure is not None:
                    raise self._failure from None

    def _fail(self, error: Exception) -> None:
        if self._failure is None:
            self._failure = error
            logger.error("Runtime failed", exc_info=error)
        self._stopping = True
        self._stop_requested.set()
        if self._committer is not None:
            self._committer.stop(error)
        for partition in [*self._partitions.assigned(), *self._finishing]:
            partition.cancel()

    def _spawn(self, function: Callable[[], Awaitable[None]]) -> None:
        async def guarded() -> None:
            try:
                await function()
            except Exception as error:  # noqa: BLE001 -- run() raises the first failure
                self._fail(error)

        assert self._tasks is not None
        self._tasks.start_soon(guarded)

    async def _serve(self) -> None:
        # External cancellation only requests a stop. Interrupting a Kafka call or the graceful drain halfway is unsafe.
        with anyio.CancelScope(shield=True):
            try:
                self._consumer = consumer = ConsumerThread(self._config._consumer_config())
                self._committer = committer = Committer(self._config, consumer, self._middleware)
                await committer.start()
                await consumer.call(
                    lambda client: client.subscribe(
                        list(self._topics),
                        on_assign=self._failing_on_error(self._on_assign),
                        on_revoke=self._failing_on_error(self._on_revoke),
                        on_lost=self._failing_on_error(self._on_lost),
                    )
                )
                async with anyio.create_task_group() as self._tasks:
                    # Counts as a poll, so that liveness also fails if the first poll never returns.
                    self._polled_at = time.monotonic()
                    self._spawn(self._poll)
                    self._spawn(committer.run)
                    self._ready.set()
                    await self._stop_requested.wait()
                    try:
                        await self._drain()
                    except Exception as error:  # noqa: BLE001 -- not raised here, or the task group wraps it
                        self._fail(error)
                    self._tasks.cancel_scope.cancel()
            except Exception as error:  # noqa: BLE001 -- run() raises the first failure
                self._fail(error)
            finally:
                await self._close()
                self._ready.set()
                self._stopped.set()

    async def _drain(self) -> None:
        self._stopping = True
        await self._poll_exited.wait()
        if self._failure is not None:
            return
        grace = self._config.shutdown_grace_ms / 1000
        # The budget that Config keeps below max_poll_interval_ms, after which the group would fence the final commit.
        deadline = time.monotonic() + grace + self._rebalance_timeout
        await self._finish(self._partitions.assigned(), grace)
        assert self._committer is not None
        with _fail_after(deadline - time.monotonic(), "shutdown exceeded shutdown_grace_ms + rebalance_timeout_ms"):
            await self._committer.flush()

    async def _finish(self, partitions: list[Partition], grace: float) -> None:
        """Let in-flight handlers finish within `grace`, then cancel the rest.

        A revocation removes its partitions from the assigned ones first, so a run failure meanwhile finds them in `_finishing`.
        """
        self._finishing.update(partitions)
        try:
            for partition in partitions:
                partition.finish()
            with anyio.move_on_after(grace):
                for partition in partitions:
                    await partition.done.wait()
            for partition in partitions:
                partition.cancel()
            # Only a handler that ignores cancellation can take longer.
            with _fail_after(
                self._rebalance_timeout,
                "cancelled handlers did not stop within rebalance_timeout_ms, e.g. because they block the event loop",
            ):
                for partition in partitions:
                    await partition.done.wait()
        finally:
            self._finishing.difference_update(partitions)

    async def _close(self) -> None:
        self._clients_closing = True
        closers = []
        if self._committer is not None:
            closers.append(self._committer.close)
        if self._consumer is not None:
            closers.append(self._consumer.close)  # leaves the group
        for close in closers:
            try:
                await close()
            except Exception as error:  # noqa: BLE001 -- run() raises the first failure
                self._fail(error)

    async def _poll(self) -> None:
        consumer = self._consumer
        assert consumer is not None
        try:
            while not self._stopping:
                fetch = await self._partitions.next_fetch()
                if self._stopping:
                    return
                records = await consumer.call(partial(self._fetch, fetch=fetch))
                self._polled_at = time.monotonic()
                if self._stopping:
                    return
                for partition in self._partitions.buffer(records):
                    logger.error(
                        "Stopped %s[%s] after a fetch error. Fix the cause, then restart.",
                        partition.topic,
                        partition.partition,
                        exc_info=partition.failure,
                    )
        finally:
            self._poll_exited.set()

    def _fetch(self, consumer: Consumer, *, fetch: Fetch) -> list[Message]:
        if fetch.pause:
            consumer.pause(fetch.pause)
        if fetch.resume:
            consumer.resume(fetch.resume)
        # Read the fetch size before each call: a rebalance callback inside one changes it.
        partitions = self._partitions
        records = consumer.consume(partitions.fetch_size, 0)
        if not records and fetch.timeout:
            # consume() waits for a full batch. Block only for the first record, in short slices, so queued consumer calls do not wait for the poll.
            assert self._consumer is not None
            deadline = time.monotonic() + fetch.timeout
            while not records and not self._consumer.has_calls() and (remaining := deadline - time.monotonic()) > 0:
                records = consumer.consume(1, min(_POLL_SLICE, remaining))
            if records and partitions.fetch_size > 1:
                records += consumer.consume(partitions.fetch_size - 1, 0)
        return records

    async def _run_partition(self, partition: Partition) -> None:
        assert self._committer is not None
        try:
            await partition.run(self._handler, self._committer)
        except Exception as error:
            if not self._config.stop_failed_partitions or self._failure is not None:
                raise
            # Stop only this partition, at the failed record. Its offset stays uncommitted, so the record replays after a reassignment.
            logger.error(
                "Stopped %s[%s] after a handler exception that no exception rule handled",
                partition.topic,
                partition.partition,
                exc_info=error,
            )
            self._partitions.stop_failed(partition, error)

    # Rebalance callbacks run on the consumer thread, inside consume() or close().

    def _failing_on_error(
        self, callback: Callable[[Consumer, list[TopicPartition]], None]
    ) -> Callable[[Consumer, list[TopicPartition]], None]:
        """Fail the run instead of raising out of a rebalance callback.

        If a callback raises out of `consume()` after partitions were assigned, `close()` never returns (confluent-kafka 2.16.0).
        """

        def guarded(consumer: Consumer, partitions: list[TopicPartition]) -> None:
            try:
                callback(consumer, partitions)
            except Exception as error:  # noqa: BLE001 -- run() raises the first failure
                self._run_on_loop(self._fail_on_loop, error)

        return guarded

    async def _fail_on_loop(self, error: Exception) -> None:
        self._fail(error)

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        if self._stopping or not partitions:
            return  # The client assigns them itself, and no partition handles them.
        deadline = time.monotonic() + self._rebalance_timeout

        def remaining() -> float:
            if (budget := deadline - time.monotonic()) <= 0:
                raise TimeoutError("partition assignment exceeded rebalance_timeout_ms")
            return budget

        assignment = []
        policy_offsets = []
        # With read_committed, this waits for a previous owner's pending transaction to finish, instead of returning the offset it replaces.
        for position in consumer.committed(partitions, timeout=remaining()):
            if position.error is not None:
                raise KafkaException(position.error)
            topic, partition = position.topic, position.partition
            committed = position.offset if position.offset >= 0 else None
            watermarks = partial(_watermarks, consumer, TopicPartition(topic, partition), deadline)
            context = OffsetContext(topic, partition, committed, watermarks=watermarks)
            offset = _resolve_offset(self._config.offset_policy, context)
            assignment.append(TopicPartition(topic, partition, offset))
            if offset != committed:
                policy_offsets.append(assignment[-1])
        consumer.incremental_assign(assignment)
        self._run_on_loop(self._start_partitions, assignment, policy_offsets, deadline)

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        if not self._clients_closing and partitions:
            self._run_on_loop(self._revoke_partitions, partitions, False)

    def _on_lost(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        if not self._clients_closing and partitions:
            self._run_on_loop(self._revoke_partitions, partitions, True)

    def _run_on_loop[*Ts](self, function: Callable[[*Ts], Coroutine[Any, Any, None]], *args: *Ts) -> None:
        assert self._consumer is not None
        self._consumer.run_on_loop(function, *args)

    async def _start_partitions(
        self, assignment: list[TopicPartition], policy_offsets: list[TopicPartition], deadline: float
    ) -> None:
        assert self._committer is not None and self._consumer is not None
        assigned = self._partitions.assign(assignment)

        def pause_and_resume(consumer: Consumer) -> None:
            # Inside the fetch that revoked the paused ones, before it returns its batch (see Partitions.assign).
            if assigned.pause:
                consumer.pause(assigned.pause)
            if assigned.resume:
                consumer.resume(assigned.resume)

        await self._consumer.call(pause_and_resume)
        for partition in assigned.partitions:
            self._spawn(partial(self._run_partition, partition))
        for position in policy_offsets:
            self._committer.add((position.topic, position.partition), position.offset, ())
        if policy_offsets:
            # Commit the policy's offsets before handling, so a restart resumes from them.
            with _fail_after(deadline - time.monotonic(), "partition assignment exceeded rebalance_timeout_ms"):
                await self._committer.flush()
        logger.info("Assigned %s", _names(assignment))

    async def _revoke_partitions(self, positions: list[TopicPartition], lost: bool) -> None:
        partitions = self._partitions.revoke(positions)
        if lost:
            for partition in partitions:
                partition.cancel()
            self._fail(PartitionsLostError(f"lost partitions {_names(positions)}, which another member may own now"))
            return
        if self._failure is None:
            # Commit completed work before the group can reassign the partitions.
            assert self._committer is not None
            with _fail_after(self._rebalance_timeout, "partition revocation exceeded rebalance_timeout_ms"):
                await self._finish(partitions, self._config.revocation_grace_ms / 1000)
                await self._committer.flush()
        else:
            for partition in partitions:
                partition.cancel()
        logger.info("Revoked %s", _names(positions))


async def _run_until_signal(run: Callable[[TaskStatus[None]], Awaitable[None]], task_status: TaskStatus[None]) -> None:
    global _handling_signals
    _handling_signals = True
    failure = None
    try:
        # Installed before running, so an unsupported environment fails right away.
        with anyio.open_signal_receiver(signal.SIGTERM, signal.SIGINT) as signals:
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(_cancel_on_signal, signals, tasks.cancel_scope)
                try:
                    await run(task_status)
                except Exception as exc:  # noqa: BLE001 -- raised below, not wrapped in an ExceptionGroup
                    failure = exc
                tasks.cancel_scope.cancel()
    finally:
        _handling_signals = False
    if failure is not None:
        raise failure


async def _cancel_on_signal(signals: AsyncIterator[int], scope: anyio.CancelScope) -> None:
    async for signum in signals:
        logger.info("Received %s, shutting down", signal.Signals(signum).name)
        scope.cancel()
        return


@contextmanager
def _fail_after(seconds: float, message: str) -> Generator[None]:
    """Like `anyio.fail_after`, with a `TimeoutError` that says what timed out."""
    with anyio.move_on_after(seconds) as scope:
        yield
    if scope.cancelled_caught:
        raise TimeoutError(message)


def _names(partitions: Iterable[TopicPartition]) -> str:
    return ", ".join(f"{tp.topic}[{tp.partition}]" for tp in partitions)
