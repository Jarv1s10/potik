"""Commit handled records' outputs and offsets in transactions, through one transactional producer."""

import logging
import time
from collections.abc import Mapping, Sequence
from functools import partial
from types import MappingProxyType

import anyio
from confluent_kafka import KafkaError, KafkaException, Message, Producer, TopicPartition

from potik.core.config import Config
from potik.core.consumer import ConsumerThread
from potik.core.errors import PotikError
from potik.core.middleware import BaseMiddleware, _call_once
from potik.core.types import ProducerRecord, Transaction

__all__: list[str] = []

logger = logging.getLogger(__name__)

_COMMIT_ATTEMPTS = 3
# How often an idle producer serves its callbacks (errors, statistics, logs). Committing serves them otherwise.
_IDLE_POLL = 1.0
# Reported for the outputs of an aborted transaction, e.g. one repeated for a stale epoch.
_PURGED = frozenset({KafkaError._PURGE_QUEUE, KafkaError._PURGE_INFLIGHT})


class CommitterStoppedError(PotikError):
    """The run stopped, so handled records can no longer commit."""


class Committer:
    """Commits completed records of all partitions through one transactional producer.

    Each transaction atomically produces a batch's outputs and commits its inputs' next offsets with the consumer's group metadata (KIP-447). With the metadata, the coordinator rejects commits from a fenced or stale group member. The next batch accumulates while a transaction is in flight. `commit_batch_size` caps it and backpressures the handlers.

    A transaction that fails with an error that requires an abort, e.g. a stale group epoch, is aborted and repeated, up to three attempts. Any other failed transaction fails the run. Its offsets stay uncommitted, and its inputs replay on the next run.

    Each transaction runs through the `commit` stage of `middleware`.
    """

    def __init__(self, config: Config, consumer: ConsumerThread, middleware: Sequence[BaseMiddleware] = ()) -> None:
        self._middleware = middleware
        self._delivery_error: KafkaError | None = None
        self._producer = Producer(config._producer_config(self._on_failed_delivery))
        self._consumer = consumer
        self._timeout = config.transaction_timeout_ms / 1000
        self._interval = config.commit_interval_ms / 1000
        self._capacity = config.commit_batch_size
        # Producer calls block in C, so they run on a worker thread. They are joined, never abandoned, when the awaiting task is cancelled.
        self._thread = anyio.CapacityLimiter(1)
        self._scope = anyio.CancelScope()
        self._offsets: dict[tuple[str, int], int] = {}  # (topic, partition) -> next offset
        self._outputs: list[ProducerRecord] = []
        self._records = 0
        self._has_work = anyio.Event()  # the batch is non-empty
        self._commit_requested = anyio.Event()  # the batch is full, or a flush waits for it
        self._batch_taken = anyio.Event()  # the batch was taken by a transaction
        self._batch_committed = anyio.Event()  # a transaction committed
        self._batches_taken = 0  # batches taken
        self._batches_committed = 0  # transactions committed
        self._next_start = 0.0
        self._pending_since: float | None = None  # monotonic time the batch became non-empty
        self._committing_since: float | None = None
        self._stopped: BaseException | None = None
        self._in_transaction = False  # begun, and neither committed nor aborted

    async def start(self) -> None:
        await anyio.to_thread.run_sync(self._producer.init_transactions, self._timeout, limiter=self._thread)

    @property
    def full(self) -> bool:
        return self._records >= self._capacity

    async def wait_space(self) -> None:
        while self._records >= self._capacity:
            self._check()
            await self._batch_taken.wait()

    @property
    def oldest_uncommitted_at(self) -> float | None:
        """The monotonic time when the oldest completed but uncommitted record was added."""
        return self._committing_since if self._committing_since is not None else self._pending_since

    def add(self, partition: tuple[str, int], next_offset: int, outputs: Sequence[ProducerRecord]) -> None:
        if not self._offsets:
            self._pending_since = time.monotonic()
        self._offsets[partition] = next_offset
        if outputs:
            self._outputs.extend(outputs)
        self._records += 1
        self._has_work.set()
        if self._records >= self._capacity:
            self._commit_requested.set()

    async def flush(self) -> None:
        """Commit everything added so far, without waiting for the commit interval."""
        target = self._batches_taken
        if self._offsets:
            target += 1
            self._commit_requested.set()
        while self._batches_committed < target:
            self._check()
            await self._batch_committed.wait()

    def stop(self, reason: BaseException) -> None:
        """Commit nothing more, and fail current and future waiters."""
        if self._stopped is None:
            self._stopped = reason
        self._scope.cancel()
        self._batch_taken.set()
        self._batch_committed.set()

    def _check(self) -> None:
        if self._stopped is not None:
            raise CommitterStoppedError("the run stopped, so handled records stay uncommitted") from self._stopped

    async def run(self) -> None:
        with self._scope:
            while True:
                with anyio.move_on_after(_IDLE_POLL):
                    await self._has_work.wait()
                if not self._has_work.is_set():
                    await anyio.to_thread.run_sync(self._producer.poll, 0, limiter=self._thread)
                    continue
                delay = self._next_start - anyio.current_time()
                if delay > 0:
                    with anyio.move_on_after(delay):
                        await self._commit_requested.wait()
                await self._commit()

    async def _commit(self) -> None:
        offsets = [TopicPartition(topic, partition, offset) for (topic, partition), offset in self._offsets.items()]
        transaction = Transaction(
            # Read-only: commit middleware must not commit outputs without their inputs' offsets.
            offsets=MappingProxyType(self._offsets),
            outputs=self._outputs,
        )
        self._offsets, self._outputs, self._records = {}, [], 0
        self._committing_since, self._pending_since = self._pending_since, None
        self._has_work, self._commit_requested = anyio.Event(), anyio.Event()
        self._batch_taken.set()
        self._batch_taken = anyio.Event()
        self._batches_taken += 1
        self._next_start = anyio.current_time() + self._interval
        await _call_once(
            self._middleware, "commit", partial(self._commit_transaction, transaction.offsets, offsets), transaction
        )
        self._batches_committed += 1
        self._committing_since = None
        self._batch_committed.set()
        self._batch_committed = anyio.Event()

    async def _commit_transaction(
        self, committing: Mapping[tuple[str, int], int], offsets: list[TopicPartition], transaction: Transaction
    ) -> None:
        if transaction.offsets is not committing:
            raise RuntimeError("commit middleware must not change a transaction's offsets")
        outputs = transaction.outputs
        for attempt in range(1, _COMMIT_ATTEMPTS + 1):
            # Read the group metadata right before the transaction. The batch holds only owned partitions' offsets, because a revocation commits its partitions' work before it completes. The coordinator rejects a stale epoch, including one bumped by an unrelated assignment change (KIP-848) while this transaction was in preparation. The coordinator checks only the epoch, never which partitions the member owns. A repeat is safe only because a fenced member keeps its stale epoch: librdkafka rejoins with a new one only after the poller serves the lost partitions, which stops the run. During a drain, nothing polls, so the member stays fenced and the coordinator rejects every repeat (tests/integration/test_fencing.py).
            metadata: bytes = await self._consumer.call(lambda consumer: consumer.consumer_group_metadata())
            try:
                await anyio.to_thread.run_sync(self._transact, outputs, offsets, metadata, limiter=self._thread)
                break
            except KafkaException as exc:
                # An error that requires an abort, e.g. a stale epoch, left nothing committed, because _transact aborted the transaction. The abort also bumps the producer epoch when the error requires it, e.g. for the sequence gap that the outputs of an aborted attempt leave.
                if attempt == _COMMIT_ATTEMPTS or not exc.args[0].txn_requires_abort():
                    raise
                logger.info("Repeating a transaction that failed and was aborted: %s", exc)
                await anyio.sleep(0.05 * attempt)

    def _transact(self, outputs: Sequence[ProducerRecord], offsets: list[TopicPartition], metadata: bytes) -> None:
        producer = self._producer
        # A failed commit notes the first failed delivery of its own attempt, not of an attempt aborted before it.
        self._delivery_error = None
        producer.begin_transaction()
        self._in_transaction = True
        try:
            produce = producer.produce
            for topic, key, value, headers, partition in outputs:
                try:
                    produce(topic, value, key, partition, headers=headers)
                except BufferError:
                    # The client queue is full. Deliver the queued outputs first. It returns within message.timeout.ms, which librdkafka keeps at or below transaction.timeout.ms.
                    producer.flush()
                    produce(topic, value, key, partition, headers=headers)
            # Without a timeout, it waits for the rest of the transaction timeout. librdkafka 2.16.0 can still wait until a broker is up again, whatever the timeout, if its offset commit loses its connection (DESIGN.md, Bounded transactions).
            producer.send_offsets_to_transaction(offsets, metadata)
        except BaseException:
            self._abort()
            raise
        try:
            # Without a timeout, librdkafka 2.16.0 waits for the commit and the abort forever, e.g. for a coordinator that it cannot find after a broker outage, though its docs promise the rest of the transaction timeout. A timeout leaves the transaction in progress, so it fails the run.
            producer.commit_transaction(self._timeout)
        except KafkaException as exc:
            # Noted first, so that the note survives an abort that raises. Its error then chains this one.
            if self._delivery_error is not None:
                exc.add_note(f"first failed delivery: {self._delivery_error}")
            # Abort only a commit known to have failed. Leave an unknown outcome to the coordinator, and never retry or abort it.
            if exc.args[0].txn_requires_abort():
                self._abort()
            raise
        self._in_transaction = False

    def _abort(self) -> None:
        self._producer.abort_transaction(self._timeout)
        self._in_transaction = False

    def _on_failed_delivery(self, error: KafkaError, message: Message) -> None:
        if self._delivery_error is None and error.code() not in _PURGED:
            self._delivery_error = error

    async def close(self) -> None:
        await anyio.to_thread.run_sync(self._close, limiter=self._thread)

    def _close(self) -> None:
        if self._in_transaction:
            # Producer.close() would abort the transaction, or resume an abort that timed out, without a timeout (confluent-kafka 2.16.0). Destroying the client instead leaves the transaction to its coordinator, which aborts it after transaction.timeout.ms.
            del self._producer
        else:
            self._producer.close()
