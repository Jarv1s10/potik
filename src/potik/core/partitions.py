"""The assigned partitions, their record buffers, the order their records run in, and the flow control that keeps each buffer within its share."""

import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import anyio
from confluent_kafka import KafkaException, Message, TopicPartition

from potik.core.middleware import _check_outputs
from potik.core.types import ConsumerRecord, PartitionStatus, ProducerRecord, RecordHandler

if TYPE_CHECKING:
    from potik.core.committer import Committer

__all__: list[str] = []

# The longest a fetch blocks for its first record, and the longest the poller waits before a fetch: for a resume while every partition is paused, then for full buffers to drain before pausing their partitions. So the poller fetches at least every 2 * POLL_TIMEOUT. It also bounds how long the poller takes to notice a shutdown, and to resume a paused partition while the others are idle.
POLL_TIMEOUT = 0.1
# The smallest share of max_buffered_bytes: one record batch of the Java producer's default size.
_MIN_SHARE = 16 * 1024
# The memory of a consumed record beyond its key and value: about 250 bytes of Python objects, and about 100 bytes of a typical `traceparent` header, which librdkafka keeps in C until something asks for it. Measuring headers would convert them to Python objects first.
_RECORD_OVERHEAD = 350
# The record size that a run assumes before its first fetch: librdkafka's largest by default, so the first fetch takes one record.
_FIRST_RECORD_BYTES = 1024 * 1024
# The most records that librdkafka's consume() takes in one call.
_MAX_FETCH = 1_000_000


def _record_bytes(record: Message) -> int:
    """Return the memory that a fetched record takes, approximately."""
    return len(record) + len(record.key() or b"") + _RECORD_OVERHEAD


def _key(position: TopicPartition) -> TopicPartition:
    """Return `position` without its offset. Keys never carry one, because equality and hashing ignore it."""
    return TopicPartition(position.topic, position.partition)


@dataclass(slots=True, eq=False)
class _Pending:
    """A record of a partition that runs by key, from its dispatch until it goes to the committer."""

    record: ConsumerRecord
    offset: int
    size: int
    outputs: Sequence[ProducerRecord] | None = None  # set once its handler returned
    scope: anyio.CancelScope | None = None  # set once its handler call started


class Partition:
    """One assignment of a partition: its buffered records, which it handles in offset order, one at a time or by key.

    By default, it handles one record at a time. With `slots`, it runs records by key: one task per key in progress drains that key's records in offset order, and a record of an idle key starts once it gets one of `slots`, which the topic's partitions share. A record that finishes before an earlier record of the partition waits for it, with its outputs, so that the committer gets the records in offset order, and a committed offset never skips an unfinished record.

    `Partitions` sets the two thresholds, in bytes, and decides when to pause and resume. A record counts toward the buffer from its fetch until it goes to the committer, so running and waiting records fill the buffer too. Handing a record over reports when the buffer drains to `pause_above`, because the poller may be waiting to fetch more. While the partition is paused, it also reports when the buffer drains to `resume_at`.

    A handler failure stops the partition at the lowest failed record, through `stop_failed`, at once. It starts no record above it, and cancels the running ones above it. The records below it still run, so that every record before the failed one commits, and then `run()` raises the failure. Without `stop_failed`, the failure fails the run, so it cancels every record at once.

    `finish()` lets the running handlers finish, and `cancel()` cancels them. Either way, the records that have not started are dropped, and they replay from the committed offset. A record whose handler returned goes to the committer even if the cancellation comes after, e.g. while it waits for room in the batch, unless it waits for an earlier record that never finishes.
    """

    def __init__(
        self,
        key: TopicPartition,
        on_drained: Callable[[], None],
        request_resume: Callable[["Partition"], None],
        slots: anyio.Semaphore | None = None,
        stop_failed: Callable[["Partition", Exception], None] | None = None,
    ) -> None:
        self.key = key
        self.topic: str = key.topic
        self.partition: int = key.partition
        self.records: deque[ConsumerRecord] = deque()
        self.sizes: deque[int] = deque()  # each buffered record's bytes, sized once when fetched
        self.buffered_bytes = 0  # of the records that have not gone to the committer yet
        self.queued = 0  # records dispatched to a key in progress, which wait for their turn
        self.running: dict[int, float] = {}  # offset -> monotonic start of each running handler call, oldest first
        self.paused = False
        self.failure: Exception | None = None  # set when stopped after a handler failure or a fetch error
        # The moving average of handler call durations, in seconds. It starts long, so the poller pauses a new partition instead of waiting for it.
        self.handler_seconds = 1.0
        self.pause_above = self.resume_at = 0  # set by Partitions before any record is fetched
        self.done = anyio.Event()
        self._finishing = False
        self._on_drained = on_drained
        self._request_resume = request_resume
        self._wakeup = anyio.Event()
        self._scope = anyio.CancelScope()
        self._slots = slots
        self._stop_failed = stop_failed  # None if a failure fails the run
        self._keys: dict[bytes | None, deque[_Pending]] = {}  # the keys in progress, and their waiting records
        self._pending: deque[_Pending] = deque()  # dispatched records not yet with the committer, in offset order
        self._committing = False  # a key's task is handing records to the committer
        self._failed: tuple[_Pending, Exception] | None = None  # the lowest failed record, and its failure
        self._dispatching = anyio.CancelScope()

    def take(self) -> tuple[ConsumerRecord, int]:
        """Remove and return the next buffered record and its size. It counts toward the buffer until `release()`."""
        return self.records.popleft(), self.sizes.popleft()

    def release(self, size: int) -> None:
        """Stop counting a record that went to the committer, and report the threshold the buffer drains to, if any."""
        if self._finishing:
            return  # finish() dropped the count with the buffered records.
        before = self.buffered_bytes
        self.buffered_bytes = after = before - size
        if after <= self.pause_above < before:
            self._on_drained()
        if after <= self.resume_at < before and self.paused:
            self._request_resume(self)

    def wake(self) -> None:
        self._wakeup.set()

    def finish(self) -> None:
        self._finishing = True
        self._clear()
        self._dispatching.cancel()
        self._wakeup.set()

    def cancel(self) -> None:
        self._scope.cancel()

    async def run(self, handler: RecordHandler, committer: "Committer") -> None:
        """Handle buffered records until finished or cancelled, and hand each one's outputs to `committer`.

        Raises:
            Exception: The failure of the lowest failed record.
        """
        try:
            with self._scope:
                if self._slots is None:
                    await self._run_in_order(handler, committer)
                else:
                    await self._run_by_key(handler, committer, self._slots)
        finally:
            self._clear()
            self._pending.clear()
            self.done.set()

    async def _run_in_order(self, handler: RecordHandler, committer: "Committer") -> None:
        handled = 0
        while (taken := await self._next()) is not None:
            record, size = taken
            outputs = await self._call(handler, record)
            try:
                if committer.full:
                    await committer.wait_space()
            finally:
                # Also when cancelled meanwhile. A revoked or draining partition stays assigned until its flush commits the record. After a run failure, the stopped committer commits nothing.
                committer.add((self.topic, self.partition), cast(int, record.offset()) + 1, outputs)
                self.release(size)
            if self._scope.cancel_called:
                return  # The handler swallowed its cancellation. Start no other record.
            handled += 1
            if handled % 64 == 0:
                # Handlers that never suspend must not starve the poller.
                await anyio.lowlevel.checkpoint()

    async def _run_by_key(self, handler: RecordHandler, committer: "Committer", slots: anyio.Semaphore) -> None:
        """Dispatch the buffered records in offset order: each to its key's task, or to a new one once it gets a slot."""
        try:
            async with anyio.create_task_group() as tasks:
                with self._dispatching:  # cancelled by finish() and by a failure
                    while (taken := await self._next()) is not None:
                        record, size = taken
                        pending = _Pending(record, cast(int, record.offset()), size)
                        self._pending.append(pending)
                        key = record.key()
                        if (queue := self._keys.get(key)) is not None:
                            queue.append(pending)
                            self.queued += 1
                            continue
                        self.queued += 1  # It waits for a slot.
                        try:
                            await slots.acquire()
                        finally:
                            self.queued -= 1
                        self._keys[key] = deque()
                        tasks.start_soon(self._run_key, key, pending, handler, committer, slots)
        finally:
            if self._failed is not None:
                # Also when cancelled meanwhile, so that a revocation or shutdown does not hide the failure.
                raise self._failed[1]

    async def _run_key(
        self,
        key: bytes | None,
        pending: _Pending,
        handler: RecordHandler,
        committer: "Committer",
        slots: anyio.Semaphore,
    ) -> None:
        """Handle the records of `key`, from `pending` on, until none waits, and then give up the key's slot.

        While other keys wait for a slot, it gives up its slot after each record and waits for its turn again, so that a key with a backlog, e.g. the only key of a partition, does not starve the topic's other partitions.
        """
        queue = self._keys[key]
        holding = True
        handled = 0
        try:
            while await self._handle(pending, handler, committer):
                if queue and slots.statistics().tasks_waiting:
                    holding = False
                    slots.release()
                    await slots.acquire()
                    holding = True
                elif (handled := handled + 1) % 64 == 0:
                    await anyio.lowlevel.checkpoint()  # See _run_in_order.
                if not queue:
                    return
                pending = queue.popleft()
                self.queued -= 1
        finally:
            del self._keys[key]
            self.queued -= len(queue)  # After a failure or finish(), which leave them unstarted.
            if holding:
                slots.release()

    async def _handle(self, pending: _Pending, handler: RecordHandler, committer: "Committer") -> bool:
        """Handle one record of a key, unless it may not start, and return whether it was handled."""
        # Checked here, right before the call, because a failure or finish() may come while a record waits for a slot or for its task's first step.
        if self._finishing or self._scope.cancel_called:
            return False
        if self._failed is not None and (self._stop_failed is None or pending.offset > self._failed[0].offset):
            return False
        with anyio.CancelScope() as scope:
            pending.scope = scope
            try:
                outputs = await self._call(handler, pending.record)
            except Exception as error:  # noqa: BLE001 -- run() raises the lowest failure once the others stop
                self._fail(pending, error)
                return False
        if scope.cancel_called:
            return False  # Above a failed record, so it never commits.
        pending.outputs = outputs
        await self._commit_handled(committer)
        return True

    async def _commit_handled(self, committer: "Committer") -> None:
        """Hand the handled records at the head of the pending ones to the committer, in offset order.

        One task at a time hands them over, and it also hands over those that other tasks finish meanwhile.
        """
        if self._committing:
            return
        self._committing = True
        pending = self._pending
        try:
            while pending and pending[0].outputs is not None:
                if committer.full:
                    await committer.wait_space()
                self._commit_head(committer)
        finally:
            # Also when cancelled meanwhile (see _run_in_order).
            while pending and pending[0].outputs is not None:
                self._commit_head(committer)
            self._committing = False

    def _commit_head(self, committer: "Committer") -> None:
        pending = self._pending.popleft()
        committer.add((self.topic, self.partition), pending.offset + 1, cast(Sequence[ProducerRecord], pending.outputs))
        self.release(pending.size)

    def _fail(self, pending: _Pending, error: Exception) -> None:
        """Stop the partition at `pending`, unless a lower record failed: dispatch nothing, and cancel the records above it.

        If the failure fails the run, it cancels the records below it too, and keeps the first failure, because later ones may answer the cancellation.
        """
        if self._failed is not None and (self._stop_failed is None or self._failed[0].offset < pending.offset):
            return
        self._failed = (pending, error)
        self._dispatching.cancel()
        below = -1 if self._stop_failed is None else pending.offset
        for other in self._pending:
            if other.offset > below and other.scope is not None:
                other.scope.cancel()
        if self._stop_failed is not None:
            self._stop_failed(self, error)  # Pause it now, since the records below it may run for long.

    async def _call(self, handler: RecordHandler, record: ConsumerRecord) -> Sequence[ProducerRecord]:
        offset = cast(int, record.offset())
        self.running[offset] = started = time.monotonic()
        try:
            outputs = await handler(record)
        finally:
            del self.running[offset]
        self.handler_seconds += (time.monotonic() - started - self.handler_seconds) / 8
        _check_outputs(outputs, "handle middleware")
        return outputs

    def _clear(self) -> None:
        """Drop the records that have not started."""
        self.records.clear()
        self.sizes.clear()
        for queue in self._keys.values():
            self.queued -= len(queue)
            queue.clear()
        self.buffered_bytes = 0

    async def _next(self) -> tuple[ConsumerRecord, int] | None:
        """Wait for the next record and its size, or return None once finishing."""
        while not self._finishing:
            if self.records:
                return self.take()
            self._wakeup = anyio.Event()
            await self._wakeup.wait()
        return None


def _status(partition: Partition, running: dict[int, float]) -> PartitionStatus:
    return PartitionStatus(
        topic=partition.topic,
        partition=partition.partition,
        buffered_records=len(partition.records) + partition.queued,
        running_handlers=len(running),
        paused=partition.paused,
        handler_started_at=next(iter(running.values()), None),
        failure=partition.failure,
    )


@dataclass(frozen=True, slots=True)
class Fetch:
    """What the poller does before its next fetch: pause and resume partitions, then block up to `timeout` for a record."""

    pause: list[TopicPartition]
    resume: list[TopicPartition]
    timeout: float


@dataclass(frozen=True, slots=True)
class Assigned:
    """The partitions that an assignment started, those to pause before the current fetch returns, and the rest, to resume.

    librdkafka keeps a partition's pause after its revocation, so a partition paused in an earlier assignment, e.g. stopped after a failure, would never fetch again.
    """

    partitions: list[Partition]
    pause: list[TopicPartition]
    resume: list[TopicPartition]


class Partitions:
    """The assigned partitions, and the flow control that keeps their buffers within `max_buffered_bytes`.

    The partitions of the topics in `key_concurrency` run their records by key, and each topic's partitions share its number of slots, one per key in progress. The others run one record at a time.

    Each partition gets an equal share of the budget, at least 16KB, recomputed on every assignment change. A fetch takes about half a share: as many records as fit, at the mean size of the previous fetch's records. A partition's records arrive a record batch at a time, and pausing it drops the rest of a batch, which librdkafka then fetches again, so a share should hold at least one of its producers' batches. Before a fetch, a partition that holds more than the other half is paused, unless its running handlers drain the excess within the poll timeout. A paused partition resumes once its buffer drains to a quarter. A partition stopped after a handler failure or a fetch error stays paused, and drops records fetched before its pause took effect.

    It makes no client calls: the runtime applies each `Fetch`, and calls `assign` and `revoke` from the rebalance callbacks. Every method runs on the event loop, except `status()`, which any thread may call, and `fetch_size`, which the consumer thread reads during a fetch.
    """

    def __init__(
        self,
        max_buffered_bytes: int,
        *,
        stop_failed_partitions: bool = True,
        key_concurrency: Mapping[str, int] | None = None,
    ) -> None:
        self._max_buffered_bytes = max_buffered_bytes
        self._stop_failed_partitions = stop_failed_partitions
        self._slots = {topic: anyio.Semaphore(limit) for topic, limit in (key_concurrency or {}).items()}
        self._partitions: dict[TopicPartition, Partition] = {}
        self._fetch_bytes, _, _ = self._share()
        self._mean_record_bytes = _FIRST_RECORD_BYTES  # the mean size of the previous fetch's records
        self._revoked_this_fetch: set[TopicPartition] = set()  # during the current fetch
        self._full: list[Partition] = []  # buffers above their pause threshold after a fetch
        self._to_pause: list[Partition] = []  # stopped after a handler failure or a fetch error
        self._drained = anyio.Event()  # a buffer fell to its pause threshold
        self._to_resume: list[Partition] = []
        self._resume_requested = anyio.Event()
        self._fetching_idle = False  # every partition was paused when the current fetch started
        self._idle = False  # an idle fetch returned nothing, so only a resume can produce records

    @property
    def fetch_size(self) -> int:
        """The most records a fetch takes: as many as fit in half a partition's share, at the mean size of the previous fetch's records.

        A fetch exceeds half a share only when its records are larger than the previous fetch's, by at most librdkafka's prefetch queue. The share changes only in `assign` and `revoke`, which rebalance callbacks call inside a fetch, so the consumer thread reads it between them.
        """
        return min(_MAX_FETCH, max(1, self._fetch_bytes // self._mean_record_bytes))

    def assigned(self) -> list[Partition]:
        return list(self._partitions.values())

    def status(self) -> tuple[PartitionStatus, ...]:
        # Iterating a live dict, of partitions or of running calls, fails if the loop changes it meanwhile, even inside tuple() on free-threaded builds. dict.copy() is atomic on both.
        return tuple(_status(p, p.running.copy()) for p in self._partitions.copy().values())

    def assign(self, assignment: Iterable[TopicPartition]) -> Assigned:
        """Add a partition for each position. Raise `RuntimeError` if one is already assigned.

        A partition revoked earlier in the current fetch starts paused, because the fetch's batch may hold records of its old assignment. Its pause keeps records of the new assignment out of that batch (see `buffer`), and the next fetch resumes it. Every other partition is resumed, in case an earlier assignment paused it.
        """
        started: list[Partition] = []
        pause: list[TopicPartition] = []
        resume: list[TopicPartition] = []
        for position in assignment:
            key = _key(position)
            if key in self._partitions:
                raise RuntimeError(f"partition {key.topic}[{key.partition}] is already assigned")
            partition = Partition(
                key,
                self._on_drained,
                self._request_resume,
                self._slots.get(key.topic),
                self.stop_failed if self._stop_failed_partitions else None,
            )
            if key in self._revoked_this_fetch:
                partition.paused = True
                pause.append(key)
                self._request_resume(partition)
            else:
                resume.append(key)
            self._partitions[key] = partition
            started.append(partition)
        self._reshare()  # Also sets the new partitions' thresholds.
        return Assigned(started, pause, resume)

    def revoke(self, positions: Iterable[TopicPartition]) -> list[Partition]:
        """Remove the partitions at `positions`, and return those that were assigned."""
        removed = []
        for position in positions:
            key = _key(position)
            self._revoked_this_fetch.add(key)
            if (partition := self._partitions.pop(key, None)) is not None:
                removed.append(partition)
        self._reshare()
        return removed

    def stop_failed(self, partition: Partition, error: Exception) -> None:
        """Stop fetching `partition` after its handler failed with `error`. It is never resumed."""
        partition.failure = error
        if self._owns(partition) and not partition.paused:
            partition.paused = True
            self._to_pause.append(partition)

    async def next_fetch(self) -> Fetch:
        """Wait until the next fetch may start, and return what to pause and resume first.

        If the previous fetch found every partition paused and returned nothing, it first waits for a resume. Then it waits for full buffers whose handlers can drain the excess in time, and pauses those still full. Both waits together take at most the poll timeout.
        """
        deadline = anyio.current_time() + POLL_TIMEOUT
        if self._idle:
            self._idle = False
            with anyio.CancelScope(deadline=deadline):
                await self._resume_requested.wait()
        pause = await self._relieve(deadline)
        # Skip partitions revoked meanwhile. librdkafka would keep the pause until a later reassignment resumes it.
        pause += (partition.key for partition in self._to_pause if self._owns(partition))
        self._to_pause.clear()
        resume = []
        for partition in self._to_resume:
            if partition.paused and partition.failure is None and self._owns(partition):
                partition.paused = False
                resume.append(partition.key)
        self._to_resume.clear()
        self._resume_requested = anyio.Event()
        # With every partition paused, only a resume can produce records. Fetch for callbacks without blocking, and wait for a resume before the next fetch.
        self._fetching_idle = bool(self._partitions) and all(p.paused for p in self._partitions.values())
        self._revoked_this_fetch.clear()
        return Fetch(pause, resume, 0 if self._fetching_idle else POLL_TIMEOUT)

    def buffer(self, records: list[Message]) -> list[Partition]:
        """Append a fetch's records to their partitions' buffers, and return the partitions that a fetch error stopped.

        librdkafka retries transient errors itself, e.g. leader changes and unknown topics, so a record carries only an error that needs an operator: an unauthorized topic, a position out of range or truncated, an unreadable record, or a fatal error. One of an assigned partition stops it, like a handler failure, because a restart would meet it again. Its records fetched before the error stay buffered, and those after it are dropped.

        Raises:
            KafkaException: If a record carries an error of no assigned partition, or stopping failed partitions is off.
            RuntimeError: If a record belongs to an unassigned partition.
        """
        self._idle = self._fetching_idle and not records
        partitions = self._partitions
        touched: set[Partition] = set()
        stopped: list[Partition] = []
        topic = ""
        partition_id = -1
        buffered: Partition | None = None
        fetched_bytes = fetched = 0
        for record in records:
            if (error := record.error()) is not None:
                failure = KafkaException(error)
                if (error_topic := record.topic()) is None or (error_partition := record.partition()) is None:
                    raise failure
                failure.add_note(f"fetching {error_topic}[{error_partition}]")
                key = TopicPartition(error_topic, error_partition)
                if key in self._revoked_this_fetch:
                    continue  # An error of the partition's revoked assignment.
                if (partition := partitions.get(key)) is None or not self._stop_failed_partitions:
                    raise failure
                if partition.failure is None:
                    self.stop_failed(partition, failure)
                    stopped.append(partition)
                topic, partition_id, buffered = error_topic, error_partition, None  # Drop its later records.
                continue
            # Records without an error always have both.
            record_topic, record_partition = cast(str, record.topic()), cast(int, record.partition())
            if record_partition != partition_id or record_topic != topic:
                topic, partition_id = record_topic, record_partition
                key = TopicPartition(topic, partition_id)
                if key in self._revoked_this_fetch:
                    # The record was fetched before this fetch revoked its partition. A reassignment stays paused until the next fetch (see assign), so this batch holds none of its records.
                    buffered = None
                elif (partition := partitions.get(key)) is None:
                    raise RuntimeError(f"fetched a record of unassigned partition {topic}[{partition_id}]")
                elif partition.failure is not None:
                    buffered = None  # Fetched before the stopped partition was paused.
                else:
                    buffered = partition
                    touched.add(partition)
            if buffered is not None:
                size = len(record) + len(record.key() or b"") + _RECORD_OVERHEAD  # _record_bytes(), inlined
                buffered.records.append(record)
                buffered.sizes.append(size)
                buffered.buffered_bytes += size
                fetched_bytes += size
                fetched += 1
        if fetched:
            self._mean_record_bytes = max(1, fetched_bytes // fetched)
        for partition in touched:
            partition.wake()
            if partition.buffered_bytes > partition.pause_above:
                self._full.append(partition)
        return stopped

    async def _relieve(self, deadline: float) -> list[TopicPartition]:
        """Return the partitions to pause, whose buffers stay above their pause threshold.

        Pausing discards the partition's prefetched records, and resuming waits for a new fetch. So it waits until `deadline` instead for a partition whose handler can drain the excess within the poll timeout. A fast handler usually falls behind because the committer is busy.
        """
        full, self._full = self._full, []
        draining = [p for p in full if self._drain_seconds(p) <= POLL_TIMEOUT]
        with anyio.CancelScope(deadline=deadline):
            while any(partition.buffered_bytes > partition.pause_above for partition in draining):
                self._drained = anyio.Event()
                await self._drained.wait()
        pause = []
        for partition in full:
            if not partition.paused and partition.buffered_bytes > partition.pause_above and self._owns(partition):
                partition.paused = True
                pause.append(partition.key)
        return pause

    def _drain_seconds(self, partition: Partition) -> float:
        """Return about how long the partition's running handlers take to drain its buffer to its pause threshold."""
        excess_records = max(0, partition.buffered_bytes - partition.pause_above) / self._mean_record_bytes
        return excess_records * partition.handler_seconds / max(1, len(partition.running))

    def _owns(self, partition: Partition) -> bool:
        """Return whether `partition` is its key's current assignment."""
        return self._partitions.get(partition.key) is partition

    def _on_drained(self) -> None:
        self._drained.set()

    def _request_resume(self, partition: Partition) -> None:
        self._to_resume.append(partition)
        self._resume_requested.set()

    def _share(self) -> tuple[int, int, int]:
        """Split a partition's share of `max_buffered_bytes` into the fetch bytes and the pause and resume thresholds.

        A fetch takes about half a share. Before fetching, a partition above the other half is paused or drained, so a buffer exceeds its share only by a fetch's excess.
        """
        share = max(_MIN_SHARE, self._max_buffered_bytes // max(1, len(self._partitions)))
        fetch = (share + 1) // 2
        pause_above = share - fetch
        return fetch, pause_above, pause_above // 2

    def _reshare(self) -> None:
        self._fetch_bytes, pause_above, resume_at = self._share()
        for partition in self._partitions.values():
            partition.pause_above, partition.resume_at = pause_above, resume_at
            if not partition.paused and partition.buffered_bytes > pause_above:
                self._full.append(partition)  # The share shrank. Pause or drain it before fetching.
            elif partition.paused and partition.failure is None and partition.buffered_bytes <= resume_at:
                self._request_resume(partition)  # The share grew, so it may already be at or below resume_at.
