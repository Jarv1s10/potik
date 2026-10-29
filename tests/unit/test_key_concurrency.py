"""Partitions of topics that run by key, driven without a broker."""

from collections.abc import Sequence
from typing import cast

import anyio
import pytest
from confluent_kafka import Message, TopicPartition

from potik import Config, Router, build_runtime
from potik.core import ConsumerRecord, ProducerRecord, Runtime
from potik.core.partitions import Partition, Partitions

pytestmark = pytest.mark.anyio


class FakeCommitter:
    """Records the next offsets it is given, in order."""

    full = False

    def __init__(self) -> None:
        self.added: list[tuple[int, int]] = []  # (partition, next offset)

    def add(self, partition: tuple[str, int], next_offset: int, outputs: Sequence[ProducerRecord]) -> None:
        self.added.append((partition[1], next_offset))

    async def wait_space(self) -> None:
        await anyio.sleep_forever()


class Handler:
    """A handler whose calls each wait until the test finishes or fails their record."""

    def __init__(self) -> None:
        self.started: list[int] = []
        self._outcomes: dict[int, anyio.Event] = {}
        self._failures: dict[int, Exception] = {}
        self.cancelled: list[int] = []

    async def __call__(self, record: ConsumerRecord) -> Sequence[ProducerRecord]:
        offset = record.offset()
        assert offset is not None
        self.started.append(offset)
        try:
            await self._outcomes.setdefault(offset, anyio.Event()).wait()
        except anyio.get_cancelled_exc_class():
            self.cancelled.append(offset)
            raise
        if (failure := self._failures.get(offset)) is not None:
            raise failure
        return ()

    def finish(self, *offsets: int) -> None:
        for offset in offsets:
            self._outcomes.setdefault(offset, anyio.Event()).set()

    def fail(self, offset: int, failure: Exception) -> None:
        self._failures[offset] = failure
        self.finish(offset)


def keyed(partition: int, *keys: str) -> list[Message]:
    """Return one record per key, at offsets 0, 1, 2, and so on."""
    return [
        Message(topic="t", partition=partition, offset=offset, key=key.encode(), value=b"x")
        for offset, key in enumerate(keys)
    ]


def by_key(*keys: str, slots: int = 10) -> Partition:
    partitions = Partitions(max_buffered_bytes=1024 * 1024, key_concurrency={"t": slots})
    (partition,) = partitions.assign([TopicPartition("t", 0)]).partitions
    partitions.buffer(keyed(0, *keys))
    return partition


async def test_records_of_a_key_run_in_order_and_other_keys_run_past_a_busy_one() -> None:
    partition, handler, committer = by_key("a", "a", "b"), Handler(), FakeCommitter()
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(partition.run, handler, committer)
            await anyio.wait_all_tasks_blocked()
            assert handler.started == [0, 2]  # 1 waits for 0, which has its key.
            handler.finish(0)
            await anyio.wait_all_tasks_blocked()
            assert handler.started == [0, 2, 1]
            partition.cancel()


async def test_a_record_that_finishes_early_waits_for_the_earlier_ones_before_it_commits() -> None:
    partition, handler, committer = by_key("a", "b", "c"), Handler(), FakeCommitter()
    record_bytes = partition.buffered_bytes // 3
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(partition.run, handler, committer)
            handler.finish(1, 2)
            await anyio.wait_all_tasks_blocked()
            assert committer.added == []
            # Held records still count toward the buffer, so they can pause the partition.
            assert partition.buffered_bytes == 3 * record_bytes
            handler.finish(0)
            await anyio.wait_all_tasks_blocked()
            assert committer.added == [(0, 1), (0, 2), (0, 3)]
            assert partition.buffered_bytes == 0
            partition.cancel()


async def test_the_partitions_of_a_topic_share_its_slots() -> None:
    partitions = Partitions(max_buffered_bytes=1024 * 1024, key_concurrency={"t": 1})
    zero, one = partitions.assign([TopicPartition("t", 0), TopicPartition("t", 1)]).partitions
    partitions.buffer([*keyed(0, "a"), *keyed(1, "b")])
    handler, committer = Handler(), FakeCommitter()
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(zero.run, handler, committer)
            tasks.start_soon(one.run, handler, committer)
            await anyio.wait_all_tasks_blocked()
            assert len(handler.started) == 1
            handler.finish(0)  # Both partitions' records are at offset 0.
            await anyio.wait_all_tasks_blocked()
            assert committer.added == [(0, 1), (1, 1)] or committer.added == [(1, 1), (0, 1)]
            zero.cancel()
            one.cancel()


async def test_a_cancelled_partition_gives_its_slots_back_to_its_topic() -> None:
    partitions = Partitions(max_buffered_bytes=1024 * 1024, key_concurrency={"t": 2})
    zero, one = partitions.assign([TopicPartition("t", 0), TopicPartition("t", 1)]).partitions
    partitions.buffer([*keyed(0, "a", "b"), *keyed(1, "c", "d")])
    handler, committer = Handler(), FakeCommitter()
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(zero.run, handler, committer)
            await anyio.wait_all_tasks_blocked()
            tasks.start_soon(one.run, handler, committer)
            await anyio.wait_all_tasks_blocked()
            assert one.running == {}  # Partition 0 holds both slots.
            zero.cancel()
            await anyio.wait_all_tasks_blocked()
            assert list(one.running) == [0, 1]
            one.cancel()


async def test_a_failure_stops_at_the_lowest_failed_record_and_lets_the_records_below_it_run() -> None:
    # Record 1 waits for 0, which has its key. Record 3 runs above the failed record 2, and 4 waits for it.
    partition, handler, committer = by_key("a", "a", "b", "c", "c"), Handler(), FakeCommitter()
    failure = ValueError("bad record")
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:

            async def run() -> None:
                with pytest.raises(ValueError) as raised:
                    await partition.run(handler, committer)
                assert raised.value is failure

            tasks.start_soon(run)
            await anyio.wait_all_tasks_blocked()
            assert handler.started == [0, 2, 3]
            handler.fail(2, failure)
            await anyio.wait_all_tasks_blocked()
            assert handler.cancelled == [3]
            handler.finish(0, 1)
    assert handler.started == [0, 2, 3, 1]
    assert committer.added == [(0, 1), (0, 2)]  # Every record before the failed one.


async def test_finishing_commits_the_records_before_the_first_unfinished_one_and_starts_no_other() -> None:
    partition, handler, committer = by_key("a", "b", "a", "c"), Handler(), FakeCommitter()
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(partition.run, handler, committer)
            await anyio.wait_all_tasks_blocked()
            assert handler.started == [0, 1, 3]  # 2 waits for 0, which has its key.
            partition.finish()
            handler.finish(1, 0, 3)
            await partition.done.wait()
    assert handler.started == [0, 1, 3]
    assert committer.added == [(0, 1), (0, 2)]  # 3 waits for 2, which never runs.


async def test_status_counts_running_calls_and_waiting_records() -> None:
    partitions = Partitions(max_buffered_bytes=1024 * 1024, key_concurrency={"t": 10})
    (partition,) = partitions.assign([TopicPartition("t", 0)]).partitions
    partitions.buffer(keyed(0, "a", "a", "b"))
    handler, committer = Handler(), FakeCommitter()
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(partition.run, handler, committer)
            await anyio.wait_all_tasks_blocked()
            (status,) = partitions.status()
            assert (status.running_handlers, status.buffered_records) == (2, 1)
            assert status.handler_started_at == partition.running[0]  # The oldest call.
            partition.cancel()


async def test_status_counts_no_buffered_records_once_a_failure_stopped_the_partition() -> None:
    partitions = Partitions(max_buffered_bytes=1024 * 1024, key_concurrency={"t": 10})
    (partition,) = partitions.assign([TopicPartition("t", 0)]).partitions
    partitions.buffer(keyed(0, "a", "a", "b"))  # 1 waits for 0, which fails.
    handler = Handler()
    handler.fail(0, ValueError("bad record"))
    with anyio.fail_after(5), pytest.raises(ValueError):
        await partition.run(handler, FakeCommitter())
    (status,) = partitions.status()
    assert (status.running_handlers, status.buffered_records) == (0, 0)


async def test_a_record_larger_than_its_partitions_share_runs_and_its_partition_resumes_once_it_commits() -> None:
    partitions = Partitions(max_buffered_bytes=1, key_concurrency={"t": 10})  # The least share, 16KB.
    (partition,) = partitions.assign([TopicPartition("t", 0)]).partitions
    handler, committer = Handler(), FakeCommitter()
    with anyio.fail_after(5):
        await partitions.next_fetch()
        partitions.buffer([Message(topic="t", partition=0, offset=0, key=b"a", value=b"x" * 32 * 1024)])
        assert (await partitions.next_fetch()).pause == [TopicPartition("t", 0)]
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(partition.run, handler, committer)
            await anyio.wait_all_tasks_blocked()
            assert handler.started == [0]
            handler.finish(0)
            await anyio.wait_all_tasks_blocked()
            assert committer.added == [(0, 1)]
            assert (await partitions.next_fetch()).resume == [TopicPartition("t", 0)]
            partition.cancel()


def test_key_concurrency_needs_subscribed_topics_and_positive_limits() -> None:
    config = Config(bootstrap_servers="localhost:1", group_id="g")

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        return ()

    with pytest.raises(ValueError, match="not among the runtime's topics"):
        Runtime(config, ["t"], handler, key_concurrency={"u": 1})
    with pytest.raises(ValueError, match="positive integer"):
        Runtime(config, ["t"], handler, key_concurrency={"t": 0})


async def test_build_runtime_runs_by_key_the_topics_of_handlers_with_key_concurrency() -> None:
    router = Router()

    @router.handler("t", key_concurrency=4)
    async def keyed_handler(value: bytes) -> None: ...

    @router.handler("u")
    async def ordered_handler(value: bytes) -> None: ...

    runtime = build_runtime(Config(bootstrap_servers="localhost:1", group_id="g"), [router], check_topics=False)
    zero, one = runtime._partitions.assign([TopicPartition("t", 0), TopicPartition("u", 0)]).partitions
    assert zero._slots is not None
    assert one._slots is None


class FullCommitter(FakeCommitter):
    """Never makes room for more records."""

    full = True


async def test_no_record_starts_above_a_failure_while_it_waited_for_a_free_slot() -> None:
    partition, committer = by_key("a", "b", "c", slots=2), FakeCommitter()
    started: list[int] = []

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        started.append(cast(int, record.offset()))
        raise ValueError("before its first checkpoint")

    with anyio.fail_after(5), pytest.raises(ValueError):
        await partition.run(handler, committer)
    assert started == [0]


async def test_no_record_starts_once_finishing() -> None:
    partition, handler, committer = by_key("a", "b"), Handler(), FakeCommitter()
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(partition.run, handler, committer)
            await anyio.lowlevel.checkpoint()  # The dispatcher took a free slot for record 0.
            partition.finish()
    assert handler.started == []
    assert committer.added == []


async def test_a_failure_pauses_its_partition_at_once_while_the_records_below_it_run() -> None:
    partitions = Partitions(max_buffered_bytes=1024 * 1024, key_concurrency={"t": 10})
    (partition,) = partitions.assign([TopicPartition("t", 0)]).partitions
    partitions.buffer(keyed(0, "a", "b"))
    handler, committer, failure = Handler(), FakeCommitter(), ValueError("bad record")
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:

            async def run() -> None:
                with pytest.raises(ValueError):
                    await partition.run(handler, committer)

            tasks.start_soon(run)
            await anyio.wait_all_tasks_blocked()
            handler.fail(1, failure)
            await anyio.wait_all_tasks_blocked()
            assert partition.failure is failure
            assert (await partitions.next_fetch()).pause == [TopicPartition("t", 0)]
            handler.finish(0)
    assert committer.added == [(0, 1)]


async def test_a_failure_that_fails_the_run_cancels_every_record_at_once() -> None:
    partitions = Partitions(max_buffered_bytes=1024 * 1024, stop_failed_partitions=False, key_concurrency={"t": 10})
    (partition,) = partitions.assign([TopicPartition("t", 0)]).partitions
    partitions.buffer(keyed(0, "a", "b"))
    handler, committer = Handler(), FakeCommitter()
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:

            async def run() -> None:
                with pytest.raises(ValueError):
                    await partition.run(handler, committer)

            tasks.start_soon(run)
            await anyio.wait_all_tasks_blocked()
            handler.fail(1, ValueError("bad record"))
    assert handler.cancelled == [0]
    assert committer.added == []


async def test_a_cancellation_does_not_hide_a_failure_whose_lower_records_still_ran() -> None:
    partition, handler, committer = by_key("a", "b"), Handler(), FakeCommitter()
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:

            async def run() -> None:
                with pytest.raises(ValueError):
                    await partition.run(handler, committer)

            tasks.start_soon(run)
            await anyio.wait_all_tasks_blocked()
            handler.fail(1, ValueError("bad record"))
            await anyio.wait_all_tasks_blocked()
            partition.cancel()


async def test_keys_with_a_backlog_take_turns_with_keys_that_wait_for_a_slot() -> None:
    """With fewer slots than partitions, e.g. of a topic without keys, every partition makes progress."""
    partitions = Partitions(max_buffered_bytes=1024 * 1024, key_concurrency={"t": 1})
    zero, one = partitions.assign([TopicPartition("t", 0), TopicPartition("t", 1)]).partitions
    partitions.buffer([*keyed(0, "a", "a"), *keyed(1, "b", "b")])
    started: list[tuple[int | None, int | None]] = []

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        started.append((record.partition(), record.offset()))
        await anyio.lowlevel.checkpoint()
        return ()

    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(zero.run, handler, FakeCommitter())
            tasks.start_soon(one.run, handler, FakeCommitter())
            await anyio.wait_all_tasks_blocked()
            zero.cancel()
            one.cancel()
    assert started == [(0, 0), (1, 0), (0, 1), (1, 1)]


async def test_records_without_a_key_share_one_key() -> None:
    partitions = Partitions(max_buffered_bytes=1024 * 1024, key_concurrency={"t": 10})
    (partition,) = partitions.assign([TopicPartition("t", 0)]).partitions
    partitions.buffer([Message(topic="t", partition=0, offset=offset, value=b"x") for offset in range(3)])
    handler, committer = Handler(), FakeCommitter()
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(partition.run, handler, committer)
            await anyio.wait_all_tasks_blocked()
            assert handler.started == [0]
            partition.cancel()


async def test_status_counts_a_record_that_waits_for_a_slot_as_buffered() -> None:
    partitions = Partitions(max_buffered_bytes=1024 * 1024, key_concurrency={"t": 1})
    (partition,) = partitions.assign([TopicPartition("t", 0)]).partitions
    partitions.buffer(keyed(0, "a", "b"))
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(partition.run, Handler(), FakeCommitter())
            await anyio.wait_all_tasks_blocked()
            (status,) = partitions.status()
            assert (status.running_handlers, status.buffered_records) == (1, 1)
            partition.cancel()


async def test_handled_records_go_to_the_committer_in_order_when_cancelled_while_it_is_full() -> None:
    partition, handler, committer = by_key("a", "b", "c"), Handler(), FullCommitter()
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(partition.run, handler, committer)
            handler.finish(1, 0)
            await anyio.wait_all_tasks_blocked()
            assert committer.added == []  # Waiting for room in the batch.
            partition.cancel()
    assert committer.added == [(0, 1), (0, 2)]  # Not 2, which never finished.
