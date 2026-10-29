"""Runtime behavior that needs no broker, driven through the runtime's own rebalance and failure paths."""

from collections.abc import Sequence

import anyio
import pytest
from confluent_kafka import Message, TopicPartition

from potik.core import Config, ConsumerRecord, ProducerRecord, Runtime
from potik.core.partitions import Partition, Partitions

pytestmark = pytest.mark.anyio


class FakeCommitter:
    """Records what it is given. A full one never makes room."""

    def __init__(self, *, full: bool = False) -> None:
        self.full = full
        self.added: list[tuple[tuple[str, int], int]] = []

    def add(self, partition: tuple[str, int], next_offset: int, outputs: Sequence[ProducerRecord]) -> None:
        self.added.append((partition, next_offset))

    async def wait_space(self) -> None:
        await anyio.sleep_forever()

    async def flush(self) -> None:
        pass

    def stop(self, reason: BaseException) -> None:
        pass


async def test_a_failure_cancels_the_handlers_that_a_revocation_lets_finish() -> None:
    started = anyio.Event()

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        started.set()
        await anyio.sleep_forever()
        return []

    config = Config(bootstrap_servers="localhost:1", group_id="g", revocation_grace_ms=30_000)
    runtime = Runtime(config, ["t"], handler)
    runtime._committer = committer = FakeCommitter()  # ty: ignore[invalid-assignment]
    (partition,) = runtime._partitions.assign([TopicPartition("t", 0)]).partitions
    runtime._partitions.buffer([Message(topic="t", partition=0, offset=0)])
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(partition.run, handler, committer)
            await started.wait()
            tasks.start_soon(runtime._revoke_partitions, [TopicPartition("t", 0)], False)
            await anyio.wait_all_tasks_blocked()
            runtime._fail(RuntimeError("failed"))
            await partition.done.wait()


def partition_with(*offsets: int) -> Partition:
    partitions = Partitions(max_buffered_bytes=1024 * 1024)
    (partition,) = partitions.assign([TopicPartition("t", 0)]).partitions
    partitions.buffer([Message(topic="t", partition=0, offset=offset) for offset in offsets])
    return partition


async def cancel_once_blocked(partition: Partition, handler, committer: FakeCommitter) -> None:
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(partition.run, handler, committer)
            await anyio.wait_all_tasks_blocked()
            partition.cancel()


async def test_a_handled_record_commits_when_cancelled_while_the_batch_is_full() -> None:
    """A revocation that cancels it still flushes the batch, so the record need not replay."""

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        return []

    committer = FakeCommitter(full=True)
    await cancel_once_blocked(partition := partition_with(0, 1), handler, committer)
    assert committer.added == [(("t", 0), 1)]
    assert partition.done.is_set()


async def test_a_handler_that_swallows_its_cancellation_commits_its_record_and_starts_no_other() -> None:
    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        try:
            await anyio.sleep_forever()
        except anyio.get_cancelled_exc_class():
            return []
        return []

    committer = FakeCommitter()
    await cancel_once_blocked(partition_with(0, 1), handler, committer)
    assert committer.added == [(("t", 0), 1)]


async def test_a_failed_handler_call_leaves_no_call_in_flight() -> None:
    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        raise ValueError("poison")

    partition = partition_with(0)
    with pytest.raises(ValueError, match="poison"):
        await partition.run(handler, FakeCommitter())  # ty: ignore[invalid-argument-type]
    assert not partition.running


async def test_a_final_commit_that_hangs_fails_the_shutdown() -> None:
    """A shutdown takes at most shutdown_grace_ms + rebalance_timeout_ms, like a revocation takes rebalance_timeout_ms."""

    class HangingCommitter(FakeCommitter):
        async def flush(self) -> None:
            await anyio.sleep_forever()

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        return []

    config = Config(bootstrap_servers="localhost:1", group_id="g", rebalance_timeout_ms=200, shutdown_grace_ms=100)
    runtime = Runtime(config, ["t"], handler)
    runtime._committer = HangingCommitter()  # ty: ignore[invalid-assignment]
    runtime._poll_exited.set()
    with anyio.fail_after(5), pytest.raises(TimeoutError, match="shutdown exceeded"):
        await runtime._drain()
