"""In-process behavior of the core `Runtime` that needs a real broker: failures, stopped partitions, and shutdown."""

import signal
from collections.abc import Sequence
from functools import partial

import anyio
import anyio.lowlevel
import pytest
from confluent_kafka import ConsumerGroupTopicPartitions, KafkaError, KafkaException, Producer, TopicPartition

from potik.core import ConsumerRecord, PermanentError, ProducerRecord, Runtime, committer
from potik.core.committer import Committer
from potik.middleware import ExceptionMiddleware, on, skip

pytestmark = pytest.mark.anyio


async def test_failed_shutdown_fails_the_run(workload, monkeypatch) -> None:
    """A failure while draining after SIGTERM must not be reported as a clean exit."""
    work = workload(1, 100)
    stopping = False
    transact = Committer._transact

    def failing_transact(self, *args) -> None:
        if stopping:
            raise RuntimeError("commit failed during shutdown")
        transact(self, *args)

    monkeypatch.setattr(Committer, "_transact", failing_transact)

    async def handler(record: ConsumerRecord) -> None:
        nonlocal stopping
        if record.offset() == 10:
            stopping = True
            signal.raise_signal(signal.SIGTERM)
            await anyio.lowlevel.checkpoint()  # The signal lands while this record is in flight.

    runtime = work.runtime(handler, shutdown_grace_ms=5_000)
    with pytest.raises(RuntimeError, match="commit failed during shutdown"):
        await runtime.run()


async def test_a_transaction_that_needs_an_abort_is_repeated(workload, monkeypatch) -> None:
    """An abortable error, e.g. the sequence gap that a repeat after a stale epoch can leave, aborts and repeats the transaction."""
    work = workload(2, 100)

    class FailingOnce:
        def __init__(self, config: dict) -> None:
            self.producer = Producer(config)
            self.failed = False

        def __getattr__(self, name: str):
            return getattr(self.producer, name)

        def commit_transaction(self, *args) -> None:
            if not self.failed:
                self.failed = True
                error = KafkaError(
                    KafkaError.OUT_OF_ORDER_SEQUENCE_NUMBER, "skipped sequence numbers", txn_requires_abort=True
                )
                raise KafkaException(error)
            self.producer.commit_transaction(*args)

    monkeypatch.setattr(committer, "Producer", FailingOnce)
    await work.run_until_committed(work.runtime())
    work.assert_exactly_once()


async def test_handler_ignoring_cancellation_fails_the_shutdown(workload) -> None:
    """The shutdown's timeout is raised as is, not in an ExceptionGroup."""
    work = workload(1, 10)

    async def handler(record: ConsumerRecord) -> None:
        signal.raise_signal(signal.SIGTERM)
        with anyio.CancelScope(shield=True):
            await anyio.sleep(3)

    runtime = work.runtime(handler, rebalance_timeout_ms=1000)
    with pytest.raises(TimeoutError, match="cancelled handlers did not stop within rebalance_timeout_ms"):
        await runtime.run()
    assert isinstance(runtime.status().failure, TimeoutError)


async def test_handler_failure_stops_its_partition(workload) -> None:
    """The failed record stops only its partition, stays uncommitted, and is retried by the next run."""
    work = workload(4, 400)
    failing = True

    async def handler(record: ConsumerRecord) -> None:
        if failing and (record.partition(), record.offset()) == (0, 20):
            raise ValueError("poison")

    runtime = work.runtime(handler)
    status = await work.run_until_committed(runtime, [20, 100, 100, 100])
    assert status.running
    (stopped,) = [p for p in status.partitions if p.failure is not None]
    assert (stopped.partition, stopped.paused, type(stopped.failure)) == (0, True, ValueError)
    assert runtime.status().failure is None

    failing = False
    await work.run_until_committed(work.runtime(handler))
    work.assert_exactly_once()


async def test_a_stopped_partition_that_moves_away_and_back_retries_its_record(workload) -> None:
    """librdkafka keeps a revoked partition's pause, so its reassignment must resume it."""
    work = workload(2, 200)
    failing = True

    async def handler(record: ConsumerRecord) -> None:
        if failing and record.offset() == 20:
            raise ValueError("poison")

    async def committed(expected: list[int]) -> None:
        with anyio.fail_after(60):
            while sorted(await anyio.to_thread.run_sync(work.committed)) != expected:
                await anyio.sleep(0.2)

    async with anyio.create_task_group() as tasks:
        await tasks.start(partial(work.runtime(handler).run, handle_signals=False))
        await committed([20, 20])
        # A second member takes one of the stopped partitions, stops it too, and leaves, so it returns to the first.
        second = work.runtime(handler)
        async with anyio.create_task_group() as joined:
            await joined.start(partial(second.run, handle_signals=False))
            with anyio.fail_after(60):
                while not any(p.failure is not None for p in second.status().partitions):
                    await anyio.sleep(0.2)
            failing = False
            joined.cancel_scope.cancel()
        # The partition that came back retries its failed record. The one that never moved stays stopped.
        await committed([20, 100])
        tasks.cancel_scope.cancel()


async def test_a_failure_fails_the_run_without_stopping_partitions(workload) -> None:
    work = workload(2, 100)

    async def handler(record: ConsumerRecord) -> None:
        if record.offset() == 5:
            raise ValueError("poison")

    runtime = work.runtime(handler, stop_failed_partitions=False)
    with pytest.raises(ValueError, match="poison"):
        await runtime.run(handle_signals=False)
    assert runtime.liveness() == runtime.readiness() == "run failed with ValueError"
    committed = work.committed()
    assert all(offset <= 5 for offset in committed), committed


async def test_invalid_result_fails_inside_the_middleware(workload) -> None:
    """A handler's invalid result fails its call, so exception rules handle it like the handler's own failure."""
    work = workload(1, 10)

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        output = ProducerRecord(work.target, record.key(), record.value())
        return output if record.offset() == 5 else [output]  # One record, not a sequence of them.

    exceptions = ExceptionMiddleware(on(PermanentError, skip()))
    await work.run_until_committed(Runtime(work.config(), [work.source], handler, middleware=[exceptions]))
    assert len(work.outputs()) == 9


async def test_a_committed_offset_out_of_the_log_stops_its_partition(workload) -> None:
    """Retention deleted the records past partition 0's committed offset, so fetching it fails. The others carry on."""
    work = workload(2, 200)
    group = ConsumerGroupTopicPartitions(work.group, [TopicPartition(work.source, 0, 5)])
    for future in [
        *work.admin.alter_consumer_group_offsets([group]).values(),
        *work.admin.delete_records([TopicPartition(work.source, 0, 50)]).values(),
    ]:
        future.result()
    runtime = work.runtime()
    async with anyio.create_task_group() as tasks:
        await tasks.start(partial(runtime.run, handle_signals=False))
        with anyio.fail_after(30):
            while not any(p.failure for p in runtime.status().partitions) or (
                await anyio.to_thread.run_sync(work.committed)
            ) != [5, 100]:
                await anyio.sleep(0.2)
        status = runtime.status()
        tasks.cancel_scope.cancel()
    (stopped,) = [p for p in status.partitions if p.failure is not None]
    assert (stopped.partition, stopped.paused) == (0, True)
    assert isinstance(stopped.failure, KafkaException)
    assert stopped.failure.args[0].code() == KafkaError._AUTO_OFFSET_RESET
    assert status.running and runtime.liveness() is None
    assert work.committed() == [5, 100]


async def test_records_that_finish_before_an_earlier_one_of_their_partition_commit_only_after_it(workload) -> None:
    """Their outputs wait for the earlier record, so a shutdown that cancels it commits none of them, and the next run handles them again exactly once."""
    work = workload(2, 40)
    handled = 0
    blocking = True

    async def handler(record: ConsumerRecord) -> None:
        nonlocal handled
        if blocking and record.offset() == 0:
            await anyio.sleep_forever()
        handled += 1

    runtime = work.runtime(handler, key_concurrency=40)
    async with anyio.create_task_group() as tasks:
        await tasks.start(partial(runtime.run, handle_signals=False))
        with anyio.fail_after(30):
            while handled < 38:  # Every record but the first of each partition.
                await anyio.sleep(0.05)
        await anyio.sleep(0.5)  # Several commit intervals, which must commit none of them.
        tasks.cancel_scope.cancel()
    assert work.committed() == [0, 0]  # The start offsets that the offset policy committed.
    assert work.outputs() == []

    blocking = False
    await work.run_until_committed(work.runtime(handler, key_concurrency=40))
    work.assert_exactly_once()
