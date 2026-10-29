"""Flow control of the assigned partitions' buffers, driven as the poller and the rebalance callbacks drive it."""

import anyio
import pytest
from confluent_kafka import OFFSET_INVALID, KafkaError, KafkaException, Message, TopicPartition

from potik.core.partitions import POLL_TIMEOUT, Fetch, Partition, Partitions, _record_bytes

pytestmark = pytest.mark.anyio


def tp(partition: int, offset: int = OFFSET_INVALID) -> TopicPartition:
    return TopicPartition("t", partition, offset)


VALUE = b"x" * 1000
RECORD = _record_bytes(Message(value=VALUE))
# A share of 40 records for one partition: a fetch of 20, pause above 20, resume at 10.
BUDGET = 40 * RECORD


def records(partition: int, count: int, value: bytes = VALUE) -> list[Message]:
    return [Message(topic="t", partition=partition, offset=offset, value=value) for offset in range(count)]


def drain(partition: Partition, count: int) -> None:
    """Hand `count` records to the committer, as their handlers would."""
    for _ in range(count):
        partition.release(partition.take()[1])


async def promptly_fetch(partitions: Partitions) -> Fetch:
    """Return the next fetch, which must not wait out the poll timeout."""
    with anyio.fail_after(POLL_TIMEOUT / 2):
        return await partitions.next_fetch()


async def test_a_fetch_takes_half_a_share_and_the_next_pauses_a_buffer_above_the_other_half() -> None:
    partitions = Partitions(max_buffered_bytes=BUDGET)
    partitions.assign([tp(0)])
    assert partitions.fetch_size == 1  # Before the first fetch, the size of records is unknown.
    await promptly_fetch(partitions)
    partitions.buffer(records(0, 1))
    assert partitions.fetch_size == 20
    await promptly_fetch(partitions)
    partitions.buffer(records(0, 20))
    # A new partition's handler latency is unknown, so it is paused rather than waited for.
    assert (await promptly_fetch(partitions)).pause == [tp(0)]


async def test_a_fetch_takes_at_most_the_records_that_librdkafka_allows() -> None:
    partitions = Partitions(max_buffered_bytes=1024**4)
    partitions.assign([tp(0)])
    await promptly_fetch(partitions)
    partitions.buffer(records(0, 1))
    assert partitions.fetch_size == 1_000_000


async def test_a_buffer_is_measured_in_bytes_and_a_fetch_takes_records_of_the_previous_fetchs_size() -> None:
    partitions = Partitions(max_buffered_bytes=BUDGET)
    partitions.assign([tp(0)])
    await promptly_fetch(partitions)
    large = b"x" * (2 * RECORD - _record_bytes(Message()))  # A record of two test records' bytes.
    partitions.buffer(records(0, 11, large))  # 22 records' bytes, above the pause threshold of 20.
    assert partitions.fetch_size == 10
    assert (await promptly_fetch(partitions)).pause == [tp(0)]


async def test_a_buffer_that_drains_within_the_poll_timeout_is_waited_for_instead_of_paused() -> None:
    partitions = Partitions(max_buffered_bytes=BUDGET)
    (partition,) = partitions.assign([tp(0)]).partitions
    await promptly_fetch(partitions)
    partitions.buffer(records(0, 25))
    partition.handler_seconds = 0.001  # Its 5 records above the pause threshold take 5ms.
    fetches: list[Fetch] = []
    with anyio.fail_after(POLL_TIMEOUT / 2):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(lambda: _append_fetch(partitions, fetches))
            await anyio.wait_all_tasks_blocked()
            assert not fetches
            drain(partition, 5)
    assert fetches[0].pause == []


async def test_a_shrinking_share_pauses_an_over_full_partition_before_the_next_fetch() -> None:
    partitions = Partitions(max_buffered_bytes=BUDGET)
    partitions.assign([tp(0)])
    await promptly_fetch(partitions)
    partitions.buffer(records(0, 20))  # At the pause threshold of a 40 record share.
    partitions.assign([tp(1)])
    assert partitions.fetch_size == 10
    assert (await promptly_fetch(partitions)).pause == [tp(0)]


async def test_a_growing_share_resumes_a_paused_partition_already_at_its_new_resume_threshold() -> None:
    partitions = Partitions(max_buffered_bytes=BUDGET)
    partition, _ = partitions.assign([tp(0), tp(1)]).partitions  # Shares of 20: pause above 10, resume at 5.
    await promptly_fetch(partitions)
    partitions.buffer(records(0, 11))
    assert (await promptly_fetch(partitions)).pause == [tp(0)]
    drain(partition, 3)
    partitions.revoke([tp(1)])  # A share of 40: pause above 20, resume at 10, above the 8 records left.
    assert (await promptly_fetch(partitions)).resume == [tp(0)]


async def test_a_paused_partition_resumes_once_it_drains_to_its_resume_threshold() -> None:
    partitions = Partitions(max_buffered_bytes=BUDGET)  # Pause above 20, resume at 10.
    (partition,) = partitions.assign([tp(0)]).partitions
    await promptly_fetch(partitions)
    partitions.buffer(records(0, 21))
    assert (await promptly_fetch(partitions)).pause == [tp(0)]
    drain(partition, 10)
    assert (await promptly_fetch(partitions)).resume == []
    drain(partition, 1)
    assert (await promptly_fetch(partitions)).resume == [tp(0)]
    assert not partition.paused


async def test_a_partition_revoked_during_a_fetch_drops_its_records_and_its_reassignment_starts_paused() -> None:
    partitions = Partitions(max_buffered_bytes=BUDGET)
    # An assignment resumes its partitions, because librdkafka keeps the pause of an earlier one.
    assert partitions.assign([tp(0)]).resume == [tp(0)]
    await promptly_fetch(partitions)
    # Within one fetch, the group revokes and reassigns the partition, and the fetch returns a record of the old assignment.
    assert [old.key for old in partitions.revoke([tp(0)])] == [tp(0)]
    assigned = partitions.assign([tp(0, offset=5)])
    (partition,) = assigned.partitions
    assert (assigned.pause, assigned.resume) == ([tp(0)], [])
    assert partition.paused
    assert partition.key.offset == OFFSET_INVALID
    partitions.buffer(records(0, 1))
    assert not partition.records
    assert (await promptly_fetch(partitions)).resume == [tp(0)]
    partitions.buffer(records(0, 1))
    assert len(partition.records) == 1


async def test_a_stopped_partition_is_paused_drops_later_records_and_never_resumes() -> None:
    partitions = Partitions(max_buffered_bytes=BUDGET)
    (partition,) = partitions.assign([tp(0)]).partitions
    await promptly_fetch(partitions)
    failure = ValueError("bad record")
    partitions.stop_failed(partition, failure)
    assert (await promptly_fetch(partitions)).pause == [tp(0)]
    partitions.buffer(records(0, 1))  # Fetched before the pause took effect.
    assert not partition.records
    # A share change resumes paused partitions at or below their resume threshold, except stopped ones.
    partitions.assign([tp(1)])
    partitions.revoke([tp(1)])
    assert (await promptly_fetch(partitions)).resume == []
    assert partitions.status()[0].failure is failure


async def test_a_fetch_error_of_an_assigned_partition_stops_only_it() -> None:
    """Its records before the error stay buffered, its later ones are dropped, and other partitions carry on."""
    partitions = Partitions(max_buffered_bytes=BUDGET)
    zero, one = partitions.assign([tp(0), tp(1)]).partitions
    error = Message(topic="t", partition=0, offset=2, error=KafkaError(KafkaError._AUTO_OFFSET_RESET))
    assert partitions.buffer([*records(0, 2), error, *records(0, 5)[3:], *records(1, 3)]) == [zero]
    assert isinstance(zero.failure, KafkaException)
    assert zero.failure.args[0].code() == KafkaError._AUTO_OFFSET_RESET
    assert zero.failure.__notes__ == ["fetching t[0]"]
    assert [record.offset() for record in zero.records] == [0, 1]
    assert (len(one.records), one.failure) == (3, None)
    assert (await promptly_fetch(partitions)).pause == [tp(0)]
    assert partitions.buffer([error]) == []  # Stopped once.


async def test_other_record_errors_and_records_of_unassigned_partitions_fail_the_fetch() -> None:
    partitions = Partitions(max_buffered_bytes=BUDGET)
    partitions.assign([tp(0)])
    with pytest.raises(KafkaException):
        partitions.buffer([Message(error=KafkaError(KafkaError._FATAL))])  # Of no partition.
    with pytest.raises(KafkaException):
        partitions.buffer([Message(topic="t", partition=1, offset=0, error=KafkaError(KafkaError._AUTO_OFFSET_RESET))])
    with pytest.raises(RuntimeError, match="unassigned"):
        partitions.buffer(records(1, 1))
    not_stopping = Partitions(max_buffered_bytes=BUDGET, stop_failed_partitions=False)
    not_stopping.assign([tp(0)])
    with pytest.raises(KafkaException):
        not_stopping.buffer(
            [Message(topic="t", partition=0, offset=0, error=KafkaError(KafkaError._AUTO_OFFSET_RESET))]
        )


async def test_with_every_partition_paused_a_fetch_does_not_block_and_the_next_waits_for_a_resume() -> None:
    partitions = Partitions(max_buffered_bytes=BUDGET)  # Pause above 20, resume at 10.
    (partition,) = partitions.assign([tp(0)]).partitions
    await promptly_fetch(partitions)
    partitions.buffer(records(0, 21))
    fetch = await promptly_fetch(partitions)
    assert (fetch.pause, fetch.timeout) == ([tp(0)], 0)
    partitions.buffer([])  # The fetch found nothing to return.
    fetches: list[Fetch] = []
    with anyio.fail_after(POLL_TIMEOUT / 2):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(lambda: _append_fetch(partitions, fetches))
            await anyio.wait_all_tasks_blocked()
            assert not fetches
            drain(partition, 11)
    assert fetches[0].resume == [tp(0)]


async def _append_fetch(partitions: Partitions, fetches: list[Fetch]) -> None:
    fetches.append(await partitions.next_fetch())
