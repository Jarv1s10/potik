"""How offset policies pick a newly assigned partition's start."""

import time
from typing import cast

import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from potik.core import InvalidOffsetError, OffsetContext, resume_or_earliest, resume_or_fail, resume_or_latest
from potik.core.consumer import _watermarks
from potik.core.offsets import _resolve_offset


def context(committed: int | None, reads: list[None] | None = None) -> OffsetContext:
    def watermarks() -> tuple[int, int]:
        if reads is not None:
            reads.append(None)
        return 5, 9

    return OffsetContext("orders", 0, committed, watermarks=watermarks)


def test_policies_resume_the_committed_offset_without_reading_the_log() -> None:
    reads: list[None] = []
    for policy in (resume_or_earliest, resume_or_latest, resume_or_fail):
        assert _resolve_offset(policy, context(7, reads)) == 7
    assert reads == []


def test_policies_differ_only_without_a_committed_offset() -> None:
    reads: list[None] = []
    assert _resolve_offset(resume_or_earliest, context(None, reads)) == 5
    assert _resolve_offset(resume_or_latest, context(None)) == 9
    with pytest.raises(InvalidOffsetError, match="no committed offset"):
        _resolve_offset(resume_or_fail, context(None))
    assert len(reads) == 1  # Read once, for the policy and the range check.


def test_a_position_outside_the_log_is_rejected() -> None:
    with pytest.raises(InvalidOffsetError, match=r"within the log \[5, 9\]"):
        _resolve_offset(lambda _: 10, context(None))


class FlakyConsumer:
    """Fails its watermark reads with `errors`, in order, then returns the log's bounds."""

    def __init__(self, *errors: int) -> None:
        self.errors = list(errors)

    def get_watermark_offsets(self, position: TopicPartition, timeout: float) -> tuple[int, int]:
        if self.errors:
            raise KafkaException(KafkaError(self.errors.pop(0)))
        return 5, 9


def test_reading_the_log_bounds_retries_a_lost_connection() -> None:
    consumer = FlakyConsumer(KafkaError._TRANSPORT, KafkaError._DESTROY_BROKER)
    deadline = time.monotonic() + 10
    assert _watermarks(cast(Consumer, consumer), TopicPartition("orders", 0), deadline) == (5, 9)


def test_reading_the_log_bounds_fails_on_other_errors_and_at_the_deadline() -> None:
    position = TopicPartition("orders", 0)
    with pytest.raises(KafkaException, match="UNKNOWN_TOPIC_OR_PART"):
        consumer = FlakyConsumer(KafkaError.UNKNOWN_TOPIC_OR_PART)
        _watermarks(cast(Consumer, consumer), position, time.monotonic() + 10)
    with pytest.raises(TimeoutError, match=r"orders\[0\]"):
        _watermarks(cast(Consumer, FlakyConsumer(*[KafkaError._TRANSPORT] * 100)), position, time.monotonic() + 0.3)
