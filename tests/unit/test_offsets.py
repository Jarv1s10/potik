"""How offset policies pick a newly assigned partition's start."""

import pytest

from potik.core import InvalidOffsetError, OffsetContext, resume_or_earliest, resume_or_fail, resume_or_latest
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
