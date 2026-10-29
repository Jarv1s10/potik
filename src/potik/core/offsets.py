"""Offset policies, which pick where a newly assigned partition starts."""

from collections.abc import Callable

from potik.core.errors import InvalidOffsetError

__all__ = [
    "OffsetContext",
    "OffsetPolicy",
    "resume_or_earliest",
    "resume_or_fail",
    "resume_or_latest",
]


class OffsetContext:
    """What an offset policy decides on: a newly assigned partition, its committed offset, and its log's bounds.

    Args:
        topic: The partition's topic.
        partition: The partition's number.
        committed: The committed offset, or None if there is none.
        watermarks: Returns the log's first and end offsets. The context calls it only once a policy reads `earliest` or `latest`, and at most once, because the runtime asks the broker. In tests, pass e.g. `lambda: (0, 100)`.

    Attributes:
        topic: The partition's topic.
        partition: The partition's number.
        committed: The committed offset, or None if there is none.
    """

    def __init__(
        self, topic: str, partition: int, committed: int | None, *, watermarks: Callable[[], tuple[int, int]]
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.committed = committed
        self._read_watermarks = watermarks
        self._bounds: tuple[int, int] | None = None

    @property
    def earliest(self) -> int:
        """The log's first offset."""
        return self._watermarks()[0]

    @property
    def latest(self) -> int:
        """The log's end offset, which the next record gets."""
        return self._watermarks()[1]

    def _watermarks(self) -> tuple[int, int]:
        if self._bounds is None:
            self._bounds = self._read_watermarks()
        return self._bounds


type OffsetPolicy = Callable[[OffsetContext], int]
"""A function that returns where a newly assigned partition starts, e.g. `resume_or_earliest`.

It returns the committed offset or an offset within the log, else the run fails with `InvalidOffsetError`. If it returns another offset than the committed one, the runtime commits it before handling, so a restart resumes from it. An offset below the committed one handles records again, whose outputs may already be committed, and an offset above it skips records. It runs on the consumer's thread during the rebalance, so it must be synchronous and return quickly. `rebalance_timeout_ms` bounds the whole assignment.
"""


def resume_or_latest(context: OffsetContext) -> int:
    """Resume the committed offset, or start at the end of the log."""
    return context.latest if context.committed is None else context.committed


def resume_or_earliest(context: OffsetContext) -> int:
    """Resume the committed offset, or start at the beginning of the log."""
    return context.earliest if context.committed is None else context.committed


def resume_or_fail(context: OffsetContext) -> int:
    """Resume the committed offset, or fail the run with `InvalidOffsetError`."""
    if context.committed is None:
        raise InvalidOffsetError(
            f"no committed offset for {context.topic}[{context.partition}], which resume_or_fail requires; "
            "use resume_or_earliest or resume_or_latest to start partitions without one"
        )
    return context.committed


def _resolve_offset(policy: OffsetPolicy, context: OffsetContext) -> int:
    offset = policy(context)
    if type(offset) is not int:
        raise InvalidOffsetError(
            f"offset policy returned {offset!r} for {context.topic}[{context.partition}], not an int"
        )
    # The client range-checks a committed offset (auto.offset.reset=error).
    if offset != context.committed and not context.earliest <= offset <= context.latest:
        raise InvalidOffsetError(
            f"offset policy picked {offset} for {context.topic}[{context.partition}], which is neither the "
            f"committed offset {context.committed} nor within the log [{context.earliest}, {context.latest}]"
        )
    return offset
