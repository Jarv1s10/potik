"""Limit concurrent handler calls, in total and per topic."""

from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager, nullcontext
from typing import cast

import anyio

from potik.core.middleware import BaseMiddleware
from potik.core.topics import _validate_topics
from potik.core.types import ConsumerRecord, ProducerRecord, RecordHandler

__all__ = [
    "ConcurrencyMiddleware",
]

_NO_LIMIT: AbstractAsyncContextManager[object] = nullcontext()


def _validate_limit(limit: object, name: str) -> None:
    if limit is not None and (type(limit) is not int or limit < 1):
        raise ValueError(f"{name} must be a positive integer or None, got {limit!r}")


class ConcurrencyMiddleware(BaseMiddleware):
    """Limits how many handler calls run at once across partitions, in total and per topic.

    A call takes a slot of its topic first, then a shared one, so a call that waits for its topic holds no shared slot. Only the middleware after it runs with a slot, so put it last. Exception rules before it then do not hold a slot during retry backoff. `potik.build_runtime()` adds one from its own `max_concurrency` and its handlers', after the exception rules.

    Args:
        max_concurrency: The maximum number of handler calls that run at once across all topics, e.g. the size of a database pool, or None for no limit.
        per_topic: The maximum number of handler calls that run at once for each topic. Topics missing from it have no limit of their own.

    Raises:
        ValueError: If a limit is not a positive integer, or a topic name is invalid.
    """

    def __init__(self, *, max_concurrency: int | None = None, per_topic: Mapping[str, int] | None = None) -> None:
        per_topic = per_topic or {}
        _validate_limit(max_concurrency, "max_concurrency")
        for topic, limit in zip(_validate_topics(per_topic), per_topic.values(), strict=True):
            _validate_limit(limit, f"the limit of topic {topic!r}")
        self._total = _NO_LIMIT if max_concurrency is None else anyio.CapacityLimiter(max_concurrency)
        self._per_topic = {topic: anyio.CapacityLimiter(limit) for topic, limit in per_topic.items()}

    async def handle(self, record: ConsumerRecord, call_next: RecordHandler) -> Sequence[ProducerRecord]:
        async with self._per_topic.get(cast(str, record.topic()), _NO_LIMIT), self._total:
            return await call_next(record)
