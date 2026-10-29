"""Concurrency limits as middleware, composed with the middleware they sit inside."""

from collections.abc import Sequence

import anyio
import pytest
from confluent_kafka import Message

from potik import ConsumerRecord, ProducerRecord
from potik.core.middleware import _chain
from potik.middleware import ConcurrencyMiddleware, ExceptionMiddleware, on, retry

pytestmark = pytest.mark.anyio


def record(topic: str) -> ConsumerRecord:
    return Message(topic=topic, partition=0, offset=0)


async def test_retry_backoff_holds_no_slot() -> None:
    attempts = 0

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        nonlocal attempts
        if record.topic() == "a":
            attempts += 1
            raise TimeoutError
        return ()

    retrying = ExceptionMiddleware(on(TimeoutError, retry(max_attempts=2, backoff=lambda _: 60)))
    handle = _chain([retrying, ConcurrencyMiddleware(max_concurrency=1)], "handle", handler)
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(handle, record("a"))
        await anyio.wait_all_tasks_blocked()  # "a" now backs off before its second attempt.
        with anyio.fail_after(1):
            assert await handle(record("b")) == ()
        tasks.cancel_scope.cancel()
    assert attempts == 1


async def test_waiting_for_a_topic_slot_holds_no_shared_slot() -> None:
    release = anyio.Event()

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        if record.topic() == "a":
            await release.wait()
        return ()

    handle = _chain([ConcurrencyMiddleware(max_concurrency=2, per_topic={"a": 1})], "handle", handler)
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(handle, record("a"))  # holds the slot of "a" and a shared one
        tasks.start_soon(handle, record("a"))  # waits for the slot of "a"
        await anyio.wait_all_tasks_blocked()
        with anyio.fail_after(1):
            assert await handle(record("b")) == ()
        release.set()
