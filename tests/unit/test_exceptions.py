"""Exception rules, composed around a checked handler as a run composes them."""

from collections.abc import Sequence

import anyio
import pytest
from confluent_kafka import Message

from potik import ConsumerRecord, InvalidOutputError, ProducerRecord
from potik.core.middleware import _chain, _checked
from potik.middleware import ExceptionContext, ExceptionMiddleware, RetryAfter, dead_letter, on, reraise

pytestmark = pytest.mark.anyio


async def test_fail_stops_the_rules_before_a_rule_for_every_exception() -> None:
    """An invalid result stops the partition instead of reaching the dead letter rule."""

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        return ProducerRecord("accepted")  # ty: ignore[invalid-return-type]

    rules = (on(InvalidOutputError, reraise()), on(Exception, dead_letter("dead")))
    handle = _chain([ExceptionMiddleware(*rules)], "handle", _checked(handler))
    with pytest.raises(InvalidOutputError, match="the handler returned a ProducerRecord, not a list or tuple"):
        await handle(Message(topic="orders", partition=0, offset=0))


def test_dead_letters_add_the_headers_function_s_headers_after_the_record_s() -> None:
    def headers(context: ExceptionContext[Exception]) -> list[tuple[str, str]]:
        return [("kafka_dlt-original-topic", context.record.topic())]

    record = Message(topic="orders", partition=0, offset=0, key=b"k", value=b"v", headers=[("tenant", b"acme")])
    context = ExceptionContext(exception=TimeoutError(), record=record, attempt=1)
    assert dead_letter("dead", headers=headers)(context) == (
        ProducerRecord("dead", b"k", b"v", [("tenant", b"acme"), ("kafka_dlt-original-topic", "orders")]),
    )


async def test_rules_see_the_single_exception_of_a_task_group() -> None:
    """A group of several exceptions stays a group, and an unhandled exception propagates as the handler raised it."""
    attempts = 0

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        nonlocal attempts
        attempts += 1
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(anyio.sleep_forever)
            if attempts == 1:
                raise ExceptionGroup("nested", [TimeoutError("first")])
            raise ValueError("second")
        return []

    seen: list[Exception] = []

    def retry_timeouts(context: ExceptionContext[TimeoutError]) -> RetryAfter:
        seen.append(context.exception)
        return RetryAfter(0)

    handle = _chain([ExceptionMiddleware(on(TimeoutError, retry_timeouts))], "handle", _checked(handler))
    with pytest.raises(ExceptionGroup) as caught:
        await handle(Message(topic="orders", partition=0, offset=0))
    assert [type(exc) for exc in seen] == [TimeoutError]
    assert caught.group_contains(ValueError, match="second")

    several = ExceptionGroup("several", [TimeoutError(), TimeoutError()])

    async def failing(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        raise several

    handle = _chain([ExceptionMiddleware(on(TimeoutError, retry_timeouts))], "handle", _checked(failing))
    with pytest.raises(ExceptionGroup) as caught:
        await handle(Message(topic="orders", partition=0, offset=0))
    assert caught.value is several
