"""The topic check without a broker: its timeout, and a start cancelled while it waits."""

import time

import anyio
import pytest
from confluent_kafka import KafkaError, KafkaException

from potik.core import ProducerScope
from potik.middleware import TopicCheckMiddleware

pytestmark = pytest.mark.anyio

# No broker listens there.
UNREACHABLE = ProducerScope(client_options={"bootstrap.servers": "127.0.0.1:1", "log_level": 0})


async def test_the_check_fails_once_no_broker_answers_within_its_timeout() -> None:
    started = time.monotonic()
    with pytest.raises(KafkaException) as raised:
        async with TopicCheckMiddleware(["orders", "audit"], timeout=0.5).lifespan(UNREACHABLE):
            pass
    assert raised.value.args[0].code() == KafkaError._TIMED_OUT
    assert time.monotonic() - started < 2  # One request for every topic, not one each.


async def test_cancelling_the_start_stops_the_check() -> None:
    """A SIGTERM during startup must not wait out the check's timeout."""
    with anyio.fail_after(2), anyio.move_on_after(0.2):
        async with TopicCheckMiddleware(["orders"], timeout=30).lifespan(UNREACHABLE):
            pytest.fail("the check passed without a broker")


async def test_no_topics_pass_without_a_broker() -> None:
    with anyio.fail_after(2):
        async with TopicCheckMiddleware([]).lifespan(UNREACHABLE):
            pass
