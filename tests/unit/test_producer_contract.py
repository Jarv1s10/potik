"""The Producer's contract without a broker: its owned options, its lifecycle, and failed deliveries."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import anyio
import pytest
from confluent_kafka import KafkaError, KafkaException

from potik import Config, Delivery, Producer, ProducerRecord
from potik.core import BaseMiddleware, ProducerScope, RunScope
from potik.core.types import RecordSender

pytestmark = pytest.mark.anyio

# No broker listens there, so every send times out after message.timeout.ms.
UNREACHABLE = "127.0.0.1:1"
TIMEOUT = {"message.timeout.ms": 300, "log_level": 0}


def test_options_cannot_weaken_its_guarantees() -> None:
    for option in ("acks", "enable.idempotence", "transactional.id", "on_delivery"):
        with pytest.raises(ValueError, match="settings that the producer manages"):
            Producer(UNREACHABLE, options={option: "x"})


def test_from_config_reuses_the_runtimes_client_options() -> None:
    config = Config(
        bootstrap_servers="kafka:9092",
        group_id="g",
        options={"security.protocol": "SSL"},
        producer_options={"linger.ms": 7},
    )
    producer = Producer.from_config(config)
    assert producer._config["security.protocol"] == "SSL"
    assert producer._config["linger.ms"] == 7
    assert producer._config["bootstrap.servers"] == "kafka:9092"


async def test_sends_only_while_open_and_opens_once() -> None:
    producer = Producer(UNREACHABLE, options=TIMEOUT)
    with pytest.raises(RuntimeError, match="enter the producer"):
        await producer.send(ProducerRecord("orders", b"k", b"v"))
    async with producer:
        with pytest.raises(RuntimeError, match="opens only once"):
            await producer.__aenter__()
    with pytest.raises(RuntimeError, match="enter the producer"):
        await producer.send(ProducerRecord("orders", b"k", b"v"))
    with pytest.raises(RuntimeError, match="opens only once"):
        await producer.__aenter__()


async def test_an_undelivered_send_raises() -> None:
    async with Producer(UNREACHABLE, options=TIMEOUT) as producer:
        with pytest.raises(KafkaException) as raised:
            await producer.send(ProducerRecord("orders", b"k", b"v"))
    assert raised.value.args[0].code() == KafkaError._MSG_TIMED_OUT
    assert producer.failure is None  # A timeout is not fatal.


async def test_exit_waits_for_and_raises_an_abandoned_send() -> None:
    """A cancelled send stays queued, so the exit waits for its report, and raises the failure nobody saw."""
    with pytest.raises(KafkaException) as raised:
        async with Producer(UNREACHABLE, options=TIMEOUT) as producer:
            with anyio.move_on_after(0.05):
                await producer.send(ProducerRecord("orders", b"k", b"v"))
    assert raised.value.args[0].code() == KafkaError._MSG_TIMED_OUT


async def test_exit_fails_the_sends_left_after_close_timeout() -> None:
    """An outage would hold a send for message.timeout.ms, past the supervisor's deadline."""
    with pytest.raises(ValueError, match="close_timeout must not be negative"):
        Producer(UNREACHABLE, close_timeout=-1)
    with anyio.fail_after(5), pytest.raises(KafkaException) as raised:
        async with Producer(UNREACHABLE, options={"log_level": 0}, close_timeout=0.2) as producer:
            with anyio.move_on_after(0.05):
                await producer.send(ProducerRecord("orders", b"k", b"v"))
    assert raised.value.args[0].code() == KafkaError._PURGE_QUEUE


async def test_middleware_wraps_each_send() -> None:
    seen: list[tuple[str, object]] = []

    class Recording(BaseMiddleware):
        def __init__(self, name: str) -> None:
            self.name = name

        async def send(self, record: ProducerRecord, call_next: RecordSender) -> Delivery:
            seen.append((self.name, record.headers))
            try:
                return await call_next(record._replace(headers=[("by", self.name.encode())]))
            except KafkaException as exc:
                seen.append((self.name, exc.args[0].code()))
                raise

    producer = Producer(UNREACHABLE, options=TIMEOUT, middleware=[Recording("outer")])
    async with producer:
        with pytest.raises(KafkaException):
            await producer.send(ProducerRecord("orders", b"k", b"v"))
    assert seen == [("outer", None), ("outer", KafkaError._MSG_TIMED_OUT)]


async def test_middleware_lifespans_surround_the_open_producer() -> None:
    """Lifespans enter outermost first, and exit in reverse with the owner's error, which they cannot suppress."""
    events: list[str] = []

    class Lifespan(BaseMiddleware):
        def __init__(self, name: str) -> None:
            self.name = name

        @asynccontextmanager
        async def lifespan(self, scope: RunScope | ProducerScope) -> AsyncGenerator[None]:
            assert isinstance(scope, ProducerScope)
            events.append(f"enter {self.name}")
            try:
                yield
            except ValueError:
                events.append(f"exit {self.name}")  # and suppress it

    middleware = [Lifespan("outer"), Lifespan("inner")]
    with pytest.raises(ValueError, match="owner"):
        async with Producer(UNREACHABLE, options=TIMEOUT, middleware=middleware):
            assert events == ["enter outer", "enter inner"]
            raise ValueError("owner")
    assert events == ["enter outer", "enter inner", "exit inner", "exit outer"]


async def test_a_dead_polling_thread_fails_pending_and_later_sends() -> None:
    """A callback that raises stops the polling thread, so no delivery report comes. The exit must not wait for one."""

    broken = False

    def error_cb(error: KafkaError) -> None:
        nonlocal broken
        if not broken:  # Once, or closing the client raises it again.
            broken = True
            raise RuntimeError("a broken callback")

    # Closing the client waits for the queued record until message.timeout.ms.
    producer = Producer(UNREACHABLE, options={"error_cb": error_cb, "message.timeout.ms": 1000, "log_level": 0})
    with anyio.fail_after(5):
        async with producer:
            with pytest.raises(KafkaException, match="a broken callback") as raised:
                await producer.send(ProducerRecord("orders", b"k", b"v"))
            assert raised.value.args[0].fatal()
            assert producer.failure is not None
            with pytest.raises(KafkaException, match="a broken callback"):
                await producer.send(ProducerRecord("orders", b"k", b"v"))


async def test_a_middleware_instance_serves_one_owner_once() -> None:
    """An instance listed twice, or entered by an earlier owner, would share its state, so the open fails."""

    @dataclass(frozen=True, slots=True)
    class Frozen(BaseMiddleware):
        name: str

    twice = Frozen("twice")
    with pytest.raises(RuntimeError, match="this Frozen was already used"):
        await Producer(UNREACHABLE, options=TIMEOUT, middleware=[twice, twice]).__aenter__()
    used = Frozen("used")
    async with Producer(UNREACHABLE, options=TIMEOUT, middleware=[used]):
        pass
    with pytest.raises(RuntimeError, match="this Frozen was already used"):
        await Producer(UNREACHABLE, options=TIMEOUT, middleware=[used]).__aenter__()
    async with Producer(UNREACHABLE, options=TIMEOUT, middleware=[Frozen("used")]):
        pass  # An equal instance is another instance.
