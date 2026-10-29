"""A member that the group fences while it drains must not commit for partitions that another member now owns."""

import socket
import struct
import threading
import time
from collections.abc import Iterator
from contextlib import suppress

import anyio
import pytest
from confluent_kafka import KafkaError, KafkaException
from harness import FAST_BROKER, SESSION_TIMEOUT_MS
from kafka_broker import Broker

from potik.core import ConsumerRecord

pytestmark = [pytest.mark.slow, pytest.mark.anyio]

VICTIM = "victim"


class Proxy:
    """Forwards every connection to the broker, and holds the traffic of the clients named `client.id=victim` while frozen."""

    def __init__(self, upstream: tuple[str, int]) -> None:
        self.upstream = upstream
        self.frozen = threading.Event()
        self._listener = socket.create_server(("localhost", 0))
        self.port = self._listener.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    def close(self) -> None:
        self._listener.close()

    def _accept(self) -> None:
        with suppress(OSError):  # The listener closed.
            while True:
                client, _ = self._listener.accept()
                broker = socket.create_connection(self.upstream)
                victim = threading.Event()
                threading.Thread(target=self._pump, args=(client, broker, victim, True), daemon=True).start()
                threading.Thread(target=self._pump, args=(broker, client, victim, False), daemon=True).start()

    def _pump(self, source: socket.socket, target: socket.socket, victim: threading.Event, requests: bool) -> None:
        first = requests
        with suppress(OSError):
            while data := source.recv(65536):
                if first:
                    # A request header: size, API key, API version, correlation ID, then the client ID as a string.
                    (length,) = struct.unpack_from(">h", data, 12)
                    if data[14 : 14 + length].decode() == VICTIM:
                        victim.set()
                    first = False
                while victim.is_set() and self.frozen.is_set():
                    time.sleep(0.05)
                target.sendall(data)
        for end in (source, target):
            with suppress(OSError):
                end.shutdown(socket.SHUT_RDWR)


@pytest.fixture
def proxy() -> Iterator[Proxy]:
    """A broker of the test's own, which advertises a proxy in front of it."""
    broker = Broker(**FAST_BROKER)
    host, port = broker.bootstrap.split(":")
    proxy = Proxy((host, int(port)))
    advertised = f"PLAINTEXT://localhost:{proxy.port},INTERNAL://localhost:29092"
    broker.container.with_env("KAFKA_ADVERTISED_LISTENERS", advertised)
    try:
        with broker:
            yield proxy
    finally:
        proxy.close()


@pytest.fixture
def bootstrap(proxy: Proxy) -> str:
    return f"localhost:{proxy.port}"


async def test_member_fenced_while_draining_commits_nothing(workload, proxy) -> None:
    """The group expires a member whose handler outlasts its session during the drain, and gives its partition away.

    The member's final commit must then fail, instead of committing an offset behind the new owner's.
    """
    work = workload(1, 100)
    blocked, release = anyio.Event(), threading.Event()

    async def handler(record: ConsumerRecord) -> None:
        if record.offset() == 5:
            blocked.set()
            await anyio.to_thread.run_sync(release.wait)

    victim = work.runtime(
        handler,
        commit_interval_ms=60_000,  # Keeps the records before the blocked one uncommitted until the drain.
        shutdown_grace_ms=60_000,
        rebalance_timeout_ms=30_000,
        max_poll_interval_ms=120_000,
        options={"client.id": VICTIM},
    )
    stop = anyio.CancelScope()
    failure: Exception | None = None

    async def run_victim() -> None:
        nonlocal failure
        with stop:
            try:
                await victim.run(handle_signals=False)
            except Exception as exc:  # noqa: BLE001 -- asserted below
                failure = exc

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(run_victim)
        await blocked.wait()
        stop.cancel()  # A graceful shutdown, which stops polling and waits for the blocked handler.
        await anyio.sleep(1)  # The poller stops, so nothing serves the loss of the partition.
        proxy.frozen.set()
        await anyio.sleep(SESSION_TIMEOUT_MS / 1000 + 4)  # The group expires the victim's session.
        await work.run_until_committed(work.runtime())
        proxy.frozen.clear()
        release.set()
    # The member stays fenced, because librdkafka rejoins only once the poller serves the loss.
    assert isinstance(failure, KafkaException), failure
    assert failure.args[0].code() == KafkaError.UNKNOWN_MEMBER_ID, failure
    work.assert_exactly_once()
