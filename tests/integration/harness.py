"""The integration tests' broker, topics, and apps."""

import os
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path

import anyio
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition
from confluent_kafka.admin import AdminClient

import potik
from potik import Config, ConsumerRecord, ProducerRecord, Router, RuntimeStatus, resume_or_earliest
from potik.core import BaseMiddleware, RecordHandler, Runtime
from potik.middleware import Rule

APP = Path(__file__).with_name("app.py")
# The test broker's KIP-848 session timeout, instead of 45s. A killed member's partitions wait until it expires.
SESSION_TIMEOUT_MS = 6_000
# Kafka's default 1GB heap is far more than the tests need. Smaller brokers let a CI runner fit one per worker.
SMALL_BROKER = {"heap_opts": "-Xmx256m -Xms256m"}
FAST_BROKER = {
    **SMALL_BROKER,
    "group_consumer_min_session_timeout_ms": SESSION_TIMEOUT_MS,
    "group_consumer_session_timeout_ms": SESSION_TIMEOUT_MS,
    "group_consumer_min_heartbeat_interval_ms": 1000,
    "group_consumer_heartbeat_interval_ms": 1000,
    # Abort a crashed producer's timed out transaction within a second, not 10s.
    "transaction_abort_timed_out_transaction_cleanup_interval_ms": 1000,
}


def read_committed(bootstrap: str, topic: str, group: str, timeout: float = 30) -> list[Message]:
    """Return a topic's read_committed records up to its last stable offsets, read without joining `group`."""
    reader = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "isolation.level": "read_committed",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )
    partitions = reader.list_topics(topic, timeout=10).topics[topic].partitions
    reader.assign([TopicPartition(topic, p, 0) for p in partitions])
    records: list[Message] = []
    pending = set(partitions)
    deadline = time.monotonic() + timeout
    try:
        while pending:
            assert time.monotonic() < deadline, f"partitions {sorted(pending)} of {topic} never ended"
            for message in reader.consume(10_000, 0.5):
                if (error := message.error()) is None:
                    records.append(message)
                elif error.code() == KafkaError._PARTITION_EOF:
                    pending.discard(message.partition())
                else:
                    raise KafkaException(error)
    finally:
        reader.close()
    return records


@dataclass(frozen=True)
class Workload:
    """An input topic of `records` keyed `0..records-1` round-robin over partitions, and an empty output topic."""

    bootstrap: str
    source: str
    target: str
    group: str
    partitions: int
    records: int
    admin: AdminClient = field(repr=False, compare=False)

    def committed(self) -> list[int]:
        """Return the group's committed input offset per partition, or -1 if none, read as a new owner would."""
        consumer = Consumer({"bootstrap.servers": self.bootstrap, "group.id": self.group})
        try:
            positions = consumer.committed([TopicPartition(self.source, p) for p in range(self.partitions)], timeout=30)
        finally:
            consumer.close()
        return [tp.offset if tp.offset >= 0 else -1 for tp in positions]

    def wait_committed(self, timeout: float, expected: list[int] | None = None) -> list[int]:
        """Wait until the committed offsets reach `expected`, by default the whole input, or `timeout` passes.

        Return the last committed offsets read.
        """
        expected = expected or [self.records // self.partitions] * self.partitions
        deadline = time.monotonic() + timeout
        while True:
            try:
                offsets = self.committed()
            except KafkaException:  # e.g. NOT_COORDINATOR while a restarted broker loads
                if time.monotonic() > deadline:
                    raise
                offsets = None
            if offsets == expected or time.monotonic() > deadline:
                return offsets or []
            time.sleep(0.2)

    def wait_progress(self, app: "Process", timeout: float = 60) -> None:
        """Wait until `app` commits some of the input."""
        deadline = time.monotonic() + timeout
        while True:
            with suppress(KafkaException):  # e.g. NOT_COORDINATOR while the broker loads offsets
                if any(offset > 0 for offset in self.committed()):
                    return
            assert app.alive(), f"the instance died on its own, see {app.log}"
            assert time.monotonic() < deadline, f"nothing was committed, see {app.log}"
            time.sleep(0.2)

    def outputs(self) -> list[Message]:
        """Return the output topic's read_committed records."""
        return read_committed(self.bootstrap, self.target, f"verify-{self.group}")

    def assert_exactly_once(self) -> None:
        assert self.committed() == [self.records // self.partitions] * self.partitions
        seen: Counter[bytes] = Counter()
        for message in self.outputs():
            key = message.key()
            assert key is not None
            assert int(key) % self.partitions == message.partition(), "output left its input's partition"
            seen[key] += 1
        duplicates = {key: count for key, count in seen.items() if count > 1}
        missing = self.records - len(seen)
        assert not duplicates and not missing, f"{len(duplicates)} duplicated keys, {missing} missing keys"

    def runtime(
        self,
        handler: Callable[[ConsumerRecord], Awaitable[bytes | None]] | None = None,
        *,
        middleware: Sequence[BaseMiddleware] = (),
        key_concurrency: int | None = None,
        **config,
    ) -> Runtime:
        """Make an in-process core service that copies each input to its partition of the output topic.

        `handler` runs first on each input, and may return the output's value instead of the input's. `key_concurrency` runs the input by key.
        """
        return Runtime(
            self.config(**config),
            [self.source],
            self._copy(handler),
            middleware=middleware,
            key_concurrency={self.source: key_concurrency} if key_concurrency else None,
        )

    def typed(
        self,
        handler: Callable[[ConsumerRecord], Awaitable[bytes | None]] | None = None,
        *,
        middleware: Sequence[BaseMiddleware] = (),
        exception_rules: Sequence[Rule[Exception]] = (),
        **config,
    ) -> Runtime:
        """Make a runtime with `potik.build_runtime()` that copies like `runtime()`, with a router's handler."""
        router = Router()
        router.handler(self.source)(self._copy(handler))
        return potik.build_runtime(
            self.config(**config), [router], middleware=middleware, exception_rules=exception_rules
        )

    def config(self, **config) -> Config:
        """Return a `Config` of the workload's broker and group, with `config`'s fields on top."""
        return Config(bootstrap_servers=self.bootstrap, group_id=self.group, offset_policy=resume_or_earliest, **config)

    def _copy(self, handler: Callable[[ConsumerRecord], Awaitable[bytes | None]] | None) -> RecordHandler:
        async def copy(record: ConsumerRecord) -> list[ProducerRecord]:
            value = await handler(record) if handler is not None else None
            value = record.value() if value is None else value
            return [ProducerRecord(self.target, record.key(), value, partition=record.partition())]

        return copy

    async def run_until_committed(self, service: Runtime, expected: list[int] | None = None) -> RuntimeStatus:
        """Run `service` until it commits `expected`, by default the whole input, then cancel it and return its last status."""
        expected = expected or [self.records // self.partitions] * self.partitions
        offsets = None
        async with anyio.create_task_group() as tasks:
            await tasks.start(partial(service.run, handle_signals=False))
            with anyio.move_on_after(60):  # A failed run cancels the wait.
                while True:
                    with suppress(KafkaException):  # e.g. NOT_COORDINATOR while the broker loads offsets
                        if (offsets := await anyio.to_thread.run_sync(self.committed)) == expected:
                            break
                    await anyio.sleep(0.2)
            status = service.status()
            tasks.cancel_scope.cancel()
        assert offsets == expected, offsets
        return status


def app_command(
    workload: Workload,
    *,
    sleep_ms: float = 0,
    instance_id: str | None = None,
    crash_mid_transaction: bool = False,
    seed: int | None = None,
    key_concurrency: int | None = None,
    **config: int,
) -> tuple[list[str], dict[str, str]]:
    """Return the command and environment of one deployed app instance (`app.py`)."""
    env = {}
    if instance_id is not None:
        env["POTIK_INSTANCE_ID"] = instance_id
    if crash_mid_transaction:
        env["POTIK_CRASH_MID_TRANSACTION"] = "1"
    if seed is not None:
        env["POTIK_SEED"] = str(seed)
    if key_concurrency is not None:
        env["POTIK_KEY_CONCURRENCY"] = str(key_concurrency)
    command = [sys.executable, str(APP), workload.bootstrap, workload.source, workload.target, workload.group]
    return [*command, str(sleep_ms), *(f"{name}={value}" for name, value in config.items())], env


class Process:
    """A process, e.g. an app, logging to `log`."""

    def __init__(self, command: list[str], log: Path, env: dict[str, str]) -> None:
        self.log = log
        with log.open("w") as output:
            self.process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT, env={**os.environ, **env})

    def alive(self) -> bool:
        return self.process.poll() is None

    def wait_ready(self, timeout: float = 30) -> None:
        """Wait until the app's runtime is ready. A SIGTERM sent before the app handles signals kills it."""
        deadline = time.monotonic() + timeout
        while "Runtime started" not in self.log.read_text():
            assert self.alive(), f"the instance died on its own, see {self.log}"
            assert time.monotonic() < deadline, f"the instance did not start, see {self.log}"
            time.sleep(0.1)

    def stop(self, timeout: float = 60) -> int:
        """Send SIGTERM and return the exit code. Fail if the process does not exit within `timeout`."""
        self.process.terminate()
        try:
            return self.process.wait(timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            raise AssertionError(f"no exit within {timeout}s of SIGTERM, see {self.log}") from None

    def kill(self) -> None:
        self.process.kill()
        self.process.wait()
