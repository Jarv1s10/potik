"""Adversarial integration tests against a real broker.

Each pytest process or xdist worker starts a broker container with `tools/kafka_broker.py` and the short sessions of `harness.FAST_BROKER`. `POTIK_BOOTSTRAP` runs the tests against an existing broker instead, which needs the same settings for the tests' timings to hold. The outage tests and the examples always get a broker container of their own, through the `broker` fixture, and the fencing test one behind a proxy.

The `workload` fixture gives each test uniquely named topics and groups, and deletes them if the test passes.
"""

import itertools
import os
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from harness import FAST_BROKER, SMALL_BROKER, Process, Workload, app_command
from kafka_broker import Broker

reports = pytest.StashKey[dict[str, pytest.TestReport]]()
workloads = pytest.StashKey[list[Workload]]()
started_processes = pytest.StashKey[list[Process]]()
LOG_LINES = 100


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Iterator[pytest.TestReport]:
    """Stash each phase's report for fixture teardown, and add the topics and process logs to a failure's report."""
    report: pytest.TestReport = yield
    item.stash.setdefault(reports, {})[report.when] = report
    if report.failed:
        for process in item.stash.get(started_processes, []):
            lines = process.log.read_text().splitlines()[-LOG_LINES:]
            report.sections.append((f"last {len(lines)} lines of {process.log}", "\n".join(lines)))
        if works := item.stash.get(workloads, []):
            kept = "\n".join(f"{work.source}, {work.target}, group {work.group}" for work in works)
            report.sections.append(("kept for inspection", kept))
    return report


def failed(item: pytest.Item) -> bool:
    return any(report.failed for report in item.stash.get(reports, {}).values())


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="session")
def bootstrap() -> Iterator[str]:
    if bootstrap := os.environ.get("POTIK_BOOTSTRAP"):
        try:
            AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10)
        except KafkaException as exc:
            pytest.fail(f"no Kafka broker at POTIK_BOOTSTRAP={bootstrap}: {exc}", pytrace=False)
        yield bootstrap
    else:
        with Broker(**FAST_BROKER) as broker:
            yield broker.bootstrap


@pytest.fixture
def broker() -> Iterator[Broker]:
    """A broker of the test's own, which it may pause, stop, and restart.

    It keeps Kafka's default 45s session timeout. An app survives only outages shorter than its session.
    """
    with Broker(**SMALL_BROKER) as broker:
        yield broker


def _wait_until_served(bootstrap: str, topics: list[str], partitions: int) -> None:
    """Wait until every partition of new `topics` answers offset requests.

    Creating a topic returns before its leader serves it, so a fresh topic can briefly look missing or answer `NOT_LEADER_FOR_PARTITION`.
    """
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "unused"})  # It never joins the group.
    deadline = time.monotonic() + 30
    try:
        for partition in (TopicPartition(topic, p) for topic in topics for p in range(partitions)):
            while True:
                try:
                    consumer.get_watermark_offsets(partition, timeout=10)
                    break
                except KafkaException:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.05)
    finally:
        consumer.close()


@pytest.fixture
def workload(bootstrap: str, request: pytest.FixtureRequest) -> Iterator:
    """Return `workload(partitions, records, payload=b"v")`, which creates and fills isolated topics."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    created = request.node.stash[workloads] = []

    def create(partitions: int, records: int, payload: bytes = b"v") -> Workload:
        assert records % partitions == 0
        prefix = f"it-{request.node.name[:40]}-{uuid4().hex[:8]}".replace("[", "-").replace("]", "")
        source, target = f"{prefix}-in", f"{prefix}-out"
        for future in admin.create_topics([NewTopic(source, partitions, 1), NewTopic(target, partitions, 1)]).values():
            future.result()
        _wait_until_served(bootstrap, [source, target], partitions)
        producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20})
        for i in range(records):
            while True:
                try:
                    producer.produce(source, payload, str(i).encode(), i % partitions)
                    break
                except BufferError:
                    producer.poll(0.01)
        assert producer.flush(30) == 0
        created.append(work := Workload(bootstrap, source, target, prefix, partitions, records, admin))
        return work

    yield create
    if created and not failed(request.node):
        # Best effort. Deleting a group fails while a killed app's member is still in it, until its session times out.
        topics = [topic for work in created for topic in (work.source, work.target)]
        groups = [group for work in created for group in (work.group, f"verify-{work.group}")]
        for future in [*admin.delete_topics(topics).values(), *admin.delete_consumer_groups(groups).values()]:
            future.exception()
    created.clear()  # The stash holds the workloads. Release their admin clients before the broker stops.


@pytest.fixture
def processes(tmp_path: Path, request: pytest.FixtureRequest) -> Iterator:
    """Return `processes(name, command, env={})`, which starts a process logging to `<name>.log`.

    Teardown kills any process still running.
    """
    started = request.node.stash[started_processes] = []

    def start(name: str, command: list[str], env: dict[str, str] | None = None) -> Process:
        process = Process(command, tmp_path / f"{name}.log", env or {})
        started.append(process)
        return process

    yield start
    for process in started:
        if process.alive():
            process.kill()


@pytest.fixture
def apps(processes) -> Callable[..., Process]:
    """Return `apps(workload, **options)`, which starts an app instance (`harness.app_command`)."""
    count = itertools.count()

    def start(workload: Workload, **options) -> Process:
        return processes(f"app-{next(count)}", *app_command(workload, **options))

    return start
