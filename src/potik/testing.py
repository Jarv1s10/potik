"""Run a whole app in tests, on an in-process Kafka cluster instead of a broker."""

import ctypes
import sys
import time
from dataclasses import replace
from functools import cache
from types import TracebackType
from typing import Self, cast
from uuid import uuid4

import anyio
import anyio.from_thread
import confluent_kafka.cimpl
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition

from potik.core import Config, ConsumerRecord, resume_or_earliest
from potik.middleware.topics import NamedTopic, _topic_names

__all__ = [
    "MockCluster",
]

# How long a read blocks before it checks its deadline and cancellation.
_READ_SLICE = 0.1
# The mock cluster holds a fetch without records for the whole wait, even when a record arrives sooner.
_FETCH_WAIT_MS = 10


class MockCluster:
    """An in-process Kafka cluster that runs a whole app in a test, without a broker.

    The app's own clients connect to it, so its runtime, middleware, and topic check run as in production. Point the app at the cluster with `config()`, create its topics, run the app, and wait for its outputs with `wait_records()`.

    The cluster has one broker. It creates no topic on its own, so a topic that the test did not create fails the app's start. It commits transactions and hides uncommitted outputs from readers. It does not store the offsets that a transaction commits, and it does not fence stale group members. So a restarted app handles its input again, and only a real broker can test what a run committed.

    Enter it with `with`. Exit it only after the app's run and every other client of the test have closed, because a client that outlives the cluster keeps trying to reconnect. It runs librdkafka's mock cluster, on Linux and macOS only.

    Raises:
        RuntimeError: On other platforms, or if the installed confluent-kafka does not export librdkafka's mock cluster API.

    Example:
        ```python
        async def test_accept() -> None:
            with MockCluster() as cluster:
                cluster.create_topic(orders)
                cluster.create_topic(accepted)
                async with Producer(cluster.bootstrap_servers) as producer:
                    await producer.send(orders(Order(sku="A"), key="c1"))
                runtime = build(cluster.config(CONFIG))  # your service's potik.build_runtime(...)
                async with anyio.create_task_group() as tasks:
                    await tasks.start(partial(runtime.run, handle_signals=False))
                    (record,) = await cluster.wait_records(accepted, 1)
                    tasks.cancel_scope.cancel()  # shuts the run down gracefully
        ```
    """

    def __init__(self) -> None:
        if sys.platform not in ("linux", "darwin"):
            raise RuntimeError("MockCluster runs only on Linux and macOS")
        _mock_api()  # Fails here, rather than halfway through a test.
        self._handle: int | None = None
        self._cluster: int | None = None

    @property
    def bootstrap_servers(self) -> str:
        """The cluster's bootstrap servers, for the app's `Config` and any other client."""
        return _mock_api().rd_kafka_mock_cluster_bootstraps(self._entered()).decode()

    def config(self, config: Config) -> Config:
        """Return a copy of `config` that connects to this cluster, for an app under test.

        The copy starts each partition at its first record (`resume_or_earliest`), so the app also handles the records that the test sent before the partition was assigned. It also sets `fetch.wait.max.ms` to 10ms, because the mock cluster holds a fetch without records for that whole wait, even when a record arrives.
        """
        return replace(
            config,
            bootstrap_servers=self.bootstrap_servers,
            offset_policy=resume_or_earliest,
            consumer_options={**config.consumer_options, "fetch.wait.max.ms": _FETCH_WAIT_MS},
        )

    def create_topic(self, topic: NamedTopic | str, partitions: int = 1) -> None:
        """Create `topic`, e.g. a `Topic` or a name, with `partitions` partitions.

        Raises:
            ValueError: If the name is invalid, or `partitions` is less than 1.
            KafkaException: If the cluster refuses the topic, e.g. one that exists.
        """
        (name,) = _topic_names([topic])
        if type(partitions) is not int or partitions < 1:
            raise ValueError(f"partitions must be a positive integer, got {partitions!r}")
        error = _mock_api().rd_kafka_mock_topic_create(self._entered(), name.encode(), partitions, 1)
        if error:
            raise KafkaException(KafkaError(error))

    async def wait_records(self, topic: NamedTopic | str, count: int, *, timeout: float = 10.0) -> list[ConsumerRecord]:
        """Read `topic` until it has at least `count` committed records and every partition has reached its end.

        It returns every record that it read, which may be more than `count`. A topic that keeps receiving records may time out. A record counts once a `read_committed` consumer can read it. A `Producer` send counts at once, and a handler's output once its transaction commits. Each partition's records come in offset order.

        Raises:
            TimeoutError: If the topic has fewer than `count` committed records, or a partition has not reached its end, after `timeout` seconds.
            KafkaException: If the topic does not exist.
        """
        (name,) = _topic_names([topic])
        return await anyio.to_thread.run_sync(self._read, name, count, time.monotonic() + timeout)

    def _read(self, topic: str, count: int, deadline: float) -> list[ConsumerRecord]:
        """Read `topic` from the start until it has `count` records and every partition has ended."""
        reader = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": f"potik-testing-{uuid4()}",  # Required, though the reader never joins it.
                "enable.auto.commit": False,
                "enable.partition.eof": True,
                "fetch.wait.max.ms": _FETCH_WAIT_MS,
                "isolation.level": "read_committed",
            }
        )
        try:
            metadata = reader.list_topics(topic, timeout=max(deadline - time.monotonic(), _READ_SLICE)).topics[topic]
            if metadata.error is not None:
                raise KafkaException(metadata.error)
            reader.assign([TopicPartition(topic, partition, OFFSET_BEGINNING) for partition in metadata.partitions])
            records: list[ConsumerRecord] = []
            ended: set[int] = set()
            while len(records) < count or len(ended) < len(metadata.partitions):
                anyio.from_thread.check_cancelled()
                if (remaining := deadline - time.monotonic()) <= 0:
                    if len(records) < count:
                        raise TimeoutError(f"topic {topic!r} has {len(records)} of {count} committed records")
                    raise TimeoutError(f"topic {topic!r} kept receiving records, so a partition never reached its end")
                for record in reader.consume(1000, min(_READ_SLICE, remaining)):
                    partition = cast(int, record.partition())
                    if (error := record.error()) is None:
                        records.append(record)
                        ended.discard(partition)
                    elif error.code() == KafkaError._PARTITION_EOF:
                        ended.add(partition)
                    else:
                        raise KafkaException(error)
            return records
        finally:
            reader.close()

    def _entered(self) -> int:
        if self._cluster is None:
            raise RuntimeError("enter the cluster with `with` before using it")
        return self._cluster

    def __enter__(self) -> Self:
        if self._cluster is not None:
            raise RuntimeError("the cluster is already entered")
        api = _mock_api()
        error = ctypes.create_string_buffer(512)
        config = api.rd_kafka_conf_new()
        # Quiet the warning that the cluster's own handle has no bootstrap servers.
        api.rd_kafka_conf_set(config, b"log_level", b"4", error, len(error))
        handle = api.rd_kafka_new(0, config, error, len(error))  # A producer handle, which takes over the config.
        if not handle:
            raise RuntimeError(f"cannot start the mock cluster: {error.value.decode()}")
        cluster = api.rd_kafka_mock_cluster_new(handle, 1)
        if not cluster:
            api.rd_kafka_destroy(handle)
            raise RuntimeError("cannot start the mock cluster")
        # Admin requests, e.g. the topic check's, go to the controller, which is otherwise no broker.
        api.rd_kafka_mock_set_controller_id(cluster, 1)
        self._handle, self._cluster = handle, cluster
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        api = _mock_api()
        api.rd_kafka_mock_cluster_destroy(self._cluster)
        api.rd_kafka_destroy(self._handle)
        self._handle = self._cluster = None


@cache
def _mock_api() -> ctypes.CDLL:
    """Return librdkafka's mock cluster API, which confluent-kafka does not wrap.

    The extension module's handle finds the symbols of the librdkafka that it links, whatever name the platform's wheel gives the library.
    """
    api = ctypes.CDLL(confluent_kafka.cimpl.__file__)
    pointer, string = ctypes.c_void_p, ctypes.c_char_p
    for name, result, arguments in (
        ("rd_kafka_conf_new", pointer, []),
        ("rd_kafka_conf_set", ctypes.c_int, [pointer, string, string, string, ctypes.c_size_t]),
        ("rd_kafka_new", pointer, [ctypes.c_int, pointer, string, ctypes.c_size_t]),
        ("rd_kafka_destroy", None, [pointer]),
        ("rd_kafka_mock_cluster_new", pointer, [pointer, ctypes.c_int]),
        ("rd_kafka_mock_cluster_destroy", None, [pointer]),
        ("rd_kafka_mock_cluster_bootstraps", string, [pointer]),
        ("rd_kafka_mock_topic_create", ctypes.c_int, [pointer, string, ctypes.c_int, ctypes.c_int]),
        ("rd_kafka_mock_set_controller_id", None, [pointer, ctypes.c_int32]),
    ):
        try:
            function = getattr(api, name)
        except AttributeError as exc:
            raise RuntimeError(
                f"MockCluster needs librdkafka's {name}, which confluent-kafka {confluent_kafka.__version__} "
                "does not export"
            ) from exc
        function.restype, function.argtypes = result, arguments
    return api
