"""A throwaway single-node Kafka broker in a container, for the tests and benchmarks.

Testcontainers starts it with Docker, or with Podman, whose API socket this module finds unless `DOCKER_HOST` is set. Testcontainers' reaper container removes the broker even if the process that started it is killed.
"""

import functools
import os
import shutil
import socket
import subprocess
import time
from typing import Self

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from testcontainers.core.config import testcontainers_config
from testcontainers.core.container import DockerContainer

IMAGE = "apache/kafka:4.1.0"


def _podman(*args: str) -> str:
    found = subprocess.run(["podman", *args], capture_output=True, text=True, check=False)
    return found.stdout.strip().removeprefix("unix://") if found.returncode == 0 else ""


@functools.cache
def _configure_engine() -> None:
    """Set up testcontainers for Podman when Docker is missing.

    The reaper container mounts the API socket as Podman sees it, and needs privileges to use it.
    """
    if shutil.which("docker") or not shutil.which("podman"):
        return
    if path := _podman("info", "--format", "{{.Host.RemoteSocket.Path}}"):
        testcontainers_config.ryuk_docker_socket = path
        testcontainers_config.ryuk_privileged = True
    # On macOS and Windows, Podman runs in a VM whose socket is forwarded to the host.
    if "DOCKER_HOST" not in os.environ and (
        local := _podman("machine", "inspect", "--format", "{{.ConnectionInfo.PodmanSocket.Path}}") or path
    ):
        os.environ["DOCKER_HOST"] = f"unix://{local}"


class Broker:
    """A single-node KRaft broker, on a fixed host port that survives restarts.

    `settings` override broker properties, e.g. `group_consumer_session_timeout_ms=6000`, and the image's other `KAFKA_*` variables, e.g. `heap_opts="-Xmx256m"`.
    """

    def __init__(self, **settings: int | str) -> None:
        with socket.socket() as probe:
            probe.bind(("localhost", 0))
            port = probe.getsockname()[1]
        self.bootstrap = f"localhost:{port}"
        self.settings = settings
        self.container = (
            DockerContainer(IMAGE)
            .with_bind_ports(9092, port)
            .with_envs(
                KAFKA_NODE_ID="1",
                KAFKA_PROCESS_ROLES="broker,controller",
                # The broker sends transaction markers to itself over INTERNAL, because it cannot reach the host port.
                KAFKA_LISTENERS="PLAINTEXT://:9092,INTERNAL://:29092,CONTROLLER://:9093",
                KAFKA_ADVERTISED_LISTENERS=f"PLAINTEXT://{self.bootstrap},INTERNAL://localhost:29092",
                KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
                KAFKA_INTER_BROKER_LISTENER_NAME="INTERNAL",
                KAFKA_CONTROLLER_QUORUM_VOTERS="1@localhost:9093",
                KAFKA_CONTROLLER_LISTENER_NAMES="CONTROLLER",
                # One partition each, so that warming up one group and one transactional ID loads them all.
                KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS="1",
                KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR="1",
                KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS="1",
                KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR="1",
                KAFKA_TRANSACTION_STATE_LOG_MIN_ISR="1",
                KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS="0",
                KAFKA_AUTO_CREATE_TOPICS_ENABLE="false",
                **{f"KAFKA_{name.upper()}": str(value) for name, value in settings.items()},
            )
        )

    def __enter__(self) -> Self:
        _configure_engine()
        self.container.start()
        self.wait_ready()
        self.warm_up()
        return self

    def __exit__(self, *exc_info) -> None:
        self.container.stop()

    def describe(self) -> dict:
        """Return what a benchmark report records about the broker."""
        return {"image": IMAGE, "settings": self.settings}

    def wait_ready(self, timeout: float = 120) -> None:
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        deadline = time.monotonic() + timeout
        while True:
            try:
                admin.list_topics(timeout=1)
                return
            except KafkaException:
                if time.monotonic() > deadline:
                    raise

    def warm_up(self, timeout: float = 60) -> None:
        """Create and load the group and transaction coordinators, as a long-running cluster has them.

        Otherwise the first group and the first transaction wait for them, or fail with NOT_COORDINATOR.

        It asks a consumer for committed offsets until the group coordinator answers. An `AdminClient`'s `list_consumer_group_offsets()` can segfault librdkafka 2.16 while the coordinator loads, in `rd_kafka_cgrp_op()`, because an admin client has no consumer group.
        """
        Producer({"bootstrap.servers": self.bootstrap, "transactional.id": "warm-up"}).init_transactions(timeout)
        consumer = Consumer({"bootstrap.servers": self.bootstrap, "group.id": "warm-up"})
        deadline = time.monotonic() + timeout
        try:
            while True:
                try:
                    consumer.committed([TopicPartition("warm-up", 0)], timeout=5)
                    return
                except KafkaException:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.1)
        finally:
            consumer.close()

    def pause(self) -> None:
        self.container.get_wrapped_container().pause()

    def unpause(self) -> None:
        self.container.get_wrapped_container().unpause()

    def stop(self) -> None:
        self.container.get_wrapped_container().stop(timeout=5)

    def start(self) -> None:
        self.container.get_wrapped_container().start()
        self.wait_ready()
