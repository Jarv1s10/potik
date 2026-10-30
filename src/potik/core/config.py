"""The runtime's limits and client options, and the client settings that the runtime owns."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any
from uuid import uuid4

from confluent_kafka import KafkaError, Message

from potik.core.offsets import OffsetPolicy, resume_or_latest

__all__ = [
    "Config",
]

# librdkafka prefetches up to 100k records per partition, 64MB in total, in responses of up to 1MB per partition, then backs off fetching for a second. Prefetch less and refill sooner, because the runtime buffers records too.
_CONSUMER_DEFAULTS = {
    "queued.min.messages": 2000,
    "queued.max.messages.kbytes": 16384,
    "max.partition.fetch.bytes": 131072,
    "fetch.max.bytes": 8388608,
    "fetch.queue.backoff.ms": 20,
}
# librdkafka backs off reconnecting for up to 10s. After an outage, a client may then stay disconnected long enough to time out the transaction in flight. Reconnect within a second of the broker's return, like the Java clients.
# librdkafka's producer asks the broker to create a missing topic, which a broker with Kafka's defaults does with one partition. Fail the send instead.
_CLIENT_DEFAULTS = {"reconnect.backoff.max.ms": 1000, "allow.auto.create.topics": False}

# Client options the runtime's correctness depends on.
_CONSUMER_OWNED = frozenset(
    {
        "bootstrap.servers",
        "group.id",
        "group.protocol",
        "enable.auto.commit",
        "enable.auto.offset.store",
        "enable.partition.eof",
        "isolation.level",
        "auto.offset.reset",
        "max.poll.interval.ms",
    }
)
# Client options that any producer's guarantees and delivery reports depend on.
_PRODUCER_OWNED = frozenset(
    {
        "bootstrap.servers",
        "transactional.id",
        "enable.idempotence",
        "acks",
        "request.required.acks",
        "delivery.report.only.error",
        "on_delivery",
        "dr_cb",
        "dr_msg_cb",
    }
)
_TRANSACTIONAL_OWNED = _PRODUCER_OWNED | {"transaction.timeout.ms", "metadata.recovery.strategy"}


@dataclass(frozen=True, kw_only=True)
class Config:
    """Runtime limits and librdkafka client options.

    The runtime owns the client settings that its guarantees depend on, e.g. `isolation.level` and `enable.auto.commit`, and the options cannot override them. Callbacks in the options, e.g. `error_cb`, run on the runtime's client threads, so they must be thread-safe functions, not coroutines.

    The producer's `transactional.id` is `{transactional_id_prefix}-{random}`, new each run. With static membership, i.e. a `group.instance.id` in the options, it derives from the group and instance IDs instead. A restarted instance then aborts the transaction that its crashed run left open, so `read_committed` readers do not wait `transaction_timeout_ms` for it. A second live process with the same instance ID fences the first one's producer.

    Args:
        bootstrap_servers: The cluster's bootstrap servers.
        group_id: The consumer group.
        offset_policy: Picks the start offset of each newly assigned partition (see `OffsetPolicy`). `resume_or_latest`, the default, `resume_or_earliest`, and `resume_or_fail` resume the committed offset, and differ only for a partition without one.
        max_buffered_bytes: The memory of the fetched records that have not gone to a transaction yet, across all partitions, i.e. those that wait for their handler, run, or wait for an earlier record of their partition (see the `key_concurrency` of `Runtime`). A record counts as its key and value plus 350 bytes for its objects and a typical `traceparent` header, so a UUID key and 1KB of JSON count as about 1.4KB. With consumer lag, the buffers stay full, so this is steady memory, not a peak. Each partition gets an equal share, at least 16KB, so the total can exceed it with more than `max_buffered_bytes / 16384` partitions. Handlers that do I/O need only a few buffered records each. Records arrive one record batch of a partition at a time, and a share smaller than the batches of the topics' producers makes partitions pause and fetch records again, which slows handlers that take about a millisecond or less. It does not count librdkafka's prefetch queue, which potik's defaults limit to about 24MB, handler outputs, or handled records that wait for their transaction, at most two `commit_batch_size` batches. A fetch of records larger than the previous fetch's can exceed a partition's share, by at most the prefetch queue, so the budget is a target, not a hard limit.
        stop_failed_partitions: Whether a failure of one partition stops it, instead of failing the run. A stopped partition handles nothing past the failed record, which stays uncommitted, while other partitions keep running. The record runs again once the partition is assigned again, e.g. after a restart. A failure is a handler exception that no exception rule handles, or a fetch error, e.g. a committed offset that retention deleted.
        commit_batch_size: The maximum number of handled records per transaction.
        commit_interval_ms: The minimum time between the starts of two transactions, unless the batch fills first. A transaction also waits for the previous one to finish, so batches grow with throughput.
        transaction_timeout_ms: The producer's `transaction.timeout.ms`, after which the broker aborts an open transaction. It also limits each commit and abort, so a broker outage longer than it fails the run, at the latest once a broker returns. At least 1000.
        rebalance_timeout_ms: The longest that the runtime may take to handle an assignment or a revocation, including the revoked partitions' grace period and final commit. Exceeding it fails the run. Must be less than `max_poll_interval_ms`.
        revocation_grace_ms: How long in-flight handlers of revoked partitions may run before the runtime cancels them. Their records run again on the partitions' next owner. The default, 0, cancels them at once. Must be less than `rebalance_timeout_ms`.
        shutdown_grace_ms: How long in-flight handlers may run on shutdown before the runtime cancels them. The default, 0, cancels them at once. Plus `rebalance_timeout_ms`, it limits the whole shutdown, including the final commit, and must be less than `max_poll_interval_ms`.
        max_poll_interval_ms: The consumer's `max.poll.interval.ms`. The runtime keeps polling while handlers run, so handlers may take longer than this.
        transactional_id_prefix: The start of the producer's `transactional.id`. Give each app its own, so that its ACL on the prefixed `TransactionalId` resource covers no other app's IDs.
        options: librdkafka options of both clients, e.g. for the connection, TLS, SASL, OAuth, `client.id`, or logging.
        consumer_options: librdkafka options of the consumer, e.g. `group.remote.assignor`, `group.instance.id`, `client.rack`, or fetch sizes.
        producer_options: librdkafka options of the transactional producer, e.g. `compression.type` or `linger.ms`.

    Raises:
        ValueError: If a limit is out of range, or options override a setting that the runtime owns.
    """

    bootstrap_servers: str
    group_id: str
    offset_policy: OffsetPolicy = resume_or_latest
    max_buffered_bytes: int = 16 * 1024 * 1024
    stop_failed_partitions: bool = True
    commit_batch_size: int = 1000
    commit_interval_ms: int = 100
    transaction_timeout_ms: int = 10_000
    rebalance_timeout_ms: int = 60_000
    revocation_grace_ms: int = 0
    shutdown_grace_ms: int = 0
    max_poll_interval_ms: int = 300_000
    transactional_id_prefix: str = "potik"
    options: Mapping[str, Any] = field(default_factory=dict)
    consumer_options: Mapping[str, Any] = field(default_factory=dict)
    producer_options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.bootstrap_servers or not self.group_id or not self.transactional_id_prefix:
            raise ValueError("bootstrap_servers, group_id, and transactional_id_prefix must not be empty")
        if not callable(self.offset_policy):
            raise TypeError("offset_policy must be callable")
        for name in (
            "max_buffered_bytes",
            "commit_batch_size",
            "transaction_timeout_ms",
            "rebalance_timeout_ms",
            "max_poll_interval_ms",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer, got {value!r}")
        for name in ("commit_interval_ms", "revocation_grace_ms", "shutdown_grace_ms"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer, got {value!r}")
        if self.transaction_timeout_ms < 1000:
            raise ValueError("transaction_timeout_ms must be at least 1000 (librdkafka minimum)")
        if self.rebalance_timeout_ms >= self.max_poll_interval_ms:
            raise ValueError("rebalance_timeout_ms must be less than max_poll_interval_ms")
        if self.revocation_grace_ms >= self.rebalance_timeout_ms:
            raise ValueError("revocation_grace_ms must be less than rebalance_timeout_ms")
        # Polling stops during a shutdown. Handlers get shutdown_grace_ms to finish, and cancelled ones get up to rebalance_timeout_ms more. If that exceeds max_poll_interval_ms, the group fences the final commit.
        if self.shutdown_grace_ms + self.rebalance_timeout_ms >= self.max_poll_interval_ms:
            raise ValueError("shutdown_grace_ms + rebalance_timeout_ms must be less than max_poll_interval_ms")
        for name, owned in (
            ("options", _CONSUMER_OWNED | _TRANSACTIONAL_OWNED),
            ("consumer_options", _CONSUMER_OWNED),
            ("producer_options", _TRANSACTIONAL_OWNED),
        ):
            options = getattr(self, name)
            if conflicts := owned.intersection(options):
                raise ValueError(f"{name} cannot include settings that the runtime manages: {sorted(conflicts)}")
            object.__setattr__(self, name, MappingProxyType(dict(options)))

    def client_config(self) -> dict[str, Any]:
        """Return librdkafka options to connect a client of your own to the cluster, e.g. an `AdminClient`."""
        return _client_config(self.bootstrap_servers, self.options)

    def _consumer_config(self) -> dict[str, Any]:
        return {
            **_CONSUMER_DEFAULTS,
            **self.client_config(),
            **self.consumer_options,
            "group.id": self.group_id,
            "group.protocol": "consumer",  # KIP-848 broker-driven incremental rebalances
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "enable.partition.eof": False,
            "isolation.level": "read_committed",
            # Out-of-range positions fail instead of silently skipping records.
            "auto.offset.reset": "error",
            "max.poll.interval.ms": self.max_poll_interval_ms,
        }

    def _producer_config(self, on_failed_delivery: Callable[[KafkaError, Message], None]) -> dict[str, Any]:
        instance = {**self.options, **self.consumer_options}.get("group.instance.id")
        # The group's length makes the ID unambiguous. Without it, group "a-b" with instance "c" and group "a" with instance "b-c" would fence each other.
        suffix = f"{len(self.group_id)}-{self.group_id}-{instance}" if instance else uuid4()
        return {
            **_idempotent({**self.client_config(), **self.producer_options}),
            "transactional.id": f"{self.transactional_id_prefix}-{suffix}",
            "transaction.timeout.ms": self.transaction_timeout_ms,
            # Re-bootstrapping while all brokers are down drops the connection that a pending transactional call waits on. That call then blocks forever, ignoring its timeout (librdkafka 2.16.0).
            "metadata.recovery.strategy": "none",
            "delivery.report.only.error": True,
            "on_delivery": on_failed_delivery,
        }


def _client_config(bootstrap_servers: str, options: Mapping[str, Any]) -> dict[str, Any]:
    """Return the options of a client to `bootstrap_servers`: potik's defaults, overridden by `options`."""
    return {**_CLIENT_DEFAULTS, **options, "bootstrap.servers": bootstrap_servers}


def _idempotent(options: Mapping[str, Any]) -> dict[str, Any]:
    """Return a producer's `options`, made idempotent and waiting for all in-sync replicas."""
    return {**options, "enable.idempotence": True, "acks": "all"}
