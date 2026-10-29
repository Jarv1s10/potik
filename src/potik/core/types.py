"""Records, handlers, transactions, and status snapshots, as the core and its middleware see them."""

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Generic, NamedTuple

from confluent_kafka import Message
from typing_extensions import TypeVar

__all__ = [
    "ConsumerRecord",
    "Delivery",
    "Headers",
    "PartitionStatus",
    "ProducerRecord",
    "RecordHandler",
    "RecordSender",
    "RuntimeStatus",
    "Transaction",
]

type ConsumerRecord = Message
"""The native `confluent_kafka.Message`, not a copy. Its fields are methods, e.g. `record.key()` and `record.offset()`."""
type Headers = list[tuple[str, str | bytes | None]] | dict[str, str | bytes | None]
"""A record's headers: a list of `(name, value)` pairs, which keeps order and allows repeated names, or a dict.

Values are `str`, `bytes`, or None.
"""


# The type of a record's value before encoding. Only type checkers see it, so it is declared covariant, not inferred.
_V_co = TypeVar("_V_co", covariant=True, default=Any)


class ProducerRecord(NamedTuple, Generic[_V_co]):
    """A record to produce.

    A handler returns it as an output, which commits in one transaction with the input record's offset. `Producer.send()` sends it at once instead.

    `ProducerRecord[V]` marks a record that a `Topic[V]` made, for type checkers only. A record made directly is a `ProducerRecord[Any]`.

    Attributes:
        topic: The topic's name.
        key: The encoded key, or None for a null key.
        value: The encoded value, or None for a tombstone.
        headers: The record's headers.
        partition: The record's partition, or -1 to let the producer's partitioner pick one from the key. librdkafka's default partitioner hashes keys differently from the Java client's. To match Java producers, set the `partitioner` option to `murmur2_random`.
    """

    topic: str
    key: bytes | None = None
    value: bytes | None = None
    headers: Headers | None = None
    partition: int = -1


type RecordHandler = Callable[[ConsumerRecord], Awaitable[Sequence[ProducerRecord]]]
"""A core handler. It takes one record and returns its outputs as a list or tuple, possibly empty.

Returning does not commit. The runtime commits the outputs and the record's offset later, in one transaction with other handled records.
"""


class Delivery(NamedTuple):
    """Where the broker appended a sent record."""

    partition: int
    offset: int


type RecordSender = Callable[[ProducerRecord], Awaitable[Delivery]]
"""The `call_next` of `BaseMiddleware.send`. It sends one record and returns once the broker acknowledges it."""


@dataclass(frozen=True, slots=True)
class Transaction:
    """The outputs and offsets of a batch of handled records, which commit atomically.

    Middleware sees it in the `commit` stage (see `BaseMiddleware.commit`).

    Attributes:
        offsets: The offset to resume from of each `(topic, partition)`, one past its last handled record. It may instead be the start offset that the offset policy picked for a new assignment, with no handled records.
        outputs: The records to produce.
    """

    offsets: Mapping[tuple[str, int], int]
    outputs: Sequence[ProducerRecord]


@dataclass(frozen=True, slots=True, kw_only=True)
class PartitionStatus:
    """A snapshot of one assigned partition.

    Attributes:
        topic: The partition's topic.
        partition: The partition's number.
        buffered_records: The number of fetched records that have not started, including those that wait for an earlier record of their key.
        running_handlers: The number of handler calls that run. It is at most 1, unless the topic runs by key (see `Runtime`).
        paused: Whether fetching is paused, to limit the buffer or because the partition is stopped.
        handler_started_at: The `time.monotonic()` time when the oldest running handler call started, or None if no call runs.
        failure: The exception that stopped the partition (see `Config.stop_failed_partitions`), or None.
    """

    topic: str
    partition: int
    buffered_records: int
    running_handlers: int
    paused: bool
    handler_started_at: float | None
    failure: Exception | None


@dataclass(frozen=True, slots=True, kw_only=True)
class RuntimeStatus:
    """A snapshot of a run, from `Runtime.status()`.

    Times are `time.monotonic()` values. `potik.core.health` checks a snapshot for liveness and readiness probes.

    Attributes:
        running: Whether the run has started and is neither stopping nor stopped.
        polled_at: When the consumer's last poll returned, or when polling started, or None before the run starts. The runtime polls at least every 200ms, except during a rebalance or a shutdown.
        oldest_uncommitted_at: When the oldest batch that waits for its transaction got its first handled record or policy-selected offset, or None if no batch waits.
        failure: The error that stopped the run, or None. An error of a middleware's lifespan is not recorded here, but `run()` raises it.
        partitions: The assigned partitions.
    """

    running: bool = False
    polled_at: float | None = None
    oldest_uncommitted_at: float | None = None
    failure: Exception | None = None
    partitions: tuple[PartitionStatus, ...] = ()
