# /// script
# requires-python = "==3.12.*"
# dependencies = ["faust-streaming==0.15.4", "aiokafka==0.14.0", "packaging"]
# ///
"""A modified Faust agent: concurrent workers, per-key locks, in-order acks.

Workers may finish out of order. The tracker holds back each event's transactional produce and ack until every earlier offset of its partition is acked, so commits stay monotonic.
"""

import asyncio
import logging
import signal
import tempfile
from bisect import insort
from collections.abc import Hashable
from contextlib import nullcontext
from dataclasses import dataclass, field

import faust
from faust.types.events import EventT
from workload import parse_args, process


@dataclass
class _Produce:
    topic: str
    key: bytes | None
    value: bytes
    transactional_id: str


@dataclass
class _PartitionAcks:
    pending: list[int] = field(default_factory=list)
    acks: list[EventT] = field(default_factory=list)
    produces: dict[int, list[_Produce]] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class AcksTracker:
    def __init__(self, app: faust.App) -> None:
        self._app = app
        self._partitions: dict[int, _PartitionAcks] = {}

    def add_pending(self, event: EventT) -> bool:
        info = self._partitions.setdefault(event.message.partition, _PartitionAcks())
        if event.message.offset in info.pending:
            return False
        insort(info.pending, event.message.offset)
        return True

    def add_produce(self, event: EventT, produce: _Produce) -> None:
        self._partitions[event.message.partition].produces.setdefault(event.message.offset, []).append(produce)

    async def process_ack(self, event: EventT) -> None:
        info = self._partitions[event.message.partition]
        async with info.lock:
            insort(info.acks, event, key=lambda item: item.message.offset)
            ready = []
            for offset, acked in zip(info.pending, info.acks, strict=False):
                if offset != acked.message.offset:
                    break
                ready.append(acked)
            for acked in ready:
                for produce in info.produces.get(acked.message.offset, ()):
                    await self._app.producer.send(
                        topic=produce.topic,
                        key=produce.key,
                        value=produce.value,
                        partition=None,
                        timestamp=None,
                        headers=None,
                        transactional_id=produce.transactional_id,
                    )
                acked.ack()
                info.pending.remove(acked.message.offset)
                info.produces.pop(acked.message.offset, None)
                info.acks.remove(acked)


async def main() -> None:
    args = parse_args()
    app = faust.App(
        args.group,
        broker=f"kafka://{args.bootstrap}",
        store="memory://",
        datadir=tempfile.mkdtemp(prefix="faust-acks-bench-"),
        web_enabled=False,
        topic_disable_leader=True,
        topic_replication_factor=1,
        topic_partitions=args.partitions,
        consumer_auto_offset_reset="earliest",
        processing_guarantee="exactly_once",
        broker_commit_every=args.commit_batch_size,
        broker_commit_interval=args.commit_interval_ms / 1000,
        stream_wait_empty=False,
        value_serializer="raw",
        key_serializer="raw",
    )
    app.in_worker = True  # Faust only starts transactional producers in worker mode.
    source = app.topic(args.input, key_type=bytes, value_type=bytes)
    tracker = AcksTracker(app)
    key_locks: dict[Hashable, asyncio.Lock] = {}

    @app.agent(source, concurrency=args.partitions)
    async def handle(stream: faust.StreamT) -> None:
        async for event in stream.noack().events():
            if not tracker.add_pending(event):
                continue
            key = event.key
            lock = key_locks.setdefault(key, asyncio.Lock()) if key is not None else nullcontext()
            transactional_id = (
                f"{app.conf.id}-{app.assignor.group_for_topic(event.message.topic)}-{event.message.partition}"
            )
            try:
                async with lock:
                    delay, output = process(event.value)
                    await asyncio.sleep(delay)
                    tracker.add_produce(event, _Produce(args.output, key, output, transactional_id))
                    await tracker.process_ack(event)
            except asyncio.CancelledError:
                await app.producer.abort_transaction(transactional_id)
                raise

    stop = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    await app.start()
    await stop.wait()
    await app.stop()


logging.basicConfig(level=logging.WARNING)
asyncio.run(main())
