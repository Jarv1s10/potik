# /// script
# requires-python = "==3.12.*"
# dependencies = ["faust-streaming==0.15.4", "aiokafka==0.14.0", "packaging"]
# ///
"""Faust (aiokafka) with `processing_guarantee="exactly_once"`, which commits offsets through one transactional producer per partition.

In faust-streaming 0.15.4 its outputs go out outside the transactions, so the app is at-least-once. The comparison README shows how we found it.

`isolated_partitions` gives each partition its own sequential agent. Commits use potik's default thresholds, 1,000 records or 100ms, in place of Faust's 10,000 records or 2.8s, which would add seconds of latency.
"""

import asyncio
import logging
import signal
import tempfile

import faust
from workload import parse_args, process


async def main() -> None:
    args = parse_args()
    app = faust.App(
        args.group,
        broker=f"kafka://{args.bootstrap}",
        store="memory://",
        datadir=tempfile.mkdtemp(prefix="faust-bench-"),
        web_enabled=False,
        topic_disable_leader=True,
        topic_replication_factor=1,
        topic_partitions=args.partitions,
        consumer_auto_offset_reset="earliest",
        processing_guarantee="exactly_once",
        broker_commit_every=args.commit_batch_size,
        broker_commit_interval=args.commit_interval_ms / 1000,
        stream_wait_empty=True,
        value_serializer="raw",
        key_serializer="raw",
    )
    app.in_worker = True  # Faust only starts transactional producers in worker mode.
    source = app.topic(args.input, key_type=bytes, value_type=bytes)
    target = app.topic(args.output, key_type=bytes, value_type=bytes)

    @app.agent(source, isolated_partitions=True)
    async def handle(stream: faust.StreamT) -> None:
        async for event in stream.events():
            delay, output = process(event.value)
            await asyncio.sleep(delay)
            await target.send(key=event.key, value=output)

    stop = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    await app.start()
    await stop.wait()
    await app.stop()


logging.basicConfig(level=logging.WARNING)
asyncio.run(main())
