# /// script
# requires-python = ">=3.14"
# dependencies = ["anyio>=4.15.1", "confluent-kafka>=2.16.0", "testcontainers>=4.15.0"]
# ///
"""Committed throughput of this checkout's runtime.

Run: uv run --script benchmarks/throughput.py --records 300000

A record counts as done when its transaction commits, not when its handler returns. A separate process produces the inputs, so peak RSS covers only the runtime. Output verification is not timed. Without --bootstrap, the session starts a throwaway broker (`tools/kafka_broker.py`). A verified run deletes its topics and group. On an existing broker, a failed run keeps them for inspection. Each report goes to `benchmarks/results/` with the revision, platform, and broker.
"""

import argparse
import json
import multiprocessing
import platform
import resource
import statistics
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import anyio
import confluent_kafka
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

# Import potik and kafka_broker from this checkout, not from installed packages.
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools")]

from potik.core import Config, ConsumerRecord, ProducerRecord, Runtime, resume_or_earliest
from potik.core.committer import Committer
from potik.middleware import ConcurrencyMiddleware

RESULTS = Path(__file__).resolve().parent / "results"


def rss_mb() -> float:
    return round(
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024 if sys.platform == "darwin" else 1024), 1
    )


def load(bootstrap: str, topic: str, total: int, partitions: int, payload: bytes, batch_bytes: int) -> None:
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20, "batch.size": batch_bytes})
    for i in range(total):
        while True:
            try:
                producer.produce(topic, payload, str(i).encode(), i % partitions)
                break
            except BufferError:
                producer.poll(0.01)
    producer.flush()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", help="an existing broker, instead of a throwaway one")
    parser.add_argument("--records", type=int, default=300_000, help="measured inputs after warm-up")
    parser.add_argument("--warmup", type=int, default=20_000)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--max-buffered-bytes", type=int, default=Config.max_buffered_bytes)
    parser.add_argument("--batch", type=int, default=Config.commit_batch_size)
    parser.add_argument("--interval", type=int, default=Config.commit_interval_ms)
    parser.add_argument("--max-concurrency", type=int, default=None)
    parser.add_argument("--key-concurrency", type=int, default=None, help="run the inputs by key")
    parser.add_argument("--handler-ms", type=float, default=0, help="simulated handler latency")
    parser.add_argument("--consumer-option", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument(
        "--input-batch-bytes",
        type=int,
        default=16384,
        help="batch.size of the producer that writes the inputs: the Java client's default, not librdkafka's 1MB",
    )
    args = parser.parse_args()
    if (args.warmup + args.records) % args.partitions:
        parser.error("total inputs must be divisible by partitions")
    # Imported here, so the spawned measured process does not load testcontainers when it imports this module.
    from kafka_broker import Broker

    with ExitStack() as stack:
        if args.bootstrap:
            broker = {"bootstrap": args.bootstrap}
        else:
            started = stack.enter_context(Broker())
            args.bootstrap, broker = started.bootstrap, started.describe()
        prefix = f"potik-throughput-{uuid4().hex[:12]}"
        try:
            with ProcessPoolExecutor(1, mp_context=multiprocessing.get_context("spawn")) as measured:
                results = measured.submit(anyio.run, benchmark, args, prefix).result()
        except BaseException:
            if "bootstrap" in broker:
                print(f"Kept topics {prefix}-in, {prefix}-out and group {prefix} for inspection.", flush=True)
            raise
    revision = subprocess.run(
        ["git", "describe", "--always", "--dirty"], cwd=ROOT, capture_output=True, text=True, check=False
    )
    report = {
        "session": datetime.now(UTC).strftime("%Y%m%dT%H%M%S"),
        "git": revision.stdout.strip(),
        "platform": platform.platform(),
        "confluent_kafka": confluent_kafka.version()[0],
        "librdkafka": confluent_kafka.libversion()[0],
        "broker": broker,
        "config": {name: value for name, value in vars(args).items() if name != "bootstrap"},
        "results": results,
    }
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / f"throughput-{report['session']}.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    with (RESULTS / "history.jsonl").open("a") as history:
        history.write(json.dumps(report) + "\n")
    print(f"Saved {path}", flush=True)


async def benchmark(args: argparse.Namespace, prefix: str) -> dict:
    """Run, verify, and delete one workload, and return its measurements."""
    total = args.warmup + args.records
    source, target = prefix + "-in", prefix + "-out"
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    for future in admin.create_topics(
        [NewTopic(source, args.partitions, 1), NewTopic(target, args.partitions, 1)]
    ).values():
        await anyio.to_thread.run_sync(future.result)
    payload = b"x" * args.payload_bytes
    loader = multiprocessing.get_context("spawn").Process(
        target=load, args=(args.bootstrap, source, total, args.partitions, payload, args.input_batch_bytes)
    )
    loader.start()
    await anyio.to_thread.run_sync(loader.join)
    assert loader.exitcode == 0

    # Time transactions by wrapping Committer._transact, so the runtime itself has no instrumentation.
    done = anyio.Event()
    positions: dict[int, int] = {}
    commits: list[tuple[int, float]] = []
    measured_start = measured_cpu = finish = finish_cpu = None
    measured_count = 0
    original = Committer._transact

    def transact(self, outputs, offsets, metadata) -> None:
        nonlocal measured_start, measured_cpu, measured_count, finish, finish_cpu
        start = time.monotonic()
        original(self, outputs, offsets, metadata)
        now = time.monotonic()
        commits.append((len(outputs), now - start))
        for partition in offsets:
            positions[partition.partition] = partition.offset
        count = sum(positions.values())
        if measured_start is None and count >= args.warmup:
            measured_start, measured_cpu, measured_count = now, time.process_time(), count
        if count == total:
            finish, finish_cpu = now, time.process_time()
            anyio.from_thread.run_sync(done.set)

    delay = args.handler_ms / 1000

    async def handler(record: ConsumerRecord) -> list[ProducerRecord]:
        if delay:
            await anyio.sleep(delay)
        return [ProducerRecord(target, record.key(), record.value(), partition=record.partition())]

    options = dict(option.split("=", 1) for option in args.consumer_option)
    runtime = Runtime(
        Config(
            bootstrap_servers=args.bootstrap,
            group_id=prefix,
            offset_policy=resume_or_earliest,
            max_buffered_bytes=args.max_buffered_bytes,
            commit_batch_size=args.batch,
            commit_interval_ms=args.interval,
            consumer_options=options,
        ),
        [source],
        handler,
        middleware=[ConcurrencyMiddleware(max_concurrency=args.max_concurrency)] if args.max_concurrency else (),
        key_concurrency={source: args.key_concurrency} if args.key_concurrency else None,
    )
    baseline_rss = rss_mb()
    Committer._transact = transact
    wall = time.monotonic()
    try:
        with anyio.fail_after(300):
            async with anyio.create_task_group() as tasks:
                await tasks.start(runtime.run)
                await done.wait()
                tasks.cancel_scope.cancel()
    finally:
        Committer._transact = original
    stopped = time.monotonic()
    peak_rss = rss_mb()
    assert all(offset == total // args.partitions for offset in positions.values()), positions
    assert len(positions) == args.partitions
    assert measured_start is not None and finish is not None and measured_cpu is not None and finish_cpu is not None
    durations = sorted(duration * 1000 for _, duration in commits)
    report = {
        "committed_records_per_second": round((total - measured_count) / (finish - measured_start)),
        "measured_records": total - measured_count,
        "measured_seconds": round(finish - measured_start, 3),
        "measured_cpu_seconds": round(finish_cpu - measured_cpu, 3),
        "startup_to_last_commit_seconds": round(finish - wall, 3),
        "startup_through_close_seconds": round(stopped - wall, 3),
        "transactions": len(commits),
        "mean_batch": round(statistics.mean(size for size, _ in commits)),
        "mean_commit_ms": round(statistics.mean(durations), 2),
        "p95_commit_ms": round(durations[int(len(durations) * 0.95)], 2),
        "baseline_rss_mb": baseline_rss,
        "peak_rss_mb": peak_rss,
    }
    print(json.dumps(report, indent=2), flush=True)

    # Verify the committed offsets and the outputs after the measurement.
    reader = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": prefix,
            "isolation.level": "read_committed",
            "enable.auto.commit": False,
        }
    )
    try:
        committed = await anyio.to_thread.run_sync(
            lambda: reader.committed([TopicPartition(source, p) for p in range(args.partitions)], timeout=10)
        )
        assert all(partition.offset == total // args.partitions for partition in committed), committed
        reader.assign([TopicPartition(target, p, 0) for p in range(args.partitions)])
        seen: set[bytes] = set()
        with anyio.fail_after(60):
            while len(seen) < total:
                for message in await anyio.to_thread.run_sync(lambda: reader.consume(10_000, 0.1)):
                    assert not message.error(), message.error()
                    key = message.key()
                    assert key is not None and key not in seen, key
                    assert int(key) % args.partitions == message.partition()
                    assert message.value() == payload
                    seen.add(key)
        # Detect late duplicates after reading the expected number of unique keys.
        assert not await anyio.to_thread.run_sync(lambda: reader.consume(10_000, 0.2))
    finally:
        await anyio.to_thread.run_sync(reader.close)
    print(f"Verified {total:,} unique read_committed outputs and exact broker offsets.", flush=True)
    for future in [*admin.delete_topics([source, target]).values(), *admin.delete_consumer_groups([prefix]).values()]:
        await anyio.to_thread.run_sync(future.result)
    return report


if __name__ == "__main__":
    main()
