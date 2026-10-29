# /// script
# requires-python = "==3.12.*"
# dependencies = ["confluent-kafka==2.16.0", "testcontainers>=4.15.0", "psutil>=7.0.0"]
# ///
"""Compare Kafka frameworks on a workload shaped like production: hundreds of partitions, I/O-bound handlers, and 1KB JSON records.

Run:
    uv run --script benchmarks/comparison/run.py steady
    uv run --script benchmarks/comparison/run.py backlog

`steady` feeds input at a fixed rate below capacity, as most of a service's life looks, and measures end-to-end latency, CPU, and memory. `backlog` seeds a lag before the app starts, as after a deploy or an outage, and measures how fast the app drains it.

Each app runs unmodified in its own process and environment, as it would be deployed. It starts, works until SIGTERM, and shuts down gracefully. This script measures everything from outside the app. Without --bootstrap, the session starts a throwaway broker (`tools/kafka_broker.py`).
"""

import argparse
import json
import math
import os
import platform
import random
import resource
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path

import psutil
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from workload import add_commit_args

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "tools"))

from kafka_broker import Broker

RESULTS = HERE / "results"
APPS = {
    "potik": ("app_potik.py", "exactly-once, ordered: shared transactions on one producer (KIP-447)"),
    "potik-starlette": ("app_potik_starlette.py", "as potik, in a Starlette app that serves the probes"),
    "potik-keys": ("app_potik_keys.py", "exactly-once, ordered per key: as potik, with 200 keys in progress at once"),
    "faststream": ("app_faststream.py", "at-least-once, ordered: a consumer per partition, a commit per record"),
    "faststream-defaults": ("app_faststream_defaults.py", "unordered, can lose or repeat records: auto-commit, acks=1"),
    "faust": ("app_faust.py", "at-least-once, ordered: offsets in transactions, outputs outside them"),
    "faust-acks": ("app_faust_acks.py", "exactly-once, ordered per key: a modified agent"),
}
# Apps that serve /livez and /readyz, which run.py probes during each run.
PROBED = {"potik-starlette"}
# Written to every output partition before an app starts. Once the observer has read them all, it is fetching every partition and can time the first output.
MARKER = b"observer-ready"
PACKAGES = ("potik", "faststream", "faust-streaming", "aiokafka", "confluent-kafka", "anyio", "starlette", "uvicorn")


class Records:
    """Makes input records shaped like production: a UUID key, about 1KB of JSON, and a `traceparent` header.

    Each record carries its I/O latency, drawn from a lognormal distribution, so every app waits the same times. The same seed makes the same records.
    """

    def __init__(self, args: argparse.Namespace) -> None:
        self._rng = random.Random(args.seed)
        self._mu, self._sigma = math.log(args.io_median_ms), args.io_sigma

    def make(self) -> tuple[bytes, bytes, list[tuple[str, bytes]]]:
        rng = self._rng
        key = str(uuid.UUID(int=rng.getrandbits(128), version=4))
        payload = {
            "id": key,
            "sent_at": time.time(),
            "io_ms": round(rng.lognormvariate(self._mu, self._sigma), 3),
            "customer": {
                "id": f"cus_{rng.getrandbits(48):012x}",
                "name": "Ada Lovelace",
                "email": "ada.lovelace@example.com",
                "tier": "gold",
                "country": "GB",
            },
            "items": [
                {
                    "sku": f"SKU-{rng.randrange(10**6):06d}",
                    "name": "Analytical engine part",
                    "quantity": rng.randint(1, 5),
                    "unit_price": round(rng.uniform(1, 200), 2),
                }
                for _ in range(6)
            ],
            "shipping": {
                "method": "express",
                "address": {"line1": "12 St James's Square", "city": "London", "postcode": "SW1Y 4JH", "country": "GB"},
            },
            "currency": "GBP",
            "created_at": "2026-10-29T14:03:12.481Z",
            "source": "checkout-api",
        }
        traceparent = f"00-{rng.getrandbits(128):032x}-{rng.getrandbits(64):016x}-01".encode()
        return key.encode(), json.dumps(payload, separators=(",", ":")).encode(), [("traceparent", traceparent)]


def produce(producer: Producer, topic: str, record: tuple[bytes, bytes, list[tuple[str, bytes]]]) -> None:
    key, value, headers = record
    while True:
        try:
            producer.produce(topic, value, key, headers=headers)
            return
        except BufferError:  # The local queue is full until the broker acknowledges a batch.
            producer.poll(0.01)


class Feeder(threading.Thread):
    """Produces records at a fixed rate, with librdkafka's default batching, until stopped."""

    def __init__(self, producer: Producer, topic: str, records: Records, rate: float) -> None:
        super().__init__(daemon=True)
        self.producer, self.topic, self.records, self.rate = producer, topic, records, rate
        self.sent = 0
        self.halt = threading.Event()

    def run(self) -> None:
        started = time.monotonic()
        while not self.halt.wait(0.005):
            due = int((time.monotonic() - started) * self.rate)
            while self.sent < due:
                produce(self.producer, self.topic, self.records.make())
                self.sent += 1
            self.producer.poll(0)
        assert self.producer.flush(30) == 0


class Observer(threading.Thread):
    """Records when each output becomes visible to read_committed consumers, and its latency since its input was sent."""

    def __init__(self, bootstrap: str, topic: str, partitions: int) -> None:
        super().__init__(daemon=True)
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"{topic}-observer",  # Assigned partitions, so it never joins the group.
                "isolation.level": "read_committed",
                "enable.auto.commit": False,
                "fetch.wait.max.ms": 5,
            }
        )
        self.consumer.assign([TopicPartition(topic, p, 0) for p in range(partitions)])
        self.partitions = partitions
        self.ready = threading.Event()
        self.keys: set[bytes] = set()
        self.total = 0
        self.arrivals: list[float] = []  # monotonic time of each unique output
        self.latencies: list[tuple[float, float]] = []  # (sent_at, seconds until visible) of each unique output
        self.halt = threading.Event()

    def run(self) -> None:
        marked = set()
        try:
            while not self.halt.is_set():
                messages = self.consumer.consume(10_000, 0.01)
                now, wall = time.monotonic(), time.time()
                for message in messages:
                    if message.error():
                        raise RuntimeError(message.error())
                    if message.value() == MARKER:
                        marked.add(message.partition())
                        if len(marked) == self.partitions:
                            self.ready.set()
                        continue
                    self.total += 1
                    if (key := message.key()) not in self.keys:
                        self.keys.add(key)
                        self.arrivals.append(now)
                        sent_at = json.loads(message.value())["sent_at"]
                        self.latencies.append((sent_at, wall - sent_at))
        finally:
            self.consumer.close()


class Prober(threading.Thread):
    """Requests /livez and /readyz every `interval` seconds, as a kubelet does, and records each answer."""

    def __init__(self, port: int, interval: float) -> None:
        super().__init__(daemon=True)
        self.port, self.interval = port, interval
        self.answers: list[tuple[float, int, float]] = []  # when, status, latency in seconds
        self.halt = threading.Event()

    def run(self) -> None:
        while not self.halt.wait(self.interval):
            for path in ("/livez", "/readyz"):
                started = time.monotonic()
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=5) as response:
                        status = response.status
                except urllib.error.HTTPError as error:
                    status = error.code
                except OSError:
                    continue  # Not listening yet, or no longer.
                self.answers.append((started, status, time.monotonic() - started))


def committed(admin: AdminClient, group: str, topic: str, partitions: int) -> int:
    request = ConsumerGroupTopicPartitions(group, [TopicPartition(topic, p) for p in range(partitions)])
    result = admin.list_consumer_group_offsets([request], require_stable=True)[group].result()
    return sum(max(tp.offset, 0) for tp in result.topic_partitions)


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def run_app(args: argparse.Namespace, admin: AdminClient, producer: Producer, python: str, name: str, tag: str) -> dict:
    """Run one app on one scenario, and return its measurements, or the error that failed the run."""
    target, group = f"{tag}-out", tag
    source = args.backlog_topic if args.scenario == "backlog" else f"{tag}-in"
    topics = [target] if args.scenario == "backlog" else [target, source]
    for future in admin.create_topics([NewTopic(topic, args.partitions, 1) for topic in topics]).values():
        future.result()
    observer = Observer(args.bootstrap, target, args.partitions)
    observer.start()
    for partition in range(args.partitions):
        producer.produce(target, MARKER, partition=partition)
    assert producer.flush(30) == 0
    if not observer.ready.wait(60):
        raise RuntimeError("the observer did not read its markers")
    log_path = RESULTS / "logs" / f"{tag}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    command = [python, str(HERE / APPS[name][0]), "--bootstrap", args.bootstrap, "--input", source, "--output", target]
    command += ["--group", group, "--partitions", str(args.partitions), "--health-port", str(port)]
    command += [
        "--commit-batch-size",
        str(args.commit_batch_size),
        "--commit-interval-ms",
        str(args.commit_interval_ms),
    ]
    prober = Prober(port, args.probe_interval) if name in PROBED and args.probe_interval else None
    feeder = Feeder(producer, source, Records(args), args.rate) if args.scenario == "steady" else None
    result: dict = {"framework": name, "group": group, "log": str(log_path)}
    with log_path.open("w") as log:
        spawned = time.monotonic()
        process = subprocess.Popen(command, cwd=HERE, stdout=log, stderr=subprocess.STDOUT)
    app = psutil.Process(process.pid)
    for thread in (prober, feeder):
        if thread is not None:
            thread.start()

    def wait(timeout: float) -> tuple[int, resource.struct_rusage] | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            pid, status, usage = os.wait4(process.pid, os.WNOHANG)
            if pid:
                process.returncode = os.waitstatus_to_exitcode(status)
                return process.returncode, usage
            time.sleep(0.002)
        return None

    try:
        # Steady: measure the records sent in [window, window + duration], after a warm-up that starts at the first output. Then stop feeding, and wait for the rest.
        window = cpu = None
        expected = args.messages if feeder is None else None
        progress, progressed, reported = 0, spawned, spawned
        while expected is None or len(observer.keys) < expected:
            now = time.monotonic()
            if process.poll() is not None:
                raise RuntimeError(f"exited with {process.returncode} before finishing")
            if observer.arrivals and window is None:
                window = observer.arrivals[0] + args.warmup
            if feeder is not None and window is not None:
                if cpu is None and now >= window:
                    cpu, window_wall = app.cpu_times(), time.time()
                if expected is None and now >= window + args.duration:
                    times = app.cpu_times()
                    result["cpu_percent"] = 100 * (times.user + times.system - cpu.user - cpu.system) / args.duration
                    result["steady_rss_mib"] = app.memory_info().rss / 2**20
                    feeder.halt.set()
                    feeder.join()
                    expected = feeder.sent
                    measured = (window_wall, window_wall + args.duration)
            if len(observer.keys) > progress:
                progress, progressed = len(observer.keys), now
            elif now - progressed > (args.stall_timeout if observer.arrivals else args.startup_timeout):
                raise RuntimeError(f"stalled at {progress:,} outputs")
            if now - spawned > args.run_timeout:
                raise RuntimeError(f"too slow: {progress:,} outputs in {args.run_timeout:.0f}s")
            if now - reported >= 10:
                print(f"  {name:<19} ... {progress:,} outputs after {now - spawned:.0f}s", flush=True)
                reported = now
            time.sleep(0.005)
        done = observer.arrivals[-1]
        while committed(admin, group, source, args.partitions) < expected:
            if time.monotonic() - done > args.stall_timeout:
                raise RuntimeError("offsets were not committed")
            time.sleep(0.01)
        stopping = time.monotonic()
        process.send_signal(signal.SIGTERM)
        if (exited := wait(args.stall_timeout)) is None:
            raise RuntimeError("did not exit within the stall timeout after SIGTERM")
        code, usage = exited
        result["shutdown_s"] = time.monotonic() - stopping
        time.sleep(1)  # Give late duplicates a second to arrive, e.g. replays during shutdown.
        result |= {
            "exit_code": code,
            "startup_s": observer.arrivals[0] - spawned,
            "cpu_s": usage.ru_utime + usage.ru_stime,
            "peak_rss_mib": usage.ru_maxrss / (2**20 if sys.platform == "darwin" else 2**10),
            "duplicates": observer.total - len(observer.keys),
        }
        if feeder is None:
            # The drain rate between 10% and 90% of the outputs leaves out startup and the last stragglers.
            low, high = (
                observer.arrivals[len(observer.arrivals) // 10],
                observer.arrivals[len(observer.arrivals) * 9 // 10],
            )
            result |= {"drain_msgs_per_s": 0.8 * expected / (high - low), "total_s": done - spawned}
        else:
            latencies = [latency for sent, latency in observer.latencies if measured[0] <= sent < measured[1]]
            result |= {
                "measured": len(latencies),
                "latency_p50_ms": 1000 * percentile(latencies, 0.5),
                "latency_p99_ms": 1000 * percentile(latencies, 0.99),
                "latency_max_ms": 1000 * max(latencies),
            }
        if prober is not None:
            # Probes while the app processed, from its first output until SIGTERM.
            timed = [
                (status, latency) for at, status, latency in prober.answers if observer.arrivals[0] <= at < stopping
            ]
            latencies = sorted(latency for status, latency in timed if status == 200)
            result |= {
                "probes": len(timed),
                "probes_failed": sum(1 for status, _ in timed if status != 200),
                "probe_p50_ms": statistics.median(latencies) * 1000 if latencies else None,
                "probe_max_ms": latencies[-1] * 1000 if latencies else None,
            }
        if code != 0:
            result["error"] = f"exited with {code} after SIGTERM"
    except Exception as error:  # noqa: BLE001 -- a failed run is a result
        result["error"] = str(error)
    finally:
        for thread in (prober, feeder):
            if thread is not None:
                thread.halt.set()
        if process.returncode is None:  # Also on KeyboardInterrupt, so the app never outlives the run.
            process.kill()
            wait(10)
        observer.halt.set()
        observer.join()
        admin.delete_topics(topics)
        admin.delete_consumer_groups([group])
    if "error" in result:
        tail = log_path.read_text().splitlines()[-15:]
        print(f"  {name} FAILED: {result['error']}\n    " + "\n    ".join(tail), flush=True)
    elif feeder is None:
        print(
            f"  {name:<19} drain {result['drain_msgs_per_s']:>6.0f} msg/s  startup {result['startup_s']:.1f}s"
            f"  total {result['total_s']:.1f}s  cpu {result['cpu_s']:.1f}s  peak rss {result['peak_rss_mib']:.0f}MiB"
            f"  duplicates {result['duplicates']}",
            flush=True,
        )
    else:
        print(
            f"  {name:<19} latency p50 {result['latency_p50_ms']:.0f}ms p99 {result['latency_p99_ms']:.0f}ms"
            f"  cpu {result['cpu_percent']:.0f}%  rss {result['steady_rss_mib']:.0f}MiB"
            f"  startup {result['startup_s']:.1f}s  duplicates {result['duplicates']}",
            flush=True,
        )
    return result


def prepare(script: Path) -> tuple[str, dict[str, str]]:
    """Resolve the app's environment before any timing, and return its Python and package versions."""
    subprocess.run(["uv", "sync", "--quiet", "--script", str(script)], check=True)
    python = subprocess.check_output(["uv", "python", "find", "--script", str(script)], text=True).strip()
    probe = f"import importlib.metadata as m, json, platform\nv = {{'python': platform.python_version()}}\nfor p in {PACKAGES!r}:\n"
    probe += "    try: v[p] = m.version(p)\n    except m.PackageNotFoundError: pass\nprint(json.dumps(v))"
    return python, json.loads(subprocess.check_output([python, "-c", probe], text=True))


def summarize(args: argparse.Namespace, runs: list[dict]) -> None:
    if args.scenario == "steady":
        print(
            f"\nSteady: {args.rate:,.0f} msg/s into {args.partitions} partitions, I/O median {args.io_median_ms}ms"
            f" (sigma {args.io_sigma}), measured for {args.duration:.0f}s after {args.warmup:.0f}s of warm-up."
        )
        columns = {
            "p50_ms": ("latency_p50_ms", "{:.0f}"),
            "p99_ms": ("latency_p99_ms", "{:.0f}"),
            "max_ms": ("latency_max_ms", "{:.0f}"),
            "cpu_%": ("cpu_percent", "{:.0f}"),
            "rss_MiB": ("steady_rss_mib", "{:.0f}"),
            "startup_s": ("startup_s", "{:.1f}"),
        }
    else:
        print(
            f"\nBacklog: {args.messages:,} messages in {args.partitions} partitions, I/O median {args.io_median_ms}ms"
            f" (sigma {args.io_sigma})."
        )
        columns = {
            "drain_msg/s": ("drain_msgs_per_s", "{:.0f}"),
            "startup_s": ("startup_s", "{:.1f}"),
            "total_s": ("total_s", "{:.1f}"),
            "cpu_s": ("cpu_s", "{:.1f}"),
            "peak_rss_MiB": ("peak_rss_mib", "{:.0f}"),
        }
    print(f"Medians of {args.repeat} run(s).\n")
    header = ("framework", *columns, "dups", "probe_ms")
    rows = [header]
    for name in args.frameworks:
        ok = [run for run in runs if run["framework"] == name and "error" not in run]
        failed = sum(1 for run in runs if run["framework"] == name and "error" in run)
        if not ok:
            rows.append((name, "failed", *[""] * (len(header) - 2)))
            continue
        probed = [run for run in ok if run.get("probe_p50_ms") is not None]
        rows.append(
            (
                name + (f" ({failed} failed)" if failed else ""),
                *(form.format(statistics.median(run[key] for run in ok)) for key, form in columns.values()),
                str(sum(run["duplicates"] for run in ok)),
                f"{statistics.median(run['probe_p50_ms'] for run in probed):.1f}" if probed else "-",
            )
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    for i, row in enumerate(rows):
        print(
            "  ".join(
                cell.ljust(w) if j == 0 else cell.rjust(w) for j, (cell, w) in enumerate(zip(row, widths, strict=True))
            )
        )
        if i == 0:
            print("  ".join("-" * width for width in widths))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scenario", choices=("steady", "backlog"))
    parser.add_argument("--bootstrap", help="an existing broker, instead of a throwaway one")
    parser.add_argument("--frameworks", nargs="+", choices=tuple(APPS), default=list(APPS))
    parser.add_argument("--partitions", type=int, default=200)
    parser.add_argument("--io-median-ms", type=float, default=10, help="median I/O latency of a handler call")
    parser.add_argument(
        "--io-sigma", type=float, default=0.8, help="lognormal shape: 0.8 puts p99 at about 6x the median"
    )
    parser.add_argument("--rate", type=float, default=1000, help="steady: input messages per second")
    parser.add_argument(
        "--warmup", type=float, default=10, help="steady: seconds after the first output before measuring"
    )
    parser.add_argument("--duration", type=float, default=30, help="steady: seconds of measured input")
    parser.add_argument("--messages", type=int, default=100_000, help="backlog: messages seeded before each app starts")
    parser.add_argument("--seed", type=int, default=1, help="the same seed makes the same records and I/O latencies")
    parser.add_argument("--repeat", type=int, default=3, help="rounds; frameworks alternate within each")
    parser.add_argument("--startup-timeout", type=float, default=180, help="seconds before the first output")
    parser.add_argument("--stall-timeout", type=float, default=30, help="seconds without progress before failing")
    parser.add_argument("--run-timeout", type=float, default=600, help="seconds per run before failing")
    parser.add_argument("--probe-interval", type=float, default=1, help="seconds between health probes, or 0 for none")
    add_commit_args(parser)
    args = parser.parse_args()
    if min(args.partitions, args.repeat, args.messages, args.rate, args.duration, args.io_median_ms) <= 0:
        parser.error("partitions, repeat, messages, rate, duration, and io-median-ms must be positive")

    environments = {name: prepare(HERE / APPS[name][0]) for name in args.frameworks}
    with ExitStack() as stack:
        if args.bootstrap:
            broker = {"bootstrap": args.bootstrap}
        else:
            started = stack.enter_context(Broker())
            args.bootstrap, broker = started.bootstrap, started.describe()
        compare(args, environments, broker)


def compare(args: argparse.Namespace, environments: dict[str, tuple[str, dict[str, str]]], broker: dict) -> None:
    session = datetime.now(UTC).strftime("bench-%Y%m%dT%H%M%S")
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    producer = Producer({"bootstrap.servers": args.bootstrap})  # librdkafka's defaults, as a typical producer
    args.backlog_topic = f"{session}-in"
    if args.scenario == "backlog":
        admin.create_topics([NewTopic(args.backlog_topic, args.partitions, 1)])[args.backlog_topic].result()
        records = Records(args)
        for _ in range(args.messages):
            produce(producer, args.backlog_topic, records.make())
            producer.poll(0)
        assert producer.flush(60) == 0
        print(
            f"Seeded {args.backlog_topic}: {args.messages:,} messages across {args.partitions} partitions.", flush=True
        )

    revision = subprocess.run(
        ["git", "describe", "--always", "--dirty"], cwd=HERE, capture_output=True, text=True, check=False
    )
    runs: list[dict] = []
    report = {
        "session": session,
        "git": revision.stdout.strip(),
        "platform": platform.platform(),
        "broker": broker,
        "config": {k: v for k, v in vars(args).items() if k != "bootstrap"},
        "frameworks": {
            name: {"semantics": APPS[name][1], "packages": environments[name][1]} for name in args.frameworks
        },
        "runs": runs,
    }
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / f"{session}.json"
    try:
        for round_ in range(args.repeat):
            print(f"Round {round_ + 1}/{args.repeat}", flush=True)
            first = round_ % len(args.frameworks)
            for name in args.frameworks[first:] + args.frameworks[:first]:
                tag = f"{session}-{name}-{round_}"
                runs.append(run_app(args, admin, producer, environments[name][0], name, tag) | {"round": round_})
                path.write_text(json.dumps(report, indent=2) + "\n")  # An interrupted session keeps its runs.
    finally:
        if args.scenario == "backlog":
            admin.delete_topics([args.backlog_topic])
    summarize(args, runs)
    with (RESULTS / "history.jsonl").open("a") as history:
        history.write(json.dumps(report) + "\n")
    print(f"\nResults: {path}")
    if any("error" in run for run in runs):
        sys.exit("Some runs failed.")


if __name__ == "__main__":
    main()
