"""The workload every app runs: decode a JSON record, wait for its I/O, and produce one transformed output."""

import argparse
import json


def process(value: bytes) -> tuple[float, bytes]:
    """Return how long the record's I/O takes, in seconds, and its output.

    The harness writes each record's I/O latency into it, so every framework waits the same times.
    """
    payload = json.loads(value)
    payload["status"] = "processed"
    payload["customer"]["name"] = payload["customer"]["name"].upper()
    return payload["io_ms"] / 1000, json.dumps(payload, separators=(",", ":")).encode()


def add_commit_args(parser: argparse.ArgumentParser) -> None:
    """Set potik's default commit thresholds, which Faust also gets in place of its own 10,000 records and 2.8s."""
    parser.add_argument("--commit-batch-size", type=int, default=1000, help="potik and Faust commit count threshold")
    parser.add_argument("--commit-interval-ms", type=int, default=100, help="potik and Faust commit timer")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run until SIGTERM.")
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--partitions", type=int, required=True)
    parser.add_argument("--health-port", type=int, default=8080, help="where apps with health probes serve them")
    add_commit_args(parser)
    args = parser.parse_args()
    if args.commit_batch_size < 1 or args.commit_interval_ms < 1:
        parser.error("commit thresholds must be positive")
    return args
