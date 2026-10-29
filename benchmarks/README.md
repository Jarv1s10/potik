# Benchmarks

## Throughput

`throughput.py` measures one app on a single local broker. Each input produces one output. Timing covers committed transactions after 20,000 warm-up records. A separate process produces the inputs, so peak RSS covers only the app.

Results from Kafka 4.1 in Podman on an Apple Silicon laptop:

| Scenario | Records/s | Peak RSS |
| --- | ---: | ---: |
| 4 partitions, 128B values | 108k | ~61MB |
| 32 partitions, 1KB values | 47k | ~74MB |
| 32 partitions, 1KB values, 1ms handler | 27k | ~75MB |

The process starts at about 43MB.

A separate producer writes the inputs in batches of up to 16KB (`--input-batch-bytes`), the Java client's default `batch.size`. librdkafka's default, which potik's own producers use, is 1MB. Larger input batches speed up CPU-bound apps: 32 partitions of 1KB values reach about 60k records/s. They slow down handlers that take about a millisecond, because the runtime fetches a partition's records one input batch at a time, and pausing a partition drops the rest of its batch, which librdkafka then fetches again. A batch larger than half a partition's share of `max_buffered_bytes` leads to such pauses. With 1MB input batches, the 1ms handler reaches 14k records/s (77MB), and 20k with `max_buffered_bytes` at 32MB (100MB).

With 180 partitions, 5KB values, a 200ms handler, and `--max-concurrency 30`, throughput is 148 records/s, the limit that the `ConcurrencyMiddleware` sets. Peak RSS is 97MB with the default buffer, and 81MB with `max_buffered_bytes` at 1MB.

These are local measurements, not production guarantees. Measure with your own payloads, handler latency, partition counts, and replication.

```sh
uv run --script benchmarks/throughput.py --records 300000
uv run --script benchmarks/throughput.py --partitions 32 --payload-bytes 1024 --handler-ms 1
uv run --script benchmarks/throughput.py --partitions 32 --payload-bytes 1024 --handler-ms 1 --input-batch-bytes 1000000
uv run --script benchmarks/throughput.py --partitions 180 --payload-bytes 5000 --warmup 1800 --records 7200 --handler-ms 200 --max-concurrency 30
```

`--key-concurrency N` runs the inputs by key, with up to N keys in progress at once. Every input has its own key. Each session starts a throwaway broker container. Pass `--bootstrap` to use an existing broker instead. The app runs in a process of its own. The benchmark checks that every output is unique and visible to `read_committed` readers, and checks the committed offsets. Then it deletes its topics and group, unless a run on an existing broker failed. Each report goes to `benchmarks/results/` with the revision, platform, librdkafka version, and broker.

## Comparison with other frameworks

[comparison/](comparison/README.md) runs potik, FastStream, and Faust on the same workload.
