# Framework comparison

This benchmark runs potik and other Python Kafka frameworks on a workload shaped like production, to show what each one costs and guarantees in ordinary use. It is not a race to the highest number: most services spend their lives below capacity, waiting on databases and HTTP calls, so latency, CPU, and memory at a steady rate matter more than a peak.

```sh
uv run --script benchmarks/comparison/run.py steady
uv run --script benchmarks/comparison/run.py backlog
uv run --script benchmarks/comparison/run.py backlog --partitions 12 --messages 30000 --frameworks potik potik-keys
```

Each session starts a throwaway broker container (`tools/kafka_broker.py`) with Docker or Podman. Pass `--bootstrap` to use an existing broker instead. Runs delete the topics and groups they create.

## Workload

- **Records:** a UUID key, about 940 bytes of order JSON, and a `traceparent` header, written by a librdkafka producer with its default settings.
- **Handler:** decodes the JSON, waits for its I/O, transforms a field, and produces one output with the input's key. The I/O latency follows a lognormal distribution with a 10ms median and a p99 of about 64ms, like a database or HTTP call. The harness writes each record's latency into it from a seeded generator, so every framework waits exactly the same times.
- **Partitions:** 200, all in one app process, as one instance of a service with hundreds of partitions.

Two scenarios:

- **`steady`** feeds 1,000 records per second, well below every app's capacity, and measures the records sent in 30 seconds after 10 seconds of warm-up. It reports each output's latency from its input's send to its visibility to a `read_committed` consumer, the app's CPU, and its RSS.
- **`backlog`** seeds 100,000 records before the app starts, as after a deploy or an outage, and measures how fast the app drains them, between 10% and 90% of the outputs, so that startup and stragglers stay out. Startup is reported on its own.

## Apps

Each `app_*.py` is the app as you would deploy it, with no instrumentation. It runs until SIGTERM, then shuts down gracefully. Each one is a PEP 723 script with its own environment, which `run.py` resolves before it times anything.

| App | Framework and settings | Guarantee in this setup |
| --- | --- | --- |
| `potik` | potik's defaults | exactly-once and ordered per partition: outputs and offsets commit in shared transactions |
| `potik-starlette` | as `potik`, in a Starlette app that serves `/livez` and `/readyz` | as `potik` |
| `potik-keys` | as `potik`, with `key_concurrency=200` on its handler | exactly-once and ordered per key: up to 200 keys in progress at once across partitions, as many as the other apps' 200 workers, and a record that finishes early waits for the earlier records of its partition before it commits |
| `faststream` | FastStream with `AckPolicy.NACK_ON_ERROR` and `max_workers` set to the partition count, producer with `acks=all` and idempotence | at-least-once and ordered per partition: each partition has its own consumer in the group, which commits each record after its output is acknowledged |
| `faststream-defaults` | FastStream's default acknowledgement and producer, with `max_workers` set to the partition count | neither at-least-once nor at-most-once, and unordered: aiokafka commits consumed records every 5 seconds, whether their handlers finished or not, so a crash can lose records in flight or repeat those since the last commit. Records of one partition run concurrently |
| `faust` | faust-streaming with `processing_guarantee="exactly_once"` and `isolated_partitions` | at-least-once and ordered per partition: offsets commit in transactions, but outputs are sent outside them (see below) |
| `faust-acks` | faust-streaming with a modified agent that sends through the input partition's transactional producer | exactly-once, ordered per key: concurrent workers with per-key locks and in-order acks |

**Faust's outputs were not transactional in this setup.** With `processing_guarantee="exactly_once"`, faust-streaming 0.15.4 picks a send's transactional producer by the output record's partition, which it looks up in metadata that its consumer does not have for the output topic. It then sends without a transaction. A topic's transaction markers take offsets that consumers never see, so we counted them: on 2,000 inputs, potik's output topic used 2,288 offsets and `faust-acks`' 3,285, while stock Faust's used exactly 2,000. Its outputs are visible at once and do not commit with their input's offset, so a crash replays them. Run `faust` and `faust-acks` with `--frameworks` to count the markers yourself, and open an issue if a Faust setting we missed changes this.

## Configuration

The apps keep each framework's defaults, except where a default would weaken the guarantee in the table or add latency for no reason:

- FastStream's strict app sets `acks=all` and idempotence, because aiokafka defaults to `acks=1`, which can lose an acknowledged output. `faststream-defaults` keeps `acks=1`, as its name says.
- Faust commits every 1,000 records or 100ms, potik's defaults, instead of its own 10,000 records or 2.8 seconds, which would add seconds of latency to every output.
- Producers otherwise keep their defaults: potik's librdkafka producer lingers 5ms, and the aiokafka producers of FastStream and Faust send at once.

Every app uses CPython 3.12 and the standard asyncio loop. Input consumers read `read_committed`, except `faststream-defaults`, which keeps aiokafka's default.

## Results

Recorded on 29 October 2026 with the commands above and their defaults. Each cell is the median of three runs. All 36 runs finished, with every input's offset committed and no duplicate outputs. The Starlette variant answered every probe with HTTP 200, in 1.5ms at the median. The key concurrency rows and the 12-partition backlog come from later sessions on the same day and machine, run with `--frameworks`. Their 12 runs also finished with every offset committed and no duplicates. In the first steady run, the p99 reached 660ms while the broker's transaction coordinator was still loading. The median of the three runs leaves it out.

### Steady: 1,000 records/s into 200 partitions

| App | Latency p50 | p99 | max | CPU (one core) | RSS | Startup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| potik | 136ms | 213ms | 390ms | 38% | 46MB | 1.1s |
| potik + Starlette | 137ms | 215ms | 384ms | 39% | 41MB | 0.6s |
| potik, key concurrency | 137ms | 210ms | 401ms | 52% | 37MB | 1.1s |
| FastStream | 26ms | 90ms | 244ms | 62% | 88MB | 0.9s |
| FastStream, defaults | 28ms | 83ms | 243ms | 75% | 50MB | 0.4s |
| Faust | 36ms | 422ms | 568ms | 71% | 85MB | 2.8s |
| Faust, modified agent | 663ms | 1,184ms | 1,897ms | 85% | 106MB | 2.7s |

The I/O alone takes 10ms at the median and 64ms at p99, so the rest of each latency is the framework's. FastStream makes an output visible as soon as the broker acknowledges it, and so does Faust, whose outputs bypass its transactions. potik makes it visible when its transaction commits: a transaction starts at most every 100ms (`commit_interval_ms`), and one that covers 200 partitions takes tens of milliseconds more. That is the price of exactly-once, about 110ms at the median here. In exchange, potik uses the least CPU and memory. With `commit_interval_ms=20`, potik's median falls to 59ms (p99 197ms) at 42% CPU, and with 50ms to 90ms (p99 174ms) at 38%. Each transaction also writes a marker to every partition it covers, so shorter intervals cost the brokers more.

Key concurrency leaves the latency unchanged, because at 5 records per second per partition, records rarely wait for the one before them, and the transaction interval dominates. It costs about 14% of a core more at this rate. Each record of a key that is not in progress starts a task of its own, and here every record has a new key. We did not profile where the rest goes.

### Backlog: 100,000 records in 200 partitions

| App | Drain rate | Startup | Total | CPU time | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| potik | 4,952/s | 0.8s | 23.6s | 9.4s | 93MB |
| potik + Starlette | 4,784/s | 0.7s | 24.5s | 8.6s | 102MB |
| potik, key concurrency | 8,233/s | 0.7s | 15.8s | 7.4s | 105MB |
| FastStream | 4,123/s | 0.9s | 28.2s | 27.2s | 325MB |
| FastStream, defaults | 3,805/s | 0.7s | 27.4s | 26.7s | 306MB |
| Faust | 1,867/s | 2.7s | 63.0s | 26.5s | 484MB |
| Faust, modified agent | 1,538/s | 2.9s | 67.3s | 38.2s | 504MB |

With 200 partitions and a mean I/O of about 14ms, ordered processing could drain about 14,500 records/s. No app comes close. FastStream spends about a core for its whole run, so its CPU limits it. potik uses under half a core: its flow control limits it instead, because each fetch takes half of one partition's share of `max_buffered_bytes`, about 30 of these records with 200 partitions, for all partitions at once. With `max_buffered_bytes` at 64MB instead of 16MB, potik drained 7,700-8,100 records/s, with 5.2s of CPU and a 130MB peak. So with hundreds of partitions, potik's default budget, not the workload, limits how fast it catches up.

Key concurrency drains 66% faster with the default budget, at 8,233 records/s, with less CPU per record and a 12MB higher peak. The records that run or wait for an earlier record of their partition count toward the budget, so it pays for the extra concurrency in memory that the budget already bounds. Its records arrive through the same fetches of half a share, which still keep it below the 14,300 records/s that 200 keys in progress allow at a mean I/O of 14ms. The modified Faust agent, the only other app here that is exactly-once and ordered per key, drains 1,538 records/s at a 504MB peak.

Peak memory differs most. The aiokafka apps reach 300-500MB while they catch up, against potik's 93MB, which its byte budget and librdkafka's prefetch limit bound. aiokafka fetches up to 1MB per partition and 50MB per request by default, the likely cause with 200 partitions, though we did not measure where their memory goes. Size a Kubernetes memory limit for this peak, not for the steady state.

### How these numbers were obtained

- **Hardware:** Apple M3 Pro, 18 GiB RAM, macOS 27.0.1 on ARM64. The apps and the harness ran on the host. Kafka ran in a Podman VM with 5 CPUs and 3.7 GiB RAM.
- **Broker:** `apache/kafka:4.1.0`, one broker, replication factor 1, a fresh container per session.
- **Software:** CPython 3.12.13; potik 0.1.0; confluent-kafka 2.16.0; FastStream 0.7.7; faust-streaming 0.15.4; aiokafka 0.14.0; AnyIO 4.15.1; Starlette 1.10.0 and uvicorn 0.54.0.

### What these results do not show

- One broker without replication: acknowledgements and transactions cost more on a replicated cluster, for every app.
- One app process: a service with several instances splits the partitions and the load among them.
- No failures: no crashes, rebalances, or broker outages were injected, so zero duplicates here does not test any guarantee under failure. The guarantees in the apps table come from the frameworks' designs and the transaction markers counted above.
- The harness ran on the same machine as the apps and shares its CPUs.

## Measurement

`run.py` measures everything from outside the app. For each run, it creates an output topic, waits until a `read_committed` observer is fetching every partition, and starts the app. In `steady`, it feeds a fresh input topic from the app's start, and stops feeding once the measured 30 seconds of input are sent. A run then lasts until every output is visible and the group's committed offsets cover every input. It records:

- **latency:** from an input's send to its output's visibility, for the inputs sent in the measured window. The observer polls every 10ms, which adds up to 10ms to every app alike.
- **cpu:** the app process's CPU time during the measured window, as a percentage of one core, read with psutil. It covers all threads, including librdkafka's and aiokafka's.
- **rss:** the app's resident memory at the end of the measured window, and in `backlog` its peak, from `wait4`.
- **startup:** from process start until the first output is visible.
- **drain:** in `backlog`, the outputs between the 10th and 90th percentile of arrivals, divided by the time between them.
- **dups:** outputs seen more than once, including those that arrive up to one second after the app exits.
- **probe_ms:** the median latency of `/livez` and `/readyz` for apps that serve them, requested every second, as a kubelet does.

Frameworks take turns within each of the `--repeat` rounds, three by default. The first framework rotates each round, and the tables show medians. A run fails if the app exits before it finishes, shows no output within `--startup-timeout`, stalls for `--stall-timeout`, exceeds `--run-timeout`, or does not exit with code 0 within `--stall-timeout` of SIGTERM.

`run.py` saves each session to `results/<session>.json` after every run, with Python and package versions. App logs go to `results/logs/`. `run.py` also appends each completed session to `results/history.jsonl`.
