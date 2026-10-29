# Configuration

A `Config` holds a runtime's connection, consumer group, limits, and librdkafka options. You pass it to `potik.build_runtime()`, or to a core `Runtime`.

```python
from potik import Config, resume_or_earliest

config = Config(
    bootstrap_servers="broker:9092",
    group_id="acceptor",
    offset_policy=resume_or_earliest,
    options={"security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN"},
    producer_options={"compression.type": "zstd"},
)
```

These are the fields you are most likely to change:

| Field | Default | Meaning |
| --- | ---: | --- |
| `offset_policy` | `resume_or_latest` | Where a partition without a committed offset starts: `resume_or_latest`, `resume_or_earliest`, or `resume_or_fail` |
| `commit_interval_ms` | 100 | Minimum time between transactions, unless a batch is full |
| `commit_batch_size` | 1000 | Records per transaction |
| `shutdown_grace_ms` | 0 | How long running handlers may finish on shutdown before the runtime cancels them |
| `revocation_grace_ms` | 0 | The same, for partitions that move to another instance |
| `stop_failed_partitions` | `True` | Whether a failure stops only its partition, or the whole run |

The [API reference](../reference.md#potik.core.Config) lists every field.

## Client options

`options` holds librdkafka settings for both the consumer and the producer, such as security settings. `consumer_options` and `producer_options` apply to one client.

potik sets the librdkafka options that its guarantees depend on, such as the isolation level and idempotence. `Config` rejects options that override them.

To open a `Producer` on the same cluster, with the same `options` and `producer_options`, use `Producer.from_config(config)`.

## ACLs

On a cluster with ACLs, the runtime's producer needs `Write` on its transactional ID. Give each app its own `transactional_id_prefix`, such as `acceptor`, and grant the ACL on that prefix. Then one app can't fence another app's producer:

```sh
kafka-acls.sh --bootstrap-server broker:9092 --add --allow-principal User:acceptor \
  --operation Write --transactional-id acceptor --resource-pattern-type prefixed
```
