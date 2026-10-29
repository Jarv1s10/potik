# How the runtime works

A `Runtime` runs a consume-process-produce loop. It joins a consumer group, reads the partitions that the group assigns to it, and calls a handler for each record. It then commits the handler's outputs and the record's offset in one Kafka transaction.

Every potik service runs on a `Runtime`. `potik.build_runtime()` builds one from typed handlers, and you can also [build one yourself](core.md). This page applies to both.

## Exactly-once outputs

The runtime commits handled records in batches. One transaction writes the outputs of a batch's handlers and commits the batch's offsets. If the app crashes or loses a partition before the commit, the transaction never commits, and the partition's next owner handles the records again. Consumers that read with `isolation.level=read_committed` see each output exactly once.

The guarantee covers only the records that a handler returns. Anything else a handler does runs again when its record runs again, such as sending records through a [`Producer`](../producer.md), writing to a database, or calling an HTTP API. Make those side effects idempotent.

## Ordering and concurrency

Each partition handles one record at a time, in offset order. Different partitions run concurrently. So records with the same key run in the order they were written, and an instance runs up to one handler call per assigned partition, unless a topic opts into key concurrency.

To handle more records at once, add partitions, or let records with different keys run concurrently with [key concurrency](../middleware/concurrency.md#key-concurrency). To spread the load, run more instances with the same `group_id`. To run fewer handler calls at once, for example no more than a database pool's size, set [concurrency limits](../middleware/concurrency.md).

A handler may run for as long as it needs. The consumer keeps polling while handlers run, so a slow handler doesn't drop the instance from its group.

## Failures

When a handler raises, the runtime stops that record's partition. It commits nothing past the failed record, and the other partitions keep running. The record runs again after the partition moves to another instance, or after a restart.

A partition that Kafka can't read stops the same way. This happens, for example, when retention deleted records before the app committed them, or when a record is corrupt. The partition stays stopped until you fix the cause and restart.

A stopped partition doesn't fail the [health checks](production.md#health-checks), because a restart would only fail the same record again. Alert on stopped partitions instead.

Set `Config(stop_failed_partitions=False)` to stop the whole app on the first handler error or unreadable partition instead.

A transaction that fails stops the whole app, and its records run again after the restart. An output larger than the producer's `message.max.bytes` or its topic's `max.message.bytes` fails every transaction it is in, on every restart, because the handler already returned and no exception rule sees the error. Check the size of large outputs in the handler, where a rule can handle the error.

To retry a failed handler, or to write its record to a dead letter topic, add [exception rules](../middleware/exceptions.md).

## Rebalances

When the group moves a partition to another instance, the runtime stops fetching from it. It gives the partition's running handler `revocation_grace_ms` to finish, then cancels it. It commits the finished records before it gives up the partition. The new owner starts at the first uncommitted record, so a cancelled handler's record runs again there.

## Runs

`runtime.run()` runs until SIGTERM or SIGINT, or until its task is cancelled. It then stops fetching records and gives running handlers `shutdown_grace_ms` to finish. It cancels the handlers still running, commits the finished work, leaves the consumer group, and returns. [Production](production.md#shutdown) shows how to fit this into a Kubernetes pod's shutdown.

A runtime runs once. For another run, for example in a test or a restart loop, build a new runtime with new middleware instances. The quickstart's `build()` function does this.

`run()` raises the run's failure, if any, such as a failed transaction. The records that the run didn't commit run again on the next run.
