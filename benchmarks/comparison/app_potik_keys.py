# /// script
# requires-python = "==3.12.*"
# dependencies = ["potik", "confluent-kafka==2.16.0"]
# [tool.uv.sources]
# potik = { path = "../..", editable = true }
# ///
"""potik with key concurrency. Records with different keys run concurrently, and each key's records run in order.

Up to 200 keys are in progress at once across all partitions, as many as the other apps' 200 workers. Every record has its own key and no record retries, so that is also the most handler calls at once. A record that finishes before an earlier one of its partition waits for it, so outputs and offsets still commit together in shared transactions.
"""

import logging

import anyio
from workload import parse_args, process

import potik
from potik import Config, ConsumerRecord, ProducerRecord, Router, resume_or_earliest

KEY_CONCURRENCY = 200

args = parse_args()
router = Router()


@router.handler(args.input, key_concurrency=KEY_CONCURRENCY)
async def handle(record: ConsumerRecord) -> list[ProducerRecord]:
    delay, output = process(record.value())
    await anyio.sleep(delay)
    return [ProducerRecord(args.output, record.key(), output)]


logging.basicConfig(level=logging.WARNING)
runtime = potik.build_runtime(
    Config(
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        offset_policy=resume_or_earliest,
        commit_batch_size=args.commit_batch_size,
        commit_interval_ms=args.commit_interval_ms,
    ),
    [router],
)
anyio.run(runtime.run)
