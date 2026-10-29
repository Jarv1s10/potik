# /// script
# requires-python = "==3.12.*"
# dependencies = ["potik", "confluent-kafka==2.16.0"]
# [tool.uv.sources]
# potik = { path = "../..", editable = true }
# ///
"""potik with its default settings. Outputs and offsets commit together in shared transactions."""

import logging

import anyio
from workload import parse_args, process

import potik
from potik import Config, ConsumerRecord, ProducerRecord, Router, resume_or_earliest

args = parse_args()
router = Router()


@router.handler(args.input)
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
