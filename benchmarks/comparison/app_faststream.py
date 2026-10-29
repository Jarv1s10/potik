# /// script
# requires-python = "==3.12.*"
# dependencies = ["faststream[kafka]==0.7.7", "aiokafka==0.14.0"]
# ///
"""FastStream (aiokafka), at-least-once and ordered. Offsets commit after the handler's confirmed publish.

`max_workers` with `AckPolicy.NACK_ON_ERROR` starts one consumer per worker in the group, so partitions run concurrently and each one stays sequential. The native acknowledgement policy commits per record and seeks back on handler failure. aiokafka defaults to `acks=1` without idempotence, so the producer sets both for the durability that at-least-once needs.
"""

import asyncio
import logging

from faststream import AckPolicy, FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
from workload import parse_args, process

args = parse_args()
broker = KafkaBroker(args.bootstrap, acks="all", enable_idempotence=True, logger=None)
publisher = broker.publisher(args.output)


@broker.subscriber(
    args.input,
    group_id=args.group,
    auto_offset_reset="earliest",
    isolation_level="read_committed",
    max_workers=args.partitions,
    ack_policy=AckPolicy.NACK_ON_ERROR,
)
async def handle(message: KafkaMessage) -> None:
    delay, output = process(message.body)
    await asyncio.sleep(delay)
    await publisher.publish(output, key=message.raw_message.key)


asyncio.run(FastStream(broker, logger=None).run(log_level=logging.WARNING))
