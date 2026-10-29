# /// script
# requires-python = "==3.12.*"
# dependencies = ["faststream[kafka]==0.7.7", "aiokafka==0.14.0"]
# ///
"""FastStream (aiokafka) with its default acknowledgement and producer, and `max_workers` for concurrency.

The default `AckPolicy.ACK_FIRST` turns on aiokafka's auto-commit, which commits consumed records every 5 seconds, whether their handlers finished or not. A crash can lose the records in flight or repeat those since the last commit. With it, `max_workers` runs records concurrently on one consumer, with no order within a partition. The producer keeps aiokafka's `acks=1`.
"""

import asyncio
import logging

from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaMessage
from workload import parse_args, process

args = parse_args()
broker = KafkaBroker(args.bootstrap, logger=None)
publisher = broker.publisher(args.output)


@broker.subscriber(args.input, group_id=args.group, auto_offset_reset="earliest", max_workers=args.partitions)
async def handle(message: KafkaMessage) -> None:
    delay, output = process(message.body)
    await asyncio.sleep(delay)
    await publisher.publish(output, key=message.raw_message.key)


asyncio.run(FastStream(broker, logger=None).run(log_level=logging.WARNING))
