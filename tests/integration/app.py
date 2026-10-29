"""A core service as deployed, on `Runtime` alone.

It copies each input to its partition of the output topic and stops on SIGTERM.

Usage: app.py BOOTSTRAP SOURCE TARGET GROUP SLEEP_MS [CONFIG_FIELD=INT ...]

Handlers sleep a uniformly random 0..2*SLEEP_MS, so partitions drift apart. `POTIK_INSTANCE_ID` sets `group.instance.id`. `POTIK_CRASH_MID_TRANSACTION` SIGKILLs the process in its first transaction with outputs, after it sends the outputs and offsets but before it commits. `POTIK_SEED` seeds the sleeps. `POTIK_KEY_CONCURRENCY` runs the input by key, with that many keys in progress at once. The process exits once its parent, the test run, is gone.
"""

import logging
import os
import random
import signal
import sys
import threading
import time

import anyio
import confluent_kafka


class CrashingProducer(confluent_kafka.Producer):
    produced = False

    def produce(self, *args, **kwargs) -> None:
        self.produced = True
        super().produce(*args, **kwargs)

    def commit_transaction(self, *args) -> None:
        if self.produced and self.flush(30) == 0:
            os.kill(os.getpid(), signal.SIGKILL)
        super().commit_transaction(*args)


if os.environ.get("POTIK_CRASH_MID_TRANSACTION"):
    # Before potik imports it. The test fails if the process does not crash.
    confluent_kafka.Producer = CrashingProducer

from potik.core import Config, ConsumerRecord, ProducerRecord, Runtime, resume_or_earliest

bootstrap, source, target, group, sleep_ms, *options = sys.argv[1:]
delay = float(sleep_ms) / 1000
random.seed(os.environ.get("POTIK_SEED"))


async def copy(record: ConsumerRecord) -> list[ProducerRecord]:
    if delay:
        await anyio.sleep(random.uniform(0, 2 * delay))
    return [ProducerRecord(target, record.key(), record.value(), partition=record.partition())]


def exit_with(parent: int) -> None:
    while os.getppid() == parent:
        time.sleep(1)
    os._exit(1)


threading.Thread(target=exit_with, args=(os.getppid(),), daemon=True).start()
logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [{os.getpid()}] %(name)s %(levelname)s %(message)s")
config = {name: int(value) for name, value in (option.split("=", 1) for option in options)}
instance = os.environ.get("POTIK_INSTANCE_ID")
key_concurrency = os.environ.get("POTIK_KEY_CONCURRENCY")
runtime = Runtime(
    Config(
        bootstrap_servers=bootstrap,
        group_id=group,
        offset_policy=resume_or_earliest,
        consumer_options={"group.instance.id": instance} if instance else {},
        **config,
    ),
    [source],
    copy,
    key_concurrency={source: int(key_concurrency)} if key_concurrency else None,
)
anyio.run(runtime.run)
