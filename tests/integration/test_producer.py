"""Sends through a Producer, on their own and from a handler, outside any transaction."""

from typing import Annotated

import anyio
import pytest
from harness import read_committed

import potik
from potik import Config, Context, Delivery, Key, Producer, ProducerRecord, Router, resume_or_earliest

pytestmark = pytest.mark.anyio


async def test_send_returns_where_the_broker_appended_it(workload) -> None:
    work = workload(2, 0)
    async with Producer(work.bootstrap) as producer:
        first = await producer.send(ProducerRecord(work.target, b"k", b"v1", [("h", b"1")], partition=1))
        second = await producer.send(ProducerRecord(work.target, b"k", b"v2", partition=1))
    assert (first, second) == (Delivery(1, 0), Delivery(1, 1))
    [one, two] = read_committed(work.bootstrap, work.target, f"verify-{work.group}")
    assert (one.value(), one.headers(), two.value()) == (b"v1", [("h", b"1")], b"v2")


async def test_a_full_queue_makes_sends_wait(workload) -> None:
    """With room for one queued record, concurrent sends wait for each other's delivery instead of failing."""
    work = workload(1, 0)
    deliveries: list[Delivery] = []

    async def send(producer: Producer, n: int) -> None:
        deliveries.append(await producer.send(ProducerRecord(work.target, str(n).encode(), b"v")))

    async with (
        Producer(work.bootstrap, options={"queue.buffering.max.messages": 1}) as producer,
        anyio.create_task_group() as tasks,
    ):
        for n in range(50):
            tasks.start_soon(send, producer, n)
    assert sorted(deliveries) == [Delivery(0, offset) for offset in range(50)]


async def test_a_handler_sends_through_its_runs_producer(workload) -> None:
    """Each send is visible on its own, while the handler's input commits in a transaction afterwards."""
    work = workload(2, 20)
    router = Router()

    @router.handler(work.source)
    async def forward(value: bytes, key: Annotated[str, Key()], producer: Annotated[Producer, Context()]) -> None:
        await producer.send(ProducerRecord(work.target, key.encode(), value))

    config = Config(bootstrap_servers=work.bootstrap, group_id=work.group, offset_policy=resume_or_earliest)
    async with Producer.from_config(config) as producer:
        await work.run_until_committed(potik.build_runtime(config, [router], context=producer))
    sent = read_committed(work.bootstrap, work.target, f"verify-{work.group}")
    assert sorted(int(record.key() or b"") for record in sent) == list(range(work.records))
