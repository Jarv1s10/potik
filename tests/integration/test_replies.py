"""Request-reply against a real broker, with a transactional responder."""

from functools import partial
from typing import Annotated

import anyio
import pytest
from confluent_kafka import Producer as KafkaProducer
from confluent_kafka.admin import NewPartitions

import potik
from potik import (
    Config,
    ConsumerRecord,
    Header,
    Key,
    Producer,
    ProducerRecord,
    ReplyListener,
    Router,
    resume_or_earliest,
)
from potik.core import replies as replies_module

pytestmark = pytest.mark.anyio


def request_id(record: ConsumerRecord) -> str | None:
    return next((value.decode() for name, value in record.headers() or () if name == "request-id" and value), None)


def listener(topic: str, bootstrap: str) -> ReplyListener:
    return ReplyListener(topic, bootstrap_servers=bootstrap, correlate=request_id)


async def test_each_replica_gets_its_own_replies(workload) -> None:
    """Replies from a transactional responder reach the replica that sent the request, wherever they land."""
    work = workload(3, 0)
    router = Router()

    @router.handler(work.source, output_topics=[work.target])
    async def echo(
        value: bytes, key: Annotated[str, Key()], request_id: Annotated[str, Header("request-id")]
    ) -> ProducerRecord:
        return ProducerRecord(work.target, key.encode(), value.upper(), [("request-id", request_id)])

    config = Config(bootstrap_servers=work.bootstrap, group_id=work.group, offset_policy=resume_or_earliest)
    answers: dict[bytes, bytes | None] = {}
    async with (
        anyio.create_task_group() as tasks,
        Producer(work.bootstrap) as producer,
        listener(work.target, work.bootstrap) as first,
        listener(work.target, work.bootstrap) as second,
    ):
        await tasks.start(partial(potik.build_runtime(config, [router]).run, handle_signals=False))

        async def ask(replica: ReplyListener, text: bytes) -> None:
            with replica.expect() as pending, anyio.fail_after(30):
                await producer.send(ProducerRecord(work.source, text, text, {"request-id": pending.correlation_id}))
                answers[text] = (await pending.wait()).value()

        async with anyio.create_task_group() as asking:
            for n in range(30):
                asking.start_soon(ask, (first, second)[n % 2], f"text-{n}".encode())
        tasks.cancel_scope.cancel()
    assert answers == {f"text-{n}".encode(): f"TEXT-{n}".encode() for n in range(30)}


async def test_only_committed_replies_from_after_the_start_arrive(workload) -> None:
    """A reply written before the listener opened, or in an aborted transaction, never reaches a request."""
    work = workload(1, 0)
    headers = [("request-id", "job-1")]
    early = KafkaProducer({"bootstrap.servers": work.bootstrap})
    early.produce(work.target, b"before", headers=headers)
    assert early.flush(10) == 0
    responder = KafkaProducer({"bootstrap.servers": work.bootstrap, "transactional.id": f"{work.group}-responder"})
    await anyio.to_thread.run_sync(responder.init_transactions, 10)

    def reply_twice() -> None:
        for value, finish in ((b"aborted", responder.abort_transaction), (b"committed", responder.commit_transaction)):
            responder.begin_transaction()
            responder.produce(work.target, value, headers=headers)
            finish(10)

    async with listener(work.target, work.bootstrap) as replica:
        with replica.expect("job-1") as pending:
            await anyio.to_thread.run_sync(reply_twice)
            with anyio.fail_after(30):
                reply = await pending.wait()
    assert reply.value() == b"committed"


async def test_a_partition_added_after_the_start_is_read_from_its_start(workload, monkeypatch) -> None:
    monkeypatch.setattr(replies_module, "_REFRESH_INTERVAL", 0.2)
    work = workload(1, 0)

    def reply_on_a_new_partition() -> None:
        for future in work.admin.create_partitions([NewPartitions(work.target, 2)]).values():
            future.result()
        producer = KafkaProducer({"bootstrap.servers": work.bootstrap})
        producer.produce(work.target, b"late", partition=1, headers=[("request-id", "job-1")])
        assert producer.flush(10) == 0

    async with listener(work.target, work.bootstrap) as replica:
        with replica.expect("job-1") as pending:
            await anyio.to_thread.run_sync(reply_on_a_new_partition)
            with anyio.fail_after(30):
                reply = await pending.wait()
    assert (reply.partition(), reply.value()) == (1, b"late")
