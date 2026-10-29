"""Request-reply on `MockCluster`: every replica gets the replies to its own requests."""

import json
from contextlib import ExitStack
from functools import partial
from typing import Annotated

import anyio
import pytest
from confluent_kafka import KafkaException

import potik
from potik import (
    Config,
    ConsumerRecord,
    Header,
    Key,
    PendingReply,
    Producer,
    ProducerRecord,
    ReplyListener,
    Router,
    Topic,
)
from potik.testing import MockCluster

pytestmark = pytest.mark.anyio
requests = Topic("requests", str)
replies = Topic("replies", str)
FAST_FETCH = {"fetch.wait.max.ms": 10}

router = Router()


@router.handler(requests, output_topics=[replies])
async def shout(text: str, key: Annotated[str, Key()], request_id: Annotated[str, Header("request-id")]):
    return replies(text.upper(), key=key, headers={"request-id": request_id})


async def respond(cluster: MockCluster, *, task_status=anyio.TASK_STATUS_IGNORED) -> None:
    config = cluster.config(Config(bootstrap_servers="kafka:9092", group_id="responder"))
    await potik.build_runtime(config, [router]).run(task_status=task_status, handle_signals=False)


def request_id(record: ConsumerRecord) -> str | None:
    return next((value.decode() for name, value in record.headers() or () if name == "request-id" and value), None)


def listener(cluster: MockCluster, correlate=request_id) -> ReplyListener:
    return ReplyListener(
        "replies", bootstrap_servers=cluster.bootstrap_servers, correlate=correlate, options=FAST_FETCH
    )


async def arrived(pending: PendingReply) -> None:
    with anyio.fail_after(10):
        while not pending._arrived.is_set():
            await anyio.sleep(0.01)


async def test_each_replica_gets_its_own_replies() -> None:
    with MockCluster() as cluster:
        cluster.create_topic(requests, partitions=3)
        cluster.create_topic(replies, partitions=3)
        async with (
            anyio.create_task_group() as tasks,
            Producer(cluster.bootstrap_servers) as producer,
            listener(cluster) as first,
            listener(cluster) as second,
        ):
            await tasks.start(partial(respond, cluster))
            answers: dict[str, str] = {}

            async def ask(replica: ReplyListener, text: str) -> None:
                with replica.expect() as pending, anyio.fail_after(20):
                    await producer.send(requests(text, key=text, headers={"request-id": pending.correlation_id}))
                    answers[text] = await replies.decode((await pending.wait()).value() or b"")

            async with anyio.create_task_group() as asking:
                for n in range(20):
                    asking.start_soon(ask, (first, second)[n % 2], f"text-{n}")
            assert answers == {f"text-{n}": f"TEXT-{n}" for n in range(20)}
            tasks.cancel_scope.cancel()


async def test_a_reply_may_arrive_before_the_wait() -> None:
    with MockCluster() as cluster:
        cluster.create_topic(replies)
        async with Producer(cluster.bootstrap_servers) as producer, listener(cluster) as replica:
            with replica.expect("job-1") as pending:
                await producer.send(ProducerRecord("replies", None, b"done", {"request-id": "job-1"}))
                await arrived(pending)
                assert (await pending.wait()).value() == b"done"


async def test_a_reply_that_correlate_fails_on_is_dropped() -> None:
    """The listener logs it and keeps reading, so other requests still get their replies."""

    def job_id(record: ConsumerRecord) -> str | None:
        return json.loads(record.value())["job"]  # fails on the tombstone

    with MockCluster() as cluster:
        cluster.create_topic(replies)
        async with Producer(cluster.bootstrap_servers) as producer, listener(cluster, job_id) as replica:
            with replica.expect("job-1") as pending:
                await producer.send(ProducerRecord("replies", b"k", None))
                await producer.send(ProducerRecord("replies", b"k", b'{"job": "job-1"}'))
                with anyio.fail_after(10):
                    assert (await pending.wait()).value() == b'{"job": "job-1"}'
            assert replica.failure is None


async def test_a_block_stops_awaiting_its_reply_when_it_exits() -> None:
    with MockCluster() as cluster:
        cluster.create_topic(replies)
        async with listener(cluster) as replica:
            with replica.expect() as pending, anyio.move_on_after(0.2):
                await pending.wait()
            assert not replica._waiting


async def test_an_id_is_awaited_once_at_a_time() -> None:
    with MockCluster() as cluster:
        cluster.create_topic(replies)
        async with listener(cluster) as replica:
            with replica.expect("job-1"), pytest.raises(ValueError, match="already awaited"), replica.expect("job-1"):
                pass
            with replica.expect("job-1"):  # The first block's exit freed the ID.
                pass


async def test_exiting_fails_awaited_replies_and_later_requests() -> None:
    with MockCluster() as cluster, ExitStack() as blocks:
        cluster.create_topic(replies)
        replica = listener(cluster)
        with pytest.raises(RuntimeError, match="not open"), replica.expect():
            pass
        async with replica:
            pending = blocks.enter_context(replica.expect("job-1"))
        with pytest.raises(RuntimeError, match="closed before the reply"):
            await pending.wait()
        with pytest.raises(RuntimeError, match="not open"), replica.expect():
            pass


async def test_a_missing_reply_topic_fails_the_start() -> None:
    with MockCluster() as cluster, pytest.raises(KafkaException):
        async with listener(cluster):
            pass


def test_options_cannot_override_owned_settings() -> None:
    with pytest.raises(ValueError, match=r"isolation\.level"):
        ReplyListener(
            "replies",
            bootstrap_servers="kafka:9092",
            correlate=request_id,
            options={"isolation.level": "read_uncommitted"},
        )
