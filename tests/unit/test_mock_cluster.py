"""A whole app on `MockCluster`: its runtime, sends, outputs, and topic check, without a broker."""

from functools import partial
from typing import Annotated

import anyio
import pytest

import potik
from potik import Config, Context, Key, Producer, Router, Runtime, Topic
from potik.middleware import TopicCheckError, dead_letter, on
from potik.testing import MockCluster

pytestmark = pytest.mark.anyio
orders = Topic("orders", str)
accepted = Topic("accepted", str)
audit = Topic("audit", str)
rejected = Topic("rejected", str)


router = Router()


@router.handler(orders, output_topics=[accepted])
async def accept(order: str, customer: Annotated[str, Key()], producer: Annotated[Producer, Context()]):
    if order == "bad":
        raise ValueError("bad order")
    await producer.send(audit(order, key=customer))
    return accepted(order.upper(), key=customer)


def shop(config: Config, producer: Producer) -> Runtime:
    """Accept orders, send each to an audit log through the run's producer, and dead-letter bad ones."""
    return potik.build_runtime(
        config, [router], context=producer, exception_rules=[on(ValueError, dead_letter(rejected))]
    )


async def serve(cluster: MockCluster, *, task_status=anyio.TASK_STATUS_IGNORED) -> None:
    """Run the shop on `cluster`, as the service's `main` runs it on Kafka."""
    config = cluster.config(Config(bootstrap_servers="kafka:9092", group_id="shop"))
    async with Producer.from_config(config) as producer:
        await shop(config, producer).run(task_status=task_status, handle_signals=False)


async def test_an_app_runs_end_to_end() -> None:
    with MockCluster() as cluster:
        for topic in (orders, accepted, audit, rejected):
            cluster.create_topic(topic, partitions=2)
        async with Producer(cluster.bootstrap_servers) as producer:
            for order in ("a", "bad", "b"):
                await producer.send(orders(order, key=f"c-{order}"))
        async with anyio.create_task_group() as tasks:
            await tasks.start(serve, cluster)
            outputs = await cluster.wait_records(accepted, 2)
            dead_letters = await cluster.wait_records(rejected, 1)
            tasks.cancel_scope.cancel()
        sends = await cluster.wait_records(audit, 2)
    assert sorted((record.key(), record.value()) for record in outputs) == [(b"c-a", b"A"), (b"c-b", b"B")]
    assert [record.value() for record in dead_letters] == [b"bad"]
    assert sorted(record.value() for record in sends) == [b"a", b"b"]


async def test_a_topic_that_the_test_did_not_create_fails_the_start() -> None:
    with MockCluster() as cluster:
        for topic in (orders, audit, rejected):
            cluster.create_topic(topic)
        with pytest.raises(TopicCheckError, match="accepted"):
            await serve(cluster)


async def test_waiting_for_more_records_than_the_topic_has_times_out() -> None:
    with MockCluster() as cluster:
        cluster.create_topic(orders)
        with pytest.raises(TimeoutError, match="has 0 of 1"):
            await cluster.wait_records(orders, 1, timeout=0.2)


async def test_probes_follow_the_run() -> None:
    """Readiness passes only while the run runs, and liveness passes while it starts, runs, and stops."""
    with MockCluster() as cluster:
        for topic in (orders, accepted, audit, rejected):
            cluster.create_topic(topic)
        config = cluster.config(Config(bootstrap_servers="kafka:9092", group_id="shop"))
        async with Producer.from_config(config) as producer:
            runtime = shop(config, producer)
            assert (runtime.liveness(), runtime.readiness()) == (None, "not running")
            async with anyio.create_task_group() as tasks:
                await tasks.start(partial(runtime.run, handle_signals=False))
                assert (runtime.liveness(), runtime.readiness()) == (None, None)
                tasks.cancel_scope.cancel()
            assert (runtime.liveness(), runtime.readiness()) == (None, "not running")


async def test_one_run_at_a_time_handles_signals() -> None:
    """Only the signal receiver installed last gets a signal, so a second run that handles them fails instead of leaving the first one running."""
    with MockCluster() as cluster:
        for topic in (orders, accepted, audit, rejected):
            cluster.create_topic(topic)
        config = cluster.config(Config(bootstrap_servers="kafka:9092", group_id="shop"))
        async with Producer.from_config(config) as producer:
            async with anyio.create_task_group() as tasks:
                await tasks.start(shop(config, producer).run)
                second = shop(config, producer)
                with pytest.raises(RuntimeError, match="another run already handles SIGTERM and SIGINT"):
                    await second.run()
                await tasks.start(partial(second.run, handle_signals=False))  # As the error advises.
                tasks.cancel_scope.cancel()
            async with anyio.create_task_group() as tasks:
                await tasks.start(shop(config, producer).run)  # The first run released the signals when it ended.
                tasks.cancel_scope.cancel()


async def test_a_run_that_cannot_handle_signals_can_run_without_them() -> None:
    """Outside the main thread, a run that handles signals fails before it starts, and the runtime can still run without them."""
    with MockCluster() as cluster:
        for topic in (orders, accepted, audit, rejected):
            cluster.create_topic(topic)
        config = cluster.config(Config(bootstrap_servers="kafka:9092", group_id="shop"))
        async with Producer.from_config(config) as producer:
            runtime = shop(config, producer)
            with pytest.raises(RuntimeError, match="main thread"):
                await anyio.to_thread.run_sync(anyio.run, runtime.run)
            async with anyio.create_task_group() as tasks:
                await tasks.start(partial(runtime.run, handle_signals=False))
                tasks.cancel_scope.cancel()
