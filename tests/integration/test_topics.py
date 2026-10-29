"""The startup check of handled and declared output topics."""

from collections.abc import Sequence
from functools import partial

import anyio
import pytest

import potik
from potik import (
    Config,
    ConsumerRecord,
    Producer,
    ProducerRecord,
    Router,
    TopicCheckError,
    resume_or_earliest,
)
from potik.core import BaseMiddleware, Runtime
from potik.middleware import TopicCheckMiddleware, dead_letter, on

pytestmark = pytest.mark.anyio


class Opened(BaseMiddleware):
    def __init__(self, opened: list[bool]) -> None:
        self.opened = opened

    def lifespan(self, scope):
        self.opened.append(True)
        return super().lifespan(scope)


async def test_missing_topics_fail_the_start(workload) -> None:
    """Missing topics fail the start, before inner lifespans and records."""
    work = workload(1, 1)
    handled = []
    opened: list[bool] = []

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        handled.append(record)
        return ()

    missing = [f"{work.group}-{name}" for name in ("in-typo", "out-typo")]
    runtime = Runtime(
        Config(bootstrap_servers=work.bootstrap, group_id=work.group, offset_policy=resume_or_earliest),
        [work.source, missing[0]],
        handler,
        middleware=[TopicCheckMiddleware((work.source, work.target, *missing)), Opened(opened)],
    )
    with pytest.raises(TopicCheckError) as raised:
        await runtime.run(handle_signals=False)
    assert raised.value.missing == tuple(sorted(missing))
    assert raised.value.unauthorized == ()
    assert not handled
    assert not opened


async def test_unchecked_run_handles_existing_topics(workload) -> None:
    work = workload(1, 1)
    handled = anyio.Event()

    async def handler(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        handled.set()
        return ()

    runtime = Runtime(
        Config(bootstrap_servers=work.bootstrap, group_id=work.group, offset_policy=resume_or_earliest),
        [work.source, f"{work.group}-in-typo"],
        handler,
    )
    async with anyio.create_task_group() as tasks:
        await tasks.start(partial(runtime.run, handle_signals=False))
        with anyio.fail_after(30):
            await handled.wait()
        tasks.cancel_scope.cancel()


async def test_runtime_checks_its_declared_outputs(workload) -> None:
    """Handler, router, and dead letter outputs all count, besides the handled topics."""
    work = workload(1, 1)
    missing = [f"{work.group}-{name}" for name in ("handler-out", "router-out", "dead", "topic-dead")]
    router = Router(output_topics=(missing[1],))

    @router.handler(work.source, output_topics=(work.target, missing[0]))
    async def handle(record: ConsumerRecord) -> None: ...

    runtime = potik.build_runtime(
        Config(bootstrap_servers=work.bootstrap, group_id=work.group),
        [router],
        exception_rules=[
            on(KeyError, dead_letter(missing[3]), topics=[work.source]),
            on(Exception, dead_letter(missing[2])),
        ],
    )
    with pytest.raises(TopicCheckError) as raised:
        await runtime.run(handle_signals=False)
    assert raised.value.missing == tuple(sorted(missing))


async def test_producer_checks_its_topics_when_entered(workload) -> None:
    work = workload(1, 0)
    missing = f"{work.group}-typo"
    producer = Producer(work.bootstrap, middleware=[TopicCheckMiddleware((work.target, missing))])
    with pytest.raises(TopicCheckError) as raised:
        await producer.__aenter__()
    assert raised.value.missing == (missing,)
    async with Producer(work.bootstrap, middleware=[TopicCheckMiddleware((work.target,))]) as checked:
        await checked.send(ProducerRecord(work.target, None, b"v"))
