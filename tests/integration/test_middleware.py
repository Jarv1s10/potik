"""The lifespan, handle, and commit stages of a runtime's middleware against a real broker."""

from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from functools import partial

import anyio
import anyio.lowlevel
import pytest
from confluent_kafka import KafkaException

from potik import Producer
from potik.core import BaseMiddleware, RunScope

pytestmark = pytest.mark.anyio


class Recorder(BaseMiddleware):
    def __init__(self) -> None:
        self.runs: list[type[BaseException] | None] = []
        self.handled = 0
        self.committed: dict[tuple[str, int], int] = {}
        self.committing = False
        self.running: set[bool] = set()  # what the scope's status said, before the run started and while handling

    @asynccontextmanager
    async def lifespan(self, scope):
        assert isinstance(scope, RunScope)
        self.scope = scope
        self.running.add(scope.status().running)
        try:
            yield
        except BaseException as exc:
            self.runs.append(type(exc))
            raise
        self.runs.append(None)

    async def handle(self, record, call_next):
        outputs = await call_next(record)
        self.running.add(self.scope.status().running)
        self.handled += 1
        return outputs

    async def commit(self, transaction, call_next):
        assert not self.committing, "transactions overlapped"
        self.committing = True
        try:
            await call_next(replace(transaction, outputs=[o._replace(value=b"final") for o in transaction.outputs]))
            await anyio.sleep(0.005)
            self.committed.update(transaction.offsets)
        finally:
            self.committing = False


async def test_cancelled_run_lets_lifespans_clean_up(workload) -> None:
    """Middleware lifespans may await after a cancelled run."""
    work = workload(1, 10)
    cleaned = []

    class Pool(BaseMiddleware):
        @asynccontextmanager
        async def lifespan(self, scope):
            try:
                yield
            finally:
                await anyio.lowlevel.checkpoint()  # e.g. closing a connection pool
                cleaned.append(True)

    async with anyio.create_task_group() as tasks:
        await tasks.start(partial(work.typed(middleware=[Pool()]).run, handle_signals=False))
        tasks.cancel_scope.cancel()
    assert cleaned == [True]


async def test_stages_wrap_every_run_record_and_commit(workload) -> None:
    """Commit middleware sees every committed offset after the commit, and may replace the outputs.

    Its lifespan's scope describes the run from its start.
    """
    work = workload(4, 400)
    recorder = Recorder()
    await work.run_until_committed(work.typed(middleware=[recorder]))

    assert recorder.runs == [anyio.get_cancelled_exc_class()]
    assert recorder.running == {False, True}
    assert recorder.handled == work.records
    assert recorder.committed == {(work.source, p): work.records // work.partitions for p in range(work.partitions)}
    work.assert_exactly_once()
    assert {message.value() for message in work.outputs()} == {b"final"}


class SkipsCommit(BaseMiddleware):
    async def commit(self, transaction, call_next):
        pass


class ChangesOffsets(BaseMiddleware):
    async def commit(self, transaction, call_next):
        await call_next(replace(transaction, offsets=dict(transaction.offsets)))


class ClearsOffsets(BaseMiddleware):
    async def commit(self, transaction, call_next):
        transaction.offsets.clear()
        await call_next(transaction)


class SwallowsCommitFailure(BaseMiddleware):
    async def commit(self, transaction, call_next):
        with suppress(Exception):
            await call_next(transaction)


class SwallowsRunFailure(BaseMiddleware):
    @asynccontextmanager
    async def lifespan(self, scope):
        with suppress(Exception):
            yield


@pytest.mark.parametrize(
    ("middleware", "error", "committed"),
    [
        # The first transaction commits the offset policy's start position, so -1 means it failed.
        (SkipsCommit, "returned without calling call_next", [-1]),
        (ChangesOffsets, "must not change a transaction's offsets", [-1]),
        (ClearsOffsets, "has no attribute 'clear'", [-1]),
        # A 2MB output fails its transaction after the start position commits. The middleware cannot hide the failure.
        (SwallowsCommitFailure, "MSG_SIZE_TOO_LARGE", [0]),
        (SwallowsRunFailure, "MSG_SIZE_TOO_LARGE", [0]),
    ],
    ids=lambda value: getattr(value, "__name__", None),
)
async def test_middleware_cannot_break_a_stage(workload, middleware, error, committed) -> None:
    work = workload(1, 10)

    async def oversize(record) -> bytes:
        return b"x" * 2_000_000

    runtime = work.typed(oversize, middleware=[middleware()])
    with pytest.raises((RuntimeError, AttributeError, KafkaException), match=error):
        await runtime.run(handle_signals=False)
    assert work.committed() == committed


async def test_an_instance_serves_one_owner(workload) -> None:
    """A second owner of an instance would overwrite the first one's state, so it fails to start instead."""
    work = workload(1, 0)
    shared = Recorder()
    await work.run_until_committed(work.typed(middleware=[shared]))
    with pytest.raises(RuntimeError, match="this Recorder was already used by a runtime or producer"):
        await work.typed(middleware=[shared]).run(handle_signals=False)
    with pytest.raises(RuntimeError, match="already used"):
        await Producer(work.bootstrap, middleware=[shared]).__aenter__()
    with pytest.raises(TypeError, match=r"pass a Recorder instance, e.g. Recorder\(\), not the class"):
        work.typed(middleware=[Recorder])
