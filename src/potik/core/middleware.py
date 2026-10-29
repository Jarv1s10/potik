"""The middleware base class, and how runtimes and producers run middleware."""

import weakref
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager, nullcontext
from dataclasses import dataclass
from functools import partial
from typing import Any, cast

import anyio

from potik.core.config import Config
from potik.core.errors import InvalidOutputError
from potik.core.types import (
    ConsumerRecord,
    Delivery,
    ProducerRecord,
    RecordHandler,
    RecordSender,
    RuntimeStatus,
    Transaction,
)

__all__ = [
    "BaseMiddleware",
    "ProducerScope",
    "RunScope",
]

type _Stage = Callable[..., Awaitable[Any]]


@dataclass(frozen=True, slots=True, kw_only=True)
class RunScope:
    """Describes a run to a middleware's `lifespan`.

    Attributes:
        config: The run's config.
        status: Returns a snapshot of the run. Any thread may call it, e.g. a metrics exporter's. Before the lifespan yields, the run is not running yet.
        client_options: librdkafka options to connect a client of the middleware's own to the run's cluster, e.g. an `AdminClient`.
    """

    config: Config
    status: Callable[[], RuntimeStatus]
    client_options: Mapping[str, Any]


@dataclass(frozen=True, slots=True, kw_only=True)
class ProducerScope:
    """Describes a `Producer` to a middleware's `lifespan`.

    Attributes:
        client_options: librdkafka options to connect a client of the middleware's own to the producer's cluster.
    """

    client_options: Mapping[str, Any]


class BaseMiddleware:
    """The base class of middleware, which wraps the stages of a `Runtime` or a `Producer`.

    Subclass it and override any of these stages:

    - `lifespan` surrounds a run, or the time a `Producer` is open.
    - `handle` wraps each handler call of a run.
    - `commit` wraps each transaction of a run.
    - `send` wraps each `Producer.send()`.

    Each stage except `lifespan` receives `call_next`, the rest of the chain. A middleware can run code around it, observe its outcome, and scope state to it, e.g. a span. One class can override every stage, so that e.g. one tracing middleware serves runtimes and producers. A runtime or producer runs its middleware outermost first.

    An instance serves one run or one `Producer`, once, so its state lasts one run. Using it a second time raises `RuntimeError`. Create the instances where you create the runtime or producer, and pass resources, e.g. a connection pool, to `__init__`.
    """

    def lifespan(self, scope: RunScope | ProducerScope) -> AbstractAsyncContextManager[None]:
        """Return an async context manager that surrounds one run, or the time a `Producer` is open.

        Write it with `@asynccontextmanager`. `scope` is a `RunScope` for a run, and a `ProducerScope` for a producer. Code before `yield` runs before the clients start, and a run is ready only once every lifespan has entered. Code after `yield` runs once the clients have closed, in reverse order. `yield` raises the run's failure, or the cancellation that stopped it, and the lifespan cannot suppress it. The exit is shielded from cancellation, so it may await, e.g. to close a connection pool. A stuck exit holds up shutdown, so cancel any background tasks after `yield`.
        """
        return nullcontext()

    async def handle(self, record: ConsumerRecord, call_next: RecordHandler) -> Sequence[ProducerRecord]:
        """Wrap one handler call, and return its outputs.

        It is the only stage that may recover from a failure, e.g. by calling `call_next` again, or by returning outputs instead of raising. It may also change the outputs.
        """
        return await call_next(record)

    async def commit(self, transaction: Transaction, call_next: Callable[[Transaction], Awaitable[None]]) -> None:
        """Wrap one transaction, including the retries when a rebalance makes the broker reject it.

        Call `call_next` exactly once. Pass it the transaction, or a copy with other outputs made with `dataclasses.replace()`, which keeps the offsets. Code after `call_next` runs once the transaction has committed, before the next transaction starts, and before a revocation or shutdown that waits for it completes. Slow code there slows commits, and so the handlers. Raising fails the run. If code after `call_next` raises, the transaction stays committed. The stage cannot suppress an exception from `call_next`. A run failure cancels the stage, so the code after `call_next` may not run even when the transaction committed.
        """
        await call_next(transaction)

    async def send(self, record: ProducerRecord, call_next: RecordSender) -> Delivery:
        """Wrap one send through a `Producer`, from producing `record` until the broker acknowledges it.

        Outputs never pass through it. They go through `handle` and `commit`.
        """
        return await call_next(record)


def _middleware_tuple(middleware: Iterable[BaseMiddleware]) -> tuple[BaseMiddleware, ...]:
    """Return `middleware` as a tuple, or raise `TypeError` if an item is not a `BaseMiddleware` instance."""
    items = tuple(middleware)
    for item in items:
        if isinstance(item, type) and issubclass(item, BaseMiddleware):
            raise TypeError(f"pass a {item.__name__} instance, e.g. {item.__name__}(), not the class")
        if not isinstance(item, BaseMiddleware):
            raise TypeError(f"middleware must be BaseMiddleware instances, got {type(item).__name__}")
    return items


def _chain(middleware: Iterable[BaseMiddleware], stage: str, inner: _Stage) -> _Stage:
    """Wrap `inner` in the middleware that override `stage`, outermost first."""
    for item in reversed(tuple(middleware)):
        if getattr(type(item), stage) is not getattr(BaseMiddleware, stage):
            inner = partial(getattr(item, stage), call_next=inner)
    return inner


def _check_outputs(outputs: object, source: str) -> None:
    """Raise `InvalidOutputError` unless `outputs`, which `source` returned, is a list or tuple of `ProducerRecord`s."""
    if isinstance(outputs, ProducerRecord):  # A tuple itself, so checked first.
        raise InvalidOutputError(f"{source} returned a ProducerRecord, not a list or tuple of them, e.g. [record]")
    if not isinstance(outputs, (list, tuple)):
        raise InvalidOutputError(f"{source} returned {type(outputs).__name__}, not a list or tuple of ProducerRecords")
    for output in cast(Sequence[object], outputs):
        if not isinstance(output, ProducerRecord):
            raise InvalidOutputError(f"{source} returned a {type(output).__name__} in its list, not a ProducerRecord")


def _checked(handler: RecordHandler) -> RecordHandler:
    """Check `handler`'s outputs where it returns, so that the middleware around it sees an invalid result as its failure."""

    async def handle(record: ConsumerRecord) -> Sequence[ProducerRecord]:
        outputs = await handler(record)
        _check_outputs(outputs, "the handler")
        return outputs

    return handle


async def _call_once(middleware: Iterable[BaseMiddleware], stage: str, inner: _Stage, *args: object) -> None:
    """Run `inner` through `stage`'s middleware, which can neither skip, repeat, nor suppress it."""
    called = False
    outcome: list[BaseException | None] = []  # set once inner finishes

    async def once(*args: object) -> None:
        nonlocal called
        # Checked on entry: concurrent calls would otherwise all run.
        if called:
            raise RuntimeError(f"{stage} middleware called call_next twice")
        called = True
        try:
            await inner(*args)
        except BaseException as exc:
            outcome.append(exc)
            raise
        outcome.append(None)

    await _chain(middleware, stage, once)(*args)
    if not outcome:
        raise RuntimeError(f"{stage} middleware returned without calling call_next")
    if outcome[0] is not None:
        raise outcome[0]


@asynccontextmanager
async def _enter_lifespan[T](manager: AbstractAsyncContextManager[T]) -> AsyncGenerator[T]:
    """Enter `manager`, and exit it shielded from cancellation, ignoring any suppression of the error."""
    value = await manager.__aenter__()
    try:
        yield value
    except BaseException as exc:
        with anyio.CancelScope(shield=True):
            await manager.__aexit__(type(exc), exc, exc.__traceback__)
        raise
    with anyio.CancelScope(shield=True):
        await manager.__aexit__(None, None, None)


@asynccontextmanager
async def _enter_lifespans(
    middleware: Iterable[BaseMiddleware], scope: RunScope | ProducerScope
) -> AsyncGenerator[None]:
    """Enter the middleware's lifespans, outermost first, like `_enter_lifespan`, and exit them in reverse.

    Raises:
        RuntimeError: If an instance was already entered, by this owner or another.
    """
    middleware = tuple(middleware)
    seen = set()
    for item in middleware:
        if id(item) in _entered or id(item) in seen:
            name = type(item).__name__
            raise RuntimeError(f"this {name} was already used by a runtime or producer, so create one for each")
        seen.add(id(item))
    for item in middleware:
        _entered[id(item)] = item
    async with AsyncExitStack() as stack:
        for item in middleware:
            await stack.enter_async_context(_enter_lifespan(item.lifespan(scope)))
        yield


# The instances whose lifespan an owner entered, by identity, since their state then belongs to that owner. An entry goes with its instance, so a new instance at the same address is not mistaken for it.
_entered: weakref.WeakValueDictionary[int, BaseMiddleware] = weakref.WeakValueDictionary()
