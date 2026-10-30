"""Typed handlers, whose parameters take parts of the input record and whose results are output records.

Each parameter takes one part of the record, independently of the others:

- `Annotated[T, Key()]` takes the key.
- `Annotated[T, Header(name)]` takes a header's last value.
- `Annotated[T, Context()]` takes the run's context.
- `Annotated[T, Depends(dependency)]` takes what a dependency returns, e.g. a database transaction for the record.
- A parameter annotated `ConsumerRecord` takes the record itself.
- The one parameter left, if any, takes the value.

A handler takes its topic's keys and values as the `Topic`'s types, or as raw `bytes`. A plain topic name is a topic of `bytes` values and `str` keys.

A handler returns None, a `ProducerRecord`, e.g. made by calling a `Topic`, or a list or tuple of them. Registration checks the parameters against the topic, so a mismatch fails before the run starts.
"""

import inspect
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Sequence
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from functools import reduce
from operator import or_
from types import FunctionType, NoneType, UnionType
from typing import Annotated, Any, TypeAliasType, Union, cast, get_args, get_origin

import anyio
import anyio.lowlevel
from confluent_kafka import Message
from typing_extensions import Sentinel, TypeVar

from potik.core.errors import PermanentError, PotikError
from potik.core.types import ConsumerRecord, ProducerRecord, RecordHandler
from potik.serde import Decoder, Resolver, _text_decoder, _type_name
from potik.topic import Topic, _Codec

__all__ = [
    "Context",
    "DecodeError",
    "Depends",
    "HandlerResult",
    "Header",
    "Key",
]

# The handler as a function of the run's context, which is _NO_CONTEXT for a runtime built without one.
type _BindHandler = Callable[[object], RecordHandler]
# A parameter's argument: the parameter's name if it is keyword-only, else None, how the call takes it, and its source.
type _Argument = tuple[str | None, int, Any]
# The exits of a call's dependencies that yield, in the order they entered, each with whether it is awaited.
type _Exits = list[tuple[Callable[..., Any], bool]]
# A call's values of its dependencies, and of the parts of its record that several parameters take, decoded.
type _Values = dict[object, Any]

_EMPTY = inspect.Parameter.empty
_NO_CONTEXT = Sentinel("NO_CONTEXT")
_UNRESOLVED = Sentinel("UNRESOLVED")

# How a call takes an argument: `source(record)`, `await source(record)`, `source(record, values)` or `await source(record, values)` for a part that several parameters take, the run's context, or the value of the `_Dependency` source. Compiling takes each part as _PENDING, until it knows which parts several parameters take.
_PART, _ASYNC_PART, _SHARED_PART, _ASYNC_SHARED_PART, _CONTEXT, _DEPENDENCY, _PENDING = range(7)
# How a call gets a dependency's value: what it returns, what it returns awaited, or what it yields, synchronously or not.
_RETURNS, _AWAITS, _YIELDS, _ASYNC_YIELDS = range(4)


class DecodeError(PotikError, PermanentError):
    """Raised from a handler call when a parameter, of the handler or of a dependency, cannot take its part of the record.

    The part fails to decode, or it is missing or null and the parameter has no default and does not allow None. It is a `PermanentError`, so `retry()` rules do not retry it.

    Attributes:
        parameter: The parameter's name.
    """

    def __init__(self, message: str, *, parameter: str) -> None:
        super().__init__(message)
        self.parameter = parameter


_V = TypeVar("_V", default=Any)

HandlerResult = TypeAliasType(
    "HandlerResult", ProducerRecord[_V] | Sequence[ProducerRecord[_V]] | None, type_params=(_V,)
)
"""What a typed handler returns: None, a `ProducerRecord`, or a list or tuple of them.

potik commits the records with the input record's offset, in one transaction.
"""


@dataclass(frozen=True, slots=True)
class Key:
    """Marks a parameter, `Annotated[T, Key()]`, that takes the record's key.

    `T` is the topic's key type, or `bytes` for the raw key. A null key takes the parameter's default if it has one, else None if `T` allows None, else the call fails with `DecodeError`.
    """


@dataclass(frozen=True, slots=True)
class Header:
    """Marks a parameter, `Annotated[T, Header(name)]`, that takes a header's value.

    If the record has the header more than once, the parameter takes the last value. A missing or null header takes the parameter's default if it has one, else None if `T` allows None, else the call fails with `DecodeError`.

    Args:
        name: The header's name.
        decoder: Decodes the header's value. Defaults to `Text`'s decoder of `T`, and `bytes` takes the value as it is.
    """

    name: str
    decoder: Decoder | None = None


@dataclass(frozen=True, slots=True)
class Context:
    """Marks a parameter, `Annotated[T, Context()]`, that takes the run's context.

    The context is the object passed as `potik.build_runtime(context=...)`, e.g. a dataclass of the service's clients. potik does not check it against `T`. `potik.build_runtime()` raises `RegistrationError` if a handler or one of its dependencies takes it and no context is given.
    """


@dataclass(frozen=True, slots=True)
class Depends:
    """Marks a parameter, `Annotated[T, Depends(dependency)]`, that takes what `dependency` returns.

    A dependency is a function, sync or async, a class, or a callable object, which must be hashable. Its parameters take parts of the record, the run's context, or other dependencies, as a handler's parameters do, and registration checks them against the topic in the same way. A call decodes each part of the record once, e.g. the value, for every parameter that takes it with the same decoder, so they all get the same object. Each one still takes its own default for a null part. A sync dependency runs on the event loop, so it must not block. potik does not check what a dependency returns against `T`.

    A dependency that yields, sync or async, gives the parameter the value that it yields, and runs the rest of its body after the handler returns or raises, e.g. to commit or roll back a transaction. Do not decorate it with `@contextmanager` or `@asynccontextmanager`: registration rejects such a function, because potik enters dependencies that yield itself.

    Each handler call resolves the handler's dependencies anew, in the call's task, one at a time, in parameter order, and each one after its own dependencies. A dependency runs once per call, even if several parameters take it, in the handler and in its dependencies, or take dependencies equal to it, e.g. the same bound method. A retry of the call by an exception rule resolves them again.

    After the call, potik resumes the dependencies that yield, last entered first. Each one resumes even if one resumed before it failed:

    - If the handler, or a dependency, raised, `yield` raises that exception in the dependency. A dependency cannot suppress it: potik raises it even if the dependency catches it, so exception rules still handle it. The one exception is a cancellation by the dependency's own cancel scope around `yield`: with `anyio.move_on_after()`, a call that runs out of time returns no outputs, so its record commits, unless the call raised another exception, which it then raises. With `anyio.fail_after()`, it raises `TimeoutError`. The scope also covers the code after `yield` of the dependencies that potik entered after it. A dependency that swallows an outer cancellation, e.g. a revocation's, suppresses nothing: the call still raises it.
    - If a dependency raises after `yield`, the call fails with that exception. Exception rules then handle it as a handler failure, and the handler's outputs are not committed.
    - If a revocation or shutdown cancels the call, the code after `yield` runs in the cancelled scope, where each await raises the cancellation again. Shield the awaits that must finish, with a timeout, e.g. `with anyio.move_on_after(5, shield=True):`.

    The code after `yield` runs before the transaction commits the handler's outputs. So a write that it commits, e.g. to a database, happens at least once, not exactly once.

    Args:
        dependency: The function or class to call.

    Raises:
        TypeError: If `dependency` is not callable.

    Example:
        ```python
        async def transaction(services: Annotated[Services, Context()]) -> AsyncIterator[Transaction]:
            tx = await services.db.begin()
            try:
                yield tx
            except BaseException:
                with anyio.move_on_after(5, shield=True):
                    await tx.rollback()
                raise
            await tx.commit()


        @router.handler(orders)
        async def save(order: Order, tx: Annotated[Transaction, Depends(transaction)]) -> None:
            await tx.execute("INSERT INTO orders VALUES ($1)", order.sku)
        ```
    """

    dependency: Callable[..., object]

    def __post_init__(self) -> None:
        if not callable(self.dependency):
            raise TypeError(f"a dependency must be callable, got {type(self.dependency).__name__}")


def _generate() -> Generator[None]:
    yield


async def _agenerate() -> AsyncGenerator[None]:
    yield


# The code of every function that `@contextmanager` or `@asynccontextmanager` returns.
_CONTEXT_MANAGER_CODES = {
    cast(FunctionType, contextmanager(_generate)).__code__,
    cast(FunctionType, asynccontextmanager(_agenerate)).__code__,
}


@dataclass(eq=False, slots=True)
class _Dependency:
    """A dependency, compiled once for each handler that takes it, so a call's values are keyed by its identity."""

    name: str
    call: Callable[..., Any]  # A context manager factory for a dependency that yields.
    calls: int
    arguments: tuple[_Argument, ...]


@dataclass(eq=False, slots=True)
class _Part:
    """A part of the record that parameters decode alike, so a call decodes it once if several of them take it."""

    description: str
    get: Callable[[ConsumerRecord], bytes | None]
    decode: Decoder | None
    resolve: Resolver | None
    takers: int = 0


@dataclass(slots=True)
class _Graph:
    """What compiling a handler collects: its dependencies, and the parts of the record that its parameters take."""

    topic: Topic[Any, Any]
    # Each compiled dependency, which is None while its own parameters compile.
    dependencies: dict[Callable[..., Any], _Dependency | None] = field(default_factory=dict)
    parts: dict[tuple[str, object, Resolver | None], _Part] = field(default_factory=dict)

    def take(
        self,
        description: str,
        get: Callable[[ConsumerRecord], bytes | None],
        decode: Decoder | None,
        resolve: Resolver | None,
        alike: object = None,
    ) -> _Part:
        """Return the part that `description` names, as parameters that decode it alike take it, and count one more of them.

        Parameters decode a part alike if they have the same decoder and resolver, or, if given, the same `alike`.
        """
        key = (description, decode if alike is None else alike, resolve)
        if (part := self.parts.get(key)) is None:
            part = self.parts[key] = _Part(description, get, decode, resolve)
        part.takers += 1
        return part


def _compile_handler(handler: Callable[..., Awaitable[Any]], topic: Topic[Any, Any]) -> _BindHandler:
    """Bind each of the handler's parameters, and its dependencies' parameters, to their sources in `topic`'s records.

    Raises:
        ValueError: If the handler is not an async function, a parameter has no source, or a dependency cannot be called.
    """
    if not callable(handler) or not (
        inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(type(handler).__call__)
    ):
        raise ValueError("must be an async function")
    graph = _Graph(topic)
    arguments = _finish(_arguments(handler, graph))
    compiled = [dependency for dependency in graph.dependencies.values() if dependency is not None]
    for dependency in compiled:
        dependency.arguments = _finish(dependency.arguments)
    if any(how == _CONTEXT for _, how, _ in arguments):
        context_error = "takes Context(), so requires build_runtime(context=...)"
    elif taker := next((d for d in compiled if any(how == _CONTEXT for _, how, _ in d.arguments)), None):
        context_error = f"takes Context() through dependency {taker.name}, so requires build_runtime(context=...)"
    else:
        context_error = None

    def bind(context: object) -> RecordHandler:
        """Bind the handler to the run's `context`. Raise LookupError for NO_CONTEXT if the handler or a dependency takes it."""
        if context_error is not None and context is _NO_CONTEXT:
            raise LookupError(context_error)

        async def handle(record: ConsumerRecord) -> Sequence[ProducerRecord]:
            exits: _Exits = []
            error = None
            try:
                args, kwargs = await _resolve(arguments, record, context, {}, exits)
                result = await handler(*args, **kwargs)
            except BaseException as exc:
                if not exits:
                    raise
                error, _ = await _exit(exits, exc)
                if error is exc:
                    raise
                result = None
            else:
                if exits:
                    error, ended = await _exit(exits, None)
                    if ended:
                        result = None
            if error is not None:
                raise error  # Outside the except clause, which would replace its context with the call's exception.
            if result is None:
                return ()
            if isinstance(result, ProducerRecord):  # A tuple itself, so checked first.
                return (result,)
            return result

        return handle

    return bind


def _arguments(function: Callable[..., Any], graph: _Graph) -> tuple[_Argument, ...]:
    """Bind each of `function`'s parameters to its source, compiling the dependencies that `graph` lacks into it.

    Raises:
        ValueError: If a parameter has no source, or a dependency cannot be called.
    """
    try:
        parameters = inspect.signature(function, eval_str=True).parameters.values()
    except (TypeError, ValueError, NameError) as exc:
        raise ValueError(f"has a signature that potik cannot read: {exc}") from exc
    arguments: list[_Argument] = []
    values = []
    for parameter in parameters:
        if parameter.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        if isinstance(parameter.default, Depends):
            raise ValueError(  # noqa: TRY004 -- the router turns ValueErrors into RegistrationErrors
                f"has Depends() as the default of parameter {parameter.name!r}; "
                "annotate it as Annotated[T, Depends(dependency)] instead"
            )
        if parameter.annotation is _EMPTY:
            raise ValueError(f"must annotate parameter {parameter.name!r} with a type")
        try:
            how, source, takes_value = _bind(parameter, graph)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"cannot bind parameter {parameter.name!r}: {exc}") from exc
        if takes_value:
            values.append(parameter.name)
        arguments.append((parameter.name if parameter.kind is inspect.Parameter.KEYWORD_ONLY else None, how, source))
    if len(values) > 1:
        raise ValueError(
            f"has parameters {', '.join(map(repr, values))} without a marker, but only the value's may lack one; "
            "mark the others, e.g. as Annotated[str, Key()]"
        )
    return tuple(arguments)


def _bind(parameter: inspect.Parameter, graph: _Graph) -> tuple[int, Any, bool]:
    """Return how a call takes the parameter's argument, its source, and whether it takes the value."""
    annotation = parameter.annotation
    annotated = get_origin(annotation) is Annotated
    tp: Any = annotation.__origin__ if annotated else annotation
    metadata = annotation.__metadata__ if annotated else ()
    markers = (Key, Header, Context, Depends)
    sources = [item for item in metadata if isinstance(item, markers)]
    if len(sources) > 1:
        raise ValueError("it has more than one of Key(), Header(), Context(), and Depends()")
    source = sources[0] if sources else None
    if source is None and (tp is ConsumerRecord or tp is Message):
        return _PART, _same, False
    if isinstance(source, Context):
        return _CONTEXT, None, False
    if isinstance(source, Depends):
        return _DEPENDENCY, _dependency(source.dependency, graph), False
    if rest := [item for item in metadata if not isinstance(item, markers)]:
        tp = Annotated[tp, *rest]  # Other metadata stays, so that it fails to match the topic's type.
    inner, nullable = _split_optional(tp)
    topic = graph.topic
    if isinstance(source, Key):
        decode, resolve = _topic_decoder(topic, tp, inner, topic.key_type, topic._key, "keys")
        return _PENDING, (graph.take("key", Message.key, decode, resolve), parameter, nullable), False
    if isinstance(source, Header):
        name = source.name
        if (decode := source.decoder) is None and inner is not bytes and (decode := _text_decoder(inner)) is None:
            raise TypeError(f"Text() does not decode {_type_name(inner)}; pass a decoder to the Header")

        def last_header(record: ConsumerRecord) -> bytes | None:
            # Consumed headers are pairs with bytes values. The stubs allow a dict and str values too.
            return cast(bytes | None, dict(record.headers() or ()).get(name))

        # Text makes a decoder of a subclass for each parameter, so its decoders are alike if their types are.
        part = graph.take(f"header {name!r}", last_header, decode, None, None if source.decoder else inner)
        return _PENDING, (part, parameter, nullable), False
    decode, resolve = _topic_decoder(topic, tp, inner, topic.value_type, topic._value, "values")
    return _PENDING, (graph.take("value", Message.value, decode, resolve), parameter, nullable), True


def _finish(arguments: tuple[_Argument, ...]) -> tuple[_Argument, ...]:
    """Replace each pending part with its extractor, which a call shares with the other parameters that decode the part."""
    finished = []
    for name, how, source in arguments:
        if how == _PENDING:
            part, parameter, nullable = source
            shared = part.takers > 1 and (part.decode is not None or part.resolve is not None)
            source = _extractor(parameter, part, nullable, shared)
            if part.resolve is None:
                how = _SHARED_PART if shared else _PART
            else:
                how = _ASYNC_SHARED_PART if shared else _ASYNC_PART
        finished.append((name, how, source))
    return tuple(finished)


def _dependency(function: Callable[..., Any], graph: _Graph) -> _Dependency:
    """Return `function` compiled as a dependency, from `graph` if it has it, which holds None while compiling it."""
    name = _callable_name(function)
    try:
        hash(function)
    except TypeError:
        raise ValueError(f"dependency {name} is not hashable, but potik keys a call's values by it") from None
    dependencies = graph.dependencies
    if function in dependencies:
        if (compiled := dependencies[function]) is None:
            raise ValueError(f"dependency {name} depends on itself")
        return compiled
    calls = _RETURNS
    for test, kind in (
        (inspect.isasyncgenfunction, _ASYNC_YIELDS),
        (inspect.isgeneratorfunction, _YIELDS),
        (inspect.iscoroutinefunction, _AWAITS),
    ):
        if test(function) or test(type(function).__call__):
            calls = kind
            break
    else:
        codes = {getattr(function, "__code__", None), getattr(type(function).__call__, "__code__", None)}
        if codes & _CONTEXT_MANAGER_CODES:
            raise ValueError(
                f"dependency {name} is a context manager; remove its decorator, "
                "e.g. @asynccontextmanager, because potik enters dependencies that yield itself"
            )
    dependencies[function] = None
    try:
        arguments = _arguments(function, graph)
    except ValueError as exc:
        raise ValueError(f"dependency {name} {exc}") from exc
    call = function
    if calls == _YIELDS:
        call = contextmanager(function)
    elif calls == _ASYNC_YIELDS:
        call = asynccontextmanager(function)
    dependencies[function] = compiled = _Dependency(name, call, calls, arguments)
    return compiled


async def _resolve(
    arguments: tuple[_Argument, ...],
    record: ConsumerRecord,
    context: object,
    values: _Values,
    exits: _Exits,
) -> tuple[list[Any], dict[str, Any]]:
    """Return the arguments' values, calling the dependencies that `values` lacks, and adding to `exits` those that yield."""
    args: list[Any] = []
    kwargs: dict[str, Any] = {}
    for name, how, source in arguments:
        if how == _PART:
            value = source(record)
        elif how == _ASYNC_PART:
            value = await source(record)
        elif how == _SHARED_PART:
            value = source(record, values)
        elif how == _ASYNC_SHARED_PART:
            value = await source(record, values)
        elif how == _CONTEXT:
            value = context
        elif (value := values.get(source, _UNRESOLVED)) is _UNRESOLVED:
            value = values[source] = await _call(source, record, context, values, exits)
        if name is None:
            args.append(value)
        else:
            kwargs[name] = value
    return args, kwargs


async def _call(
    dependency: _Dependency, record: ConsumerRecord, context: object, values: _Values, exits: _Exits
) -> Any:
    """Return the dependency's value, after resolving its arguments, and enter it if it yields."""
    args, kwargs = await _resolve(dependency.arguments, record, context, values, exits)
    calls = dependency.calls
    if calls == _RETURNS:
        return dependency.call(*args, **kwargs)
    if calls == _AWAITS:
        return await dependency.call(*args, **kwargs)
    manager = dependency.call(*args, **kwargs)
    if calls == _YIELDS:
        value = manager.__enter__()
        exits.append((manager.__exit__, False))
    else:
        value = await manager.__aenter__()
        exits.append((manager.__aexit__, True))
    return value


async def _exit(exits: _Exits, exc: BaseException | None) -> tuple[BaseException | None, bool]:
    """Resume the dependencies that yield, last entered first.

    Each one sees the exception that the call, or a dependency resumed before it, raised. A dependency cannot suppress an exception, except a cancellation, which its own cancel scope caught, e.g. `anyio.move_on_after()` around `yield`. Raising that one again would leave the task with a cancellation that nothing cancelled. So the next dependencies see the last exception before it that is not a cancellation, if any. A dependency that swallows an outer cancellation, e.g. a revocation's, suppresses nothing: that scope is still cancelled, and the checkpoint raises it again.

    Returns:
        The exception that the call raises, or None, and whether a dependency's cancel scope ended the call without outputs.
    """
    error = exc
    failure = None if exc is None or _cancels(exc) else exc  # The last exception that is not a cancellation.
    ended = False
    for exit_, awaits in reversed(exits):
        details = (None, None, None) if error is None else (type(error), error, error.__traceback__)
        try:
            suppressed = await exit_(*details) if awaits else exit_(*details)
        except BaseException as raised:  # noqa: BLE001 -- the next exit sees it, and the caller raises the last one
            error = raised
            if not _cancels(raised):
                failure = raised
        else:
            if suppressed and error is not None and _cancels(error):
                error = failure
                ended = True
    if ended and error is None:
        await anyio.lowlevel.checkpoint_if_cancelled()
    return error, ended


def _cancels(exc: BaseException) -> bool:
    return isinstance(exc, anyio.get_cancelled_exc_class())


def _topic_decoder(
    topic: Topic[Any, Any],
    tp: object,
    inner: object,
    expected: object,
    codec: _Codec[Any],
    part: str,
) -> tuple[Decoder | None, Resolver | None]:
    """Return the topic's decoder and resolver of its keys or values, or neither for `bytes`, taken as they are.

    A parameter takes the topic's type `expected` as declared, e.g. `str | None`, or as `inner`, the parameter's type without None, where None stands for a null key or value.
    """
    if inner is bytes:
        return None, None
    if expected not in (tp, inner):
        raise TypeError(f"topic {topic.name!r} has {part} of {_type_name(expected)}, not {_type_name(tp)}")
    return codec.decode, codec.resolve


def _extractor(parameter: inspect.Parameter, part: _Part, nullable: bool, shared: bool) -> Callable[..., Any]:
    """Return a function that takes the parameter's part of a record, and a call's values if `shared`, which then hold the part decoded once.

    A null part takes the parameter's default if it has one, else None if its type allows None, else fails. A part that fails to decode raises `DecodeError`, which names the parameter that decoded it first.
    """
    name, default = parameter.name, parameter.default
    get, decode, resolve, description = part.get, part.decode, part.resolve, part.description

    def null() -> Any:
        if default is not _EMPTY:
            return default
        if nullable:
            return None
        raise DecodeError(f"parameter {name!r} requires the {description}, which is missing or null", parameter=name)

    def convert(data: bytes, decode: Decoder | None) -> Any:
        if decode is None:
            return data
        try:
            return decode(data)
        except Exception as exc:
            raise DecodeError(f"parameter {name!r} cannot decode the {description}: {exc}", parameter=name) from exc

    # A resolver's failure, e.g. a registry's timeout, propagates as it is, so that `retry()` rules retry it.
    if not shared:
        if resolve is None:
            return lambda record: null() if (data := get(record)) is None else convert(data, decode)

        async def extract(record: ConsumerRecord) -> Any:
            return null() if (data := get(record)) is None else convert(data, await resolve(data))

        return extract

    if resolve is None:

        def extract_shared(record: ConsumerRecord, values: _Values) -> Any:
            if (data := get(record)) is None:
                return null()
            if (value := values.get(part, _UNRESOLVED)) is _UNRESOLVED:
                value = values[part] = convert(data, decode)
            return value

        return extract_shared

    async def extract_shared_async(record: ConsumerRecord, values: _Values) -> Any:
        if (data := get(record)) is None:
            return null()
        if (value := values.get(part, _UNRESOLVED)) is _UNRESOLVED:
            value = values[part] = convert(data, await resolve(data))
        return value

    return extract_shared_async


def _same(record: ConsumerRecord) -> ConsumerRecord:
    return record


def _split_optional(tp: Any) -> tuple[Any, bool]:
    """Split `T | None` into `T` and whether None is allowed, keeping `Annotated` metadata."""
    if get_origin(tp) is Annotated:
        inner, nullable = _split_optional(tp.__origin__)
        return Annotated[inner, *tp.__metadata__], nullable
    if get_origin(tp) in (Union, UnionType) and NoneType in (args := get_args(tp)):
        return reduce(or_, (arg for arg in args if arg is not NoneType)), True
    return tp, False


def _callable_name(function: object) -> str:
    name = getattr(function, "__qualname__", None) or getattr(function, "__name__", None)
    return name or f"a {type(function).__qualname__} object"
