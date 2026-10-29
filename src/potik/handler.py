"""Typed handlers, whose parameters take parts of the input record and whose results are output records.

Each parameter takes one part of the record, independently of the others:

- `Annotated[T, Key()]` takes the key.
- `Annotated[T, Header(name)]` takes a header's last value.
- `Annotated[T, Context()]` takes the run's context.
- A parameter annotated `ConsumerRecord` takes the record itself.
- The one parameter left, if any, takes the value.

A handler takes its topic's keys and values as the `Topic`'s types, or as raw `bytes`. A plain topic name is a topic of `bytes` values and `str` keys.

A handler returns None, a `ProducerRecord`, e.g. made by calling a `Topic`, or a list or tuple of them. Registration checks the parameters against the topic, so a mismatch fails before the run starts.
"""

import inspect
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from functools import reduce
from operator import or_
from types import NoneType, UnionType
from typing import Annotated, Any, TypeAliasType, Union, cast, get_args, get_origin

from confluent_kafka import Message
from typing_extensions import Sentinel, TypeVar

from potik.core.errors import PermanentError, PotikError
from potik.core.types import ConsumerRecord, ProducerRecord, RecordHandler
from potik.serde import Decoder, Resolver, _text_decoder, _type_name
from potik.topic import Topic, _Codec

__all__ = [
    "Context",
    "DecodeError",
    "HandlerResult",
    "Header",
    "Key",
]

# Takes a parameter's part of the record. It is a coroutine function if the part's serde is an `AsyncSerde`.
type _Extractor = Callable[[ConsumerRecord], Any]
# The handler as a function of the run's context, which is _NO_CONTEXT for a runtime built without one.
type _BindHandler = Callable[[object], RecordHandler]

_EMPTY = inspect.Parameter.empty
_NO_CONTEXT = Sentinel("NO_CONTEXT")


class DecodeError(PotikError, PermanentError):
    """Raised from a handler call when a parameter cannot take its part of the record.

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

    The context is the object passed as `potik.build_runtime(context=...)`, e.g. a dataclass of the service's clients. potik does not check it against `T`. `potik.build_runtime()` raises `RegistrationError` if a handler takes it and no context is given.
    """


def _compile_handler(handler: Callable[..., Awaitable[Any]], topic: Topic[Any, Any]) -> _BindHandler:
    """Bind each of the handler's parameters to its part of `topic`'s records.

    Raises:
        ValueError: If the handler is not an async function, or a parameter has no part to take.
    """
    if not callable(handler) or not (
        inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(type(handler).__call__)
    ):
        raise ValueError("must be an async function")
    try:
        parameters = inspect.signature(handler, eval_str=True).parameters.values()
    except (TypeError, ValueError, NameError) as exc:
        raise ValueError(f"has a signature that potik cannot read: {exc}") from exc
    positional: list[_Extractor | None] = []  # None takes the run's context.
    keyword: list[tuple[str, _Extractor | None]] = []
    values = []
    for parameter in parameters:
        if parameter.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        if parameter.annotation is _EMPTY:
            raise ValueError(f"must annotate parameter {parameter.name!r} with a type")
        try:
            extract, takes_value = _bind(parameter, topic)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"cannot bind parameter {parameter.name!r}: {exc}") from exc
        if takes_value:
            values.append(parameter.name)
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY:
            keyword.append((parameter.name, extract))
        else:
            positional.append(extract)
    if len(values) > 1:
        raise ValueError(
            f"has parameters {', '.join(map(repr, values))} without a marker, but only the value's may lack one; "
            "mark the others, e.g. as Annotated[str, Key()]"
        )

    takes_context = any(extract is None for extract in [*positional, *(extract for _, extract in keyword)])

    def bind(context: object) -> RecordHandler:
        """Bind the handler to the run's `context`. Raise LookupError for NO_CONTEXT if the handler takes it."""
        if takes_context and context is _NO_CONTEXT:
            raise LookupError("takes Context(), so requires build_runtime(context=...)")

        def with_context(extract: _Extractor | None) -> tuple[_Extractor, bool]:
            """Return the extractor, and whether it awaits, as only the parts of an `AsyncSerde` do."""
            if extract is None:
                return (lambda _: context), False
            return extract, inspect.iscoroutinefunction(extract)

        args = [with_context(extract) for extract in positional]
        kwargs = [(name, *with_context(extract)) for name, extract in keyword]

        async def handle(record: ConsumerRecord) -> Sequence[ProducerRecord]:
            result = await handler(
                *[await extract(record) if awaits else extract(record) for extract, awaits in args],
                **{name: await extract(record) if awaits else extract(record) for name, extract, awaits in kwargs},
            )
            if result is None:
                return ()
            if isinstance(result, ProducerRecord):  # A tuple itself, so checked first.
                return (result,)
            return result

        return handle

    return bind


def _bind(parameter: inspect.Parameter, topic: Topic[Any, Any]) -> tuple[_Extractor | None, bool]:
    """Return the parameter's extractor, None for the run's context, and whether it takes the value."""
    annotation = parameter.annotation
    annotated = get_origin(annotation) is Annotated
    tp = annotation.__origin__ if annotated else annotation
    if tp is ConsumerRecord or tp is Message:
        return _same, False
    metadata = annotation.__metadata__ if annotated else ()
    sources = [item for item in metadata if isinstance(item, (Key, Header, Context))]
    if len(sources) > 1:
        raise ValueError("it has more than one of Key(), Header(), and Context()")
    if rest := [item for item in metadata if not isinstance(item, (Key, Header, Context))]:
        tp = Annotated[tp, *rest]  # Other metadata stays, so that it fails to match the topic's type.
    source = sources[0] if sources else None
    if isinstance(source, Context):
        return None, False
    inner, nullable = _split_optional(tp)
    if isinstance(source, Key):
        decode, resolve = _topic_decoder(topic, tp, inner, topic.key_type, topic._key, "keys")
        return _extractor(parameter, Message.key, decode, resolve, nullable, "key"), False
    if isinstance(source, Header):
        name = source.name
        if (decode := source.decoder) is None and inner is not bytes and (decode := _text_decoder(inner)) is None:
            raise TypeError(f"Text() does not decode {_type_name(inner)}; pass a decoder to the Header")

        def last_header(record: ConsumerRecord) -> bytes | None:
            # Consumed headers are pairs with bytes values. The stubs allow a dict and str values too.
            return cast(bytes | None, dict(record.headers() or ()).get(name))

        return _extractor(parameter, last_header, decode, None, nullable, f"header {name!r}"), False
    decode, resolve = _topic_decoder(topic, tp, inner, topic.value_type, topic._value, "values")
    return _extractor(parameter, Message.value, decode, resolve, nullable, "value"), True


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


def _extractor(
    parameter: inspect.Parameter,
    get: Callable[[ConsumerRecord], bytes | None],
    decode: Decoder | None,
    resolve: Resolver | None,
    nullable: bool,
    part: str,
) -> _Extractor:
    """Return a function that decodes `part`, which `get` takes from the record, and awaits `resolve` if given.

    A null `part` takes the parameter's default if it has one, else None if its type allows None, else fails.
    """
    name, default = parameter.name, parameter.default

    def convert(data: bytes | None, decode: Decoder | None) -> Any:
        if data is None:
            if default is not _EMPTY:
                return default
            if nullable:
                return None
            raise DecodeError(f"parameter {name!r} requires the {part}, which is missing or null", parameter=name)
        if decode is None:
            return data
        try:
            return decode(data)
        except Exception as exc:
            raise DecodeError(f"parameter {name!r} cannot decode the {part}: {exc}", parameter=name) from exc

    if resolve is None:
        return lambda record: convert(get(record), decode)

    async def extract(record: ConsumerRecord) -> Any:
        data = get(record)
        # A resolver's failure, e.g. a registry's timeout, propagates as it is, so that `retry()` rules retry it.
        return convert(data, None if data is None else await resolve(data))

    return extract


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
