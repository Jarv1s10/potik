"""Serdes, which convert a topic's keys and values between bytes and Python types.

A `Serde` returns a decoder and an encoder for each type it supports, and None for the others. A `SerdeContext` tells it which topic's keys or values they serve. An `AsyncSerde` returns a resolver in place of a decoder, for formats whose decoding needs I/O, e.g. a schema registry's. Type checkers infer these functions' types from the requested type. A `Topic` names the serdes of its values and keys, so the wire format never depends on how a handler annotates a type.

Decoders validate the data against the type, as far as the serde's library does. Encoders do not validate.
"""

import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import partial
from typing import Any, Literal, NewType, Protocol, TypeAliasType, runtime_checkable
from uuid import UUID

from typing_extensions import TypeForm, TypeVar

__all__ = [
    "AsyncSerde",
    "Decoder",
    "Encoder",
    "MsgspecJson",
    "PydanticJson",
    "Resolver",
    "Serde",
    "SerdeContext",
    "Text",
]

_T = TypeVar("_T", default=Any)

Decoder = TypeAliasType("Decoder", Callable[[bytes], _T], type_params=(_T,))
"""Decodes bytes to a `T`, e.g. a key, a value, or a header's value."""
Encoder = TypeAliasType("Encoder", Callable[[_T], bytes], type_params=(_T,))
"""Encodes a `T` to bytes."""
Resolver = TypeAliasType("Resolver", Callable[[bytes], Awaitable[Decoder[_T]]], type_params=(_T,))
"""Does the I/O that decoding one record needs, e.g. fetches its writer schema, and returns the record's `Decoder`."""


@dataclass(frozen=True, slots=True)
class SerdeContext:
    """The keys or values of one topic, which a serde makes a decoder and an encoder for.

    A serde that depends on the topic reads it, e.g. one that names a schema registry subject after the topic, like Confluent's `TopicNameStrategy` (`orders-value`). Other serdes ignore it.

    Attributes:
        topic: The topic's name.
        part: `"key"` for the topic's keys, `"value"` for its values.
    """

    topic: str
    part: Literal["key", "value"]


@runtime_checkable
class Serde(Protocol):
    """Makes the decoders and encoders of the types it supports.

    A `Topic` calls `decoder()` and `encoder()` once for its keys and once for its values, when it is declared, usually at import, so they must not do I/O. The functions they return run synchronously on the event loop for every record, so they must not do I/O either. When potik decodes a handler parameter, a decoder's exception becomes a `DecodeError`. When a `Topic` call encodes a record, an encoder's exception becomes an `EncodeError`. Both are permanent, so `retry()` rules do not retry them. For a format whose decoding needs I/O, write an `AsyncSerde`.
    """

    def decoder[T](self, tp: TypeForm[T], context: SerdeContext, /) -> Decoder[T] | None:
        """Return a function that decodes bytes to a `tp` of `context`, or None if this serde does not support `tp`."""
        ...

    def encoder[T](self, tp: TypeForm[T], context: SerdeContext, /) -> Encoder[T] | None:
        """Return a function that encodes a `tp` of `context` to bytes, or None if this serde does not support `tp`."""
        ...


@runtime_checkable
class AsyncSerde(Protocol):
    """Makes the resolvers and encoders of the types it supports, for formats whose decoding needs I/O.

    A resolver does the I/O that decoding one record needs, e.g. fetching the writer schema that the record names from a registry, and returns the decoder of that record. A resolver's exceptions reach the exception rules as they are, so `retry()` rules retry e.g. a timeout. Raise a `PermanentError` for a failure that would repeat, e.g. an unknown schema. The decoder that the resolver returns, and the encoders, work as a `Serde`'s do: they are synchronous, must not do I/O, and their failures are permanent.

    A `Topic` calls `resolver()` and `encoder()` once for its keys and once for its values, when it is declared, so they must not do I/O. An encoder whose format needs I/O, e.g. the registry ID of its writer schema, gets it before the run. The serde learns every topic's keys and values when they are declared, so a method of its own, which the service's `main` awaits before the run, can register their schemas. potik does not open or close a client that the serde uses, e.g. a registry's. Open and close it around the run, e.g. in the service's `main`.
    """

    def resolver[T](self, tp: TypeForm[T], context: SerdeContext, /) -> Resolver[T] | None:
        """Return an async function that returns a record's decoder to a `tp` of `context`, or None if this serde does not support `tp`."""
        ...

    def encoder[T](self, tp: TypeForm[T], context: SerdeContext, /) -> Encoder[T] | None:
        """Return a function that encodes a `tp` of `context` to bytes, or None if this serde does not support `tp`."""
        ...


class Text:
    """Converts `str` as UTF-8, `int` and `UUID` as their text, and `bytes` as they are.

    It is the default serde of topics' keys and values, and the default decoder of `Header` parameters. A subclass counts as its base, e.g. a `StrEnum` as its value, and a `NewType` counts as its supertype. It does not support `bool`, or `Annotated` types such as `Annotated[str, Field(min_length=3)]`, rather than ignore their constraints.

    An `int` decodes only from ASCII digits with an optional leading `-`, the form that it encodes to, so different keys never decode to the same `int`.
    """

    def decoder[T](self, tp: TypeForm[T], context: SerdeContext, /) -> Decoder[T] | None:
        return _text_decoder(tp)

    def encoder[T](self, tp: TypeForm[T], context: SerdeContext, /) -> Encoder[T] | None:
        return None if (found := _text_type(tp)) is None else _ENCODE[found[0]]

    def __repr__(self) -> str:
        return "Text()"


def _text_decoder[T](tp: TypeForm[T]) -> Decoder[T] | None:
    """Return `Text`'s decoder of `tp`, which also decodes headers, or None if `Text` does not support `tp`."""
    if (found := _text_type(tp)) is None:
        return None
    base, cls = found
    decode = _DECODE[base]
    return decode if cls is base else lambda data: cls(decode(data))


def _text_type(tp: Any) -> tuple[type, type] | None:
    """Return the base type that `Text` takes `tp` as, and `tp` without `NewType`."""
    while isinstance(tp, NewType):
        tp = tp.__supertype__
    if not isinstance(tp, type) or issubclass(tp, bool):
        return None
    for base in _DECODE:
        if issubclass(tp, base):
            return base, tp
    return None


_INT = re.compile(rb"-?[0-9]+")


def _decode_int(data: bytes) -> int:
    if _INT.fullmatch(data) is None:
        raise ValueError(f"invalid literal for int(): {data!r}")
    return int(data)


_DECODE: dict[type, Decoder] = {
    bytes: bytes,
    str: bytes.decode,
    int: _decode_int,
    UUID: lambda data: UUID(data.decode()),
}
_ENCODE: dict[type, Encoder] = {
    bytes: bytes,
    str: str.encode,  # A str subclass, e.g. a StrEnum, is its value.
    int: lambda value: b"%d" % value,  # Not str(), which spells an int mixin enum's name.
    UUID: lambda value: str(value).encode(),
}


class PydanticJson:
    """JSON through pydantic, for any type that pydantic's `TypeAdapter` supports.

    Decoding validates the data against the type. Encoding serializes without validating, but fails on a value that pydantic would warn about, e.g. an `int` in a `str` field. Pass `warnings=...` to change that. Install the `pydantic` extra to use it.

    Args:
        **dump_options: Options of `TypeAdapter.dump_json`, e.g. `by_alias=True`.

    Raises:
        ImportError: If pydantic is not installed.
    """

    def __init__(self, **dump_options: Any) -> None:
        try:
            from pydantic import TypeAdapter
        except ImportError as exc:
            raise ImportError("PydanticJson requires pydantic: install potik[pydantic]") from exc
        self._adapter = TypeAdapter
        self._dump_options = {"warnings": "error", **dump_options}

    def decoder[T](self, tp: TypeForm[T], context: SerdeContext, /) -> Decoder[T]:
        return self._adapter(tp).validate_json

    def encoder[T](self, tp: TypeForm[T], context: SerdeContext, /) -> Encoder[T]:
        return partial(self._adapter(tp).dump_json, **self._dump_options)

    def __repr__(self) -> str:
        return "PydanticJson()"


class MsgspecJson:
    """JSON through msgspec, for any type that msgspec supports without `dec_hook` or `enc_hook`.

    Decoding validates the data against the type. Encoding serializes by the value's own type. Install the `msgspec` extra to use it.

    Raises:
        ImportError: If msgspec is not installed.
    """

    def __init__(self) -> None:
        try:
            import msgspec
            import msgspec.inspect
        except ImportError as exc:
            raise ImportError("MsgspecJson requires msgspec: install potik[msgspec]") from exc
        self._decoder = msgspec.json.Decoder
        self._encode = msgspec.json.Encoder().encode
        self._inspect = msgspec.inspect

    def decoder[T](self, tp: TypeForm[T], context: SerdeContext, /) -> Decoder[T]:
        self._check(tp)
        return self._decoder(tp).decode

    def encoder[T](self, tp: TypeForm[T], context: SerdeContext, /) -> Encoder[T]:
        self._check(tp)
        return self._encode

    def _check(self, tp: object) -> None:
        """Raise `TypeError` if `tp` holds a type that msgspec takes only through hooks, which it accepts at first."""
        pending, seen = [self._inspect.type_info(tp)], set()
        while pending:
            if id(node := pending.pop()) in seen:  # A recursive type refers back to its own node.
                continue
            seen.add(id(node))
            if isinstance(node, self._inspect.CustomType):
                raise TypeError(f"msgspec does not support {_type_name(node.cls)} without hooks")
            if isinstance(node, tuple):
                pending.extend(node)
            elif isinstance(node, (self._inspect.Type, self._inspect.Field)):
                pending.extend(getattr(node, name) for name in node.__struct_fields__)

    def __repr__(self) -> str:
        return "MsgspecJson()"


def _type_name(tp: object) -> str:
    return tp.__qualname__ if isinstance(tp, type) else repr(tp)
