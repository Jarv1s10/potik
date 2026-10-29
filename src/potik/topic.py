"""Topic declarations: a name, and the types and serdes of the topic's values and keys."""

from dataclasses import dataclass
from typing import Any, Generic, Protocol, cast, overload

from typing_extensions import TypeForm, TypeVar, is_typeddict

from potik.core.errors import PermanentError, PotikError
from potik.core.topics import _validate_topic
from potik.core.types import Headers, ProducerRecord
from potik.middleware.topics import NamedTopic
from potik.serde import AsyncSerde, Decoder, Encoder, Resolver, Serde, SerdeContext, Text, _type_name

__all__ = [
    "EncodeError",
    "OutputTopic",
    "Topic",
]

_TEXT = Text()

_V = TypeVar("_V")
_K = TypeVar("_K", default=str)


class EncodeError(PotikError, PermanentError):
    """Raised when a topic cannot encode a key or value.

    The key or value has the wrong type, or the topic's serde fails on it. It is a `PermanentError`, so `retry()` rules do not retry it.

    Attributes:
        topic: The topic's name.
    """

    def __init__(self, message: str, *, topic: str) -> None:
        super().__init__(message)
        self.topic = topic


@dataclass(frozen=True, slots=True)
class _Codec[T]:
    """Converts a topic's keys or values. Exactly one of `decode` and `resolve` is set, by the serde's kind."""

    decode: Decoder[T] | None
    resolve: Resolver[T] | None
    encode: Encoder[T]
    instance_of: type | None  # the class of the objects to encode, if isinstance() can check it

    async def decoded(self, data: bytes) -> T:
        decode = self.decode if self.resolve is None else await self.resolve(data)
        return cast(Decoder[T], decode)(data)


class Topic(Generic[_V, _K]):
    """A Kafka topic's name, and the types and serdes of its values and keys.

    Declare each topic once, e.g. in a module that its producers and consumers share, so that they all use the same wire format. Pass it to `Router.handler()` to handle it, and call it to make a `ProducerRecord` to it. Type checkers infer its types from the given ones, e.g. `Topic("events", A | B, serde=json)` is a `Topic[A | B]`. A topic is immutable.

    The declaration gets its decoders and encoders from the serdes, so a serde that does not support a type fails here, not on the first record.

    A tombstone, a record with a null value, has no value to decode. A handler's value parameter takes its default for it, else `None` if its type allows None, e.g. `Order | None` for a `Topic[Order]`. Otherwise the handler call raises `DecodeError`.

    Args:
        name: The topic's name.
        value_type: The type of the topic's values.
        key_type: The type of the topic's keys. Defaults to `str`.
        serde: The `Serde` or `AsyncSerde` of the values. Defaults to `Text()`.
        key_serde: The `Serde` or `AsyncSerde` of the keys. Defaults to `Text()`.

    Raises:
        TypeError: If a serde is neither a `Serde` nor an `AsyncSerde`, or both, or does not support its type.
        ValueError: If `name` is not a valid topic name.

    Example:
        ```python
        json = PydanticJson()
        orders = Topic("orders", Order, serde=json)  # Topic[Order], str keys
        by_number = Topic("orders.by_number", Order, key_type=int, serde=json)
        ```
    """

    __slots__ = ("_key", "_value", "key_serde", "key_type", "name", "serde", "value_type")
    name: str
    value_type: TypeForm[_V]
    key_type: TypeForm[_K]
    serde: Serde | AsyncSerde
    key_serde: Serde | AsyncSerde
    _value: _Codec[_V]
    _key: _Codec[_K]

    @overload
    def __init__(
        self,
        name: str,
        value_type: TypeForm[_V],
        *,
        serde: Serde | AsyncSerde = _TEXT,
        key_serde: Serde | AsyncSerde = _TEXT,
    ) -> None: ...
    @overload
    def __init__(
        self,
        name: str,
        value_type: TypeForm[_V],
        *,
        key_type: TypeForm[_K],
        serde: Serde | AsyncSerde = _TEXT,
        key_serde: Serde | AsyncSerde = _TEXT,
    ) -> None: ...
    def __init__(
        self,
        name: str,
        value_type: Any,
        *,
        key_type: Any = str,
        serde: Serde | AsyncSerde = _TEXT,
        key_serde: Serde | AsyncSerde = _TEXT,
    ) -> None:
        _validate_topic(name)
        for given in (serde, key_serde):
            if isinstance(given, Serde) == isinstance(given, AsyncSerde):
                raise TypeError(
                    f"a serde is a Serde or an AsyncSerde, with encoder() and either decoder() or resolver(), "
                    f"got {type(given).__name__}"
                )
        set_field = super().__setattr__
        for field, value in (
            ("name", name),
            ("value_type", value_type),
            ("key_type", key_type),
            ("serde", serde),
            ("key_serde", key_serde),
            ("_value", _codec(SerdeContext(name, "value"), serde, value_type)),
            ("_key", _codec(SerdeContext(name, "key"), key_serde, key_type)),
        ):
            set_field(field, value)

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("a Topic is immutable")

    def __call__(
        self, value: _V | None, *, key: _K | None, headers: Headers | None = None, partition: int = -1
    ) -> ProducerRecord[_V]:
        """Encode `value` and `key` into a `ProducerRecord` to this topic.

        Args:
            value: The value, or None for a tombstone.
            key: The key, or None for a null key. It has no default, so every call decides the key.
            headers: The record's headers, which the topic does not encode.
            partition: The record's partition, or -1 to let the producer's partitioner pick one.

        Returns:
            The record, with the encoded key and value.

        Raises:
            EncodeError: If the topic does not take the value or key, or its serde fails on them.
            ValueError: If `partition` is neither -1 nor a partition number.
        """
        if type(partition) is not int or partition < -1:
            raise ValueError(
                f"partition must be -1, to let the partitioner pick, or a partition number, got {partition!r}"
            )
        encoded_key = None if key is None else self._encode_part(key, self.key_type, self._key, "key")
        data = None if value is None else self._encode_part(value, self.value_type, self._value, "value")
        return ProducerRecord(self.name, encoded_key, data, headers, partition)

    async def decode(self, data: bytes) -> _V:
        """Decode a value of this topic, e.g. in middleware.

        A failure raises the serde's own exception, not `DecodeError`.
        """
        return await self._value.decoded(data)

    async def decode_key(self, data: bytes) -> _K:
        """Decode a key of this topic, e.g. in middleware.

        A failure raises the serde's own exception, not `DecodeError`.
        """
        return await self._key.decoded(data)

    def _encode_part[T](self, obj: T, tp: object, codec: _Codec[T], part: str) -> bytes:
        # Serdes may not check: pydantic serializes any object with the model's attributes as the model, and True as an int's 1. bool subclasses int, so it is checked apart.
        if (cls := codec.instance_of) is not None and (
            not isinstance(obj, cls) or (cls is int and isinstance(obj, bool))
        ):
            raise EncodeError(
                f"topic {self.name!r} takes {_type_name(tp)} {part}s, got {_type_name(type(obj))}", topic=self.name
            )
        try:
            return codec.encode(obj)
        except Exception as exc:
            raise EncodeError(f"cannot encode a {part} to topic {self.name!r}: {exc}", topic=self.name) from exc

    def __repr__(self) -> str:
        key_type = "" if self.key_type is str else f", key_type={_type_name(self.key_type)}"
        return f"Topic({self.name!r}, {_type_name(self.value_type)}{key_type})"


def _codec(context: SerdeContext, serde: Serde | AsyncSerde, tp: TypeForm[Any]) -> _Codec[Any]:
    topic, part = context.topic, f"{context.part}s"
    try:
        if isinstance(serde, Serde):
            decode, resolve = serde.decoder(tp, context), None
        else:
            decode, resolve = None, serde.resolver(tp, context)
        encode = serde.encoder(tp, context)
    except Exception as exc:  # e.g. pydantic's PydanticSchemaGenerationError
        raise TypeError(f"topic {topic!r}: {serde!r} cannot take {_type_name(tp)} {part}: {exc}") from exc
    if (decode is None and resolve is None) or encode is None:
        option = "key_serde" if context.part == "key" else "serde"
        raise TypeError(
            f"topic {topic!r}: {serde!r} does not take {_type_name(tp)} {part}; "
            f"pass a serde that does, e.g. {option}=PydanticJson()"
        )
    return _Codec(decode, resolve, encode, _instance_of(tp))


def _instance_of(tp: object) -> type | None:
    """Return the class that `tp`'s objects are instances of, or None if `isinstance()` cannot check them."""
    if is_typeddict(tp):
        return dict
    if not isinstance(tp, type):
        return None
    try:
        isinstance(None, tp)
    except TypeError:  # e.g. a Protocol that is not runtime_checkable
        return None
    return tp


class OutputTopic[V](NamedTopic, Protocol):
    """The type of the topics in a handler's `output_topics`, e.g. a `Topic[V]`.

    You rarely name it yourself. Unlike `Topic`, it is covariant in `V`, so type checkers accept `output_topics=[a, b]`, with `a: Topic[A]` and `b: Topic[B]`, as outputs of `A | B`.
    """

    @property
    def value_type(self) -> TypeForm[V]: ...
