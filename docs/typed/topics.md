# Topics and serdes

A `Topic` declares a topic's name, the types of its values and keys, and the serdes that encode them. Producers and consumers of a topic should share one declaration, for example from a module that every service imports.

```python
from potik import Topic
from potik.serde import PydanticJson

json = PydanticJson()
orders = Topic("orders", Order, serde=json)                                # Topic[Order], str keys
events = Topic("orders.events", Created | Deleted, serde=json)             # a union of models
by_number = Topic("orders.by_number", Order, key_type=int, serde=json)     # int keys
```

Keys are `str` by default. `key_serde=` sets the serde of the keys, which is `Text` by default.

A topic is used in three ways:

- `@router.handler(orders)` registers a handler that takes its decoded records (see [Handlers](handlers.md)).
- Calling it, as in `orders(order, key="c1")`, encodes a `ProducerRecord` to it (see [Outputs](handlers.md#outputs)).
- `await orders.decode(data)` decodes a raw value, for example a reply that an [API](../request-reply.md) receives.

## Serdes

potik ships three serdes:

| Serde | Types | Extra |
| --- | --- | --- |
| `Text` (the default) | `bytes` as is, `str` as UTF-8, `int` and `UUID` as text | none |
| `PydanticJson` | anything pydantic can validate | `pydantic` |
| `MsgspecJson` | anything msgspec can decode | `msgspec` |

A declaration fails if its serde can't handle its types. Serdes run on the event loop for every record, so they must not do I/O.

## Serdes that need I/O

Some formats need I/O to decode a record. For example, an Avro record names its writer schema, which is stored in a schema registry. Write an `AsyncSerde` for them. Its `resolver()` returns an async function. That function does one record's I/O and returns the decoder of that record.

Every serde method also gets a `SerdeContext`. It holds the topic's name, and whether the serde serves the keys or the values. Use it, for example, to name a registry subject.

This sketch decodes Avro. `RegistryClient`, `Schema`, `schema_of()`, and `read_avro()` stand for the API of your Avro and registry libraries, and the encoder is left out:

```python
from collections.abc import Awaitable, Callable
from functools import partial

from potik.serde import Decoder, Encoder, SerdeContext


class AvroRegistry:
    def __init__(self, url: str) -> None:
        self._client = RegistryClient(url)  # opened and closed by your main()
        self._schemas: dict[int, Schema] = {}

    def resolver(self, tp: type, context: SerdeContext) -> Callable[[bytes], Awaitable[Decoder]]:
        reader = schema_of(tp)

        async def resolve(data: bytes) -> Decoder:
            schema_id = int.from_bytes(data[1:5])
            if schema_id not in self._schemas:
                self._schemas[schema_id] = await self._client.schema(schema_id)
            return partial(read_avro, writer=self._schemas[schema_id], reader=reader)

        return resolve

    def encoder(self, tp: type, context: SerdeContext) -> Encoder: ...  # left out of this sketch
```

A failure of the resolver, such as a timeout, reaches your exception rules as it is, so `retry()` retries it. Raise a `PermanentError` there for failures that would repeat. A record that the returned decoder can't decode raises `DecodeError`, as with any serde.

Encoders stay synchronous. An encoder that needs I/O, such as looking up its writer schema's ID, does it before the run. The serde sees every topic when the topic is declared, so give it a method that registers their schemas, and await it in your `main()`. Open and close the serde's client there too.
