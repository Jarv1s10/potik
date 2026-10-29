"""Typed handlers through a built runtime's pipeline, covering what each parameter takes, what each return value produces, and middleware."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from enum import Enum, StrEnum
from typing import Annotated, NewType, TypedDict

import anyio.lowlevel
import pytest
from confluent_kafka import Message
from pydantic import BaseModel, Field

import potik
from potik import (
    Config,
    ConsumerRecord,
    Context,
    DecodeError,
    EncodeError,
    Header,
    Key,
    PermanentError,
    ProducerRecord,
    RegistrationError,
    Router,
    Runtime,
    Topic,
)
from potik.core import BaseMiddleware
from potik.core.types import RecordHandler
from potik.middleware import (
    ExceptionMiddleware,
    dead_letter,
    on,
    retry,
    skip,
)
from potik.serde import Decoder, Encoder, MsgspecJson, PydanticJson, Resolver, SerdeContext, Text

pytestmark = pytest.mark.anyio
CONFIG = Config(bootstrap_servers="localhost:9092", group_id="unit")
JSON = PydanticJson()


class Order(BaseModel):
    sku: str


class Accepted(BaseModel):
    sku: str


class Status(StrEnum):
    PLACED = "placed"


class Priority(int, Enum):
    HIGH = 1


CustomerId = NewType("CustomerId", str)
orders = Topic("orders", Order, serde=JSON)
accepted = Topic("accepted", Accepted, serde=JSON)


def runtime_of(*routers: Router, **options) -> Runtime:
    return potik.build_runtime(CONFIG, routers, **options)


def pipeline(runtime: Runtime) -> RecordHandler:
    """Return the handler that `runtime` would call, from its private parts, without starting a run."""
    return runtime._handler


def consumed(record: ProducerRecord, offset: int = 0) -> ConsumerRecord:
    """Return `record` as a run would consume it, at `offset` of its partition, or of partition 0 for any."""
    headers = record.headers.items() if isinstance(record.headers, Mapping) else record.headers or ()
    return Message(
        topic=record.topic,
        partition=max(record.partition, 0),
        offset=offset,
        key=record.key,
        value=record.value,
        headers=[(name, value.encode() if isinstance(value, str) else value) for name, value in headers] or None,
    )


async def handle_one(runtime: Runtime, record: ProducerRecord, *, offset: int = 0) -> list[ProducerRecord]:
    """Handle `record` as a run of `runtime` would, without Kafka, and return its outputs."""
    return list(await pipeline(runtime)(consumed(record, offset)))


async def test_parameters_take_their_parts() -> None:
    router = Router()
    seen = {}

    @router.handler(orders)
    async def handle(
        order: Order,
        record: Annotated[ConsumerRecord, "the record itself"],
        customer: Annotated[str, Key()],
        attempt: Annotated[int | None, Header("x-attempt")],
        sent: Annotated[datetime, Header("sent-at", decoder=lambda data: datetime.fromisoformat(data.decode()))],
        tenant: Annotated[str, Header("x-tenant")] = "none",
        *,
        services: Annotated[dict, Context()],
    ) -> None:
        seen.update(order=order, offset=record.offset(), customer=customer, attempt=attempt, sent=sent)
        seen.update(tenant=tenant, services=services)

    headers = [("x-attempt", "1"), ("x-attempt", "2"), ("sent-at", "2026-10-20T12:00:00")]
    runtime = runtime_of(router, context={"db": 1})
    assert await handle_one(runtime, orders(Order(sku="A"), key="c1", headers=headers), offset=7) == []
    assert seen == {
        "order": Order(sku="A"),
        "offset": 7,
        "customer": "c1",
        "attempt": 2,  # The last value, like Java's lastHeader().
        "sent": datetime(2026, 10, 20, 12),
        "tenant": "none",
        "services": {"db": 1},
    }


async def test_context_requires_a_context() -> None:
    router = Router()

    @router.handler("orders")
    async def handle(value: bytes, services: Annotated[object, Context()]) -> None: ...

    with pytest.raises(RegistrationError, match=r"handle takes Context\(\), so requires build_runtime\(context=...\)"):
        runtime_of(router)
    assert await handle_one(runtime_of(router, context=None), ProducerRecord("orders", value=b"x")) == []


async def test_text_takes_subclasses_and_new_types_as_their_base() -> None:
    router = Router()
    events = Topic("events", bytes, key_type=Priority)

    @router.handler(Topic("statuses", bytes, key_type=Status))
    async def handle(
        status: Annotated[Status, Key()], priority: Annotated[Priority, Header("priority")]
    ) -> ProducerRecord:
        assert (status, priority) == (Status.PLACED, Priority.HIGH)
        return events(b"", key=priority)

    [record] = await handle_one(runtime_of(router), ProducerRecord("statuses", b"placed", headers=[("priority", b"1")]))
    assert record.key == b"1"  # Its value, not the name that str() spells for an int mixin enum.
    assert Topic("customers", bytes, key_type=CustomerId)(b"", key=CustomerId("c1")).key == b"c1"
    with pytest.raises(EncodeError, match="takes Priority keys, got bool"):
        events(b"", key=True)  # ty: ignore[invalid-argument-type]


async def test_text_decodes_ints_only_in_the_form_it_encodes() -> None:
    decode = Text().decoder(int, SerdeContext("numbers", "key"))
    assert decode is not None
    assert decode(b"-12") == -12
    for data in (b" 10", b"1_0", b"+10", b"", b"-"):
        with pytest.raises(ValueError, match="invalid literal for int"):
            decode(data)


async def test_text_rejects_constraints_instead_of_ignoring_them() -> None:
    with pytest.raises(RegistrationError, match=r"parameter 'tenant': Text\(\) does not decode"):

        @Router().handler("orders")
        async def constrained_header(order: bytes, tenant: Annotated[str, Header("x-tenant"), Field(min_length=3)]): ...

    with pytest.raises(RegistrationError, match="parameter 'customer': topic 'orders' has keys of str, not"):

        @Router().handler("orders")
        async def constrained_key(order: bytes, customer: Annotated[str, Key(), Field(min_length=3)]) -> None: ...


async def test_the_topic_not_the_annotation_chooses_the_wire_format() -> None:
    router = Router()
    seen = []

    @router.handler(Topic("plain", str))
    async def plain(text: str) -> None:
        seen.append(text)

    @router.handler(Topic("quoted", str, serde=JSON))
    async def quoted(text: str) -> None:
        seen.append(text)

    @router.handler(Topic("raw", str, serde=JSON))
    async def raw(text: bytes) -> None:  # As they are, whatever the serde.
        seen.append(text)

    runtime = runtime_of(router)
    for topic in ("plain", "quoted", "raw"):
        await handle_one(runtime, ProducerRecord(topic, value=b"hi" if topic == "plain" else b'"hi"'))
    assert seen == ["hi", "hi", b'"hi"']


async def test_topics_require_serdes_of_their_types() -> None:
    with pytest.raises(TypeError, match=r"topic 'orders': Text\(\) does not take Order values; .* serde="):
        Topic("orders", Order)
    with pytest.raises(TypeError, match=r"topic 'orders': Text\(\) does not take Order keys; .* key_serde="):
        Topic("orders", bytes, key_type=Order)
    with pytest.raises(TypeError, match=r"does not take bool keys"):
        Topic("flags", bytes, key_type=bool)

    class Opaque: ...

    with pytest.raises(TypeError, match=r"topic 'opaque': PydanticJson\(\) cannot take"):
        Topic("opaque", Opaque, serde=JSON)
    with pytest.raises(TypeError, match=r"MsgspecJson\(\) cannot take .* does not support .*Opaque without hooks"):
        Topic("opaque", list[Opaque | None], serde=MsgspecJson())
    with pytest.raises(ValueError, match="invalid Kafka topic name"):
        Topic("", Order, serde=JSON)

    class Both(FlakyRegistry):
        def decoder(self, tp: object, context: SerdeContext) -> Decoder: ...

    with pytest.raises(TypeError, match=r"either decoder\(\) or resolver\(\), got Both"):
        Topic("both", Order, serde=Both())


async def test_parameters_must_take_their_topics_types() -> None:
    for annotation in (Order, Order | None, bytes):
        Router().handler(orders)(_taking(annotation))
    for annotation in (Accepted, Annotated[Order, Field(description="pinned by the topic")]):
        with pytest.raises(RegistrationError, match="topic 'orders' has values of Order, not"):
            Router().handler(orders)(_taking(annotation))
    with pytest.raises(RegistrationError, match="topic 'plain' has values of bytes, not str"):
        Router().handler("plain")(_taking(str))  # A plain name is a topic of bytes.
    for key in (str, str | None, bytes):
        Router().handler(orders)(_taking(bytes, key))
    with pytest.raises(RegistrationError, match="topic 'orders' has keys of str, not int"):
        Router().handler(orders)(_taking(bytes, int))


async def test_nullable_topics_take_their_declared_types() -> None:
    """A JSON null decodes by the serde, and a Kafka null takes the parameter's default, else None."""
    maybe = Topic("maybe", str | None, key_type=int | None, serde=JSON, key_serde=JSON)
    router = Router()
    seen = []

    @router.handler(maybe)
    async def handle(value: str | None, key: Annotated[int | None, Key()]) -> None:
        seen.append((value, key))

    runtime = runtime_of(router)
    await handle_one(runtime, maybe("a", key=1))
    await handle_one(runtime, ProducerRecord("maybe", b"null", b"null"))
    await handle_one(runtime, maybe(None, key=None))
    assert seen == [("a", 1), (None, None), (None, None)]
    with pytest.raises(RegistrationError, match=r"topic 'maybe' has values of str \| None, not str"):
        Router().handler(maybe)(_taking(str))  # Whether it may skip nulls is not the topic's to decide.


def _taking(annotation: object, key: object = bytes):
    async def handle(value: annotation, k: Annotated[key, Key()]) -> None: ...  # ty: ignore[invalid-type-form]

    return handle


async def test_explicit_outputs() -> None:
    router = Router()

    @router.handler(orders)
    async def handle(order: Order, customer: Annotated[str, Key()]) -> list[ProducerRecord]:
        return [
            accepted(None, key=customer),  # A tombstone of the input's key.
            accepted(Accepted(sku=order.sku), key=None, headers={"tenant": "t1"}),
            accepted(Accepted(sku=order.sku), key="c2", partition=1),
            ProducerRecord("raw", b"k", b"v"),
        ]

    assert await handle_one(runtime_of(router), orders(Order(sku="A"), key="c1")) == [
        ProducerRecord("accepted", b"c1", None),
        ProducerRecord("accepted", None, b'{"sku":"A"}', {"tenant": "t1"}),
        ProducerRecord("accepted", b"c2", b'{"sku":"A"}', partition=1),
        ProducerRecord("raw", b"k", b"v"),
    ]


async def test_outputs_fail_where_they_are_made() -> None:
    """Outputs fail in the handler's call, before the core sees them, so middleware can handle them as the input's failure."""
    with pytest.raises(EncodeError, match="takes Accepted values, got Order") as caught:
        accepted(Order(sku="A"), key=None)  # ty: ignore[invalid-argument-type]
    assert caught.value.topic == "accepted"
    with pytest.raises(ValueError, match="partition"):
        accepted(Accepted(sku="A"), key=None, partition=-2)

    router = Router()

    @router.handler(orders)
    async def handle(order: Order) -> dict:  # ty: ignore[invalid-argument-type]
        return {}

    with pytest.raises(PermanentError, match="the handler returned dict, not a list or tuple of ProducerRecord"):
        await handle_one(runtime_of(router), orders(Order(sku="A"), key="c1"))


async def test_topics_encode_keys_as_their_key_type() -> None:
    """Equal keys always encode to the same bytes, so each key's records stay in one partition."""
    by_number = Topic("by_number", bytes, key_type=int, key_serde=JSON)
    assert by_number(b"", key=1).key == b"1"
    with pytest.raises(EncodeError, match="takes int keys, got str"):
        by_number(b"", key="1")  # ty: ignore[invalid-argument-type]
    with pytest.raises(EncodeError, match="takes int keys, got bool"):
        by_number(b"", key=True)
    with pytest.raises(EncodeError, match="takes int values, got bool"):
        Topic("counts", int)(True, key=None)


async def test_topics_of_typed_dicts_take_dicts() -> None:
    """`isinstance()` cannot check a TypedDict, so the topic checks for a dict and leaves the rest to the serde."""

    class Point(TypedDict):
        x: int

    points = Topic("points", Point, serde=JSON)
    assert points({"x": 1}, key=None).value == b'{"x":1}'
    with pytest.raises(EncodeError, match="Point values, got list"):
        points([1], key=None)  # ty: ignore[invalid-argument-type]


async def test_decode_errors_name_their_parameter() -> None:
    router = Router()

    @router.handler(orders)
    async def handle(order: Order, attempt: Annotated[int, Header("x-attempt")]) -> None: ...

    runtime = runtime_of(router)
    with pytest.raises(DecodeError, match="parameter 'order' cannot decode the value") as caught:
        await handle_one(runtime, ProducerRecord("orders", value=b"not json", headers=[("x-attempt", b"1")]))
    assert caught.value.parameter == "order"
    with pytest.raises(DecodeError, match="parameter 'attempt' requires the header 'x-attempt'"):
        await handle_one(runtime, orders(Order(sku="A"), key="c1"))


async def test_middleware_handles_records_as_in_a_run() -> None:
    """Permanent failures, e.g. undecodable records, skip retries."""
    router = Router()
    attempts = []

    class UnpayableError(PermanentError): ...

    @router.handler(orders)
    async def handle(order: Order) -> None:
        attempts.append(order.sku)
        if order.sku == "flaky" and len(attempts) < 3:
            raise TimeoutError
        if order.sku == "unpayable":
            raise UnpayableError

    runtime = runtime_of(
        router,
        exception_rules=[retry(max_attempts=3, backoff=lambda _: 0), on(Exception, dead_letter("orders.dlq"))],
    )
    assert await handle_one(runtime, orders(Order(sku="flaky"), key="c1")) == []
    assert attempts == ["flaky"] * 3
    [unpayable] = await handle_one(runtime, orders(Order(sku="unpayable"), key="c2"))
    assert unpayable[:3] == ("orders.dlq", b"c2", b'{"sku":"unpayable"}')
    assert dict(unpayable.headers or ())["potik.dlq.exception.type"].endswith(".UnpayableError")
    # The record's bytes, which do not decode.
    [undecodable] = await handle_one(runtime, ProducerRecord("orders", b"c3", b"not json"))
    assert undecodable[:3] == ("orders.dlq", b"c3", b"not json")
    assert dict(undecodable.headers or ())["potik.dlq.exception.type"] == "potik.handler.DecodeError"
    assert attempts == ["flaky"] * 3 + ["unpayable"]


class FlakyRegistry:
    """An async serde of JSON whose resolver times out `outages` times before it resolves. It records the contexts it serves."""

    def __init__(self, outages: int = 0) -> None:
        self.outages = outages
        self.contexts: list[SerdeContext] = []

    def resolver(self, tp: object, context: SerdeContext) -> Resolver:
        self.contexts.append(context)
        decode = JSON.decoder(tp, context)

        async def resolve(data: bytes) -> Decoder:
            await anyio.lowlevel.checkpoint()
            if self.outages:
                self.outages -= 1
                raise TimeoutError
            return decode

        return resolve

    def encoder(self, tp: object, context: SerdeContext) -> Encoder:
        self.contexts.append(context)
        return JSON.encoder(tp, context)


async def test_async_serdes_retry_failures_to_resolve_but_not_to_decode() -> None:
    registry = FlakyRegistry(outages=1)
    registered = Topic("registered", Order, key_type=int, serde=registry, key_serde=registry)
    value, key = SerdeContext("registered", "value"), SerdeContext("registered", "key")
    assert registry.contexts == [value, value, key, key]  # Each part's resolver, then its encoder.
    router = Router()
    seen = []

    @router.handler(registered)
    async def handle(order: Order, number: Annotated[int, Key()]) -> None:
        seen.append((order, number))

    rules = [retry(max_attempts=2, backoff=lambda _: 0), on(Exception, dead_letter("registered.dlq"))]
    runtime = runtime_of(router, exception_rules=rules)
    assert await handle_one(runtime, registered(Order(sku="A"), key=7)) == []
    assert (seen, registry.outages) == ([(Order(sku="A"), 7)], 0)
    [undecodable] = await handle_one(runtime, ProducerRecord("registered", b"7", b"not json"))
    headers = dict(undecodable.headers or ())
    assert headers["potik.dlq.exception.type"] == "potik.handler.DecodeError"
    assert headers["potik.dlq.exception.message"].startswith("parameter 'order' cannot decode the value")
    assert headers["potik.dlq.attempts"] == "1"
    assert await registered.decode(b'{"sku": "B"}') == Order(sku="B")


async def test_routers_combine_and_rules_may_apply_to_some_topics() -> None:
    tracked, untracked = Router(), Router()

    @tracked.handler("tracked")
    async def handle_tracked(record: ConsumerRecord) -> None:
        raise KeyError

    @untracked.handler("untracked")
    async def handle_untracked(record: ConsumerRecord) -> None:
        raise KeyError

    runtime = runtime_of(tracked, untracked, exception_rules=[on(KeyError, skip(), topics=["tracked"])])
    assert await handle_one(runtime, ProducerRecord("tracked")) == []
    with pytest.raises(KeyError):
        await handle_one(runtime, ProducerRecord("untracked"))
    with pytest.raises(RegistrationError, match="topic 'tracked' is handled by both"):
        runtime_of(tracked, tracked)
    with pytest.raises(RegistrationError, match="at least one handler"):
        runtime_of(Router())


async def test_signatures_take_one_value() -> None:
    with pytest.raises(RegistrationError, match="'a', 'b' without a marker"):

        @Router().handler("orders")
        async def two_values(a: bytes, b: bytes) -> None: ...

    with pytest.raises(RegistrationError, match=r"more than one of Key\(\)"):

        @Router().handler("orders")
        async def two_sources(a: Annotated[str, Key(), Header("h")]) -> None: ...


async def test_msgspec_decodes_and_encodes() -> None:
    router = Router()
    numbers = Topic("numbers", list[int], serde=MsgspecJson())
    doubled = Topic("doubled", list[int], serde=MsgspecJson())

    @router.handler(numbers)
    async def double(values: list[int]) -> ProducerRecord:
        return doubled([value * 2 for value in values], key=None)

    runtime = runtime_of(router)
    [record] = await handle_one(runtime, numbers([1, 2], key=None))
    assert record.value == b"[2,4]"
    with pytest.raises(DecodeError):
        await handle_one(runtime, ProducerRecord("numbers", value=b'["x"]'))


async def test_topics_are_immutable_and_decode_their_keys_and_values() -> None:
    with pytest.raises(AttributeError, match="immutable"):
        orders.name = "other"  # ty: ignore[invalid-assignment]
    assert await orders.decode(b'{"sku": "A"}') == Order(sku="A")
    assert await orders.decode_key(b"c1") == "c1"
    assert await Topic("by_number", bytes, key_type=int).decode_key(b"7") == 7


async def test_declared_outputs_do_not_restrict_what_handlers_return() -> None:
    router = Router(output_topics=("orders.audit",))

    @router.handler(orders, output_topics=(accepted, "orders.raw"))
    async def handle(order: Order) -> ProducerRecord:
        return ProducerRecord("undeclared", None, b"v")

    assert router.registrations["orders"].output_topics == ("accepted", "orders.raw")
    assert router.output_topics == ("orders.audit",)
    assert await handle_one(runtime_of(router), orders(Order(sku="A"), key="c1")) == [
        ProducerRecord("undeclared", None, b"v")
    ]
    with pytest.raises(RegistrationError, match="invalid Kafka topic name"):
        Router().handler(orders, output_topics=("not a topic",))
    with pytest.raises(TypeError, match="collection of topics"):
        Router(output_topics="orders.audit")


async def test_dead_letters_copy_the_record_and_say_why_it_failed() -> None:
    router = Router()

    @router.handler("payments")
    async def handle(record: ConsumerRecord) -> None:
        raise TimeoutError("gateway timed out")

    outputs = await handle_one(
        runtime_of(router, exception_rules=[on(TimeoutError, dead_letter("payments.dlq"))]),
        ProducerRecord("payments", b"k", b"v", [("traceparent", "00-1-2-01")], partition=2),
        offset=7,
    )
    assert outputs == [
        ProducerRecord(
            "payments.dlq",
            b"k",
            b"v",
            [
                ("traceparent", b"00-1-2-01"),
                ("potik.dlq.source.topic", "payments"),
                ("potik.dlq.source.partition", "2"),
                ("potik.dlq.source.offset", "7"),
                ("potik.dlq.exception.type", "builtins.TimeoutError"),
                ("potik.dlq.exception.message", "gateway timed out"),
                ("potik.dlq.attempts", "1"),
            ],
        )
    ]


async def test_a_dead_letter_cuts_a_long_exception_message() -> None:
    """A message as long as a large validation error's would push the dead letter past the broker's size limit."""
    router = Router()

    @router.handler("payments")
    async def handle(record: ConsumerRecord) -> None:
        raise ValueError("x" * 1_000_000)

    (output,) = await handle_one(
        runtime_of(router, exception_rules=[on(ValueError, dead_letter("payments.dlq"))]),
        ProducerRecord("payments", b"k", b"v"),
    )
    assert isinstance(output.headers, list)
    message = dict(output.headers)["potik.dlq.exception.message"]
    assert message == "x" * 997 + "..."


async def test_exception_rules_run_inside_the_middleware() -> None:
    """Middleware sees the dead letter that a rule returns, not the exception."""
    seen = []

    class Recorder(BaseMiddleware):
        async def handle(self, record: ConsumerRecord, call_next: RecordHandler) -> Sequence[ProducerRecord]:
            outputs = await call_next(record)
            seen.append([output.topic for output in outputs])
            return outputs

    router = Router()

    @router.handler("payments")
    async def handle(record: ConsumerRecord) -> None:
        raise TimeoutError

    runtime = runtime_of(router, middleware=[Recorder()], exception_rules=[dead_letter("payments.dlq")])
    await handle_one(runtime, ProducerRecord("payments"))
    assert seen == [["payments.dlq"]]
    with pytest.raises(RegistrationError, match=r"runtime\(exception_rules=...\)"):
        runtime_of(router, middleware=[ExceptionMiddleware(skip())])


def test_exception_middleware_declares_its_dead_letter_topics() -> None:
    exceptions = ExceptionMiddleware(
        retry(max_attempts=3), on(DecodeError, dead_letter(accepted)), dead_letter("orders.dlq")
    )
    assert exceptions.output_topics == ("accepted", "orders.dlq")
    with pytest.raises(ValueError, match="invalid Kafka topic name"):
        dead_letter("not a topic")
