"""Typed handlers through a built runtime's pipeline, covering what each parameter takes, what each return value produces, and middleware."""

from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
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
    Depends,
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


async def test_dependencies_take_what_handlers_take_and_resolve_once_per_call() -> None:
    """Each kind of dependency, in parameter order, each after its own dependencies."""
    router = Router()
    calls = []

    def tenant(name: Annotated[str, Header("x-tenant")]) -> str:
        calls.append("tenant")
        return name

    async def customer(key: Annotated[str, Key()], tenant: Annotated[str, Depends(tenant)]) -> str:
        calls.append("customer")
        return f"{tenant}/{key}"

    def scope(record: ConsumerRecord, *, services: Annotated[dict, Context()]) -> Iterator[tuple[int, str]]:
        calls.append("scope")
        yield record.offset(), services["db"]

    async def sku(order: Order, customer: Annotated[str, Depends(customer)]) -> AsyncIterator[str]:
        calls.append("sku")
        yield f"{customer}:{order.sku}"

    @dataclass
    class Audit:
        owner: Annotated[str, Depends(customer)]

    class Counter:
        async def __call__(self, scope: Annotated[tuple[int, str], Depends(scope)]) -> int:
            calls.append("counter")
            return scope[0] + 1

    count = Counter()

    @router.handler(orders)
    async def handle(
        sku: Annotated[str, Depends(sku)],
        scope: Annotated[tuple[int, str], Depends(scope)],
        audit: Annotated[Audit, Depends(Audit)],
        *,
        next_offset: Annotated[int, Depends(count)],
        again: Annotated[str, Depends(customer)],
    ) -> ProducerRecord:
        calls.append("handle")
        assert again == audit.owner == "acme/c1"
        return accepted(Accepted(sku=f"{sku} {scope} {next_offset}"), key=None)

    runtime = runtime_of(router, context={"db": "pg"})
    record = orders(Order(sku="A"), key="c1", headers=[("x-tenant", "acme")])
    [output] = await handle_one(runtime, record, offset=7)
    assert output.value == b'{"sku":"acme/c1:A (7, \'pg\') 8"}'
    assert calls == ["tenant", "customer", "sku", "scope", "counter", "handle"]
    calls.clear()
    await handle_one(runtime, record, offset=8)
    assert calls == ["tenant", "customer", "sku", "scope", "counter", "handle"]  # Anew for each call.


async def test_dependencies_that_yield_resume_last_first_and_see_the_failure() -> None:
    """A dependency cannot suppress an exception, from the handler or from another dependency."""
    router = Router()
    seen = []

    async def outer() -> AsyncIterator[None]:
        try:
            yield
        except Exception as exc:
            seen.append(("outer", repr(exc)))
            raise

    def swallowing() -> Iterator[None]:
        try:
            yield
        except Exception as exc:  # noqa: BLE001 -- suppresses it, which potik ignores
            seen.append(("swallowing", repr(exc)))

    async def failing() -> AsyncIterator[None]:
        try:
            yield
        finally:
            seen.append(("failing", None))
            raise ConnectionError("rollback failed")

    @router.handler("orders")
    async def handle(
        value: bytes,
        a: Annotated[None, Depends(outer)],
        b: Annotated[None, Depends(swallowing)],
        c: Annotated[None, Depends(failing)],
    ) -> None:
        if value == b"fail":
            raise KeyError("handler")

    runtime = runtime_of(router)
    with pytest.raises(ConnectionError, match="rollback failed") as caught:
        await handle_one(runtime, ProducerRecord("orders", value=b"fail"))
    assert repr(caught.value.__context__) == "KeyError('handler')"
    assert seen == [
        ("failing", None),
        ("swallowing", "ConnectionError('rollback failed')"),
        ("outer", "ConnectionError('rollback failed')"),
    ]
    seen.clear()
    with pytest.raises(ConnectionError):  # The handler returned, but a dependency failed after it.
        await handle_one(runtime, ProducerRecord("orders", value=b"ok"))
    assert seen == [
        ("failing", None),
        ("swallowing", "ConnectionError('rollback failed')"),
        ("outer", "ConnectionError('rollback failed')"),
    ]


async def test_a_dependency_cannot_suppress_the_handlers_exception() -> None:
    router = Router()

    def swallowing() -> Iterator[None]:
        with suppress(KeyError):
            yield

    @router.handler("orders")
    async def handle(value: bytes, _: Annotated[None, Depends(swallowing)]) -> None:
        raise KeyError("handler")

    with pytest.raises(KeyError, match="handler"):
        await handle_one(runtime_of(router), ProducerRecord("orders", value=b""))


async def test_a_failed_resolution_resumes_the_dependencies_entered_before_it() -> None:
    router = Router()
    seen = []

    async def entered() -> AsyncIterator[None]:
        try:
            yield
        except Exception as exc:
            seen.append(repr(exc))
            raise

    async def failing() -> None:
        raise TimeoutError("lookup")

    @router.handler(orders)
    async def handle(
        order: Order,
        a: Annotated[None, Depends(entered)],
        b: Annotated[None, Depends(failing)],
    ) -> None:
        seen.append("handled")

    runtime = runtime_of(router)
    with pytest.raises(TimeoutError):
        await handle_one(runtime, orders(Order(sku="A"), key=None))
    assert seen == ["TimeoutError('lookup')"]
    with pytest.raises(DecodeError, match="parameter 'order'"):  # The value comes first, so nothing was entered.
        await handle_one(runtime, ProducerRecord("orders", value=b"not json"))
    assert seen == ["TimeoutError('lookup')"]


async def test_dependency_failures_meet_exception_rules_and_retries_resolve_again() -> None:
    """Each attempt enters and exits its own dependencies, and a failed exit discards the handler's outputs."""
    router = Router()
    events = []

    async def connection() -> AsyncIterator[int]:
        attempt = len([event for event in events if event == "enter"]) + 1
        events.append("enter")
        try:
            yield attempt
        finally:
            events.append("exit")
            if attempt == 2:
                raise TimeoutError("commit")

    @router.handler(orders)
    async def handle(order: Order, attempt: Annotated[int, Depends(connection)]) -> ProducerRecord:
        events.append(f"handle {attempt}")
        if attempt == 1:
            raise TimeoutError("query")
        return accepted(Accepted(sku=order.sku), key=None)

    rules = [on(TimeoutError, retry(max_attempts=3, backoff=lambda _: 0))]
    [output] = await handle_one(runtime_of(router, exception_rules=rules), orders(Order(sku="A"), key=None))
    assert output.topic == "accepted"
    assert events == ["enter", "handle 1", "exit", "enter", "handle 2", "exit", "enter", "handle 3", "exit"]


async def test_dependencies_resume_in_a_cancelled_scope() -> None:
    """Awaits after `yield` raise the cancellation again, unless shielded."""
    router = Router()
    seen = []
    started = anyio.Event()

    async def shielded() -> AsyncIterator[None]:
        try:
            yield
        finally:
            with anyio.move_on_after(1, shield=True):
                await anyio.lowlevel.checkpoint()
                seen.append("shielded")

    async def unshielded() -> AsyncIterator[None]:
        try:
            yield
        finally:
            try:
                await anyio.lowlevel.checkpoint()
            except anyio.get_cancelled_exc_class():
                seen.append("cancelled")
                raise

    @router.handler("orders")
    async def handle(
        value: bytes, a: Annotated[None, Depends(shielded)], b: Annotated[None, Depends(unshielded)]
    ) -> None:
        started.set()
        await anyio.sleep_forever()

    runtime = runtime_of(router)
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(handle_one, runtime, ProducerRecord("orders", value=b""))
        await started.wait()
        tasks.cancel_scope.cancel()
    assert seen == ["cancelled", "shielded"]


async def test_middleware_hands_values_to_dependencies_in_context_variables() -> None:
    router = Router()
    tenant: ContextVar[str] = ContextVar("tenant")

    class Tenant(BaseMiddleware):
        async def handle(self, record: ConsumerRecord, call_next: RecordHandler) -> Sequence[ProducerRecord]:
            token = tenant.set("acme")
            try:
                return await call_next(record)
            finally:
                tenant.reset(token)

    def current_tenant() -> str:
        return tenant.get()

    @router.handler("orders")
    async def handle(value: bytes, tenant: Annotated[str, Depends(current_tenant)]) -> ProducerRecord:
        return ProducerRecord("tenants", value=tenant.encode())

    [output] = await handle_one(runtime_of(router, middleware=[Tenant()]), ProducerRecord("orders", value=b""))
    assert output.value == b"acme"


async def test_a_dependency_may_set_a_deadline_for_the_call() -> None:
    """Its own cancel scope ends the call, which runs in one task with its dependencies."""
    router = Router()
    tenant: ContextVar[str] = ContextVar("tenant", default="none")

    async def deadline(fail: Annotated[str | None, Header("x-fail")]) -> AsyncIterator[None]:
        with anyio.fail_after(0.01) if fail else anyio.move_on_after(0.01):
            tenant.set("acme")
            yield

    @router.handler("orders")
    async def handle(value: bytes, _: Annotated[None, Depends(deadline)]) -> ProducerRecord:
        assert tenant.get() == "acme"
        if value == b"slow":
            await anyio.sleep_forever()
        return ProducerRecord("done", value=value)

    runtime = runtime_of(router)
    assert await handle_one(runtime, ProducerRecord("orders", value=b"slow")) == []  # Moved on, with no outputs.
    await anyio.lowlevel.checkpoint()  # No cancellation is left behind.
    [output] = await handle_one(runtime, ProducerRecord("orders", value=b"fast"))
    assert output.value == b"fast"
    with pytest.raises(TimeoutError):
        await handle_one(runtime, ProducerRecord("orders", value=b"slow", headers=[("x-fail", b"1")]))


async def test_a_deadline_ends_the_call_without_hiding_its_failure() -> None:
    """A deadline that cuts off another dependency's commit drops the outputs, and one that cuts off its rollback keeps the failure."""
    router = Router()
    events = []

    async def deadline() -> AsyncIterator[None]:
        with anyio.move_on_after(0.01):
            yield

    async def transaction() -> AsyncIterator[None]:
        try:
            yield
        except BaseException:
            await anyio.sleep(1)  # An unshielded rollback, which the deadline cuts off.
            raise
        await anyio.sleep(1)  # A commit, which the deadline cuts off.
        events.append("committed")

    @router.handler("orders")
    async def handle(
        value: bytes, a: Annotated[None, Depends(deadline)], b: Annotated[None, Depends(transaction)]
    ) -> ProducerRecord:
        if value == b"bad":
            raise ValueError("bad order")
        return ProducerRecord("done", value=value)

    runtime = runtime_of(router)
    assert await handle_one(runtime, ProducerRecord("orders", value=b"ok")) == []
    assert events == []
    with pytest.raises(ValueError, match="bad order"):
        await handle_one(runtime, ProducerRecord("orders", value=b"bad"))


async def test_a_dependency_cannot_swallow_an_outer_cancellation() -> None:
    """A cancelled call, e.g. by a revocation, stays cancelled, so its record does not commit."""
    router = Router()
    started = anyio.Event()
    seen = []

    async def swallowing() -> AsyncIterator[None]:
        try:
            yield
        except BaseException:  # noqa: BLE001 -- swallows the cancellation, which potik raises again
            seen.append("rolled back")

    @router.handler("orders")
    async def handle(value: bytes, _: Annotated[None, Depends(swallowing)]) -> None:
        started.set()
        await anyio.sleep_forever()

    async def call() -> None:
        try:
            seen.append(await handle_one(runtime_of(router), ProducerRecord("orders", value=b"")))
        except anyio.get_cancelled_exc_class():
            seen.append("cancelled")
            raise

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(call)
        await started.wait()
        tasks.cancel_scope.cancel()
    assert seen == ["rolled back", "cancelled"]


async def test_failed_exits_keep_their_chain() -> None:
    router = Router()

    def outer() -> Iterator[None]:
        try:
            yield
        finally:
            raise RuntimeError("outer")

    def inner() -> Iterator[None]:
        try:
            yield
        finally:
            raise ConnectionError("inner")

    @router.handler("orders")
    async def handle(value: bytes, a: Annotated[None, Depends(outer)], b: Annotated[None, Depends(inner)]) -> None:
        raise KeyError("handler")

    with pytest.raises(RuntimeError, match="outer") as caught:
        await handle_one(runtime_of(router), ProducerRecord("orders", value=b""))
    assert repr(caught.value.__context__) == "ConnectionError('inner')"
    assert repr(caught.value.__context__.__context__) == "KeyError('handler')"


class CountingJson:
    """A serde of JSON that counts its decodes."""

    def __init__(self) -> None:
        self.decodes = 0

    def decoder(self, tp: object, context: SerdeContext) -> Decoder:
        decode = JSON.decoder(tp, context)

        def counted(data: bytes) -> object:
            self.decodes += 1
            return decode(data)

        return counted

    def encoder(self, tp: object, context: SerdeContext) -> Encoder:
        return JSON.encoder(tp, context)


class CountingRegistry:
    """An async serde of JSON that counts its resolves."""

    def __init__(self) -> None:
        self.decodes = 0

    def resolver(self, tp: object, context: SerdeContext) -> Resolver:
        decode = JSON.decoder(tp, context)

        async def resolve(data: bytes) -> Decoder:
            self.decodes += 1
            return decode

        return resolve

    def encoder(self, tp: object, context: SerdeContext) -> Encoder:
        return JSON.encoder(tp, context)


@pytest.mark.parametrize("serde", [CountingJson, CountingRegistry])
async def test_a_call_decodes_each_part_once_for_all_its_parameters(
    serde: type[CountingJson | CountingRegistry],
) -> None:
    """Parameters that take a part alike share one decoded object, and each keeps its own null handling."""
    counting = serde()
    counted = Topic("counted", Order, serde=counting)
    router = Router()
    seen = []
    tenants = []

    def tenant_of(data: bytes) -> str:
        tenants.append(data)
        return data.decode()

    def sku(order: Order | None, tenant: Annotated[str, Header("x-tenant", decoder=tenant_of)] = "none") -> str:
        return "none" if order is None else f"{tenant}:{order.sku}"

    def same(order: Order | None = None) -> Order | None:
        return order

    @router.handler(counted)
    async def handle(
        order: Order | None,
        sku: Annotated[str, Depends(sku)],
        same: Annotated[Order | None, Depends(same)],
        tenant: Annotated[str, Header("x-tenant", decoder=tenant_of)] = "none",
        text: Annotated[str, Header("x-tenant")] = "none",  # Decoded by Text, not tenant_of, so on its own.
    ) -> None:
        seen.append((order, sku, same is order, tenant, text))

    runtime = runtime_of(router)
    record = counted(Order(sku="A"), key=None, headers=[("x-tenant", "acme")])
    await handle_one(runtime, record)
    await handle_one(runtime, record)
    assert seen == [(Order(sku="A"), "acme:A", True, "acme", "acme")] * 2
    assert (counting.decodes, len(tenants)) == (2, 2)  # Once per call.
    seen.clear()
    await handle_one(runtime, ProducerRecord("counted"))  # A tombstone, without the header.
    assert seen == [(None, "none", True, "none", "none")]


async def test_a_part_that_fails_to_decode_names_the_parameter_that_decoded_it_first() -> None:
    router = Router()

    def sku(placed: Order) -> str:
        return placed.sku

    @router.handler(orders)
    async def handle(sku: Annotated[str, Depends(sku)], order: Order) -> None: ...

    with pytest.raises(DecodeError, match="parameter 'placed' cannot decode the value") as caught:
        await handle_one(runtime_of(router), ProducerRecord("orders", value=b"not json"))
    assert caught.value.parameter == "placed"


async def test_callable_objects_are_dependencies() -> None:
    router = Router()

    @dataclass(frozen=True)
    class Pool:
        size: int

        async def __call__(self) -> AsyncIterator[int]:
            yield self.size

    @router.handler("orders")
    async def handle(
        value: bytes,
        pool: Annotated[int, Depends(Pool(3))],
        record: Annotated[ConsumerRecord, Depends(lambda: "a marker wins over the type")],
    ) -> ProducerRecord:
        return ProducerRecord("pools", value=f"{pool} {record}".encode())

    [output] = await handle_one(runtime_of(router), ProducerRecord("orders", value=b""))
    assert output.value == b"3 a marker wins over the type"


def cycle_a(b: "Annotated[None, Depends(cycle_b)]") -> None: ...


def cycle_b(a: Annotated[None, Depends(cycle_a)]) -> None: ...


async def test_registration_checks_dependencies() -> None:
    async def by_number(number: Annotated[int, Key()]) -> int:
        return number

    async def nested(number: Annotated[int, Depends(by_number)]) -> int:
        return number

    with pytest.raises(
        RegistrationError,
        match=r"handler .*handle for topic 'orders' cannot bind parameter 'number': dependency .*nested "
        r"cannot bind parameter 'number': dependency .*by_number cannot bind parameter 'number': "
        r"topic 'orders' has keys of str, not int",
    ):

        @Router().handler(orders)
        async def handle(order: Order, number: Annotated[int, Depends(nested)]) -> None: ...

    with pytest.raises(RegistrationError, match=r"dependency .*cycle_a depends on itself"):

        @Router().handler("orders")
        async def cyclic(value: bytes, a: Annotated[None, Depends(cycle_a)]) -> None: ...

    @asynccontextmanager
    async def decorated() -> AsyncIterator[None]:
        yield

    with pytest.raises(RegistrationError, match=r"dependency .*decorated is a context manager; remove its decorator"):

        @Router().handler("orders")
        async def entered(value: bytes, a: Annotated[None, Depends(decorated)]) -> None: ...

    def two_values(a: bytes, b: bytes) -> None: ...

    with pytest.raises(RegistrationError, match=r"dependency .*two_values has parameters 'a', 'b' without a marker"):

        @Router().handler("orders")
        async def valued(value: bytes, a: Annotated[None, Depends(two_values)]) -> None: ...

    with pytest.raises(RegistrationError, match=r"more than one of Key\(\), Header\(\), Context\(\), and Depends\(\)"):

        @Router().handler("orders")
        async def two_markers(a: Annotated[str, Key(), Depends(str)]) -> None: ...

    class Entered:
        @contextmanager
        def __call__(self) -> Iterator[None]:
            yield

    with pytest.raises(RegistrationError, match=r"dependency .*Entered.* is a context manager"):

        @Router().handler("orders")
        async def entered_object(value: bytes, a: Annotated[None, Depends(Entered())]) -> None: ...

    @dataclass
    class Unhashable:
        def __call__(self) -> None: ...

    with pytest.raises(RegistrationError, match=r"dependency .* is not hashable, but potik keys a call's values by it"):

        @Router().handler("orders")
        async def unhashable(value: bytes, a: Annotated[None, Depends(Unhashable())]) -> None: ...

    with pytest.raises(RegistrationError, match=r"has Depends\(\) as the default of parameter 'a'; annotate it as"):

        @Router().handler("orders")
        async def default(value: bytes, a: bytes = Depends(by_number)) -> None: ...  # type: ignore[assignment]

    with pytest.raises(TypeError, match="a dependency must be callable, got int"):
        Depends(1)  # ty: ignore[invalid-argument-type]


async def test_a_dependency_that_takes_the_context_requires_one() -> None:
    def settings(services: Annotated[dict, Context()]) -> dict:
        return services

    def database(settings: Annotated[dict, Depends(settings)]) -> object:
        return settings["db"]

    router = Router()

    @router.handler("orders")
    async def handle(value: bytes, db: Annotated[object, Depends(database)]) -> None: ...

    with pytest.raises(
        RegistrationError,
        match=r"handle takes Context\(\) through dependency .*settings, so requires build_runtime\(context=...\)",
    ):
        runtime_of(router)


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
