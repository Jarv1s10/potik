"""The public API's types as users see them. ty, mypy, and pyrefly check this module, and nothing runs it.

Each call that type checkers must reject has one ignore comment per checker, with that checker's error code: `type: ignore` for mypy, `ty: ignore`, and `pyrefly: ignore`. Each checker fails on an unused ignore comment of its own, so a call that stops being rejected fails the check.
"""

from typing import assert_type

from pydantic import BaseModel

import potik
from potik import Config, Delivery, Producer, ProducerRecord, Router, Runtime, Topic
from potik.middleware import ExceptionContext, TopicCheckMiddleware, dead_letter, on, skip
from potik.serde import PydanticJson


class Order(BaseModel):
    sku: str


class Accepted(BaseModel):
    sku: str


class Rejected(BaseModel):
    reason: str


router = Router()
json = PydanticJson()
orders = Topic("orders", Order, serde=json)
accepted = Topic("accepted", Accepted, serde=json)
rejected = Topic("rejected", Rejected, serde=json)
by_number = Topic("by_number", Order, key_type=int, serde=json)
assert_type(orders, Topic[Order])
assert_type(orders, Topic[Order, str])
assert_type(Topic("events", Order | Accepted, serde=json), Topic[Order | Accepted])
assert_type(by_number, Topic[Order, int])


async def decoded() -> None:
    assert_type(await orders.decode(b"{}"), Order)
    assert_type(await by_number.decode_key(b"1"), int)


assert_type(accepted(Accepted(sku="A"), key="c1"), ProducerRecord[Accepted])
accepted(Accepted(sku="A"), key=None)
by_number(Order(sku="A"), key=1)
accepted(Accepted(sku="A"))  # type: ignore[call-arg]  # ty: ignore[missing-argument]  # pyrefly: ignore[missing-argument]
accepted(Order(sku="A"), key=None)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]  # pyrefly: ignore[bad-argument-type]
accepted(Accepted(sku="A"), key=1)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]  # pyrefly: ignore[bad-argument-type]
by_number(Order(sku="A"), key="1")  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]  # pyrefly: ignore[bad-argument-type]


@router.handler(orders)
async def single(order: Order) -> ProducerRecord:
    return accepted(Accepted(sku=order.sku), key=None)


@router.handler(orders)
async def several(order: Order) -> list[ProducerRecord]:
    return []


async def returns() -> None:
    """The decorator keeps the handlers' exact types, so tests can call them as plain functions."""
    assert_type(await single(Order(sku="A")), ProducerRecord)
    assert_type(await several(Order(sku="A")), list[ProducerRecord])


@router.handler(orders)  # type: ignore[type-var]  # ty: ignore[invalid-argument-type]  # pyrefly: ignore[bad-specialization]
async def wrong_explicit(order: Order) -> dict:
    return {}


async def sends(producer: Producer) -> None:
    """A send takes the record that calling a topic makes, and returns where the broker appended it."""
    assert_type(await producer.send(accepted(Accepted(sku="A"), key="c1")), Delivery)
    await producer.send(Accepted(sku="A"))  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]  # pyrefly: ignore[bad-argument-type]


@router.handler("declared", output_topics=(accepted,))
async def declared(order: Order) -> ProducerRecord[Accepted]:
    return accepted(Accepted(sku=order.sku), key=None)


@router.handler("declared_several", output_topics=(accepted, rejected))
async def declared_several(order: Order) -> list[ProducerRecord[Accepted] | ProducerRecord[Rejected]]:
    return [accepted(Accepted(sku=order.sku), key=None), rejected(Rejected(reason="r"), key=None)]


@router.handler("declared_name", output_topics=(accepted, "raw"))
async def declared_name(order: Order) -> ProducerRecord[bytes]:
    """A plain name is a topic of bytes values."""
    return Topic("raw", bytes)(b"v", key=None)


@router.handler("undeclared", output_topics=(accepted,))  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]  # pyrefly: ignore[bad-argument-type]
async def undeclared(order: Order) -> ProducerRecord[Rejected]:
    return rejected(Rejected(reason="r"), key=None)


@router.handler("undeclared_several", output_topics=(accepted,))  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]  # pyrefly: ignore[bad-argument-type]
async def undeclared_several(order: Order) -> list[ProducerRecord[Accepted] | ProducerRecord[Rejected]]:
    return []


async def declared_returns() -> None:
    """A handler with outputs keeps its parameters, and its return type as the outputs' records."""
    await declared(Order(sku="A"))
    await declared("A")  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]  # pyrefly: ignore[bad-argument-type]


def rule(context: ExceptionContext[Exception]) -> ProducerRecord:
    return ProducerRecord("dead-letters", None, context.record.value())


dead_letter("dead-letters")
dead_letter(accepted)
dead_letter(rule)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]  # pyrefly: ignore[bad-argument-type]
Producer("localhost:9092", middleware=[TopicCheckMiddleware((accepted, "raw"))])
Producer("localhost:9092", middleware=[TopicCheckMiddleware])  # type: ignore[list-item]  # ty: ignore[invalid-argument-type]  # pyrefly: ignore[bad-argument-type]
on(KeyError, skip(), topics=[orders, "raw"])
config = Config(bootstrap_servers="localhost:9092", group_id="typing")
assert_type(
    potik.build_runtime(config, [router], context=object(), middleware=[TopicCheckMiddleware(["raw"])]), Runtime
)
