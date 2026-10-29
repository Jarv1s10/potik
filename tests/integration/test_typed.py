"""Typed handlers against a real broker: parameters decoded from records, outputs encoded from objects."""

import json
from typing import Annotated

import pytest
from pydantic import BaseModel

import potik
from potik import Config, DecodeError, Key, ProducerRecord, Router, Runtime, Topic, resume_or_earliest
from potik.middleware import dead_letter, on
from potik.serde import PydanticJson

pytestmark = pytest.mark.anyio
JSON = PydanticJson()


class Order(BaseModel):
    sku: str


class Accepted(BaseModel):
    number: int
    sku: str


def typed_runtime(work, router: Router | None = None, **options) -> Runtime:
    """Run `router`, by default one that copies each order to its partition of the output topic as an `Accepted`."""
    if router is None:
        router = Router()
        accepted = Topic(work.target, Accepted, key_type=int, serde=JSON)

        @router.handler(Topic(work.source, Order, key_type=int, serde=JSON))
        async def accept(order: Order, number: Annotated[int, Key()]) -> ProducerRecord:
            return accepted(Accepted(number=number, sku=order.sku), key=number, partition=number % 2)

    config = Config(bootstrap_servers=work.bootstrap, group_id=work.group, offset_policy=resume_or_earliest)
    return potik.build_runtime(config, [router], **options)


async def test_typed_handler_decodes_inputs_and_encodes_outputs(workload) -> None:
    work = workload(2, 40, payload=b'{"sku": "A-1"}')
    await work.run_until_committed(typed_runtime(work))
    work.assert_exactly_once()
    for message in work.outputs():
        assert json.loads(message.value() or b"") == {"number": int(message.key() or b""), "sku": "A-1"}


async def test_undecodable_records_reach_exception_middleware(workload) -> None:
    """A DecodeError is a handler failure like any other, so a rule can dead-letter the record.

    One partition, because the partitioner picks dead letters' partitions, and the harness wants inputs' ones.
    """
    work = workload(1, 40, payload=b"not json")
    rules = [on(DecodeError, dead_letter(work.target))]
    await work.run_until_committed(typed_runtime(work, exception_rules=rules))
    work.assert_exactly_once()
    for message in work.outputs():
        headers = dict(message.headers() or ())
        assert message.value() == b"not json"
        assert headers["potik.dlq.source.topic"] == work.source.encode()
        assert headers["potik.dlq.exception.type"] == b"potik.handler.DecodeError"
