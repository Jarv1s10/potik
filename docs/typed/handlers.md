# Handlers

A handler is an `async def` function that handles one record of its topic. `@router.handler(topic)` registers it and returns the function unchanged.

The examples on this page are excerpts. Models such as `Order` and `Payment` stand for your own pydantic models, and topics such as `orders` and `audit_log` for `Topic`s declared as in [Topics and serdes](topics.md).

## Parameters

Each parameter of a handler takes one part of the input record:

```python
from datetime import datetime
from typing import Annotated

from potik import ConsumerRecord, Context, Header, Key


@router.handler(orders)
async def audit(
    order: Order,                                         # the value
    customer: Annotated[str, Key()],                      # the key
    sent: Annotated[datetime, Header("sent-at", decoder=parse_time)],
    record: ConsumerRecord,                               # the raw record
    services: Annotated[Services, Context()],             # the run's context
    tenant: Annotated[str, Header("x-tenant")] = "none",  # a header, with a default
) -> None: ...
```

The one parameter without a marker takes the value. Its type must be the topic's value type, or `bytes` for the raw value. A `Key()` parameter takes the topic's key type, or `bytes`. A mismatch raises `RegistrationError` when the handler is registered, before the app starts. A `Context()` parameter takes the run's [context](index.md#context).

A missing or null part takes the parameter's default, or `None` if the type allows it. A part that doesn't decode raises `DecodeError` from the handler call. Exception rules handle it like any other handler error.

A tombstone is a record with a null value, so it raises `DecodeError` unless the value parameter allows `None`. To handle deletes on a compacted topic, make the parameter optional:

```python
@router.handler(orders)
async def sync(order: Order | None, customer: Annotated[str, Key()]) -> None:
    if order is None:
        ...  # The customer's order was deleted.
```

## Outputs

A handler returns `None`, one `ProducerRecord`, or a list of them. The runtime commits them with the input record's offset, so downstream consumers see them exactly once (see [Exactly-once outputs](../runtime/index.md#exactly-once-outputs)).

Calling a topic makes a record and encodes it on the spot, so an encoding error points at the line that made the record:

```python
@router.handler(orders)
async def split(order: Order, customer: Annotated[str, Key()]) -> list[ProducerRecord]:
    return [
        audit_log(AuditEvent.of(order), key=customer),
        by_number(order, key=order.number),
    ]
```

Every call names its key. Pass `key=None` for a null key, and `None` as the value for a tombstone. To keep a customer's records in one partition downstream, take the input key with `Key()` and pass it on.

`output_topics=` lists the topics that a handler writes to. The app then checks that these topics exist before it starts, and type checkers check the handler's return type against them:

```python
@router.handler(orders, output_topics=[audit_log, by_number])
async def split(
    order: Order, customer: Annotated[str, Key()]
) -> list[ProducerRecord[AuditEvent] | ProducerRecord[Order]]: ...
```

## Failures

A handler that raises stops its record's partition, as described in [Failures](../runtime/index.md#failures). To retry it, skip the record, or write it to a dead letter topic, add [exception rules](../middleware/exceptions.md).

## Side effects

A handler may run more than once for the same record, after a failure, a rebalance, or a crash. Only its returned outputs commit exactly once. Make its other side effects idempotent, such as database writes, HTTP calls, and records sent through a [`Producer`](../producer.md).
