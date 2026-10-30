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

The one parameter without a marker takes the value. Its type must be the topic's value type, or `bytes` for the raw value. A `Key()` parameter takes the topic's key type, or `bytes`. A mismatch raises `RegistrationError` when the handler is registered, before the app starts. A `Context()` parameter takes the run's [context](index.md#context), and a `Depends()` parameter takes a [dependency](#dependencies)'s result.

A missing or null part takes the parameter's default, or `None` if the type allows it. A part that doesn't decode raises `DecodeError` from the handler call. Exception rules handle it like any other handler error.

A tombstone is a record with a null value, so it raises `DecodeError` unless the value parameter allows `None`. To handle deletes on a compacted topic, make the parameter optional:

```python
@router.handler(orders)
async def sync(order: Order | None, customer: Annotated[str, Key()]) -> None:
    if order is None:
        ...  # The customer's order was deleted.
```

## Dependencies

A dependency is a function whose result a handler takes, such as the customer that a record's key names, or a database transaction for each record. It can be a function, sync or async, a class, or a callable object, and its parameters take the same markers as a handler's. A handler takes its result with `Depends()`:

```python
from typing import Annotated

from potik import Context, Depends, Header, Key


def tenant(name: Annotated[str, Header("x-tenant")]) -> str:
    return name.lower()


async def customer(
    key: Annotated[str, Key()],
    tenant: Annotated[str, Depends(tenant)],
    services: Annotated[Services, Context()],
) -> Customer:
    return await services.customers.get(tenant, key)


@router.handler(orders)
async def accept(order: Order, customer: Annotated[Customer, Depends(customer)]) -> ProducerRecord: ...
```

Registration checks a dependency's parameters against the topic, like the handler's. A call decodes each part of the record once, so a dependency that takes the value gets the same object as the handler. A sync dependency runs on the event loop, so it must not block.

Each handler call resolves its dependencies anew, one at a time, in parameter order. A dependency that several parameters take runs once per call. A retry by an [exception rule](../middleware/exceptions.md) resolves them again. To fetch several things at once, write one dependency that fetches them in a task group.

A dependency that yields gives the handler the value that it yields, and runs the rest of its body after the handler returns or raises. If the handler raised, `yield` raises the exception in the dependency, so that it can roll back. The dependency can't suppress the exception, so the exception rules still handle it. If the code after `yield` raises, the record fails as if the handler had raised.

A revocation or shutdown cancels the call, and the code after `yield` then runs cancelled. Shield the awaits that must finish, with a timeout:

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
```

A dependency can also limit how long each call runs. Around `yield`, `anyio.fail_after(10)` fails a call that runs longer, so exception rules handle it. `anyio.move_on_after(10)` ends the call without outputs, and its record commits, so it is neither retried nor dead-lettered. The deadline also covers the code after `yield` of the dependencies that the handler takes after it, e.g. a transaction's commit.

The commit after `yield` runs before Kafka commits the handler's outputs, so like the handler's other [side effects](#side-effects), it can happen more than once.

In a unit test, pass the dependency's result to the handler directly. In an app test, pass fakes through the run's [context](index.md#context), which dependencies take as `Context()`.

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
