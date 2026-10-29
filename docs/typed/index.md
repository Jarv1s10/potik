# Typed API

The typed API at the package root, `potik`, builds a core `Runtime` from typed handlers. You use three names from it:

- A [`Topic`](topics.md) declares a topic's name, the types of its values and keys, and their serdes.
- A `Router` registers a [handler](handlers.md) for each topic. Each parameter of the handler takes one decoded part of the record.
- `potik.build_runtime()` builds a runtime from routers, a context, and middleware.

```python
import potik
from potik import Config, Key, ProducerRecord, Router, Topic

router = Router()


@router.handler(orders)
async def accept(order: Order, customer: Annotated[str, Key()]) -> ProducerRecord:
    return accepted(Accepted(sku=order.sku, total=order.amount * 1.2), key=customer)


runtime = potik.build_runtime(Config(bootstrap_servers="localhost:9092", group_id="acceptor"), [router])
```

## Routers

A router holds the handlers of a group of topics. A module can define one at import, because a router needs no runtime. A service with handlers in several modules passes all their routers to `potik.build_runtime()`.

Each topic has one handler. Registering a second handler for a topic, in the same router or in another router of the runtime, raises `RegistrationError`.

## Building a runtime

`potik.build_runtime()` takes the config and the routers, and these optional arguments:

| Argument | Effect |
| --- | --- |
| `context` | What handlers take as `Context()` parameters (see [Context](#context)) |
| `middleware` | Middleware instances, outermost first (see [Middleware](../middleware/index.md)) |
| `exception_rules` | Rules that retry failed handlers or write dead letters (see [Exception rules](../middleware/exceptions.md)) |
| `max_concurrency` | The maximum number of handler calls at once (see [Concurrency limits](../middleware/concurrency.md)) |
| `check_topics` | Whether the run first checks that its topics exist, `True` by default (see [Topic check](../middleware/topic-check.md)) |

A runtime runs once, so build each run's runtime in a function. Tests then call the same function with a test cluster's config:

```python
def build(config: Config) -> Runtime:
    return potik.build_runtime(config, [router], exception_rules=[on(Exception, dead_letter(dead_letters))])
```

## Context

Handlers get the resources they need, such as a database pool or a `Producer`, from the run's context. Open the resources before you build the runtime, and pass them as `context`. A handler takes the context with a `Context()` parameter:

```python
from dataclasses import dataclass

from potik import Context, Producer


@dataclass
class Services:
    db: Pool  # your database pool, e.g. from asyncpg
    producer: Producer


@router.handler(orders)
async def save(order: Order, services: Annotated[Services, Context()]) -> None:
    await services.db.execute("INSERT INTO orders VALUES ($1, $2)", order.sku, order.amount)


def build(config: Config, services: Services) -> Runtime:
    return potik.build_runtime(config, [router], context=services)


async def main() -> None:
    async with create_pool() as db, Producer.from_config(CONFIG) as producer:
        await build(CONFIG, Services(db, producer)).run()


anyio.run(main)
```

potik doesn't check the context against the parameter's type. `potik.build_runtime()` raises `RegistrationError` if a handler takes `Context()` and you pass no context.

Pass middleware its resources as arguments, not through the context. [Production](../runtime/production.md#resources) covers how resources close when the run stops.

## What the typed API adds to the core

A core [`Runtime`](../runtime/core.md) takes one function that receives raw records. `potik.build_runtime()` builds that function and the runtime's middleware for you. It adds:

- a handler per topic, registered on a `Router`;
- typed handler parameters, decoded by each `Topic`'s serdes;
- type-checked outputs, encoded by the output topic's serdes;
- a context that handlers take as `Context()`;
- exception rules, concurrency limits, and a topic check that covers every declared output topic.
