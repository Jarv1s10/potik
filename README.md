# potik

[![PyPI](https://img.shields.io/pypi/v/potik)](https://pypi.org/project/potik/) [![Python](https://img.shields.io/pypi/pyversions/potik)](https://pypi.org/project/potik/) [![License](https://img.shields.io/pypi/l/potik)](https://github.com/Jarv1s10/potik/blob/main/LICENSE)

potik is an async Python framework for Kafka services that read records, process them, and write new ones. A handler returns its output records, and potik commits them in one Kafka transaction with the input's offset. Consumers downstream see each output exactly once, even after a crash or a rebalance.

> [!WARNING]
> potik is alpha. Its API can change between minor versions until 1.0.

## Example

```python
from typing import Annotated

import anyio
from pydantic import BaseModel

import potik
from potik import Config, Key, ProducerRecord, Router, Topic
from potik.serde import PydanticJson


class Order(BaseModel):
    sku: str
    amount: float


class Accepted(BaseModel):
    sku: str
    total: float


json = PydanticJson()
orders = Topic("orders", Order, serde=json)
accepted = Topic("orders.accepted", Accepted, serde=json)
router = Router()


@router.handler(orders)
async def accept(order: Order, customer: Annotated[str, Key()]) -> ProducerRecord:
    return accepted(Accepted(sku=order.sku, total=order.amount * 1.2), key=customer)


runtime = potik.build_runtime(Config(bootstrap_servers="localhost:9092", group_id="acceptor"), [router])
anyio.run(runtime.run)
```

## Features

- One transactional producer per app commits outputs and offsets for all partitions together (KIP-447).
- Partitions run concurrently, and records within a partition run in order, or with key concurrency, in order per key. A handler can run for minutes without dropping the app from its consumer group.
- A `Topic` declares its value and key types, and type checkers check every record a handler returns.
- A failing handler stops only its own partition. Middleware adds retries and dead letter topics.
- The app shuts down on SIGTERM after it commits finished work. Its liveness and readiness checks are ready to serve as Kubernetes probes from any HTTP framework.
- OpenTelemetry tracing follows each record through its handler, and its outputs to the transaction that commits them.

## Install

potik needs Python 3.12+ and Kafka 4.0+.

```sh
uv add "potik[pydantic]"
```

## Documentation

- [Quickstart](https://jarv1s10.github.io/potik/quickstart/)
- [Runtime](https://jarv1s10.github.io/potik/runtime/): delivery, failures, configuration, and production
- [Middleware](https://jarv1s10.github.io/potik/middleware/): retries, dead letters, concurrency limits, tracing, and metrics
- [Typed API](https://jarv1s10.github.io/potik/typed/): topics, serdes, handlers, and testing
- [Producer](https://jarv1s10.github.io/potik/producer/) and [request-reply](https://jarv1s10.github.io/potik/request-reply/)

To contribute, see [CONTRIBUTING.md](https://github.com/Jarv1s10/potik/blob/main/CONTRIBUTING.md).
