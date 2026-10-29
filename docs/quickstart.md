# Quickstart

This guide builds a service that reads orders from one topic and writes accepted orders to another. You need Python 3.12+ and Docker or Podman.

## Start Kafka

Run a single-node Kafka broker:

```sh
docker run -d --name kafka -p 9092:9092 apache/kafka:4.1.0
```

Create the two topics. potik doesn't create topics, and an app whose topics are missing fails at startup.

```sh
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders.accepted
```

## Install potik

Install potik with the `pydantic` extra for JSON:

```sh
uv init --python 3.14 shop && cd shop
uv add "potik[pydantic]"
```

## Write the app

Save this as `app.py`:

```python
from typing import Annotated

import anyio
from pydantic import BaseModel

import potik
from potik import Config, Key, ProducerRecord, Router, Runtime, Topic
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


CONFIG = Config(bootstrap_servers="localhost:9092", group_id="acceptor")


def build(config: Config) -> Runtime:
    return potik.build_runtime(config, [router])


if __name__ == "__main__":
    anyio.run(build(CONFIG).run)
```

A `Topic` declares a topic's name, the type of its values, and how to encode them. Declare each topic once and import it in every service that reads or writes it.

`@router.handler(orders)` registers `accept` for the `orders` topic. The parameter without a marker takes the decoded value, and the `Key()` parameter takes the record's key.

Calling a topic, as in `accepted(...)`, encodes a record for that topic. The handler returns the record instead of sending it. potik then commits it together with the order's offset, in one transaction.

`potik.build_runtime()` builds a runtime of the router's handlers, and `run()` runs it until it is stopped. A runtime runs once, so `build()` makes a new one for each run.

## Run it

Start the app:

```sh
uv run app.py
```

A new consumer group starts reading from the end of each partition, so start the app before you produce. In a second terminal, produce an order with the key `c1`:

```sh
echo 'c1:{"sku":"A","amount":10}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic orders --property parse.key=true --property key.separator=:
```

Then read the accepted order:

```sh
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.accepted --from-beginning \
  --isolation-level read_committed --property print.key=true --max-messages 1
```

```
c1	{"sku":"A","total":12.0}
```

`read_committed` makes the console consumer skip records from transactions that never committed. Every consumer of potik's outputs needs it to get exactly-once results.

Press ++ctrl+c++ to stop the app. It finishes the transaction in flight and leaves the consumer group before it exits.

## Test it

`MockCluster` runs a Kafka cluster inside the test process. A test then runs the whole app, with its middleware and transactions, without a broker. Add pytest:

```sh
uv add --dev pytest
```

Save this as `test_app.py`:

```python
from functools import partial

import anyio
import pytest

from app import CONFIG, Order, accepted, build, orders
from potik import Producer
from potik.testing import MockCluster


@pytest.mark.anyio
async def test_accept():
    with MockCluster() as cluster:
        cluster.create_topic(orders)
        cluster.create_topic(accepted)
        async with Producer(cluster.bootstrap_servers) as producer:
            await producer.send(orders(Order(sku="A", amount=10), key="c1"))
        runtime = build(cluster.config(CONFIG))
        async with anyio.create_task_group() as tasks:
            await tasks.start(partial(runtime.run, handle_signals=False))
            (record,) = await cluster.wait_records(accepted, 1)
            tasks.cancel_scope.cancel()
    assert record.key() == b"c1"
    assert record.value() == b'{"sku":"A","total":12.0}'
```

`build()` makes the service's runtime from a config, so the test builds it with `cluster.config()`, which points it at the cluster. The test sends an order, runs the app, and waits for the accepted order to commit. Cancelling the run then shuts the app down gracefully.

Run it with `uv run pytest`.

## Next steps

- [How the runtime works](runtime/index.md) covers delivery, ordering, failures, and rebalances.
- [Exception rules](middleware/exceptions.md) cover retries and dead letter topics.
- [Handlers](typed/handlers.md) cover handler parameters and outputs.
- [Production](runtime/production.md) covers shutdown and health checks on Kubernetes.
