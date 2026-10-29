# Testing

potik supports two kinds of tests. A unit test calls a handler directly. An app test runs the whole runtime against a `MockCluster`.

## Calling a handler

`@router.handler()` returns the handler unchanged, so a test can call it with plain objects in place of the record's parts and the run's context. It then checks the records that the handler returns. This test calls the quickstart's `accept`:

```python
@pytest.mark.anyio
async def test_accept():
    record = await accept(Order(sku="A", amount=10), "c1")
    assert record == accepted(Accepted(sku="A", total=12.0), key="c1")
```

A plain call skips the middleware, the exception rules, and the commit.

## Running the app

`potik.testing.MockCluster` runs a Kafka cluster inside the test process, so a test runs the whole app without a broker. `cluster.config(config)` points a config at the cluster, and `cluster.wait_records()` waits for the records that the app commits.

The [quickstart](../quickstart.md#test-it) shows a complete test. It creates the topics, sends an input with a `Producer`, runs the app in a task group, and waits for the output:

```python
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
```

Pass `handle_signals=False` in tests, and cancel the run to shut it down gracefully.

A handler that sends through a `Producer` in its context needs a real producer to the cluster. The test then reads the sends with `wait_records()`, like the outputs:

```python
outputs = await cluster.wait_records(receipts, 1)
sends = await cluster.wait_records(audit_log, 1)
```

The cluster doesn't store the offsets that the app commits, so a test can't check them, or what a restarted app resumes from.
