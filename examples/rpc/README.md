# Request-reply across replicas

An API sends each CRM update to Kafka and answers its HTTP request with the updater's reply:

```
api (Starlette, N replicas)  POST /records/{id} -> crm.updates, then awaits crm.updates.status
updater (potik)               crm.updates -> crm.updates.status, copying the correlation-id header
```

Each API replica opens a `ReplyListener` in its lifespan. It reads every partition of `crm.updates.status`, without a consumer group, and hands each reply to the request in this replica that awaits its correlation ID. The API enters `expect()` before it sends a request, so that a fast reply cannot arrive first. A shared consumer group would hand each partition to one replica, so most replies would reach a replica that does not await them.

```sh
uv sync --extra pydantic
podman compose -f examples/tracing/compose.yml up -d --wait kafka   # KAFKA_PORT=19092 if port 9092 is taken

# KAFKA_BOOTSTRAP=localhost:19092 for a moved broker
uv run python examples/rpc/updater.py &
for port in 8000 8001 8002; do uv run uvicorn api:app --app-dir examples/rpc --port $port & done

for port in 8000 8001 8002 8000 8001 8002; do
  curl -s -X POST localhost:$port/records/opp-$RANDOM -H 'content-type: application/json' -d '{"Stage": "Won"}'; echo
done
kill %1 %2 %3 %4
podman compose -f examples/tracing/compose.yml down
```

Each request gets its own status: `200` with `{"ok": true}`, or `422` with `{"ok": false}` for the one update in five that the updater rejects.

## Other ways to correlate

potik leaves the correlation ID's place to the app. Here the API sends it in a `correlation-id` header, and the updater copies it to its reply. It can also travel in the records' values, or in an HTTP call that makes a service reply to Kafka. `correlate` then reads it from wherever the reply carries it:

```python
def job_id(record: ConsumerRecord) -> str | None:
    return json.loads(record.value() or b"{}").get("job_id")


listener = ReplyListener("jobs.status", bootstrap_servers=BOOTSTRAP, correlate=job_id)

with listener.expect() as pending:
    await scheduler.schedule(job_id=pending.correlation_id, fields=fields)
    with anyio.fail_after(30):
        status = await pending.wait()
```
