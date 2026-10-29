# Production

potik apps run as several instances in containers, for example as pods on Kubernetes. This page covers each instance's resources, shutdown, health checks, and restarts.

## Resources

Open the resources that handlers and middleware need, such as a database pool, before you build the runtime. Close them after `run()` returns:

```python
async def main() -> None:
    async with create_pool() as db:
        await build(CONFIG, db).run()


anyio.run(main)
```

Here `build()` builds the service's runtime, as in the [quickstart](../quickstart.md#write-the-app), and passes `db` on to the handlers as the run's [context](../typed/index.md#context).

On SIGTERM, `run()` returns once the runtime has stopped, and the resources close as usual. A cancelled `run()`, for example in a test, raises the cancellation, so the resources close in a cancelled scope. Shield any awaits in their cleanup with `anyio.CancelScope(shield=True)`.

## Shutdown

On SIGTERM, `run()` shuts down gracefully, as described in [Runs](index.md#runs).

Kubernetes kills a pod `terminationGracePeriodSeconds` after SIGTERM, 30 seconds by default. Keep `shutdown_grace_ms` well below that, so the final commit has time to finish. A `Producer` then waits up to its `close_timeout`, 10 seconds by default, for its last sends, and fails those still pending.

Only one run in a process can handle signals. To run several runtimes at once, for example for two consumer groups, pass `handle_signals=False` to each and cancel them on a signal yourself:

```python
import signal
from functools import partial

import anyio


async def main() -> None:
    with anyio.open_signal_receiver(signal.SIGTERM, signal.SIGINT) as signals:
        async with anyio.create_task_group() as tasks:
            for runtime in (build_orders(CONFIG), build_payments(CONFIG)):
                tasks.start_soon(partial(runtime.run, handle_signals=False))
            async for _ in signals:
                tasks.cancel_scope.cancel()  # Each run shuts down as on SIGTERM.
                break
```

## Health checks

`runtime.liveness()` and `runtime.readiness()` check the run. Each returns the reason it fails, or `None` if it passes. Serve them from an HTTP framework, next to checks of your own, such as a database ping:

```python
from fastapi import FastAPI, Response

from potik import Runtime


def probes(runtime: Runtime, db: Pool) -> FastAPI:  # ping(db) stands for a query such as SELECT 1
    api = FastAPI()

    @api.get("/livez")
    async def livez() -> Response:
        return probe(runtime.liveness())

    @api.get("/readyz")
    async def readyz() -> Response:
        return probe(runtime.readiness() or await ping(db))

    return api


def probe(reason: str | None) -> Response:
    return Response("ok\n") if reason is None else Response(f"{reason}\n", 503)
```

Run the server as a task next to the runtime. The runtime then still handles SIGTERM, and a failed run stops the server, so the process exits and Kubernetes restarts it:

```python
import anyio
import uvicorn


async def main() -> None:
    async with create_pool() as db:
        runtime = build(CONFIG, db)
        server = uvicorn.Server(uvicorn.Config(probes(runtime, db), host="0.0.0.0", port=8080))
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(server.serve)
            try:
                await runtime.run()
            finally:
                server.should_exit = True
```

`liveness()` fails only when a restart would help, such as after the run failed, or when it stopped polling or committing. A broker outage alone doesn't fail it. `readiness()` passes only while the run is running.

Check dependencies such as the database only in the readiness probe. In the liveness probe, a database outage would restart every replica. If the process also opens a `Producer` or a `ReplyListener`, check its `failure` in the liveness probe.

Neither probe fails when a handler failure or a fetch error stops a partition. A restart would retry the same record, which usually fails again, and stop every other partition in the meantime. Alert on stopped partitions instead. [`MetricsMiddleware`](../middleware/metrics.md) counts them in `potik.partition.stopped`, and `runtime.status()` lists each partition's `failure`.

## Faster restarts

Give each instance a stable `group.instance.id`, for example the pod name in a StatefulSet:

```python
Config(..., consumer_options={"group.instance.id": os.environ["POD_NAME"]})
```

A restarted instance then keeps its partitions if it rejoins within the group's session timeout. It also aborts any transaction that its previous run left open, instead of waiting for the transaction to time out.
