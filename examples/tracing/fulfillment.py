"""The first processor. It reserves stock for each order, charges it, and confirms it.

Payment times out at random, and the middleware retries it. Out-of-stock orders go to a dead letter topic. The first order of a new item crashes the run, and the restarted run replays what it had not committed. Run several instances to share the partitions of `orders`.
"""

import logging
import random

import anyio
from common import BOOTSTRAP, KEYED, Order, confirmed, dead_letters, orders, probe_server, setup
from opentelemetry import trace
from opentelemetry.trace import SpanKind

import potik
from potik import Config, ProducerRecord, Router, Runtime, resume_or_earliest
from potik.middleware import ExponentialBackoff, dead_letter, on, retry
from potik.middleware.tracing import TracingMiddleware

GROUP = "fulfillment"
logger = logging.getLogger(GROUP)
tracer = trace.get_tracer("fulfillment")
router = Router()


class OutOfStockError(Exception):
    pass


class PaymentTimeoutError(Exception):
    pass


class CatalogBugError(Exception):
    pass


crashed = False


@router.handler(orders)
async def fulfill(order: Order) -> ProducerRecord:
    global crashed
    trace.get_current_span().set_attribute("order.id", order.id)
    if order.sku == "SKU-NEW" and not crashed:
        crashed = True
        raise CatalogBugError(f"{order.sku} is missing from the catalog cache")
    with tracer.start_as_current_span(
        "UPDATE stock", kind=SpanKind.CLIENT, attributes={"db.system.name": "postgresql", "db.operation.name": "UPDATE"}
    ):
        await anyio.sleep(random.uniform(0.005, 0.03))
        if order.sku == "SKU-SOLD-OUT":
            raise OutOfStockError(f"{order.sku} is out of stock")
    with tracer.start_as_current_span(
        "POST /charges", kind=SpanKind.CLIENT, attributes={"http.request.method": "POST", "server.address": "payments"}
    ):
        await anyio.sleep(random.uniform(0.02, 0.08))
        if random.random() < 0.3:
            raise PaymentTimeoutError("payment gateway timed out")
    return confirmed(order.model_copy(update={"status": "confirmed"}), key=order.customer)


def build() -> Runtime:
    return potik.build_runtime(
        # An unhandled error fails the whole run. At this traffic a transaction starts every 2s, so a crash leaves recently handled orders uncommitted.
        Config(
            bootstrap_servers=BOOTSTRAP,
            group_id=GROUP,
            offset_policy=resume_or_earliest,
            stop_failed_partitions=False,
            commit_interval_ms=2000,
        ),
        [router],
        middleware=[TracingMiddleware(optional_attributes=KEYED)],
        # The rules run inside the middleware, so one span covers the retries and the dead letters, and each exception is an event on it.
        exception_rules=[
            on(PaymentTimeoutError, retry(max_attempts=5, backoff=ExponentialBackoff(initial_seconds=0.05))),
            on(OutOfStockError, dead_letter(dead_letters)),
        ],
    )


async def supervise() -> None:
    """Restart failed runs with a new runtime, like a process supervisor would. Return after SIGTERM or SIGINT.

    The probes check the current run.
    """
    runtime = build()
    server = probe_server(lambda: runtime, 8081)
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(server.serve)
        try:
            while True:
                try:
                    return await runtime.run()
                except Exception:  # noqa: BLE001 -- the runtime has logged it
                    logger.warning("Run failed, restarting")
                    runtime = build()
        finally:
            server.should_exit = True


setup(GROUP)


anyio.run(supervise)
