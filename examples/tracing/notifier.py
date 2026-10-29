"""The second processor. It notifies customers of confirmed orders and alerts support to dead letters.

Orders above 100 also get an SMS, so their traces fan out into two notifications.
"""

import random
from typing import Annotated

import anyio
from common import BOOTSTRAP, KEYED, Notification, Order, confirmed, dead_letters, notifications, probe_server, setup
from opentelemetry import trace
from opentelemetry.trace import SpanKind

import potik
from potik import Config, Key, ProducerRecord, Router, resume_or_earliest
from potik.middleware.tracing import TracingMiddleware

GROUP = "notifier"
tracer = trace.get_tracer("notifier")
router = Router()


@router.handler(confirmed)
async def notify_customer(order: Order, customer: Annotated[str, Key()]) -> list[ProducerRecord]:
    trace.get_current_span().set_attribute("order.id", order.id)
    with tracer.start_as_current_span("render template"):
        await anyio.sleep(random.uniform(0.002, 0.01))
    channels = ["email", "sms"] if order.amount > 100 else ["email"]
    return [notifications(Notification(channel=channel, order=order.id), key=customer) for channel in channels]


@router.handler(dead_letters)
async def alert_support(order: Order) -> None:
    trace.get_current_span().set_attribute("order.id", order.id)
    with tracer.start_as_current_span(
        "POST /tickets", kind=SpanKind.CLIENT, attributes={"http.request.method": "POST", "server.address": "helpdesk"}
    ):
        await anyio.sleep(random.uniform(0.02, 0.05))


async def main() -> None:
    """Run until SIGTERM or SIGINT, and serve the run's probes meanwhile."""
    runtime = potik.build_runtime(
        Config(bootstrap_servers=BOOTSTRAP, group_id=GROUP, offset_policy=resume_or_earliest),
        [router],
        middleware=[TracingMiddleware(optional_attributes=KEYED)],
    )
    server = probe_server(lambda: runtime, 8082)
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(server.serve)
        try:
            await runtime.run()
        finally:
            server.should_exit = True


setup(GROUP)
anyio.run(main)
