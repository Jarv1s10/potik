"""A checkout API that sends one order per request through a potik Producer, starting each order's trace.

Usage: checkout.py [ORDERS=20] [INTERVAL_S=0.5]

Every 7th order is for an out-of-stock item, and fulfillment dead-letters it. The 10th is for a new item, which crashes fulfillment's run once.
"""

import random
import sys
from uuid import uuid4

import anyio
from common import BOOTSTRAP, KEYED, Order, orders, setup
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from potik import Producer
from potik.middleware.tracing import TracingMiddleware

setup("checkout")
tracer = trace.get_tracer("checkout")
count = int(sys.argv[1]) if len(sys.argv) > 1 else 20
interval = float(sys.argv[2]) if len(sys.argv) > 2 else 0.5


async def main() -> None:
    # The same middleware as the potik services, so each send gets a span and carries its context.
    async with Producer(BOOTSTRAP, middleware=[TracingMiddleware(optional_attributes=KEYED)]) as producer:
        for n in range(1, count + 1):
            order = Order(
                id=str(uuid4()),
                customer=f"customer-{random.randint(1, 5)}",
                sku="SKU-SOLD-OUT" if n % 7 == 0 else "SKU-NEW" if n == 10 else f"SKU-{random.randint(100, 999)}",
                amount=round(random.uniform(5, 250), 2),
            )
            with tracer.start_as_current_span(
                "POST /checkout",
                kind=SpanKind.SERVER,
                attributes={"http.request.method": "POST", "http.route": "/checkout"},
            ) as request:
                request.set_attribute("order.id", order.id)
                with tracer.start_as_current_span("validate cart"):
                    await anyio.sleep(random.uniform(0.005, 0.02))
                await producer.send(orders(order, key=order.customer))
            print(f"order {n}/{count}: {order.id} {order.sku} trace {request.get_span_context().trace_id:032x}")
            await anyio.sleep(interval)


anyio.run(main)
