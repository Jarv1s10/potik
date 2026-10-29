"""The examples must keep working, run as their READMEs say, on a broker of their own."""

import json
import signal
import socket
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from harness import read_committed

pytestmark = pytest.mark.slow

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
TRACING = EXAMPLES / "tracing"
RPC = EXAMPLES / "rpc"


@pytest.fixture
def bootstrap(broker):
    """Return the test's own broker, because the examples' topics and groups have fixed names."""
    return broker.bootstrap


def test_tracing_orders_settle_exactly_once(bootstrap, processes) -> None:
    """Every order is confirmed and notified, or dead-lettered, once, through fulfillment's crash and restart."""
    env = {"KAFKA_BOOTSTRAP": bootstrap, "OTEL_TRACES_EXPORTER": "none"}
    ports = {"fulfillment": free_port(), "notifier": free_port()}
    fulfillment = processes(
        "fulfillment",
        [sys.executable, str(TRACING / "fulfillment.py")],
        {**env, "HEALTH_PORT": str(ports["fulfillment"])},
    )
    notifier = processes(
        "notifier", [sys.executable, str(TRACING / "notifier.py")], {**env, "HEALTH_PORT": str(ports["notifier"])}
    )
    checkout = processes("checkout", [sys.executable, str(TRACING / "checkout.py"), "21", "0"], env)
    assert checkout.process.wait(60) == 0, f"checkout failed, see {checkout.log}"

    orders = {order["id"]: order["sku"] for order in values(bootstrap, "orders")}
    sold_out = {order for order, sku in orders.items() if sku == "SKU-SOLD-OUT"}
    assert len(orders) == 21 and len(sold_out) == 3
    deadline = time.monotonic() + 60
    while True:
        confirmed = Counter(order["id"] for order in values(bootstrap, "orders.confirmed"))
        dead = Counter(order["id"] for order in values(bootstrap, "orders.dlq"))
        notified = Counter(notification["order"] for notification in values(bootstrap, "notifications"))
        if confirmed.keys() | dead.keys() == orders.keys() and notified.keys() == confirmed.keys():
            break
        assert fulfillment.alive() and notifier.alive(), f"a service died, see {fulfillment.log} and {notifier.log}"
        assert time.monotonic() < deadline, f"orders did not settle, see {fulfillment.log} and {notifier.log}"
        time.sleep(0.5)

    assert set(dead) == sold_out
    assert max(confirmed.values()) == max(dead.values()) == 1, "an order was settled twice"
    assert "Run failed, restarting" in fulfillment.log.read_text(), "fulfillment's run did not crash"
    for port in ports.values():  # The restarted run serves the probes again.
        assert wait_ok(f"http://localhost:{port}/readyz", timeout=30) == b"ok\n"
    for service in (fulfillment, notifier):
        assert service.stop() == 0, f"graceful stop failed, see {service.log}"


def test_rpc_each_api_replica_answers_its_own_requests(bootstrap, processes) -> None:
    """Concurrent requests to two API replicas each get the updater's status for their own record."""
    env = {"KAFKA_BOOTSTRAP": bootstrap}
    updater = processes("updater", [sys.executable, str(RPC / "updater.py")], env)
    wait_logged(updater, "Assigned", timeout=60)  # It also creates the topics.
    ports = [free_port(), free_port()]
    apis = [
        processes(
            f"api-{port}", [sys.executable, "-m", "uvicorn", "api:app", "--app-dir", str(RPC), "--port", str(port)], env
        )
        for port in ports
    ]
    for port in ports:
        assert wait_ok(f"http://localhost:{port}/livez", timeout=60) == b"ok\n"

    asked = [(ports[n % 2], f"opp-{n}") for n in range(20)]
    with ThreadPoolExecutor(len(asked)) as pool:
        answers = list(pool.map(lambda request: post(*request), asked))
    for (_, record_id), (code, status) in zip(asked, answers, strict=True):
        assert (code, status["record_id"]) == (200 if status["ok"] else 422, record_id)
    for api in apis:  # uvicorn shuts down, then raises the SIGTERM again.
        assert api.stop() == -signal.SIGTERM and "Application shutdown complete" in api.log.read_text(), api.log
    assert updater.stop() == 0, f"graceful stop failed, see {updater.log}"


def post(port: int, record_id: str) -> tuple[int, dict]:
    """POST an update of `record_id` to the API on `port`, and return its status code and JSON body."""
    request = urllib.request.Request(
        f"http://localhost:{port}/records/{record_id}",
        data=b'{"Stage": "Won"}',
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def wait_logged(process, text: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while text not in process.log.read_text():
        assert process.alive(), f"the process died, see {process.log}"
        assert time.monotonic() < deadline, f"{text!r} never logged, see {process.log}"
        time.sleep(0.2)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("localhost", 0))
        return sock.getsockname()[1]


def wait_ok(url: str, timeout: float) -> bytes:
    """Return the body of `url` once it answers 200, retrying until `timeout` passes."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                return response.read()
        except OSError:  # Refused, or an error status, e.g. a probe's 503.
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


def values(bootstrap: str, topic: str) -> list[dict]:
    return [json.loads(message.value() or b"") for message in read_committed(bootstrap, topic, f"verify-{topic}")]
