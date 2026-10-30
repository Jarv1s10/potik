"""A running app must survive broker outages shorter than its transactions, and fail fast on longer ones, then finish exactly once."""

import subprocess
import time

import pytest

pytestmark = pytest.mark.slow


@pytest.fixture
def bootstrap(broker):
    """Return the test's own broker, for its workload."""
    return broker.bootstrap


@pytest.mark.parametrize("outage", ["pause", "restart"])
def test_app_survives_broker_outage(workload, apps, broker, outage) -> None:
    work = workload(4, 4_000)
    # The open transaction must outlast the outage. A restarted broker can take longer than the default 10s to serve again.
    app = apps(work, sleep_ms=5, transaction_timeout_ms=60_000)
    work.wait_progress(app)
    if outage == "pause":  # The paused broker does not respond, so requests time out.
        broker.pause()
        time.sleep(6)
        broker.unpause()
    else:  # The stopped broker resets connections, then refuses them.
        broker.stop()
        time.sleep(3)
        broker.start()
    assert app.alive(), f"the app died during the outage, see {app.log}"
    work.wait_committed(timeout=120)
    assert app.stop() == 0, f"graceful stop failed, see {app.log}"
    work.assert_exactly_once()


def test_outage_longer_than_a_transaction_fails_the_run(workload, apps, broker) -> None:
    """The open transaction times out, and so does its abort, so the run fails instead of waiting for the broker."""
    work = workload(4, 4_000)
    app = apps(work, sleep_ms=20, transaction_timeout_ms=2_000)
    work.wait_progress(app)
    broker.stop()
    try:
        assert app.process.wait(30) != 0, f"the run did not fail, see {app.log}"
    except subprocess.TimeoutExpired:
        raise AssertionError(f"the app did not exit during the outage, see {app.log}") from None
    finally:
        broker.start()
    # The failed run's member stays in the group until its session times out, then its partitions move.
    successor = apps(work)
    work.wait_committed(timeout=120)
    assert successor.stop() == 0, f"graceful stop failed, see {successor.log}"
    work.assert_exactly_once()
