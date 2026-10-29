"""A running app must survive broker outages, then finish exactly once."""

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
    app = apps(work, sleep_ms=5)
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
