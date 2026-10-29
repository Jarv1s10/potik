"""With static membership, an instance's transactional.id outlives its runs."""

import signal
import time

import pytest
from confluent_kafka import IsolationLevel, TopicPartition
from confluent_kafka.admin import OffsetSpec
from harness import SESSION_TIMEOUT_MS

pytestmark = pytest.mark.slow

# Long enough that waiting it out would fail the test.
TRANSACTION_TIMEOUT_MS = 300_000


def undecided(work) -> int:
    """Return the output records past the last stable offset, which are in transactions still open."""
    request = {TopicPartition(work.target, p): OffsetSpec.latest() for p in range(work.partitions)}

    def latest(isolation: IsolationLevel) -> int:
        return sum(f.result().offset for f in work.admin.list_offsets(request, isolation_level=isolation).values())

    return latest(IsolationLevel.READ_UNCOMMITTED) - latest(IsolationLevel.READ_COMMITTED)


def supervise(work, apps, instance, deadline: float, **config):
    """Restart the instance whenever its run fails, like a supervisor, until the input is committed, and return it."""
    expected = [work.records // work.partitions] * work.partitions
    while work.wait_committed(2, expected) != expected:
        assert time.monotonic() < deadline, f"the input was not committed, see {instance.log}"
        if not instance.alive():
            instance = apps(work, **config)
    assert instance.stop() == 0, f"graceful stop failed, see {instance.log}"
    return instance


def test_restart_aborts_the_crashed_runs_transaction(workload, apps) -> None:
    """A restarted instance aborts the transaction its SIGKILLed run left open, without waiting out its timeout."""
    work = workload(2, 2_000)
    config = {"instance_id": "i0", "transaction_timeout_ms": TRANSACTION_TIMEOUT_MS}
    crashed = apps(work, crash_mid_transaction=True, **config)
    assert crashed.process.wait(60) == -signal.SIGKILL, f"no crash mid-transaction, see {crashed.log}"
    crashed_at = time.monotonic()
    assert undecided(work) > 0, "the crash left no transaction open"

    instance = apps(work, **config)
    while undecided(work):
        assert time.monotonic() - crashed_at < 30, f"the open transaction was not aborted, see {instance.log}"
        time.sleep(0.2)
    # The group rejects the instance until the crashed run's session times out.
    supervise(work, apps, instance, crashed_at + SESSION_TIMEOUT_MS / 1000 + 60, **config)
    work.assert_exactly_once()


def test_duplicate_instance_fences_the_live_one(workload, apps) -> None:
    """A second live process of an instance fences the first one's producer, and nothing is committed twice.

    The group rejects the duplicate (`UNRELEASED_INSTANCE_ID`) unless the fenced process left it first, in which case the duplicate takes over.
    """
    work = workload(2, 2_000)
    live = apps(work, sleep_ms=10, instance_id="i0")
    work.wait_progress(live)

    duplicate = apps(work, sleep_ms=10, instance_id="i0")
    assert live.process.wait(60) != 0, f"the fenced instance kept running, see {live.log}"
    assert "_FENCED" in live.log.read_text(), f"the instance failed for another reason, see {live.log}"
    supervise(work, apps, duplicate, time.monotonic() + SESSION_TIMEOUT_MS / 1000 + 60, instance_id="i0")
    work.assert_exactly_once()
