"""Rolling restarts that SIGTERM or SIGKILL instances at random while a group works through its input."""

import os
import random
import time

import pytest
from harness import SESSION_TIMEOUT_MS

pytestmark = pytest.mark.slow

CHAOS_SECONDS = float(os.environ.get("POTIK_CHAOS_SECONDS", "30"))


@pytest.mark.parametrize(
    ("partitions", "records", "sleep_ms", "config"),
    [
        pytest.param(12, 60_000, 1, {}, id="fast-handlers"),
        pytest.param(
            24,
            9_600,
            50,
            {"revocation_grace_ms": 300, "shutdown_grace_ms": 1000, "commit_interval_ms": 0},
            id="slow-handlers-with-grace",
        ),
        # Handlers finish out of order within each partition, so instances die with finished records held.
        pytest.param(6, 12_000, 20, {"key_concurrency": 16, "shutdown_grace_ms": 500}, id="by-key"),
    ],
)
def test_rolling_restarts_are_exactly_once(workload, apps, partitions, records, sleep_ms, config) -> None:
    seed = int(os.environ.get("POTIK_CHAOS_SEED", random.randrange(2**32)))
    print(f"POTIK_CHAOS_SEED={seed}")
    chaos = random.Random(seed)
    work = workload(partitions, records)
    group = [apps(work, sleep_ms=sleep_ms, seed=chaos.randrange(2**32), **config) for _ in range(3)]

    deadline = time.monotonic() + CHAOS_SECONDS
    while time.monotonic() < deadline:
        time.sleep(chaos.uniform(1.5, 4.5))
        victim = chaos.choice(group)
        if chaos.random() < 0.3:
            victim.kill()  # Its partitions wait for its session to time out, its open transaction for its own timeout.
        else:
            assert victim.stop() == 0, f"graceful stop failed, see {victim.log}"
        group[group.index(victim)] = apps(work, sleep_ms=sleep_ms, seed=chaos.randrange(2**32), **config)
        assert all(app.alive() for app in group), "an instance died on its own"

    # A killed member's partitions are reassigned after its session times out.
    work.wait_committed(timeout=SESSION_TIMEOUT_MS / 1000 + 60)
    for app in group:
        app.wait_ready()
        assert app.stop() == 0, f"graceful stop failed, see {app.log}"
    work.assert_exactly_once()
