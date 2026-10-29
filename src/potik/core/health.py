"""Liveness and readiness checks of a `RuntimeStatus`, to serve over any probe transport, e.g. HTTP.

`Runtime.liveness()` and `Runtime.readiness()` call them. Each check returns why the probe fails, or None if it passes. A reason names at most an exception's type, never its message, so it is safe to expose.
"""

import time

from potik.core.config import Config
from potik.core.types import RuntimeStatus

__all__ = [
    "liveness",
    "readiness",
]


def liveness(status: RuntimeStatus, config: Config, *, now: float | None = None, slack: float = 30.0) -> str | None:
    """Fail the probe when a restart would help, i.e. the run failed, or it stopped polling or committing.

    Polling counts as stuck when no poll has returned for `rebalance_timeout_ms` plus `slack`. Committing counts as stuck when a handled record stays uncommitted for `commit_interval_ms` plus twice `transaction_timeout_ms`, plus `slack`.

    A broker outage alone does not fail the check, because a restart cannot fix it. The check fails only if the outage fails the run, e.g. by timing out a transaction. The check also passes while a run starts or stops, unless the run failed. A handler may run for any time, so the check ignores how long it runs. It ignores stopped partitions too, because a restart would run the failed record again, which usually fails again, and stop every other partition meanwhile. Alert on them instead, e.g. with `MetricsMiddleware`.

    Args:
        status: The runtime's status, e.g. from `Runtime.status()`.
        config: The run's config.
        now: The `time.monotonic()` time to check at. Defaults to the current time.
        slack: Seconds added to each limit, to allow for a slow event loop and slow `commit` middleware.

    Returns:
        Why the probe fails, or None if it passes.
    """
    if status.failure is not None:
        return f"run failed with {type(status.failure).__name__}"
    if not status.running:
        return None
    now = time.monotonic() if now is None else now
    if status.polled_at is not None:
        stuck = now - status.polled_at
        if stuck > config.rebalance_timeout_ms / 1000 + slack:
            return f"no poll returned for {stuck:.0f}s"
    if status.oldest_uncommitted_at is not None:
        stuck = now - status.oldest_uncommitted_at
        if stuck > (config.commit_interval_ms + 2 * config.transaction_timeout_ms) / 1000 + slack:
            return f"handled records uncommitted for {stuck:.0f}s"
    return None


def readiness(status: RuntimeStatus) -> str | None:
    """Pass the probe while the run is running, whether or not it has assigned or stopped partitions.

    A consumer serves no requests, so readiness only paces rollouts and disruption budgets. It ignores partitions, because an instance may get none, e.g. with more replicas than partitions.

    Returns:
        Why the probe fails, or None if it passes.
    """
    if status.failure is not None:
        return f"run failed with {type(status.failure).__name__}"
    if not status.running:
        return "not running"
    return None
