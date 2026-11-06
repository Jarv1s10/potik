"""Run every call of the runtime's consumer on one dedicated thread, in order."""

import asyncio
import threading
import time
from collections.abc import Callable, Coroutine
from concurrent.futures import Future
from queue import SimpleQueue
from typing import Any

import anyio
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

__all__: list[str] = []

type _Call = Callable[[Consumer], None]

# librdkafka 2.16.0 fails a watermark read, rather than retrying it, when it loses the connection, e.g. to an IPv6 address that the broker does not listen on, or destroys its broker handle, e.g. the bootstrap one.
_WATERMARKS_RETRIED = frozenset({KafkaError._TRANSPORT, KafkaError._DESTROY_BROKER})
# librdkafka 2.16.0 retries a committed-offsets read that loses its connection or waits for a coordinator, but fails it when the coordinator's connection times out, e.g. when a heartbeat to a stalled broker times out, or when the group's coordinator moves to another broker.
_COMMITTED_RETRIED = frozenset(
    {
        KafkaError._TIMED_OUT,
        KafkaError._TIMED_OUT_QUEUE,
        KafkaError.NOT_COORDINATOR,
        KafkaError.COORDINATOR_NOT_AVAILABLE,
    }
)
_RETRY_BACKOFF = 0.1  # seconds


class ConsumerThread:
    """Owns a `Consumer` and runs every call on one dedicated thread, in order.

    The client blocks calls from other threads while one polls, and runs rebalance callbacks inside the poll. So the consumer is never exposed. Instead, the event loop passes functions of it to `call()`. A rebalance callback that waits for the event loop keeps running queued calls, so the loop's work may call the consumer too, as long as it does not wait for a poll.

    A long poll must return early when `has_calls()` is true, or queued calls wait for it.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._consumer = Consumer(config)
        self._loop = asyncio.get_running_loop()
        self._calls: SimpleQueue[_Call | None] = SimpleQueue()  # None ends the thread
        threading.Thread(target=self._serve, name="potik-consumer", daemon=True).start()

    async def call[T](self, function: Callable[[Consumer], T]) -> T:
        """Return `function(consumer)`, run on the consumer thread.

        Blocking client calls cannot be interrupted. If the awaiting task is cancelled, it still waits for the call to finish.
        """
        future: Future[T] = Future()

        def run(consumer: Consumer) -> None:
            try:
                future.set_result(function(consumer))
            except BaseException as error:  # noqa: BLE001 -- raised in the awaiting task
                future.set_exception(error)

        self._calls.put(run)
        with anyio.CancelScope(shield=True):
            result = await asyncio.wrap_future(future)
        return result

    def has_calls(self) -> bool:
        return not self._calls.empty()

    def run_on_loop[*Ts](self, function: Callable[[*Ts], Coroutine[Any, Any, object]], *args: *Ts) -> None:
        """Run `function(*args)` on the event loop, from a rebalance callback.

        Runs queued calls until it finishes, then raises its exception, if any.
        """
        future = asyncio.run_coroutine_threadsafe(function(*args), self._loop)
        future.add_done_callback(lambda _: self._calls.put(_wake))
        while not future.done():
            run = self._calls.get()
            assert run is not None  # None comes only after close(), whose callbacks never wait for the loop.
            run(self._consumer)
        future.result()

    async def close(self) -> None:
        """Close the consumer, leaving the group, and end the thread."""
        try:
            await self.call(lambda consumer: consumer.close())
        finally:
            self._calls.put(None)

    def _serve(self) -> None:
        while (run := self._calls.get()) is not None:
            run(self._consumer)


def _wake(consumer: Consumer) -> None:
    """Unblock `run_on_loop()` when the loop's work finishes."""


def _watermarks(consumer: Consumer, position: TopicPartition, deadline: float) -> tuple[int, int]:
    """Read the first and end offsets of `position`'s log from the broker by the monotonic `deadline`."""
    return _retrying(
        lambda timeout: consumer.get_watermark_offsets(position, timeout=timeout),
        _WATERMARKS_RETRIED,
        deadline,
        f"the first and end offsets of {position.topic}[{position.partition}]",
    )


def _committed(consumer: Consumer, partitions: list[TopicPartition], deadline: float) -> list[TopicPartition]:
    """Read the group's committed offsets of `partitions` from its coordinator by the monotonic `deadline`."""
    return _retrying(
        lambda timeout: consumer.committed(partitions, timeout=timeout),
        _COMMITTED_RETRIED,
        deadline,
        "the committed offsets",
    )


def _retrying[T](read: Callable[[float], T | None], retried: frozenset[int], deadline: float, what: str) -> T:
    """Return `read(timeout)` by the monotonic `deadline`, reading again after a `None` or an error in `retried`."""
    while (timeout := deadline - time.monotonic()) > 0:
        try:
            if (result := read(timeout)) is not None:
                return result
        except KafkaException as exc:
            if exc.args[0].code() not in retried:
                raise
            time.sleep(min(_RETRY_BACKOFF, max(deadline - time.monotonic(), 0)))
    raise TimeoutError(f"timed out reading {what}")
