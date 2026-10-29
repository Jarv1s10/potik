"""Retry or recover from a handler's exceptions with rules that are checked in order, like `except` clauses."""

import random
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

import anyio

from potik.core.errors import PermanentError
from potik.core.middleware import BaseMiddleware
from potik.core.topics import _validate_topic
from potik.core.types import ConsumerRecord, Headers, ProducerRecord, RecordHandler
from potik.middleware.topics import NamedTopic, _declared_topics, _topic_names

try:
    from opentelemetry.trace import get_current_span
except ImportError:  # Without the otel extra, ExceptionMiddleware records nothing on spans.
    get_current_span = None  # type: ignore[assignment]

__all__ = [
    "Backoff",
    "Decision",
    "ExceptionContext",
    "ExceptionMiddleware",
    "ExponentialBackoff",
    "Match",
    "Reraise",
    "RetryAfter",
    "Rule",
    "dead_letter",
    "dead_letter_headers",
    "on",
    "reraise",
    "retry",
    "retry_forever",
    "skip",
]

type Decision = RetryAfter | Sequence[ProducerRecord] | Reraise | None
"""What a rule returns.

It returns `RetryAfter` to call the handler again, a list or tuple of records to commit as the record's outputs, `Reraise` to let the exception propagate, or None to ask the next rule.
"""
type Rule[ExcT: Exception] = Callable[[ExceptionContext[ExcT]], Decision | Awaitable[Decision]]
"""A sync or async function of an `ExceptionContext` that returns a `Decision`."""
type Match[ExcT: Exception] = type[ExcT] | tuple[type[ExcT], ...] | Callable[[Exception], bool]
"""The exceptions that `on()` applies a rule to: a type, a tuple of them as in an `except` clause, or a predicate."""
type Backoff = Callable[[ExceptionContext[Any]], float]
"""Returns the seconds to wait before the next attempt, e.g. from a `Retry-After` value that the exception carries."""

# The longest exception message in a dead letter's header, in characters. A validation error can list every field, and a header larger than the broker's message size limit would fail the dead letter's transaction.
_MAX_MESSAGE = 1000


@dataclass(frozen=True, kw_only=True)
class ExceptionContext[ExcT: Exception]:
    """What an exception rule decides on: the exception, the record, and the attempt.

    Attributes:
        exception: The handler's exception. For an `ExceptionGroup` that holds a single exception, e.g. from a task group in which one task failed, it is that exception.
        record: The input record.
        attempt: The number of handler calls of the record so far, including the failed one.
    """

    exception: ExcT
    record: ConsumerRecord
    attempt: int


@dataclass(frozen=True)
class RetryAfter:
    """A decision to call the handler again after `seconds`."""

    seconds: float


@dataclass(frozen=True)
class Reraise:
    """A decision to ask no more rules and let the exception propagate, like a bare `raise` in an `except` clause.

    The exception then stops the partition, or fails the run (see `Config.stop_failed_partitions`).
    """


@dataclass(frozen=True, kw_only=True)
class ExponentialBackoff:
    """A backoff whose delay grows by a factor after each attempt, up to a cap.

    Args:
        initial_seconds: The delay after the first attempt.
        max_seconds: The longest delay, or None for no cap. The default cap keeps `retry_forever()` retrying at least every 30 seconds during a long outage.
        multiplier: The factor that multiplies the delay after each attempt.
        jitter: Whether each wait is a random time between zero and the computed delay, so that partitions do not retry in step.

    Raises:
        ValueError: If a delay is negative, or `multiplier` is less than 1.
    """

    initial_seconds: float = 0.1
    max_seconds: float | None = 30.0
    multiplier: float = 2.0
    jitter: bool = False

    def __post_init__(self) -> None:
        if self.initial_seconds < 0:
            raise ValueError(f"initial_seconds must not be negative, got {self.initial_seconds!r}")
        if self.max_seconds is not None and self.max_seconds < 0:
            raise ValueError(f"max_seconds must not be negative, got {self.max_seconds!r}")
        if self.multiplier < 1:
            raise ValueError(f"multiplier must be at least 1, got {self.multiplier!r}")

    def __call__(self, context: ExceptionContext[Any]) -> float:
        try:
            delay = self.initial_seconds * self.multiplier ** (context.attempt - 1)
        except OverflowError:  # Only the power can overflow, and zero times any power is still zero.
            delay = float("inf") if self.initial_seconds else 0.0
        if self.max_seconds is not None:
            delay = min(delay, self.max_seconds)
        return random.uniform(0, delay) if self.jitter else delay


class ExceptionMiddleware(BaseMiddleware):
    """Retries or recovers from a handler's exceptions by asking rules in order, like `except` clauses.

    With `potik.build_runtime()`, pass the rules as `exception_rules` instead of creating this middleware. Build rules with `on()`, `retry()`, `retry_forever()`, `skip()`, `reraise()`, and `dead_letter()`.

    The first rule that returns something other than None decides. `RetryAfter`, e.g. from `retry()`, calls the handler again after its delay. Records, e.g. from `skip()` or `dead_letter()`, commit as the record's outputs in place of the failure. `Reraise`, from `reraise()`, lets the exception propagate. An exception that no rule handles propagates too, and stops the partition, or fails the run (see `Config.stop_failed_partitions`).

    Rules see the single exception of an `ExceptionGroup` that holds only one, so `on(TimeoutError, ...)` matches a `TimeoutError` from a task group in the handler. A group of several exceptions stays a group, which `on(ExceptionGroup, ...)` or a function matches. Either way, an exception that no rule handles propagates as the handler raised it.

    A partition handles one record at a time, so a retry holds up its partition until the record succeeds or a later rule handles it. With key concurrency (see `Runtime`), a retry holds up only its key, though the records after it commit only once it succeeds or a rule handles it. A `retry()` rule that has used up its attempts returns None, so a later rule handles the exception. So does a `retry()` rule for a `PermanentError`, e.g. a `DecodeError`, which would fail the same way again.

    The middleware's `output_topics` collects its rules' (see `DeclaresOutputs`), e.g. the dead letter topics of `dead_letter()` rules, so that the topic check covers them.

    With the `otel` extra, the middleware records each exception that it retries or handles as an `exception` event on the current span. Inside `TracingMiddleware`, that span is the record's `process` span.

    Args:
        *rules: Sync or async functions of an `ExceptionContext` that return a `Decision`, asked in order.

    Raises:
        TypeError: If a rule is a type or is not callable.

    Example:
        ```python
        ExceptionMiddleware(
            on(is_transient, retry(max_attempts=5)),
            on(Exception, dead_letter(dead_letters)),
        )
        ```
    """

    def __init__(self, *rules: Rule[Any]) -> None:
        for rule in rules:
            if isinstance(rule, type) or not callable(rule):
                raise TypeError(f"rules are functions of an ExceptionContext, got {rule!r}; match types with on()")
        self._rules = rules
        self.output_topics = tuple(topic for rule in rules for topic in _declared_topics(rule))

    async def handle(self, record: ConsumerRecord, call_next: RecordHandler) -> Sequence[ProducerRecord]:
        attempt = 1
        while True:
            try:
                return await call_next(record)
            except Exception as exc:
                context = ExceptionContext(exception=_single(exc), record=record, attempt=attempt)
                decision = await self._decide(context)
                if decision is None or isinstance(decision, Reraise):
                    raise
                if get_current_span is not None:
                    get_current_span().record_exception(context.exception)
                if isinstance(decision, RetryAfter):
                    await anyio.sleep(decision.seconds)
                    attempt += 1
                    continue
                return decision

    async def _decide(self, context: ExceptionContext[Exception]) -> Decision:
        for rule in self._rules:
            if (decision := await _resolved(rule(context))) is not None:
                return decision
        return None


def _single(exc: Exception) -> Exception:
    """Return the exception that `exc` holds if it is a group of one, e.g. nested task groups', else `exc`."""
    while isinstance(exc, ExceptionGroup) and len(exc.exceptions) == 1:
        exc = exc.exceptions[0]
    return exc


def on[ExcT: Exception](
    match: Match[ExcT], rule: Rule[ExcT], *, topics: Iterable[NamedTopic | str] | None = None
) -> Rule[Exception]:
    """Apply `rule` only to matching exceptions, and optionally only to some topics.

    For other exceptions and topics, the returned rule returns None, so the next rule decides.

    Args:
        match: An exception type, a tuple of them as in an `except` clause, or a function that takes the exception and returns whether it matches.
        rule: The rule to apply.
        topics: The topics whose records the rule applies to, as `Topic`s or names, or None for every topic.

    Raises:
        TypeError: If `match` names something other than `Exception` subclasses.
        ValueError: If a topic name is invalid.

    Example:
        ```python
        on(TimeoutError, retry(max_attempts=5), topics=[orders])
        ```
    """
    matches: Callable[[Exception], bool]
    if isinstance(match, (type, tuple)):
        types = match if isinstance(match, tuple) else (match,)
        for exc_type in types:
            if not (isinstance(exc_type, type) and issubclass(exc_type, Exception)):
                raise TypeError(f"on() matches Exception subclasses, got {exc_type!r}")

        def matches(exc: Exception) -> bool:
            return isinstance(exc, types)
    else:
        matches = match
    return _Matched(matches, cast(Rule[Exception], rule), None if topics is None else frozenset(_topic_names(topics)))


@dataclass(frozen=True, slots=True)
class _Matched:
    matches: Callable[[Exception], bool]
    rule: Rule[Exception]
    topics: frozenset[str] | None

    @property
    def output_topics(self) -> Sequence[str]:
        return _declared_topics(self.rule)

    def __call__(self, context: ExceptionContext[Exception]) -> Decision | Awaitable[Decision]:
        if self.topics is not None and context.record.topic() not in self.topics:
            return None
        return self.rule(context) if self.matches(context.exception) else None


def retry(*, max_attempts: int, backoff: Backoff | None = None) -> Rule[Exception]:
    """Retry until the handler has run `max_attempts` times, then pass the exception to the next rule.

    A `PermanentError`, e.g. a `DecodeError`, goes straight to the next rule, because retrying cannot fix it.

    Args:
        max_attempts: The maximum number of handler calls of a record, including the first.
        backoff: Returns the seconds to wait before each retry. Defaults to `ExponentialBackoff()`.

    Raises:
        ValueError: If `max_attempts` is not a positive integer.
    """
    if type(max_attempts) is not int or max_attempts < 1:
        raise ValueError(f"max_attempts must be a positive integer, got {max_attempts!r}")
    return _Retry(max_attempts, backoff or ExponentialBackoff())


def retry_forever(*, backoff: Backoff | None = None) -> Rule[Exception]:
    """Retry until the handler succeeds.

    Like `retry()`, it passes a `PermanentError` to the next rule. Its partition makes no progress until the record succeeds.

    Args:
        backoff: Returns the seconds to wait before each retry. Defaults to `ExponentialBackoff()`.
    """
    return _Retry(None, backoff or ExponentialBackoff())


@dataclass(frozen=True, slots=True)
class _Retry:
    max_attempts: int | None
    backoff: Backoff

    def __call__(self, context: ExceptionContext[Exception]) -> RetryAfter | None:
        if isinstance(context.exception, PermanentError):
            return None
        if self.max_attempts is not None and context.attempt >= self.max_attempts:
            return None
        return RetryAfter(self.backoff(context))


def reraise() -> Rule[Exception]:
    """Ask no more rules and let the exception propagate, so that it stops the partition.

    Put it before a rule that handles every exception, e.g. `on(InvalidOutputError, reraise())` before a `dead_letter()` rule. With `Config(stop_failed_partitions=False)`, the exception fails the run instead.
    """

    def rule(context: ExceptionContext[Exception]) -> Reraise:
        return Reraise()

    return rule


def skip() -> Rule[Exception]:
    """Drop the failed record: commit its offset with no outputs."""

    def rule(context: ExceptionContext[Exception]) -> Sequence[ProducerRecord]:
        return ()

    return rule


def dead_letter_headers(context: ExceptionContext[Exception]) -> list[tuple[str, str | None]]:
    """Return the headers that `dead_letter()` adds by default.

    - `potik.dlq.source.topic`, `potik.dlq.source.partition`, and `potik.dlq.source.offset`: where the record came from.
    - `potik.dlq.exception.type`: the exception's qualified class name.
    - `potik.dlq.exception.message`: `str()` of the exception, which may contain data from the record. A message longer than 1000 characters is cut, and ends with `...`.
    - `potik.dlq.attempts`: the number of handler calls.

    Args:
        context: The failed record and its exception.
    """
    record, exception = context.record, context.exception
    message = str(exception)
    if len(message) > _MAX_MESSAGE:
        message = f"{message[: _MAX_MESSAGE - 3]}..."
    return [
        ("potik.dlq.source.topic", record.topic()),
        ("potik.dlq.source.partition", str(record.partition())),
        ("potik.dlq.source.offset", str(record.offset())),
        ("potik.dlq.exception.type", f"{type(exception).__module__}.{type(exception).__qualname__}"),
        ("potik.dlq.exception.message", message),
        ("potik.dlq.attempts", str(context.attempt)),
    ]


def dead_letter(
    topic: NamedTopic | str,
    *,
    headers: Callable[[ExceptionContext[Exception]], Iterable[tuple[str, str | bytes | None]]] = dead_letter_headers,
) -> Rule[Exception]:
    """Write a copy of the failed record to `topic`, as the record's outputs.

    The copy commits in the same transaction as the failed record's offset. It keeps the record's key, value, and headers as bytes, even if they do not decode, and adds the headers that `headers` returns. The producer's partitioner picks its partition from the key, because the dead letter topic may have fewer partitions than the input's.

    The rule declares `topic` as its output topic, so the topic check covers it. To write a different dead letter, write a rule of your own and declare its topic, e.g. in `Router(output_topics=...)`.

    Args:
        topic: The dead letter topic, as a `Topic` or a name.
        headers: Returns the headers to add, e.g. names that your dead letter consumers expect, or `dead_letter_headers` without the exception message. Defaults to `dead_letter_headers`.
    """
    name = topic if isinstance(topic, str) else topic.name
    _validate_topic(name)
    return _DeadLetters(name, headers)


@dataclass(frozen=True, slots=True)
class _DeadLetters:
    topic: str
    headers: Callable[[ExceptionContext[Exception]], Iterable[tuple[str, str | bytes | None]]]

    @property
    def output_topics(self) -> Sequence[str]:
        return (self.topic,)

    def __call__(self, context: ExceptionContext[Exception]) -> Sequence[ProducerRecord]:
        record = context.record
        received = record.headers() or ()
        headers: Headers = [
            *(received.items() if isinstance(received, Mapping) else received),
            *self.headers(context),
        ]
        return (ProducerRecord(self.topic, record.key(), record.value(), headers),)


async def _resolved[T](value: T | Awaitable[T]) -> T:
    if isinstance(value, Awaitable):
        return await cast(Awaitable[T], value)
    return cast(T, value)
