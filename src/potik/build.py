"""Build a core `Runtime` from routers of typed handlers."""

from collections.abc import Awaitable, Iterable, Sequence
from typing import cast

from potik.core.config import Config
from potik.core.middleware import BaseMiddleware, _chain, _middleware_tuple
from potik.core.runtime import Runtime
from potik.core.types import ConsumerRecord, ProducerRecord, RecordHandler
from potik.handler import _NO_CONTEXT, _callable_name
from potik.middleware.concurrency import ConcurrencyMiddleware, _validate_limit
from potik.middleware.exception import ExceptionMiddleware, Rule
from potik.middleware.topics import TopicCheckMiddleware, _declared_topics
from potik.router import RegistrationError, Router, _Registration

__all__ = [
    "build_runtime",
]


def build_runtime(
    config: Config,
    routers: Iterable[Router],
    *,
    context: object = _NO_CONTEXT,
    middleware: Iterable[BaseMiddleware] = (),
    exception_rules: Iterable[Rule[Exception]] = (),
    max_concurrency: int | None = None,
    check_topics: bool = True,
) -> Runtime:
    """Build a `Runtime` that runs the routers' handlers.

    Open the resources that handlers need first, e.g. a database pool or a `Producer`, then build the runtime with them and run it. A runtime runs once, so another run needs a new runtime with new middleware instances.

    Each record passes through `middleware`, then the exception rules, then the concurrency limits, then its handler.

    Args:
        config: The runtime limits and client options.
        routers: The routers whose handlers to run. The runtime copies their handlers, so later registrations have no effect.
        context: What `Context()` parameters take, of handlers and of their dependencies, e.g. a dataclass of the service's clients.
        middleware: Middleware instances, outermost first (see `BaseMiddleware`). Pass exception rules as `exception_rules`, not as an `ExceptionMiddleware`.
        exception_rules: Rules that retry or recover from handler exceptions, tried in order (see `ExceptionMiddleware`). They run inside `middleware`, so e.g. tracing and metrics see one call per record, including its retries. An exception that no rule handles stops the record's partition, or fails the run (see `Config.stop_failed_partitions`). To apply a rule to some topics only, use `on(..., topics=...)`.
        max_concurrency: The maximum number of handler calls that run at once, across all partitions and topics, e.g. the size of a database pool, or None for no limit. Like each handler's own `max_concurrency`, it limits only handler calls, so retry backoff holds no slot.
        check_topics: Whether `run()` first checks that the topics exist, with a `TopicCheckMiddleware` before `middleware`. It checks the handled topics and the output topics of the handlers, routers, middleware, and exception rules. A missing topic fails the run with `TopicCheckError`. Turn it off e.g. to start before the topics are created.

    Returns:
        The runtime. Call its `run()` to run it.

    Raises:
        RegistrationError: If the routers have no handlers, two routers handle the same topic, a handler or one of its dependencies takes `Context()` and there is no `context`, or `middleware` includes an `ExceptionMiddleware`.
        TypeError: If a router is not a `Router`, a middleware is not a `BaseMiddleware` instance, or an exception rule is a type or is not callable.
        ValueError: If `max_concurrency` is not a positive integer or None.

    Example:
        ```python
        async def main() -> None:
            async with create_pool() as db:
                await potik.build_runtime(
                    config,
                    [router],
                    context=db,
                    middleware=[TracingMiddleware()],
                    exception_rules=[
                        on(TimeoutError, retry(max_attempts=5)),
                        on(Exception, dead_letter(dead_letters)),
                    ],
                ).run()
        ```
    """
    _validate_limit(max_concurrency, "max_concurrency")
    stack = _middleware_tuple(middleware)
    for item in stack:
        if isinstance(item, ExceptionMiddleware):
            raise RegistrationError("pass exception rules as build_runtime(exception_rules=...)")
    if rules := tuple(exception_rules):
        stack = (*stack, ExceptionMiddleware(*rules))

    registrations: dict[str, _Registration] = {}
    outputs = [topic for item in stack for topic in _declared_topics(item)]
    for router in routers:
        if not isinstance(router, Router):
            raise TypeError(f"routers must be Router instances, got {type(router).__name__}")
        for topic, registration in router.registrations.items():
            if (existing := registrations.get(topic)) is not None:
                raise RegistrationError(
                    f"topic {topic!r} is handled by both {_callable_name(existing.handler)} "
                    f"and {_callable_name(registration.handler)}"
                )
            registrations[topic] = registration
            outputs += registration.output_topics
        outputs += router.output_topics
    if not registrations:
        raise RegistrationError("a runtime needs at least one handler")

    per_topic = {
        topic: limit for topic, registration in registrations.items() if (limit := registration.max_concurrency)
    }
    limits = (
        (ConcurrencyMiddleware(max_concurrency=max_concurrency, per_topic=per_topic),)
        if max_concurrency or per_topic
        else ()
    )
    handlers = {
        topic: cast(RecordHandler, _chain(limits, "handle", _bind(registration, context)))
        for topic, registration in registrations.items()
    }

    def handle(record: ConsumerRecord) -> Awaitable[Sequence[ProducerRecord]]:
        return handlers[cast(str, record.topic())](record)

    topics = tuple(registrations)
    if check_topics:
        stack = (TopicCheckMiddleware((*topics, *outputs)), *stack)
    key_concurrency = {
        topic: limit for topic, registration in registrations.items() if (limit := registration.key_concurrency)
    }
    return Runtime(config, topics, handle, middleware=stack, key_concurrency=key_concurrency)


def _bind(registration: _Registration, context: object) -> RecordHandler:
    try:
        return registration.bind(context)
    except LookupError as exc:
        raise RegistrationError(f"handler {_callable_name(registration.handler)} {exc}") from exc
