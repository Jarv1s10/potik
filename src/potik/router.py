"""Register typed handlers of topics on routers, which `potik.build_runtime()` builds a runtime from."""

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, overload

from potik.core.errors import PotikError
from potik.handler import HandlerResult, _BindHandler, _compile_handler
from potik.middleware.concurrency import _validate_limit
from potik.middleware.topics import _topic_names
from potik.topic import OutputTopic, Topic

__all__ = [
    "RegistrationError",
    "Router",
]

type _UserHandler = Callable[..., Awaitable[HandlerResult]]


class RegistrationError(PotikError):
    """Raised when a router cannot register a handler, or `potik.build_runtime()` cannot build a runtime from routers."""


@dataclass(frozen=True, slots=True)
class _Registration:
    topic: str
    handler: _UserHandler
    max_concurrency: int | None
    key_concurrency: int | None
    output_topics: tuple[str, ...]
    bind: _BindHandler


class Router:
    """A group of topic handlers, which `potik.build_runtime()` builds a runtime from.

    Register handlers with `@router.handler(topic)`. A router needs no runtime, so a module can define one at import, and tests can call its handlers directly. A router has no middleware of its own. To apply an exception rule to some topics only, use `on(..., topics=...)`, and in middleware, check `record.topic()`.

    Args:
        output_topics: Topics that records go to beyond the handlers' own `output_topics`, e.g. the topic of your own exception rule. `potik.build_runtime()` checks that they exist.

    Raises:
        RegistrationError: If an output is not a valid topic name.
    """

    def __init__(self, *, output_topics: Iterable[OutputTopic[Any] | str] = ()) -> None:
        try:
            self.output_topics = _topic_names(output_topics)
        except ValueError as exc:
            raise RegistrationError(str(exc)) from exc
        self.registrations: dict[str, _Registration] = {}

    @overload
    def handler[H: _UserHandler](
        self,
        topic: str | Topic[Any, Any],
        *,
        max_concurrency: int | None = None,
        key_concurrency: int | None = None,
    ) -> Callable[[H], H]: ...
    @overload
    def handler[**P, V](
        self,
        topic: str | Topic[Any, Any],
        *,
        max_concurrency: int | None = None,
        key_concurrency: int | None = None,
        output_topics: Iterable[OutputTopic[V]],
    ) -> Callable[[Callable[P, Awaitable[HandlerResult[V]]]], Callable[P, Awaitable[HandlerResult[V]]]]: ...
    @overload
    def handler[**P, V](
        self,
        topic: str | Topic[Any, Any],
        *,
        max_concurrency: int | None = None,
        key_concurrency: int | None = None,
        output_topics: Iterable[OutputTopic[V] | str],
    ) -> Callable[
        [Callable[P, Awaitable[HandlerResult[V | bytes]]]], Callable[P, Awaitable[HandlerResult[V | bytes]]]
    ]: ...
    def handler(
        self,
        topic: str | Topic[Any, Any],
        *,
        max_concurrency: int | None = None,
        key_concurrency: int | None = None,
        output_topics: Iterable[OutputTopic[Any] | str] = (),
    ) -> Callable[[_UserHandler], _UserHandler]:
        """Register an async function as the handler of `topic`, and return the function unchanged.

        Each of the handler's parameters needs a type annotation, and takes one part of the input record:

        - `Annotated[T, Key()]` takes the key, as the topic's key type, or as `bytes` for the raw key.
        - `Annotated[T, Header(name)]` takes a header's last value.
        - `Annotated[T, Context()]` takes the run's context.
        - A parameter annotated `ConsumerRecord` takes the record itself.
        - The one parameter left, if any, takes the value, as the topic's value type, or as `bytes` for the raw value.

        The handler returns None, a `ProducerRecord`, or a list or tuple of them. Anything else raises `InvalidOutputError` from the handler's call.

        Args:
            topic: The handled topic, as a `Topic`, or the name of a topic of `bytes` values and `str` keys.
            max_concurrency: The maximum number of calls of this handler that run at once, across the topic's partitions, or None for no limit.
            key_concurrency: The maximum number of the topic's keys in progress at once, across its partitions, or None to handle each partition's records one at a time. Records with the same key still run one at a time, in offset order. A key stays in progress during its retries' backoff, so set `max_concurrency` too, to limit the handler calls, e.g. `key_concurrency=64, max_concurrency=20` for a pool of 20 connections. See the `key_concurrency` of `potik.core.Runtime`.
            output_topics: The topics that the handler returns records to, as `Topic`s or names of `bytes` topics. `potik.build_runtime()` checks that they exist before the run starts. Type checkers then require the handler to return records of their value types, e.g. `ProducerRecord[Accepted]`. The handler may still return records to other topics, which the check does not cover.

        Raises:
            RegistrationError: If the router already handles `topic`, a limit is not a positive integer or None, the handler is not an async function, or a parameter does not fit the topic, e.g. its type differs from the topic's value type.

        Example:
            ```python
            @router.handler(orders, output_topics=[accepted])
            async def accept(order: Order, customer: Annotated[str, Key()]) -> ProducerRecord[Accepted]:
                return accepted(Accepted(order.id), key=customer)
            ```
        """
        try:
            if not isinstance(topic, Topic):
                topic = Topic(topic, bytes)
            _validate_limit(max_concurrency, "max_concurrency")
            _validate_limit(key_concurrency, "key_concurrency")
            names = _topic_names(output_topics)
        except ValueError as exc:
            raise RegistrationError(str(exc)) from exc
        contract, name = topic, topic.name

        def register(handler: _UserHandler) -> _UserHandler:
            try:
                bind = _compile_handler(handler, contract)
            except ValueError as exc:
                raise RegistrationError(f"handler {_callable_name(handler)} for topic {name!r} {exc}") from exc
            if (existing := self.registrations.get(name)) is not None:
                raise RegistrationError(
                    f"cannot register {_callable_name(handler)} for topic {name!r}, "
                    f"which {_callable_name(existing.handler)} already handles"
                )
            self.registrations[name] = _Registration(name, handler, max_concurrency, key_concurrency, names, bind)
            return handler

        return register


def _callable_name(handler: object) -> str:
    return getattr(handler, "__qualname__", None) or getattr(handler, "__name__", None) or repr(handler)
