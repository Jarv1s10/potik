"""Validate Kafka topic names."""

import re
from collections.abc import Iterable

__all__ = []

_TOPIC = re.compile(r"[a-zA-Z0-9._-]{1,249}")


def _validate_topic(topic: object) -> None:
    if not isinstance(topic, str) or not _TOPIC.fullmatch(topic) or topic in (".", ".."):
        raise ValueError(f"invalid Kafka topic name: {topic!r}")


def _validate_topics(topics: Iterable[str]) -> tuple[str, ...]:
    if isinstance(topics, str):
        raise TypeError(f"topics must be a collection of names, not the string {topics!r}")
    topics = tuple(topics)
    for topic in topics:
        _validate_topic(topic)
    return topics
