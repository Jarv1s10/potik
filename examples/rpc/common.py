"""The contracts that the API and the updater share: the request and reply topics, and their values."""

import logging
import os

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import BaseModel

from potik import Topic
from potik.serde import PydanticJson

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


class Update(BaseModel):
    record_id: str
    fields: dict[str, str | int | float | bool | None]


class Status(BaseModel):
    record_id: str
    ok: bool
    error: str | None = None


# The header that carries a request's ID, which the updater copies to its reply. potik leaves the convention to the app.
CORRELATION_ID = "correlation-id"
# Both topics are keyed by the record's ID, so each record's updates run in order.
JSON = PydanticJson()
updates = Topic("crm.updates", Update, serde=JSON)
statuses = Topic("crm.updates.status", Status, serde=JSON)
# Partitions per topic.
TOPICS = {updates.name: 3, statuses.name: 3}


def setup(service: str) -> None:
    """Configure logging and create missing topics."""
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s {service}[{os.getpid()}] %(levelname)s %(message)s")
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = admin.list_topics(timeout=10).topics
    missing = [NewTopic(name, partitions) for name, partitions in TOPICS.items() if name not in existing]
    for future in admin.create_topics(missing).values() if missing else ():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:  # Another service created it meanwhile.
                raise
