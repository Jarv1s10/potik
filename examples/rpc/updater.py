"""The responder: a plain potik service that applies each update and replies with its status.

Each reply copies its request's `correlation-id` header, and commits in one transaction with the request's offset. One update in five fails, to show an error reply.
"""

import random
from typing import Annotated

import anyio
from common import BOOTSTRAP, CORRELATION_ID, Status, Update, setup, statuses, updates

import potik
from potik import Config, Header, Key, ProducerRecord, Router, resume_or_earliest

router = Router()


@router.handler(updates, output_topics=[statuses])
async def apply(
    update: Update, record_id: Annotated[str, Key()], correlation_id: Annotated[str | None, Header(CORRELATION_ID)]
) -> ProducerRecord[Status]:
    await anyio.sleep(random.uniform(0.05, 0.5))  # the CRM's API call
    error = "the CRM rejected the update" if random.random() < 0.2 else None
    status = Status(record_id=update.record_id, ok=error is None, error=error)
    # A request without a correlation ID, e.g. a fire-and-forget one, still gets a status that nobody awaits.
    return statuses(status, key=record_id, headers={CORRELATION_ID: correlation_id})


async def main() -> None:
    # A request sent before the updater first joins its group still gets its reply.
    config = Config(bootstrap_servers=BOOTSTRAP, group_id="updater", offset_policy=resume_or_earliest)
    await potik.build_runtime(config, [router]).run()


setup("updater")
anyio.run(main)
