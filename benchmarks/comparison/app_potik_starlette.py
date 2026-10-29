# /// script
# requires-python = "==3.12.*"
# dependencies = ["potik", "confluent-kafka==2.16.0", "starlette", "uvicorn"]
# [tool.uv.sources]
# potik = { path = "../..", editable = true }
# ///
"""potik with its default settings, serving /livez and /readyz from Starlette and uvicorn."""

import logging

import anyio
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from workload import parse_args, process

import potik
from potik import Config, ConsumerRecord, ProducerRecord, Router, resume_or_earliest

args = parse_args()
router = Router()


@router.handler(args.input)
async def handle(record: ConsumerRecord) -> list[ProducerRecord]:
    delay, output = process(record.value())
    await anyio.sleep(delay)
    return [ProducerRecord(args.output, record.key(), output)]


def probe(reason: str | None) -> PlainTextResponse:
    return PlainTextResponse("ok\n") if reason is None else PlainTextResponse(f"{reason}\n", 503)


async def main() -> None:
    runtime = potik.build_runtime(
        Config(
            bootstrap_servers=args.bootstrap,
            group_id=args.group,
            offset_policy=resume_or_earliest,
            commit_batch_size=args.commit_batch_size,
            commit_interval_ms=args.commit_interval_ms,
        ),
        [router],
    )

    async def livez(request: Request) -> PlainTextResponse:
        return probe(runtime.liveness())

    async def readyz(request: Request) -> PlainTextResponse:
        return probe(runtime.readiness())

    app = Starlette(routes=[Route("/livez", livez), Route("/readyz", readyz)])
    config = uvicorn.Config(app, port=args.health_port, log_level="warning", lifespan="off", http="h11", loop="asyncio")
    server = uvicorn.Server(config)
    async with anyio.create_task_group() as tasks:
        tasks.start_soon(server.serve)
        try:
            await runtime.run()
        finally:
            server.should_exit = True


logging.basicConfig(level=logging.WARNING)
anyio.run(main)
