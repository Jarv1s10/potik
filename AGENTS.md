# potik

`potik` is an async Python Kafka framework for consume-process-produce workloads, with transactional exactly-once commits of outputs and offsets.

IMPORTANT: This project is PRE-ALPHA. It has no real users yet, and everything is subject to change.
Agents are strongly encouraged to propose bold, sweeping changes that improve the codebase's long-term quality, maintainability, and readability.

## Stack

- Python 3.12+. Write code that runs on 3.12, e.g. take type parameter defaults from `typing_extensions`, and quote forward references in annotations.
- `confluent-kafka` (librdkafka) synchronous clients, driven from dedicated AnyIO worker threads; Kafka 4.0+
- `anyio` (on the asyncio backend) for structured concurrency, task groups, timeouts, cancellation, and streams
- `uv` for environment and command execution
- `ruff` for formatting and linting
- `ty`, `mypy`, and `pyrefly` for type checking. Type inference of the public API must work in all three.

## Runtime philosophy

- Apps built on this framework run as multiple instances in containers, e.g. on Kubernetes. Startup, shutdown, and runtime behavior should follow that platform's best practices.
- Records are processed concurrently across partitions.
- A handler may run for arbitrarily long. The runtime must not limit it.
- The consumer must stay in the group even when every partition is busy, without exceeding `max.poll.interval.ms`.
- Core runtime correctness comes before anything else.
- Rebalances, shutdown, cancellation, fencing, and replay safety are core correctness concerns.
- Speed and memory usage matter too.
- Keep public API and DX features, such as decorators, dependency injection, and message (de)serialization, out of the core runtime. The core should stay a minimal wrapper around `confluent-kafka`.
- potik has three layers: the core (`potik.core`), the built-in middleware (`potik.middleware`), and the typed layer at the package root. Each imports only the layers below it, and nested `ruff.toml` files enforce it (ADR 0006). The core must stay usable on its own, like Starlette under FastAPI.

### Code quality

- Correctness comes first, simplicity second. Change or remove bad assumptions, brittle abstractions, and complicated logic when you find them.
- The best code is code never written. Keep the amount of code small.
- Add an abstraction only when it makes the code simpler.
- Each module's `__all__` lists its public names. Every other module-level name starts with an underscore, even when other potik modules import it. Wholly internal modules, e.g. `core/committer.py`, have an empty `__all__`.

## Tooling

- Run Python commands through `uv`, for example `uv run python ...`.
- Format and lint with `uvx ruff format .` and `uvx ruff check . --fix --unsafe-fixes`.
- Type check with `uvx ty check`, `uv run mypy`, and `uv run pyrefly check`. `tests/typing` holds the public API's expected types, and marks each call that type checkers must reject with one ignore comment per checker.
- At the end of your work, run `prek run`. It runs `ruff` and the three type checkers.

## Documentation

- `README.md` and `docs/` are for users. Keep them short: they show how to use the framework, not every detail of its behavior.
- `docs/reference.md` renders the API reference from docstrings, so docstrings hold the precise behavior of public APIs.
- `.agents/DESIGN.md` describes the runtime's behavior in detail, and `.agents/adr/` records decisions. Update them when behavior changes.
- Build the docs with `uv run --group docs zensical build`.
- Do not wrap prose at a fixed width. This applies to Markdown, docstrings, and comments. Write each paragraph or list item on one line, and break a line only where the break means something, e.g. between paragraphs or list items. The 120-character limit applies only to Python code, and `ruff format` enforces it there.

### Docstrings

- Write Google-style docstrings. mkdocstrings renders them as Markdown, so mark code with single backticks.
- Give private helpers and tests one line, or a short paragraph when the reason is not obvious, with no sections.
- Keep detail that only maintainers and AI agents need in `.agents/DESIGN.md`, not in public docstrings.
- Use plain words and the active voice, write one idea per sentence in whole sentences, and cut filler, hedging, and adverbs. Use no em dashes, and use colons only before a list or an example.
