# Contributing

potik is alpha, so open an issue to discuss a change before you send a large pull request.

## Setup

Install [uv](https://docs.astral.sh/uv/), then:

```sh
uv sync
```

Before you commit, format, lint, and type check:

```sh
uvx ruff format .
uvx ruff check . --fix --unsafe-fixes
uvx ty check
uv run mypy
uv run pyrefly check
prek run
```

## Tests

The tests start their own Kafka broker in a container through testcontainers, with Docker or Podman. They find Podman's socket unless `DOCKER_HOST` is set.

```sh
uv run pytest                    # fast scenarios, about 20 seconds
uv run pytest -m slow            # slow scenarios, a few minutes
uv run pytest -m "" -n 4         # everything, on 4 workers with a broker each
```

The slow scenarios restart groups of app processes at random with SIGTERM and SIGKILL, pause and restart the broker, and inject handler and commit failures. Each one checks that every output is visible exactly once to `read_committed` readers, and that the committed offsets are exact. A failed test keeps its topics and prints the end of its app processes' logs.

To replay a chaos schedule, set its seed:

```sh
POTIK_CHAOS_SEED=123 uv run pytest -m slow tests/integration/test_chaos.py
```

To use an existing broker, set `POTIK_BOOTSTRAP`. Configure the broker with the settings in `tests/integration/harness.py` (`FAST_BROKER`), or the tests' timings won't hold. The outage tests still start a broker of their own.

## Docs

The docs use [Zensical](https://zensical.org). Preview them at http://localhost:8000 with:

```sh
uv run --group docs zensical serve
```

The API reference comes from docstrings in `src/`.

The release workflow publishes the docs of each release. To publish a docs fix between releases, run the Docs workflow from `main` by hand.

## Releasing

Pull requests add their user-facing changes to the `unreleased` section at the top of `CHANGELOG.md`. To release:

1. Set the version with `uv version X.Y.Z`, and replace `unreleased` in the changelog with today's date, e.g. `## X.Y.Z (2026-11-30)`.
2. Commit both to `main`, then tag and push the tag: `git tag vX.Y.Z && git push origin vX.Y.Z`.
3. Approve the `pypi` deployment in the Actions tab.

The release workflow checks the tag against the version, runs the full suite, slow scenarios included, and smoke-tests the built wheel. Then it publishes to PyPI with attestations through trusted publishing, creates the GitHub release, and deploys the docs.
