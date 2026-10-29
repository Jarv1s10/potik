"""Tests of the public API, e.g. parameter binding, serdes, and whole apps on a `MockCluster`, without a broker container."""

import pytest


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
