"""The nested ruff.toml files that keep the core and the middleware from importing the layers above them (ADR 0006)."""

import tomllib
from pathlib import Path

import pytest

import potik
import potik.core
import potik.middleware

SRC = Path(potik.__file__).parent
# The modules of the typed layer, at the package root.
TYPED_MODULES = {path.stem for path in SRC.glob("*.py") if path.stem != "__init__"}


def banned(layer: str) -> set[str]:
    config = tomllib.loads((SRC / layer / "ruff.toml").read_text())
    return {name.removeprefix("potik.") for name in config["lint"]["flake8-tidy-imports"]["banned-api"]}


@pytest.mark.parametrize("layer", ["core", "middleware"])
def test_layers_below_the_typed_one_ban_all_of_it(layer: str) -> None:
    """Ruff cannot ban the root package without its subpackages, so each typed module and root name is listed."""
    below = set(potik.core.__all__) | (set(potik.middleware.__all__) if layer == "middleware" else set())
    assert TYPED_MODULES | (set(potik.__all__) - below) <= banned(layer)
