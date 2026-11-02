"""Presentation changes for the generated API reference."""

from griffe import Extension as _Extension
from griffe import Function as _Function

__all__: list[str] = []


class _OmittedContext(_Extension):
    """Display an omitted context as `...` without changing the runtime's default."""

    def on_function_instance(self, *, func: _Function, **_: object) -> None:
        if func.path == "potik.build.build_runtime" and str(func.parameters["context"].default) == "_NO_CONTEXT":
            func.parameters["context"].default = "..."
