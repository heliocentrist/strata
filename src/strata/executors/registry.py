from __future__ import annotations

from collections.abc import Callable
from typing import Any

from strata.executors.protocols import OperationRunner

OperationRunnerFactory = Callable[[dict[str, Any] | None], OperationRunner]

_operation_runners: dict[str, OperationRunnerFactory] = {}


def register_operation_runner(name: str, factory: OperationRunnerFactory) -> None:
    if not name:
        raise ValueError("operation runner name cannot be empty")
    _operation_runners[name] = factory


def get_operation_runner(
    name: str,
    config: dict[str, Any] | None = None,
) -> OperationRunner:
    try:
        factory = _operation_runners[name]
    except KeyError as exc:
        known = ", ".join(sorted(_operation_runners)) or "(none)"
        raise ValueError(f"unknown operation runner '{name}'. Known: {known}") from exc
    return factory(config)


def registered_operation_runners() -> list[str]:
    return sorted(_operation_runners)


def registered_executors() -> list[str]:
    return registered_operation_runners()


from strata.executors.local import register_builtin_operation_runners  # noqa: E402

register_builtin_operation_runners()
