from __future__ import annotations

from strata.executors.protocols import ExecutorAdapter

_executors: dict[str, ExecutorAdapter] = {}


def register_executor(name: str, executor: ExecutorAdapter) -> None:
    if not name:
        raise ValueError("executor name cannot be empty")
    _executors[name] = executor


def get_executor(name: str) -> ExecutorAdapter:
    try:
        return _executors[name]
    except KeyError as exc:
        known = ", ".join(sorted(_executors)) or "(none)"
        raise ValueError(f"unknown executor '{name}'. Known: {known}") from exc


def registered_executors() -> list[str]:
    return sorted(_executors)


from strata.executors.local import register_builtin_executors  # noqa: E402

register_builtin_executors()
