from strata.executors.local.executor import (
    LocalExecutor,
    apply_operations,
    register_builtin_executors,
)
from strata.executors.protocols import ApplyResult

__all__ = ["ApplyResult", "LocalExecutor", "apply_operations", "register_builtin_executors"]
