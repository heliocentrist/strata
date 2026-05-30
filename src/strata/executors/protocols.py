from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from strata.core.operations import OperationInput, OperationOutput


class ApplyResult(dict[str, Any]):
    pass


@dataclass(frozen=True)
class OperationInvocation:
    invocation_id: str
    operation_name: str
    inputs: list[OperationInput]
    config: dict[str, Any]


class OperationRunner(Protocol):
    async def run(self, invocation: OperationInvocation) -> list[OperationOutput]: ...

    async def run_many(
        self,
        invocations: list[OperationInvocation],
    ) -> list[list[OperationOutput]]: ...
