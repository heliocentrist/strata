from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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


@dataclass(frozen=True)
class OperationWindow:
    window_id: str
    asset_name: str
    artifact_root: Path
    artifact_collection: str
    transform_version: str
    config_hash: str
    determinism: str
    invocations: list[OperationInvocation]


@dataclass(frozen=True)
class OperationWindowResult:
    output_groups: list[list[OperationOutput]]


class OperationRunner(Protocol):
    async def run(self, invocation: OperationInvocation) -> list[OperationOutput]: ...

    async def run_many(
        self,
        invocations: list[OperationInvocation],
    ) -> list[list[OperationOutput]]: ...

    async def run_window(self, window: OperationWindow) -> OperationWindowResult: ...
