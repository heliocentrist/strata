from __future__ import annotations

from typing import Any, Protocol

from strata.core.models import Manifest, Operation, SourceSnapshot


class ApplyResult(dict[str, Any]):
    pass


class ExecutorAdapter(Protocol):
    def apply(
        self,
        *,
        manifest: Manifest,
        repo: Any,
        source_snapshots: dict[str, SourceSnapshot],
        operations: list[Operation],
        config: dict[str, Any] | None = None,
    ) -> ApplyResult: ...
