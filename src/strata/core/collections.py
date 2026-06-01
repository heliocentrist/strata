from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class ArtifactPayload:
    data: Any
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ArtifactWrite:
    instance_key: str
    input_fingerprint: str
    payload: ArtifactPayload
    content_hash: str | None = None


@dataclass(frozen=True)
class ArtifactWriteResult:
    instance_key: str
    input_fingerprint: str
    output_ref: str
    output_hash: str | None
    content_hash: str | None = None


@dataclass(frozen=True)
class CollectionWriteContext:
    root_path: Path
    asset_name: str
    partition_key: str | None = None
    window_id: str | None = None


class ArtifactCollection(Protocol):
    def write_one(
        self,
        context: CollectionWriteContext,
        item: ArtifactWrite,
    ) -> ArtifactWriteResult: ...

    def write_many(
        self,
        context: CollectionWriteContext,
        items: list[ArtifactWrite],
    ) -> list[ArtifactWriteResult]: ...

    def read(self, root_path: Path, ref: str) -> dict[str, Any]: ...

    def read_many(self, root_path: Path, refs: list[str]) -> list[dict[str, Any]]: ...
