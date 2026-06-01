from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class OperationInput:
    role: str
    asset_name: str | None
    instance_key: str
    input_id: str = ""
    data: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)
    input_fingerprint: str | None = None
    content_hash: str | None = None
    source_name: str | None = None
    source_content_hash: str | None = None
    path: Path | None = None
    uri: str | None = None
    artifact_location: str | None = None
    artifact_collection: str | None = None
    partition_key: str | None = None


@dataclass(frozen=True)
class OperationOutput:
    instance_key: str | None = None
    data: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)
    parent_input_ids: list[str] = field(default_factory=list)
    content_hash: str | None = None
    output_location: str | None = None
    output_hash: str | None = None


__all__ = ["OperationInput", "OperationOutput"]
