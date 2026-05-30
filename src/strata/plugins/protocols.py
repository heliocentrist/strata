from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from strata.core.collections import ArtifactCollection
from strata.core.models import SourceSnapshot, SourceSpec
from strata.core.operations import OperationInput, OperationOutput

STRATA_PLUGIN_API_VERSION = "0.1"

@dataclass(frozen=True)
class AdapterMetadata:
    name: str
    kind: str
    source: str
    api_version: str = STRATA_PLUGIN_API_VERSION
    supported_asset_kinds: tuple[str, ...] = ()
    config_schema: dict[str, Any] | None = None
    dependency_requirements: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExternalPluginDiscoveryResult:
    loaded: list[AdapterMetadata] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class OperationPlugin(Protocol):
    def run(
        self,
        inputs: list[OperationInput],
        config: dict[str, Any],
    ) -> list[OperationOutput]: ...


class SourceAdapter(Protocol):
    def snapshot(self, source: SourceSpec, *, root: Path) -> SourceSnapshot: ...


__all__ = [
    "STRATA_PLUGIN_API_VERSION",
    "AdapterMetadata",
    "ArtifactCollection",
    "ExternalPluginDiscoveryResult",
    "OperationInput",
    "OperationOutput",
    "OperationPlugin",
    "SourceAdapter",
]
