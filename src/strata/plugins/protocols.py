from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from strata.core.collections import ArtifactCollection
from strata.core.models import HybridChunkEmbeddingItem, SinkWriteResult, SourceSnapshot, SourceSpec

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


class ParserAdapter(Protocol):
    def parse(self, path: Path) -> str: ...


class ChunkerAdapter(Protocol):
    def chunk(self, text: str, config: dict[str, Any]) -> list[str]: ...


class EmbeddingAdapter(Protocol):
    def embed(self, text: str, config: dict[str, Any]) -> list[float]: ...


class SinkAdapter(Protocol):
    def write_hybrid(
        self,
        *,
        item: HybridChunkEmbeddingItem,
        config: dict[str, Any],
    ) -> SinkWriteResult: ...

    def write(
        self,
        *,
        repo: Any,
        instance_key: str,
        embedding_fingerprint: str,
        source_item_key: str,
        chunk_text: str,
        embedding: list[float],
    ) -> None: ...


class SourceAdapter(Protocol):
    def snapshot(self, source: SourceSpec, *, root: Path) -> SourceSnapshot: ...


__all__ = [
    "STRATA_PLUGIN_API_VERSION",
    "AdapterMetadata",
    "ArtifactCollection",
    "ChunkerAdapter",
    "EmbeddingAdapter",
    "ExternalPluginDiscoveryResult",
    "ParserAdapter",
    "SinkAdapter",
    "SourceAdapter",
]
