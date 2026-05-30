from __future__ import annotations

from typing import Generic, TypeVar

from strata.plugins.protocols import (
    AdapterMetadata,
    ArtifactCollection,
    ChunkerAdapter,
    EmbeddingAdapter,
    ParserAdapter,
    SinkAdapter,
    SourceAdapter,
)

AdapterT = TypeVar("AdapterT")


class Registry(Generic[AdapterT]):
    def __init__(self, kind: str) -> None:
        self.kind = kind
        self._items: dict[str, AdapterT] = {}
        self._metadata: dict[str, AdapterMetadata] = {}

    def register(
        self,
        name: str,
        adapter: AdapterT,
        *,
        metadata: AdapterMetadata | None = None,
    ) -> None:
        if not name:
            raise ValueError(f"{self.kind} adapter name cannot be empty")
        self._items[name] = adapter
        self._metadata[name] = metadata or AdapterMetadata(
            name=name,
            kind=self.kind,
            source="in-process",
        )

    def get(self, name: str) -> AdapterT:
        try:
            return self._items[name]
        except KeyError as exc:
            known = ", ".join(sorted(self._items)) or "(none)"
            raise ValueError(f"unknown {self.kind} adapter '{name}'. Known: {known}") from exc

    def metadata(self) -> list[AdapterMetadata]:
        return [self._metadata[name] for name in sorted(self._metadata)]


parsers: Registry[ParserAdapter] = Registry("parser")
chunkers: Registry[ChunkerAdapter] = Registry("chunker")
embedders: Registry[EmbeddingAdapter] = Registry("embedding")
sinks: Registry[SinkAdapter] = Registry("sink")
sources: Registry[SourceAdapter] = Registry("source")
artifact_collections: Registry[ArtifactCollection] = Registry("artifact_collection")


def register_parser(name: str, adapter: ParserAdapter) -> None:
    parsers.register(name, adapter)


def register_chunker(name: str, adapter: ChunkerAdapter) -> None:
    chunkers.register(name, adapter)


def register_embedder(name: str, adapter: EmbeddingAdapter) -> None:
    embedders.register(name, adapter)


def register_sink(name: str, adapter: SinkAdapter) -> None:
    sinks.register(name, adapter)


def register_source(name: str, adapter: SourceAdapter) -> None:
    sources.register(name, adapter)


def register_artifact_collection(name: str, adapter: ArtifactCollection) -> None:
    artifact_collections.register(name, adapter)


def get_parser(name: str) -> ParserAdapter:
    return parsers.get(name)


def get_chunker(name: str) -> ChunkerAdapter:
    return chunkers.get(name)


def get_embedder(name: str) -> EmbeddingAdapter:
    return embedders.get(name)


def get_sink(name: str) -> SinkAdapter:
    return sinks.get(name)


def get_source(name: str) -> SourceAdapter:
    return sources.get(name)


def get_artifact_collection(name: str) -> ArtifactCollection:
    return artifact_collections.get(name)


def registered_plugins() -> list[AdapterMetadata]:
    return [
        *parsers.metadata(),
        *chunkers.metadata(),
        *embedders.metadata(),
        *sinks.metadata(),
        *sources.metadata(),
        *artifact_collections.metadata(),
    ]


from strata.plugins.builtins.adapters import register_builtin_plugins  # noqa: E402
from strata.plugins.builtins.collections import register_builtin_collections  # noqa: E402

register_builtin_plugins()
register_builtin_collections()
