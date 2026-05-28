from __future__ import annotations

from pathlib import Path
from typing import Any, Generic, Protocol, TypeVar

from strata.transforms import fake_embedding, fixed_char_chunks, parse_pdf


class ParserAdapter(Protocol):
    def parse(self, path: Path) -> str: ...


class ChunkerAdapter(Protocol):
    def chunk(self, text: str, config: dict[str, Any]) -> list[str]: ...


class EmbeddingAdapter(Protocol):
    def embed(self, text: str, config: dict[str, Any]) -> list[float]: ...


class SinkAdapter(Protocol):
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


AdapterT = TypeVar("AdapterT")


class Registry(Generic[AdapterT]):
    def __init__(self, kind: str) -> None:
        self.kind = kind
        self._items: dict[str, AdapterT] = {}

    def register(self, name: str, adapter: AdapterT) -> None:
        if not name:
            raise ValueError(f"{self.kind} adapter name cannot be empty")
        self._items[name] = adapter

    def get(self, name: str) -> AdapterT:
        try:
            return self._items[name]
        except KeyError as exc:
            known = ", ".join(sorted(self._items)) or "(none)"
            raise ValueError(f"unknown {self.kind} adapter '{name}'. Known: {known}") from exc


parsers: Registry[ParserAdapter] = Registry("parser")
chunkers: Registry[ChunkerAdapter] = Registry("chunker")
embedders: Registry[EmbeddingAdapter] = Registry("embedding")
sinks: Registry[SinkAdapter] = Registry("sink")


def register_parser(name: str, adapter: ParserAdapter) -> None:
    parsers.register(name, adapter)


def register_chunker(name: str, adapter: ChunkerAdapter) -> None:
    chunkers.register(name, adapter)


def register_embedder(name: str, adapter: EmbeddingAdapter) -> None:
    embedders.register(name, adapter)


def register_sink(name: str, adapter: SinkAdapter) -> None:
    sinks.register(name, adapter)


def get_parser(name: str) -> ParserAdapter:
    return parsers.get(name)


def get_chunker(name: str) -> ChunkerAdapter:
    return chunkers.get(name)


def get_embedder(name: str) -> EmbeddingAdapter:
    return embedders.get(name)


def get_sink(name: str) -> SinkAdapter:
    return sinks.get(name)


class MarkdownNoopParser:
    def parse(self, path: Path) -> str:
        if path.suffix.lower() != ".md":
            raise ValueError(f"markdown_noop parser only supports .md files: {path}")
        return path.read_text(encoding="utf-8")


class LiteParseParser:
    def parse(self, path: Path) -> str:
        suffix = path.suffix.lower()
        if suffix in {".txt", ".md"}:
            return path.read_text(encoding="utf-8")
        if suffix == ".pdf":
            return parse_pdf(path)
        raise ValueError(f"liteparse parser does not support file extension: {suffix}")


class FixedTokenChunker:
    def chunk(self, text: str, config: dict[str, Any]) -> list[str]:
        return fixed_char_chunks(
            text,
            max_chars=int(config.get("max_chars", 1200)),
            overlap_chars=int(config.get("overlap_chars", 120)),
        )


class FakeEmbedding:
    def embed(self, text: str, config: dict[str, Any]) -> list[float]:
        return fake_embedding(text, dimensions=int(config.get("dimensions", 16)))


class LocalSqliteVectorSink:
    def write(
        self,
        *,
        repo: Any,
        instance_key: str,
        embedding_fingerprint: str,
        source_item_key: str,
        chunk_text: str,
        embedding: list[float],
    ) -> None:
        repo.upsert_vector(
            instance_key=instance_key,
            embedding_fingerprint=embedding_fingerprint,
            source_item_key=source_item_key,
            chunk_text=chunk_text,
            embedding=embedding,
        )


register_parser("markdown_noop", MarkdownNoopParser())
register_parser("liteparse", LiteParseParser())
register_parser("auto", LiteParseParser())
register_chunker("fixed_token_chunker", FixedTokenChunker())
register_embedder("fake_embedding", FakeEmbedding())
register_sink("local_sqlite_vector_sink", LocalSqliteVectorSink())
