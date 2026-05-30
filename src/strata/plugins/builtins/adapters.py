from __future__ import annotations

from pathlib import Path
from typing import Any

from strata.core.models import HybridChunkEmbeddingItem, SinkWriteResult
from strata.plugins.builtins.operations import fake_embedding, fixed_char_chunks, parse_pdf
from strata.plugins.registry import (
    register_chunker,
    register_embedder,
    register_parser,
    register_sink,
)


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
    def write_hybrid(
        self,
        *,
        item: HybridChunkEmbeddingItem,
        config: dict[str, Any],
    ) -> SinkWriteResult:
        _ = config
        output_hash = fake_sink_output_hash(item)
        return SinkWriteResult(
            output_location=f"sqlite://local_vector_sink/{item.document_id}",
            output_hash=output_hash,
            external_id=item.document_id,
        )

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


def fake_sink_output_hash(item: HybridChunkEmbeddingItem) -> str:
    from strata.core.hashing import hash_canonical

    return hash_canonical(
        {
            "chunk": item.chunk.instance_key,
            "embedding": item.embedding.vector,
            "source": item.source.item_key,
        }
    )


def register_builtin_plugins() -> None:
    register_parser("markdown_noop", MarkdownNoopParser())
    register_parser("liteparse", LiteParseParser())
    register_parser("auto", LiteParseParser())
    register_chunker("fixed_token_chunker", FixedTokenChunker())
    register_embedder("fake_embedding", FakeEmbedding())
    register_sink("local_sqlite_vector_sink", LocalSqliteVectorSink())
