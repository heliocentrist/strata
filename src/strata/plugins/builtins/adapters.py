from __future__ import annotations

from typing import Any, cast

from strata.core.hashing import hash_canonical, sha256_text
from strata.core.operations import OperationInput, OperationOutput
from strata.plugins.builtins.operations import fake_embedding, fixed_char_chunks, parse_pdf
from strata.plugins.protocols import AdapterMetadata
from strata.plugins.registry import register_operation


class MarkdownNoopOperation:
    def run(
        self, inputs: list[OperationInput], config: dict[str, Any]
    ) -> list[OperationOutput]:
        _ = config
        source = _one_input(inputs)
        if source.path is None:
            raise ValueError(f"markdown_noop requires a local path: {source.instance_key}")
        if source.path.suffix.lower() != ".md":
            raise ValueError(f"markdown_noop only supports .md files: {source.path}")
        text = source.path.read_text(encoding="utf-8")
        return [
            OperationOutput(
                instance_key=source.instance_key,
                data=text,
                parent_input_ids=[source.input_id],
                content_hash=sha256_text(text),
                metadata={"source_path": str(source.path), "source_uri": source.uri},
            )
        ]


class LiteParseOperation:
    def run(
        self, inputs: list[OperationInput], config: dict[str, Any]
    ) -> list[OperationOutput]:
        _ = config
        source = _one_input(inputs)
        if source.path is None:
            raise ValueError(f"liteparse requires a local path: {source.instance_key}")
        suffix = source.path.suffix.lower()
        if suffix in {".txt", ".md"}:
            text = source.path.read_text(encoding="utf-8")
        elif suffix == ".pdf":
            text = parse_pdf(source.path)
        else:
            raise ValueError(f"liteparse does not support file extension: {suffix}")
        return [
            OperationOutput(
                instance_key=source.instance_key,
                data=text,
                parent_input_ids=[source.input_id],
                content_hash=sha256_text(text),
                metadata={"source_path": str(source.path), "source_uri": source.uri},
            )
        ]


class FixedTokenChunkerOperation:
    def run(
        self, inputs: list[OperationInput], config: dict[str, Any]
    ) -> list[OperationOutput]:
        upstream = _one_input(inputs)
        text = str(upstream.data)
        label = str(config.get("output_label") or "chunk")
        chunks = fixed_char_chunks(
            text,
            max_chars=int(config.get("max_chars", 1200)),
            overlap_chars=int(config.get("overlap_chars", 120)),
        )
        return [
            OperationOutput(
                instance_key=f"{upstream.instance_key}#{label}:{index:04d}",
                data=chunk,
                parent_input_ids=[upstream.input_id],
                content_hash=sha256_text(chunk),
                metadata={"ordinal": index},
            )
            for index, chunk in enumerate(chunks)
        ]


class FakeEmbeddingOperation:
    def run(
        self, inputs: list[OperationInput], config: dict[str, Any]
    ) -> list[OperationOutput]:
        dimensions = int(config.get("dimensions", 16))
        outputs: list[OperationOutput] = []
        for upstream in inputs:
            vector = fake_embedding(str(upstream.data), dimensions=dimensions)
            outputs.append(
                OperationOutput(
                    instance_key=upstream.instance_key,
                    data=vector,
                    parent_input_ids=[upstream.input_id],
                    content_hash=hash_canonical(vector),
                    metadata={"upstream_instance_key": upstream.instance_key},
                )
            )
        return outputs


class LocalSqliteVectorSinkOperation:
    def run(
        self, inputs: list[OperationInput], config: dict[str, Any]
    ) -> list[OperationOutput]:
        by_role = {input_item.role: input_item for input_item in inputs}
        chunk = by_role.get("chunk")
        embedding = by_role.get("embedding")
        if chunk is None or embedding is None:
            roles = ", ".join(sorted(by_role))
            raise ValueError(
                "local_sqlite_vector_sink requires chunk and embedding inputs, "
                f"got: {roles}"
            )
        vector = cast(list[float], embedding.data)
        source_item_key = str(
            chunk.metadata.get("source_item_key") or _source_key(chunk.instance_key)
        )
        output_hash = hash_canonical(
            {
                "chunk": chunk.instance_key,
                "embedding": vector,
                "source": source_item_key,
            }
        )
        return [
            OperationOutput(
                instance_key=chunk.instance_key,
                output_location=f"sqlite://local_vector_sink/{chunk.instance_key}",
                output_hash=output_hash,
                data={
                    "instance_key": chunk.instance_key,
                    "embedding_fingerprint": embedding.input_fingerprint or "",
                    "source_item_key": source_item_key,
                    "chunk_text": str(chunk.data),
                    "embedding": vector,
                },
                parent_input_ids=[chunk.input_id, embedding.input_id],
                metadata={
                    "source_item_key": source_item_key,
                    "chunk_instance_key": chunk.instance_key,
                },
            )
        ]


def _one_input(inputs: list[OperationInput]) -> OperationInput:
    if len(inputs) != 1:
        raise ValueError(f"expected exactly one input, got {len(inputs)}")
    return inputs[0]


def _source_key(instance_key: str) -> str:
    return instance_key.split("#", 1)[0]


def _metadata(
    name: str,
    *,
    supported_asset_kinds: tuple[str, ...],
) -> AdapterMetadata:
    return AdapterMetadata(
        name=name,
        kind="operation",
        source="built-in",
        supported_asset_kinds=supported_asset_kinds,
    )


def register_builtin_plugins() -> None:
    register_operation(
        "markdown_noop",
        MarkdownNoopOperation(),
        metadata=_metadata(
            "markdown_noop",
            supported_asset_kinds=("parsed",),
        ),
    )
    register_operation(
        "liteparse",
        LiteParseOperation(),
        metadata=_metadata(
            "liteparse",
            supported_asset_kinds=("parsed",),
        ),
    )
    register_operation(
        "auto",
        LiteParseOperation(),
        metadata=_metadata("auto", supported_asset_kinds=("parsed",)),
    )
    register_operation(
        "fixed_token_chunker",
        FixedTokenChunkerOperation(),
        metadata=_metadata(
            "fixed_token_chunker",
            supported_asset_kinds=("chunks",),
        ),
    )
    register_operation(
        "fake_embedding",
        FakeEmbeddingOperation(),
        metadata=_metadata(
            "fake_embedding",
            supported_asset_kinds=("embeddings",),
        ),
    )
    register_operation(
        "local_sqlite_vector_sink",
        LocalSqliteVectorSinkOperation(),
        metadata=_metadata(
            "local_sqlite_vector_sink",
            supported_asset_kinds=("sink",),
        ),
    )
