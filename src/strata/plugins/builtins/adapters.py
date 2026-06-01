from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from strata.core.hashing import hash_canonical, sha256_text
from strata.core.operations import OperationInput, OperationOutput
from strata.plugins.builtins.operations import fake_embedding, fixed_char_chunks, parse_pdf
from strata.plugins.protocols import AdapterMetadata
from strata.plugins.registry import register_operation
from strata.state.connection import bootstrap, connect_state
from strata.state.schema import vector_sink


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
                    metadata={
                        "upstream_instance_key": upstream.instance_key,
                        "chunk_instance_key": upstream.instance_key,
                        "chunk_text": str(upstream.data),
                        "source_name": upstream.source_name,
                        "source_item_key": (
                            upstream.metadata.get("source_item_key")
                            or _source_key(upstream.instance_key)
                        ),
                    },
                )
            )
        return outputs


class LocalSqliteVectorSinkOperation:
    def run(
        self, inputs: list[OperationInput], config: dict[str, Any]
    ) -> list[OperationOutput]:
        outputs: list[OperationOutput] = []
        rows: list[dict[str, Any]] = []
        for embedding in inputs:
            vector = cast(list[float], embedding.data)
            chunk_instance_key = str(
                embedding.metadata.get("chunk_instance_key") or embedding.instance_key
            )
            source_item_key = str(
                embedding.metadata.get("source_item_key") or _source_key(chunk_instance_key)
            )
            source_name = str(embedding.metadata.get("source_name") or embedding.source_name or "")
            chunk_text = str(embedding.metadata.get("chunk_text") or "")
            output_hash = hash_canonical(
                {
                    "chunk": chunk_instance_key,
                    "embedding": vector,
                    "source_name": source_name,
                    "source": source_item_key,
                }
            )
            outputs.append(
                OperationOutput(
                    instance_key=chunk_instance_key,
                    output_location=f"sqlite://local_vector_sink/{chunk_instance_key}",
                    output_hash=output_hash,
                    parent_input_ids=[embedding.input_id],
                    metadata={
                        "source_name": source_name,
                        "source_item_key": source_item_key,
                        "chunk_instance_key": chunk_instance_key,
                    },
                )
            )
            rows.append(
                {
                    "instance_key": chunk_instance_key,
                    "embedding_fingerprint": embedding.input_fingerprint or "",
                    "source_name": source_name,
                    "source_item_key": source_item_key,
                    "chunk_text": chunk_text,
                    "embedding": vector,
                }
            )
        _write_local_vector_rows(config, rows)
        return outputs


def _one_input(inputs: list[OperationInput]) -> OperationInput:
    if len(inputs) != 1:
        raise ValueError(f"expected exactly one input, got {len(inputs)}")
    return inputs[0]


def _source_key(instance_key: str) -> str:
    return instance_key.split("#", 1)[0]


def _write_local_vector_rows(config: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    runtime = config.get("_strata")
    if not isinstance(runtime, dict):
        raise ValueError("local_sqlite_vector_sink requires Strata runtime context")
    state_url = str(runtime["state_url"])
    project_root = Path(str(runtime["project_root"]))
    project_id = str(runtime["project_id"])
    tenant_id = str(runtime["tenant_id"])
    engine = connect_state(_state_path_from_url(state_url, project_root))
    bootstrap(engine)
    timestamp = datetime.now(UTC)
    values = [
        {
            "project_id": project_id,
            "tenant_id": tenant_id,
            "source_name": row["source_name"],
            "source_item_key": row["source_item_key"],
            "instance_key": row["instance_key"],
            "embedding_fingerprint": row["embedding_fingerprint"],
            "chunk_text": row["chunk_text"],
            "embedding_json": json.dumps(row["embedding"]),
            "updated_at": timestamp,
        }
        for row in rows
    ]
    with engine.begin() as conn:
        stmt = sqlite_insert(vector_sink).values(values)
        conn.execute(
            stmt.on_conflict_do_update(
                index_elements=[
                    vector_sink.c.project_id,
                    vector_sink.c.tenant_id,
                    vector_sink.c.source_name,
                    vector_sink.c.instance_key,
                    vector_sink.c.embedding_fingerprint,
                ],
                set_={
                    "source_item_key": stmt.excluded.source_item_key,
                    "chunk_text": stmt.excluded.chunk_text,
                    "embedding_json": stmt.excluded.embedding_json,
                    "updated_at": stmt.excluded.updated_at,
                },
            )
        )


def _state_path_from_url(url: str, root: Path) -> Path:
    if url.startswith("sqlite:///"):
        raw_path = url.removeprefix("sqlite:///")
        path = Path(raw_path)
        if not path.is_absolute():
            path = root / path
        return path
    raise ValueError(f"unsupported state url: {url}")


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
