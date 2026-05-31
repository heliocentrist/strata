from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from conftest import write_project
from sqlalchemy import select

from strata.core.config import load_manifest, state_path_from_url
from strata.core.operations import OperationInput, OperationOutput
from strata.core.planning import plan
from strata.execution.apply import apply_operations
from strata.plugins.discovery import discover_external_plugins
from strata.plugins.protocols import AdapterMetadata
from strata.plugins.registry import get_operation, register_operation, registered_plugins
from strata.sources.registry import snapshot_sources
from strata.state.connection import bootstrap, connect_state
from strata.state.repository import StateRepository
from strata.state.schema import vector_sink


class OneChunker:
    def run(
        self, inputs: list[OperationInput], config: dict[str, Any]
    ) -> list[OperationOutput]:
        _ = inputs, config
        return [
            OperationOutput(
                instance_key="a.md#chunk:0000",
                data="plugin generated chunk",
            )
        ]


class RecordingBatchEmbedding:
    calls: list[int] = []

    def run(
        self, inputs: list[OperationInput], config: dict[str, Any]
    ) -> list[OperationOutput]:
        dimensions = int(config.get("dimensions", 8))
        self.calls.append(len(inputs))
        return [
            OperationOutput(
                instance_key=input_item.instance_key,
                data=[float(index + 1)] * dimensions,
                parent_input_ids=[input_item.input_id],
                metadata={
                    "chunk_instance_key": input_item.instance_key,
                    "chunk_text": str(input_item.data),
                    "source_item_key": input_item.metadata.get("source_item_key")
                    or input_item.instance_key.split("#", 1)[0],
                },
            )
            for index, input_item in enumerate(inputs)
        ]


def test_custom_chunker_registry_adapter_is_used_by_executor(tmp_path: Path) -> None:
    register_operation(
        "test_one_chunker",
        OneChunker(),
        metadata=AdapterMetadata(
            name="test_one_chunker",
            kind="operation",
            source="test",
        ),
    )
    project = write_project(tmp_path, max_chars=25)
    project.write_text(
        project.read_text(encoding="utf-8").replace(
            "operation: fixed_token_chunker", "operation: test_one_chunker"
        ),
        encoding="utf-8",
    )
    manifest = load_manifest(project)
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    repo = StateRepository(engine, manifest.context)

    snapshots = snapshot_sources(manifest.sources)
    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )

    assert result["failed"] == 0
    with repo.engine.connect() as conn:
        rows = conn.execute(select(vector_sink.c.chunk_text)).all()
    assert [row[0] for row in rows] == ["plugin generated chunk"]


def test_operation_inputs_can_be_batched_per_asset(tmp_path: Path) -> None:
    embedding = RecordingBatchEmbedding()
    register_operation(
        "test_batch_embedding",
        embedding,
        metadata=AdapterMetadata(
            name="test_batch_embedding",
            kind="operation",
            source="test",
        ),
    )
    project = write_project(tmp_path, max_chars=18)
    project.write_text(
        project.read_text(encoding="utf-8")
        .replace("operation: fake_embedding", "operation: test_batch_embedding")
        .replace(
            "  embeddings:\n"
            "    input: chunks\n"
            "    operation: test_batch_embedding\n"
            "    version: fake_embedding@0.1.0\n",
            "  embeddings:\n"
            "    input: chunks\n"
            "    operation: test_batch_embedding\n"
            "    version: fake_embedding@0.1.0\n"
            "    execution:\n"
            "      inputs_per_call: 2\n",
        ),
        encoding="utf-8",
    )
    manifest = load_manifest(project)
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    repo = StateRepository(engine, manifest.context)

    snapshots = snapshot_sources(manifest.sources)
    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )

    assert result["failed"] == 0
    assert max(embedding.calls) == 2
    with repo.engine.connect() as conn:
        sink_rows = conn.execute(select(vector_sink.c.chunk_text)).all()
    assert len(sink_rows) > 1


def test_unknown_plugin_reports_known_names() -> None:
    with pytest.raises(ValueError, match="unknown operation adapter 'missing'"):
        get_operation("missing")


def test_parser_operations_are_pipeline_selectable(tmp_path: Path) -> None:
    md = tmp_path / "a.md"
    txt = tmp_path / "a.txt"
    md.write_text("# Markdown\n", encoding="utf-8")
    txt.write_text("Plain text\n", encoding="utf-8")

    assert (
        get_operation("markdown_noop").run(
            [OperationInput(role="source", asset_name=None, instance_key="a.md", path=md)],
            {},
        )[0].data
        == "# Markdown\n"
    )
    with pytest.raises(ValueError, match="only supports .md"):
        get_operation("markdown_noop").run(
            [OperationInput(role="source", asset_name=None, instance_key="a.txt", path=txt)],
            {},
        )
    assert (
        get_operation("liteparse").run(
            [OperationInput(role="source", asset_name=None, instance_key="a.txt", path=txt)],
            {},
        )[0].data
        == "Plain text\n"
    )


def test_external_plugin_discovery_registers_entry_points(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class ExternalParser:
        def run(
            self, inputs: list[OperationInput], config: dict[str, Any]
        ) -> list[OperationOutput]:
            _ = config
            path = inputs[0].path
            if path is None:
                raise ValueError("path is required")
            return [
                OperationOutput(
                    instance_key=inputs[0].instance_key,
                    data=path.read_text(encoding="utf-8").upper(),
                )
            ]

    class FakeEntryPoint:
        name = "external_markdown"
        value = "fake.module:ExternalParser"

        def load(self) -> type[ExternalParser]:
            return ExternalParser

    class FakeEntryPoints:
        def select(self, *, group: str) -> list[FakeEntryPoint]:
            if group == "strata.operations":
                return [FakeEntryPoint()]
            return []

    monkeypatch.setattr("strata.plugins.discovery.entry_points", lambda: FakeEntryPoints())

    result = discover_external_plugins(force=True)

    assert result.errors == []
    assert result.loaded == [
        AdapterMetadata(
            name="external_markdown",
            kind="operation",
            source="entry_point:strata.operations:fake.module:ExternalParser",
            supported_asset_kinds=(),
        )
    ]
    doc = tmp_path / "external.md"
    doc.write_text("plugin text", encoding="utf-8")
    assert (
        get_operation("external_markdown").run(
            [OperationInput(role="source", asset_name=None, instance_key="external.md", path=doc)],
            {},
        )[0].data
        == "PLUGIN TEXT"
    )
    assert any(plugin.name == "external_markdown" for plugin in registered_plugins())
