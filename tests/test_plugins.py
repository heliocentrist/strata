from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from conftest import write_project
from sqlalchemy import select

from strata.core.config import load_manifest, state_path_from_url
from strata.core.planning import plan
from strata.executors.local import apply_operations
from strata.plugins.discovery import discover_external_plugins
from strata.plugins.protocols import AdapterMetadata
from strata.plugins.registry import get_chunker, get_parser, register_chunker, registered_plugins
from strata.sources.registry import snapshot_sources
from strata.state.connection import bootstrap, connect_state
from strata.state.repository import StateRepository
from strata.state.schema import vector_sink


class OneChunker:
    def chunk(self, text: str, config: dict[str, Any]) -> list[str]:
        return ["plugin generated chunk"]


def test_custom_chunker_registry_adapter_is_used_by_executor(tmp_path: Path) -> None:
    register_chunker("test_one_chunker", OneChunker())
    project = write_project(tmp_path, max_chars=25)
    project.write_text(
        project.read_text(encoding="utf-8").replace(
            "transform: fixed_token_chunker", "transform: test_one_chunker"
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


def test_unknown_plugin_reports_known_names() -> None:
    with pytest.raises(ValueError, match="unknown chunker adapter 'missing'"):
        get_chunker("missing")


def test_parser_adapters_are_pipeline_selectable(tmp_path: Path) -> None:
    md = tmp_path / "a.md"
    txt = tmp_path / "a.txt"
    md.write_text("# Markdown\n", encoding="utf-8")
    txt.write_text("Plain text\n", encoding="utf-8")

    assert get_parser("markdown_noop").parse(md) == "# Markdown\n"
    with pytest.raises(ValueError, match="only supports .md"):
        get_parser("markdown_noop").parse(txt)
    assert get_parser("liteparse").parse(txt) == "Plain text\n"


def test_external_plugin_discovery_registers_entry_points(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class ExternalParser:
        def parse(self, path: Path) -> str:
            return path.read_text(encoding="utf-8").upper()

    class FakeEntryPoint:
        name = "external_markdown"
        value = "fake.module:ExternalParser"

        def load(self) -> type[ExternalParser]:
            return ExternalParser

    class FakeEntryPoints:
        def select(self, *, group: str) -> list[FakeEntryPoint]:
            if group == "strata.parsers":
                return [FakeEntryPoint()]
            return []

    monkeypatch.setattr("strata.plugins.discovery.entry_points", lambda: FakeEntryPoints())

    result = discover_external_plugins(force=True)

    assert result.errors == []
    assert result.loaded == [
        AdapterMetadata(
            name="external_markdown",
            kind="parser",
            source="entry_point:strata.parsers:fake.module:ExternalParser",
            supported_asset_kinds=("parsed",),
        )
    ]
    doc = tmp_path / "external.md"
    doc.write_text("plugin text", encoding="utf-8")
    assert get_parser("external_markdown").parse(doc) == "PLUGIN TEXT"
    assert any(plugin.name == "external_markdown" for plugin in registered_plugins())
