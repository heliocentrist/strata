from __future__ import annotations

import json
from pathlib import Path

from strata.core.config import load_manifest, state_path_from_url
from strata.core.planning import plan
from strata.execution.apply import apply_operations
from strata.sources.registry import snapshot_sources
from strata.state.connection import bootstrap, connect_state
from strata.state.repository import StateRepository


def _write_object_project(tmp_path: Path, *, mode: str = "authoritative_snapshot") -> Path:
    raw = tmp_path / "objects"
    raw.mkdir()
    (raw / "a.md").write_text("Alpha document with enough content for chunks.", encoding="utf-8")
    manifest_json = tmp_path / "source.json"
    manifest_json.write_text(
        json.dumps(
            {
                "mode": mode,
                "connection_id": "fixture",
                "items": [
                    {
                        "item_key": "remote/a.md",
                        "object_uri": str(raw / "a.md"),
                        "content_hash": "etag-a-v1",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    project = tmp_path / "strata.yml"
    manifest_uri = json.dumps(str(manifest_json))
    project.write_text(
        f"""
project_id: object-source-test
tenant_id: tenant-a
state:
  url: sqlite:///./.strata/state.db
artifacts:
  path: ./.strata/artifacts
sources:
  docs:
    type: object_manifest
    manifest_uri: {manifest_uri}
pipeline:
  parsed:
    source: docs
    operation: liteparse
    version: parsed@0.1.0
  chunks:
    input: parsed
    operation: fixed_token_chunker
    version: fixed_token_chunker@0.1.0
    config:
      output_label: chunk
      max_chars: 80
      overlap_chars: 10
  embeddings:
    input: chunks
    operation: fake_embedding
    version: fake_embedding@0.1.0
    config:
      dimensions: 8
  sink:
    inputs:
      chunk: chunks
      embedding: embeddings
    operation: local_sqlite_vector_sink
    version: local_sqlite_vector_sink@0.1.0
""",
        encoding="utf-8",
    )
    return project


def _repo(project: Path) -> tuple[object, StateRepository]:
    manifest = load_manifest(project)
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    return manifest, StateRepository(engine, manifest.context)


def test_object_manifest_source_runs_full_pipeline(tmp_path: Path) -> None:
    project = _write_object_project(tmp_path)
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)

    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )

    assert result["failed"] == 0
    assert result["built"] > 0
    assert (
        plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources, root=manifest.root))
        == []
    )


def test_incremental_manifest_absence_does_not_delete(tmp_path: Path) -> None:
    project = _write_object_project(tmp_path)
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )

    source_manifest = tmp_path / "source.json"
    source_manifest.write_text(
        json.dumps({"mode": "incremental_delta", "connection_id": "fixture", "items": []}),
        encoding="utf-8",
    )
    assert (
        plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources, root=manifest.root))
        == []
    )


def test_incremental_manifest_explicit_delete_emits_delete(tmp_path: Path) -> None:
    project = _write_object_project(tmp_path)
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )

    source_manifest = tmp_path / "source.json"
    source_manifest.write_text(
        json.dumps(
            {
                "mode": "incremental_delta",
                "connection_id": "fixture",
                "items": [{"item_key": "remote/a.md", "deleted": True}],
            }
        ),
        encoding="utf-8",
    )
    operations = plan(
        manifest,
        repo.snapshot(),
        snapshot_sources(manifest.sources, root=manifest.root),
    )
    assert [operation.op_type for operation in operations] == ["delete_scope"]
