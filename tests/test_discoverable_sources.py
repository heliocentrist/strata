from __future__ import annotations

from pathlib import Path

from sqlalchemy import select

from strata.core.config import load_manifest, state_path_from_url
from strata.core.models import Manifest
from strata.core.planning import plan
from strata.execution.apply import apply_operations
from strata.sources.registry import snapshot_sources
from strata.state.connection import bootstrap, connect_state
from strata.state.repository import StateRepository
from strata.state.schema import asset_instances, source_state, vector_sink


def test_object_store_source_discovers_full_and_delta_loads_without_manifest(
    tmp_path: Path,
) -> None:
    project = _write_object_store_project(tmp_path)
    objects = tmp_path / "objects"
    (objects / "a.md").write_text("alpha beta gamma delta", encoding="utf-8")
    (objects / "b.md").write_text("red blue green gold", encoding="utf-8")
    manifest, repo = _repo(project)

    snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    operations = plan(manifest, repo.snapshot(), snapshots)
    assert [operation.asset_name for operation in operations] == [
        "parsed",
        "chunks",
        "embeddings",
        "sink",
    ]
    assert [operation.scope.item_keys for operation in operations] == [["a.md", "b.md"]] * 4
    with repo.engine.connect() as conn:
        assert conn.execute(select(source_state)).all() == []

    first = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=operations,
    )
    assert first["failed"] == 0
    assert _source_state_keys(repo) == ["a.md", "b.md"]
    assert _source_scoped_assets(repo) == {
        ("chunks", "docs", "a.md"),
        ("chunks", "docs", "b.md"),
        ("embeddings", "docs", "a.md"),
        ("embeddings", "docs", "b.md"),
        ("parsed", "docs", "a.md"),
        ("parsed", "docs", "b.md"),
        ("sink", "docs", "a.md"),
        ("sink", "docs", "b.md"),
    }
    assert plan(
        manifest,
        repo.snapshot(),
        snapshot_sources(manifest.sources, root=manifest.root),
    ) == []

    (objects / "a.md").write_text("alpha beta changed gamma delta", encoding="utf-8")
    changed_snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    changed_operations = plan(manifest, repo.snapshot(), changed_snapshots)
    assert [operation.reason for operation in changed_operations] == ["source_changed"] * 4
    assert [operation.scope.item_keys for operation in changed_operations] == [["a.md"]] * 4

    assert not hasattr(repo, "materialized_descendants")
    changed = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=changed_snapshots,
        operations=changed_operations,
    )
    assert changed["failed"] == 0

    (objects / "b.md").unlink()
    delete_snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    delete_operations = plan(manifest, repo.snapshot(), delete_snapshots)
    assert [
        (operation.op_type, operation.asset_name, operation.scope.item_key)
        for operation in delete_operations
    ] == [("delete_scope", "sink", "b.md")]
    deleted = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=delete_snapshots,
        operations=delete_operations,
    )
    assert deleted["failed"] == 0
    with repo.engine.connect() as conn:
        active_b_instances = conn.execute(
            select(asset_instances).where(
                asset_instances.c.status == "materialized",
                asset_instances.c.instance_key.like("b.md%"),
            )
        ).all()
        b_sink_rows = conn.execute(
            select(vector_sink).where(vector_sink.c.instance_key.like("b.md%"))
        ).all()
    assert active_b_instances == []
    assert b_sink_rows == []


def _write_object_store_project(root: Path) -> Path:
    (root / "objects").mkdir()
    project = root / "strata.yml"
    project.write_text(
        """
project_id: discoverable-source
tenant_id: default

state:
  url: sqlite:///./.strata/state.db

artifacts:
  path: ./.strata/artifacts

sources:
  docs:
    type: object_store
    uri: ./objects
    include: ["**/*.md"]

pipeline:
  parsed:
    source: docs
    operation: markdown_noop
    version: markdown_noop@0.1.0

  chunks:
    input: parsed
    operation: fixed_token_chunker
    version: fixed_token_chunker@0.1.0
    config:
      output_label: chunk
      max_chars: 12
      overlap_chars: 0

  embeddings:
    input: chunks
    operation: fake_embedding
    version: fake_embedding@0.1.0
    config:
      dimensions: 4

  sink:
    input: embeddings
    operation: local_sqlite_vector_sink
    version: local_sqlite_vector_sink@0.1.0
""",
        encoding="utf-8",
    )
    return project


def _repo(project: Path) -> tuple[Manifest, StateRepository]:
    manifest = load_manifest(project)
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    return manifest, StateRepository(engine, manifest.context)


def _source_state_keys(repo: StateRepository) -> list[str]:
    with repo.engine.connect() as conn:
        rows = conn.execute(select(source_state.c.item_key).order_by(source_state.c.item_key))
    return [str(row[0]) for row in rows]


def _source_scoped_assets(repo: StateRepository) -> set[tuple[str, str, str]]:
    with repo.engine.connect() as conn:
        rows = conn.execute(
            select(
                asset_instances.c.asset_name,
                asset_instances.c.source_name,
                asset_instances.c.source_item_key,
            ).where(asset_instances.c.status == "materialized")
        )
    return {(str(row[0]), str(row[1]), str(row[2])) for row in rows}
