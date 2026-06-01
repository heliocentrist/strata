from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from conftest import write_project
from sqlalchemy import delete, insert, select

import strata.execution.apply as apply_module
from strata.core.config import load_manifest, state_path_from_url
from strata.core.models import Manifest
from strata.core.operations import OperationInput, OperationOutput
from strata.core.planning import plan
from strata.execution.apply import apply_operations
from strata.plugins.registry import register_operation
from strata.sources.registry import snapshot_sources
from strata.state.connection import bootstrap, connect_state
from strata.state.repository import StateRepository, now
from strata.state.schema import (
    apply_locks,
    asset_instances,
    asset_scope_completions,
    runs,
    source_checkpoints,
    vector_sink,
)
from strata.tools.doctor import run_doctor


class AlwaysFailsOperation:
    def run(
        self,
        inputs: list[OperationInput],
        config: dict[str, Any],
    ) -> list[OperationOutput]:
        _ = inputs, config
        raise RuntimeError("intentional reliability test failure")


def test_lock_failure_does_not_create_running_run(tmp_path: Path) -> None:
    project = write_project(tmp_path)
    manifest, repo = _repo(project)
    timestamp = now()
    with repo.begin() as conn:
        conn.execute(
            insert(apply_locks).values(
                project_id=manifest.context.project_id,
                tenant_id=manifest.context.tenant_id,
                run_id="active-run",
                acquired_at=timestamp,
            )
        )

    snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    operations = plan(manifest, repo.snapshot(), snapshots)
    with pytest.raises(RuntimeError, match="apply already running"):
        apply_operations(
            manifest=manifest,
            repo=repo,
            source_snapshots=snapshots,
            operations=operations,
        )

    with repo.engine.connect() as conn:
        run_rows = conn.execute(select(runs)).all()
    assert run_rows == []


def test_release_lock_does_not_delete_another_runs_lock(tmp_path: Path) -> None:
    project = write_project(tmp_path)
    manifest, repo = _repo(project)
    timestamp = now()
    with repo.begin() as conn:
        conn.execute(
            insert(apply_locks).values(
                project_id=manifest.context.project_id,
                tenant_id=manifest.context.tenant_id,
                run_id="new-run",
                acquired_at=timestamp,
            )
        )

    repo.release_lock("old-run")

    with repo.engine.connect() as conn:
        lock_rows = conn.execute(select(apply_locks.c.run_id)).scalars().all()
    assert lock_rows == ["new-run"]


def test_existing_lock_blocks_until_doctor_fix(tmp_path: Path) -> None:
    project = write_project(tmp_path)
    manifest, repo = _repo(project)
    timestamp = now()
    with repo.begin() as conn:
        conn.execute(
            insert(apply_locks).values(
                project_id=manifest.context.project_id,
                tenant_id=manifest.context.tenant_id,
                run_id="locked-run",
                acquired_at=timestamp,
            )
        )

    with pytest.raises(RuntimeError, match="apply already running"):
        repo.acquire_lock("replacement-run")

    result = run_doctor(manifest=manifest, repo=repo)
    assert any(issue.code == "apply_lock" and issue.fixable for issue in result.issues)

    fixed = run_doctor(manifest=manifest, repo=repo, fix=True)
    assert fixed.fixed == 1
    repo.acquire_lock("replacement-run")

    with repo.engine.connect() as conn:
        lock_rows = conn.execute(select(apply_locks.c.run_id)).scalars().all()
    assert lock_rows == ["replacement-run"]


def test_failed_apply_does_not_advance_source_checkpoint(tmp_path: Path) -> None:
    register_operation("test_always_fails_reliability", AlwaysFailsOperation())
    project = write_project(tmp_path)
    project.write_text(
        project.read_text(encoding="utf-8").replace(
            "operation: fixed_token_chunker",
            "operation: test_always_fails_reliability",
        ),
        encoding="utf-8",
    )
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)

    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )

    assert result["failed"] > 0
    with repo.engine.connect() as conn:
        checkpoints = conn.execute(select(source_checkpoints)).all()
        run_statuses = conn.execute(select(runs.c.status)).scalars().all()
    assert checkpoints == []
    assert run_statuses == ["failed"]


def test_unexpected_apply_exception_marks_run_failed_and_releases_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = write_project(tmp_path)
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    operations = plan(manifest, repo.snapshot(), snapshots)

    def fail_layering(*args: Any, **kwargs: Any) -> None:
        _ = args, kwargs
        raise RuntimeError("unexpected apply crash")

    monkeypatch.setattr(apply_module, "operation_layers", fail_layering)

    with pytest.raises(RuntimeError, match="unexpected apply crash"):
        apply_operations(
            manifest=manifest,
            repo=repo,
            source_snapshots=snapshots,
            operations=operations,
        )

    with repo.engine.connect() as conn:
        run_statuses = conn.execute(select(runs.c.status)).scalars().all()
        lock_rows = conn.execute(select(apply_locks)).all()

    assert run_statuses == ["failed"]
    assert lock_rows == []


def test_delete_is_scoped_by_source_name_and_item_key(tmp_path: Path) -> None:
    project = _write_two_source_project(tmp_path)
    source_a = tmp_path / "source_a"
    source_b = tmp_path / "source_b"
    source_a.mkdir()
    source_b.mkdir()
    (source_a / "shared.md").write_text(
        "same file name in source A with enough words for chunks",
        encoding="utf-8",
    )
    (source_b / "shared.md").write_text(
        "same file name in source B with enough words for chunks",
        encoding="utf-8",
    )
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    first = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )
    assert first["failed"] == 0

    (source_a / "shared.md").unlink()
    delete_snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    operations = plan(manifest, repo.snapshot(), delete_snapshots)
    assert [
        (operation.op_type, operation.asset_name, operation.scope.source_name)
        for operation in operations
    ] == [("delete_scope", "sink_a", "docs_a")]

    deleted = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=delete_snapshots,
        operations=operations,
    )

    assert deleted["failed"] == 0
    with repo.engine.connect() as conn:
        active_a = conn.execute(
            select(asset_instances).where(
                asset_instances.c.status == "materialized",
                asset_instances.c.source_name == "docs_a",
                asset_instances.c.source_item_key == "shared.md",
            )
        ).all()
        active_b = conn.execute(
            select(asset_instances).where(
                asset_instances.c.status == "materialized",
                asset_instances.c.source_name == "docs_b",
                asset_instances.c.source_item_key == "shared.md",
            )
        ).all()
        sink_a = conn.execute(
            select(vector_sink).where(
                vector_sink.c.source_name == "docs_a",
                vector_sink.c.source_item_key == "shared.md",
            )
        ).all()
        sink_b = conn.execute(
            select(vector_sink).where(
                vector_sink.c.source_name == "docs_b",
                vector_sink.c.source_item_key == "shared.md",
            )
        ).all()
    assert active_a == []
    assert sink_a == []
    assert active_b
    assert sink_b


def test_plan_detects_missing_non_first_fanout_instance(tmp_path: Path) -> None:
    project = write_project(tmp_path, max_chars=18)
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )
    assert result["failed"] == 0

    with repo.begin() as conn:
        chunk_rows = conn.execute(
            select(asset_instances.c.id, asset_instances.c.instance_key)
            .where(
                asset_instances.c.asset_name == "chunks",
                asset_instances.c.status == "materialized",
            )
            .order_by(asset_instances.c.instance_key)
        ).all()
        assert len(chunk_rows) > 1
        non_first_chunk = next(
            row for row in chunk_rows if not str(row.instance_key).endswith("#chunk:0000")
        )
        conn.execute(delete(asset_instances).where(asset_instances.c.id == non_first_chunk.id))

    operations = plan(
        manifest,
        repo.snapshot(),
        snapshot_sources(manifest.sources, root=manifest.root),
    )

    assert [operation.asset_name for operation in operations] == [
        "chunks",
        "embeddings",
        "sink",
    ]


def test_plan_uses_scope_completions_not_materialized_instance_fallback(
    tmp_path: Path,
) -> None:
    project = write_project(tmp_path, max_chars=18)
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )
    assert result["failed"] == 0

    with repo.begin() as conn:
        conn.execute(
            delete(asset_scope_completions).where(
                asset_scope_completions.c.asset_name == "chunks"
            )
        )
        chunk_count = conn.execute(
            select(asset_instances.c.id).where(
                asset_instances.c.asset_name == "chunks",
                asset_instances.c.status == "materialized",
            )
        ).all()
    assert chunk_count

    operations = plan(
        manifest,
        repo.snapshot(),
        snapshot_sources(manifest.sources, root=manifest.root),
    )

    assert [operation.asset_name for operation in operations] == [
        "chunks",
        "embeddings",
        "sink",
    ]


def _repo(project: Path) -> tuple[Manifest, StateRepository]:
    manifest = load_manifest(project)
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    return manifest, StateRepository(engine, manifest.context)


def _write_two_source_project(root: Path) -> Path:
    project = root / "strata.yml"
    project.write_text(
        """
project_id: two-source-delete
tenant_id: default

state:
  url: sqlite:///./.strata/state.db

artifacts:
  path: ./.strata/artifacts

sources:
  docs_a:
    type: local_files
    path: ./source_a
    include: ["**/*.md"]
  docs_b:
    type: local_files
    path: ./source_b
    include: ["**/*.md"]

pipeline:
  parsed_a:
    source: docs_a
    operation: markdown_noop
    version: markdown_noop@0.1.0

  chunks_a:
    input: parsed_a
    operation: fixed_token_chunker
    version: fixed_token_chunker@0.1.0
    config:
      output_label: chunk
      max_chars: 20
      overlap_chars: 0

  embeddings_a:
    input: chunks_a
    operation: fake_embedding
    version: fake_embedding@0.1.0
    config:
      dimensions: 4

  sink_a:
    input: embeddings_a
    operation: local_sqlite_vector_sink
    version: local_sqlite_vector_sink@0.1.0

  parsed_b:
    source: docs_b
    operation: markdown_noop
    version: markdown_noop@0.1.0

  chunks_b:
    input: parsed_b
    operation: fixed_token_chunker
    version: fixed_token_chunker@0.1.0
    config:
      output_label: chunk
      max_chars: 20
      overlap_chars: 0

  embeddings_b:
    input: chunks_b
    operation: fake_embedding
    version: fake_embedding@0.1.0
    config:
      dimensions: 4

  sink_b:
    input: embeddings_b
    operation: local_sqlite_vector_sink
    version: local_sqlite_vector_sink@0.1.0
""",
        encoding="utf-8",
    )
    return project
