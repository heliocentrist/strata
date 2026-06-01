from __future__ import annotations

from pathlib import Path

from conftest import write_project
from sqlalchemy import select

from strata.core.config import load_manifest, state_path_from_url
from strata.core.models import Manifest
from strata.core.planning import plan
from strata.execution.apply import apply_operations
from strata.sources.registry import snapshot_sources
from strata.state.connection import bootstrap, connect_state
from strata.state.repository import StateRepository
from strata.state.schema import asset_instances
from strata.tools.inspect import inspect_instance


def setup_repo(project: Path) -> tuple[Manifest, StateRepository]:
    manifest = load_manifest(project)
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    return manifest, StateRepository(engine, manifest.context)


def test_inspect_embedding_shows_upstream_chunk_and_downstream_sink(tmp_path: Path) -> None:
    project = write_project(tmp_path, max_chars=25)
    manifest, repo = setup_repo(project)
    snapshots = snapshot_sources(manifest.sources)
    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )

    with repo.engine.connect() as conn:
        row = conn.execute(
            select(asset_instances).where(asset_instances.c.asset_name == "embeddings")
        ).mappings().first()
    assert row is not None

    report = inspect_instance(
        manifest=manifest,
        repo=repo,
        asset_name="embeddings",
        instance_key=str(row["instance_key"]),
    )

    assert report.found
    assert report.asset is not None
    assert report.asset["asset_name"] == "embeddings"
    assert report.artifact is not None
    assert report.artifact["kind"] == "list"
    assert report.artifact["length"] == 8
    assert [item["asset_name"] for item in report.upstreams or []] == ["chunks", "parsed"]
    assert [item["asset_name"] for item in report.downstreams or []] == ["sink"]


def test_inspect_missing_instance_reports_not_found(tmp_path: Path) -> None:
    project = write_project(tmp_path)
    manifest, repo = setup_repo(project)

    report = inspect_instance(
        manifest=manifest,
        repo=repo,
        asset_name="chunks",
        instance_key="missing",
    )

    assert not report.found
    assert report.message == "No materialized instance found for chunks/missing"
