from __future__ import annotations

from pathlib import Path

from conftest import write_project

from strata.core.config import load_manifest, state_path_from_url
from strata.core.models import Manifest
from strata.core.planning import plan
from strata.execution.apply import apply_operations
from strata.sources.registry import snapshot_sources
from strata.state.connection import bootstrap, connect_state
from strata.state.repository import StateRepository
from strata.tools.testing import run_tests


def setup_repo(project: Path) -> tuple[Manifest, StateRepository]:
    manifest = load_manifest(project)
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    return manifest, StateRepository(engine, manifest.context)


def test_configured_asset_tests_pass_after_apply(tmp_path: Path) -> None:
    project = write_project(tmp_path, max_chars=25)
    manifest, repo = setup_repo(project)
    snapshots = snapshot_sources(manifest.sources)

    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )

    results = run_tests(manifest=manifest, repo=repo)
    assert {result.name: result.status for result in results} == {
        "chunks_not_empty": "passed",
        "embeddings_have_expected_dimensions": "passed",
        "chunks_keep_source_identity": "passed",
    }


def test_configured_asset_tests_fail_without_materialized_state(tmp_path: Path) -> None:
    project = write_project(tmp_path)
    manifest, repo = setup_repo(project)

    results = run_tests(manifest=manifest, repo=repo)

    assert all(result.status == "failed" for result in results)
    assert any("no materialized instances" in result.message for result in results)
