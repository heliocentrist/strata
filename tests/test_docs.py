from __future__ import annotations

import json
from pathlib import Path

from conftest import write_project

from strata.core.config import load_manifest, state_path_from_url
from strata.core.planning import plan
from strata.execution.apply import apply_operations
from strata.sources.registry import snapshot_sources
from strata.state.connection import bootstrap, connect_state
from strata.state.repository import StateRepository
from strata.tools.docs import build_docs_site


def setup_repo(project: Path) -> tuple[object, StateRepository]:
    manifest = load_manifest(project)
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    return manifest, StateRepository(engine, manifest.context)


def test_docs_build_writes_searchable_static_site(tmp_path: Path) -> None:
    project = write_project(tmp_path, max_chars=25)
    manifest, repo = setup_repo(project)
    snapshots = snapshot_sources(manifest.sources)
    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )

    output = tmp_path / "site"
    result = build_docs_site(manifest=manifest, repo=repo, output_path=output)

    assert result.instance_count > 0
    assert result.edge_count > 0
    assert (output / "index.html").exists()
    assert (output / "styles.css").exists()
    assert (output / "app.js").exists()
    data = json.loads((output / "data.json").read_text(encoding="utf-8"))
    assert data["project"]["project_id"] == "default"
    assert data["counts"]["by_asset"]["parsed"] == 1
    assert any(
        instance["asset_name"] == "chunks"
        and "alpha document" in instance["search_text"]
        and instance["upstream_ids"]
        and instance["downstream_ids"]
        for instance in data["instances"]
    )
