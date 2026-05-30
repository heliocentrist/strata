from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import select

from strata.core.config import load_manifest, state_path_from_url
from strata.core.planning import plan
from strata.execution.apply import apply_operations
from strata.sources.registry import snapshot_sources
from strata.state.connection import bootstrap, connect_state
from strata.state.repository import StateRepository
from strata.state.schema import asset_instances, vector_sink


def test_planner_and_apply_use_plugin_contracts_not_canonical_asset_names(
    tmp_path: Path,
) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "a.md").write_text(
        "Alpha document for generic planning.\nIt has enough text to split.\n",
        encoding="utf-8",
    )
    project = tmp_path / "strata.yml"
    project.write_text(
        """
project_id: generic
tenant_id: default

state:
  url: sqlite:///./.strata/state.db

artifacts:
  path: ./.strata/artifacts

sources:
  docs:
    type: local_files
    path: ./docs
    include: ["**/*.md"]

pipeline:
  raw_text:
    source: docs
    operation: markdown_noop
    version: markdown_noop@0.1.0

  segments:
    input: raw_text
    operation: fixed_token_chunker
    version: fixed_token_chunker@0.1.0
    config:
      output_label: segment
      max_chars: 25
      overlap_chars: 5

  vectors:
    input: segments
    operation: fake_embedding
    version: fake_embedding@0.1.0
    config:
      dimensions: 8

  search:
    operation: local_sqlite_vector_sink
    version: sink@0.1.0
    inputs:
      chunk: segments
      embedding: vectors
""",
        encoding="utf-8",
    )
    manifest = load_manifest(project)
    assert manifest.asset_order == ["raw_text", "segments", "vectors", "search"]
    assert manifest.assets["raw_text"].source == "docs"
    assert manifest.assets["segments"].input == "raw_text"
    assert manifest.assets["vectors"].input == "segments"
    assert manifest.assets["search"].inputs == {"chunk": "segments", "embedding": "vectors"}

    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    repo = StateRepository(engine, manifest.context)
    snapshots = snapshot_sources(manifest.sources)

    operations = plan(manifest, repo.snapshot(), snapshots)
    assert [operation.asset_name for operation in operations] == [
        "raw_text",
        "segments",
        "vectors",
        "search",
    ]

    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=operations,
    )

    assert result["failed"] == 0
    assert plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources)) == []
    with repo.engine.connect() as conn:
        rows = conn.execute(select(asset_instances)).mappings().all()
        sink_rows = conn.execute(select(vector_sink)).mappings().all()

    asset_names = {row["asset_name"] for row in rows}
    assert asset_names == {"raw_text", "segments", "vectors", "search"}
    segment_rows = [row for row in rows if row["asset_name"] == "segments"]
    vector_rows = [row for row in rows if row["asset_name"] == "vectors"]
    assert len(segment_rows) > 1
    assert len(vector_rows) == len(segment_rows)
    assert all(str(row["instance_key"]).startswith("a.md#segment:") for row in segment_rows)
    assert all(
        str(row["output_location"]).startswith("artifact://segments/")
        for row in segment_rows
    )
    assert all(str(row["output_location"]).startswith("artifact://vectors/") for row in vector_rows)
    assert len(sink_rows) == len(segment_rows)

    first_manifest_ref = str(segment_rows[0]["output_location"]).removeprefix("artifact://").split("#")[0]
    manifest_doc = json.loads((manifest.artifacts_path / first_manifest_ref).read_text("utf-8"))
    assert manifest_doc["asset_name"] == "segments"
    assert manifest_doc["items"][0]["instance_key"].startswith("a.md#segment:")
