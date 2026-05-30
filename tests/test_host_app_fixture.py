from __future__ import annotations

from pathlib import Path

from strata.api import open_project, plan_project
from strata.host_testing import HostSourceItem, TestHostApp
from strata.state.schema import vector_sink


def test_host_app_stages_batches_and_runs_strata(tmp_path: Path) -> None:
    source_a = tmp_path / "incoming-a.md"
    source_b = tmp_path / "incoming-b.md"
    source_a.write_text("Alpha document with enough content for chunking.", encoding="utf-8")
    source_b.write_text("Bravo document with enough content for chunking.", encoding="utf-8")

    host = TestHostApp(tmp_path / "host")
    host.stage_batch([HostSourceItem(item_key="economy/a.md", source_file=source_a)])
    host.stage_batch([HostSourceItem(item_key="economy/b.md", source_file=source_b)])
    host.write_source_manifest()
    project = host.write_project()

    result = host.run_strata()

    assert result["failed"] == 0
    assert result["built"] > 0
    assert plan_project(project) == []
    handle = open_project(project)
    with handle.engine.connect() as conn:
        sink_rows = conn.execute(vector_sink.select()).all()
    assert len(sink_rows) == 2


def test_host_app_incremental_delta_does_not_delete_absent_items(tmp_path: Path) -> None:
    source_a = tmp_path / "incoming-a.md"
    source_b = tmp_path / "incoming-b.md"
    source_a.write_text("Alpha document with enough content for chunking.", encoding="utf-8")
    source_b.write_text("Bravo document with enough content for chunking.", encoding="utf-8")

    host = TestHostApp(tmp_path / "host")
    host.stage_batch(
        [
            HostSourceItem(item_key="economy/a.md", source_file=source_a),
            HostSourceItem(item_key="economy/b.md", source_file=source_b),
        ]
    )
    host.write_source_manifest()
    project = host.write_project()
    assert host.run_strata()["failed"] == 0

    changed_a = tmp_path / "incoming-a-v2.md"
    changed_a.write_text(
        "Alpha document changed with enough content for chunking.",
        encoding="utf-8",
    )
    host = TestHostApp(tmp_path / "host")
    host.stage_batch([HostSourceItem(item_key="economy/a.md", source_file=changed_a)])
    host.write_source_manifest(mode="incremental_delta")
    host.write_project()

    operations = plan_project(project)
    assert [operation.scope.item_key for operation in operations] == ["economy/a.md"] * 4
