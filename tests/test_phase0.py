from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from conftest import write_project
from sqlalchemy import insert, select

from strata.config import load_manifest, state_path_from_url
from strata.doctor import run_doctor
from strata.executor import apply_operations
from strata.planner import plan
from strata.source import snapshot_sources
from strata.state import (
    StateRepository,
    apply_locks,
    asset_instances,
    bootstrap,
    connect_state,
    now,
    vector_sink,
)


def setup_repo(project: Path) -> tuple[object, StateRepository]:
    manifest = load_manifest(project)
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    return manifest, StateRepository(engine, manifest.context)


def test_first_apply_then_cached_second_apply(tmp_path: Path) -> None:
    project = write_project(tmp_path)
    manifest, repo = setup_repo(project)
    snapshots = snapshot_sources(manifest.sources)
    operations = plan(manifest, repo.snapshot(), snapshots)
    assert [op.asset_name for op in operations] == ["parsed", "chunks", "embeddings", "sink"]

    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=operations,
    )
    assert result["failed"] == 0
    assert result["built"] > 0
    assert repo.progress(result["run_id"])["total"] > 0

    second_ops = plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources))
    assert second_ops == []


def test_each_embedding_instance_has_distinct_output_location(tmp_path: Path) -> None:
    project = write_project(tmp_path, max_chars=25)
    manifest, repo = setup_repo(project)
    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshot_sources(manifest.sources),
        operations=plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources)),
    )

    with repo.engine.connect() as conn:
        rows = conn.execute(
            asset_instances.select().where(asset_instances.c.asset_name == "embeddings")
        ).mappings().all()
    locations = [row["output_location"] for row in rows]
    assert len(locations) > 1
    assert len(locations) == len(set(locations))


def test_fanout_instances_have_distinct_fingerprints_and_artifact_identity(
    tmp_path: Path,
) -> None:
    project = write_project(tmp_path, max_chars=25)
    manifest, repo = setup_repo(project)
    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshot_sources(manifest.sources),
        operations=plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources)),
    )

    with repo.engine.connect() as conn:
        rows = conn.execute(
            asset_instances.select().where(asset_instances.c.asset_name == "chunks")
        ).mappings().all()

    fingerprints = [row["input_fingerprint"] for row in rows]
    assert len(fingerprints) > 1
    assert len(fingerprints) == len(set(fingerprints))

    output_location = rows[0]["output_location"]
    assert output_location.startswith("artifact://chunks/")
    manifest_ref, item_ref = output_location.removeprefix("artifact://").split("#item=")
    parent_fingerprint = manifest_ref.split("/")[1]

    fanout_manifest_path = manifest.artifacts_path / manifest_ref
    fanout_manifest = json.loads(fanout_manifest_path.read_text(encoding="utf-8"))
    assert fanout_manifest_path.parent.name == "manifests"
    assert fanout_manifest_path.name.startswith(fanout_manifest["created_at"][:10].replace("-", ""))
    assert fanout_manifest["manifest_hash"][:12] in fanout_manifest_path.name
    assert fanout_manifest["asset_name"] == "chunks"
    assert fanout_manifest["parent"]["asset_name"] == "parsed"
    assert fanout_manifest["parent"]["input_fingerprint"] == parent_fingerprint
    assert fanout_manifest["transform"]["config_hash"]
    assert fanout_manifest["source"]["name"] == "docs"
    assert fanout_manifest["source"]["item_key"] == "a.md"
    assert fanout_manifest["upstreams"][0]["asset_name"] == "parsed"

    first_item = fanout_manifest["items"][int(item_ref)]
    assert first_item["instance_key"] == rows[0]["instance_key"]
    assert first_item["input_fingerprint"] == rows[0]["input_fingerprint"]
    assert first_item["content_hash"]
    assert first_item["payload"]["path"].startswith("payloads/")
    assert first_item["payload"]["path"].endswith(".jsonl")
    assert first_item["payload"]["format"] == "jsonl"
    assert "upstreams" not in first_item
    assert first_item["metadata"]["ordinal"] == 0

    payload_path = fanout_manifest_path.parent.parent / first_item["payload"]["path"]
    record = json.loads(
        payload_path.read_text(encoding="utf-8").splitlines()[first_item["payload"]["record"]]
    )
    assert sorted(record) == ["data"]


def test_chunk_config_change_reuses_parsed_and_rebuilds_downstream(tmp_path: Path) -> None:
    project = write_project(tmp_path, max_chars=40)
    manifest, repo = setup_repo(project)
    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshot_sources(manifest.sources),
        operations=plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources)),
    )

    project.write_text(
        project.read_text(encoding="utf-8").replace("max_chars: 40", "max_chars: 30"),
        encoding="utf-8",
    )
    changed_manifest = load_manifest(project)
    operations = plan(changed_manifest, repo.snapshot(), snapshot_sources(changed_manifest.sources))
    assert [op.asset_name for op in operations] == ["chunks", "embeddings", "sink"]


def test_select_chunks_only_limits_plan_to_chunks(tmp_path: Path) -> None:
    project = write_project(tmp_path, max_chars=40)
    manifest, repo = setup_repo(project)
    operations = plan(
        manifest,
        repo.snapshot(),
        snapshot_sources(manifest.sources),
        selection="chunks",
    )
    assert [operation.asset_name for operation in operations] == ["chunks"]


def test_source_selector_limits_plan_to_source(tmp_path: Path) -> None:
    project = write_project(tmp_path, max_chars=40)
    docs = tmp_path / "docs"
    (docs / "b.md").write_text("Bravo document with enough text for chunks.\n", encoding="utf-8")
    manifest, repo = setup_repo(project)

    operations = plan(
        manifest,
        repo.snapshot(),
        snapshot_sources(manifest.sources),
        selection="source:docs+",
    )

    assert [operation.scope.source_name for operation in operations] == ["docs"] * len(operations)
    assert {operation.scope.item_key for operation in operations} == {"a.md", "b.md"}


def test_deleted_file_tombstones_instances_and_removes_sink_rows(tmp_path: Path) -> None:
    project = write_project(tmp_path)
    manifest, repo = setup_repo(project)
    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshot_sources(manifest.sources),
        operations=plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources)),
    )
    (tmp_path / "docs" / "a.md").unlink()

    operations = plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources))
    assert len(operations) == 1
    assert operations[0].op_type == "delete_scope"
    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshot_sources(manifest.sources),
        operations=operations,
    )

    with repo.engine.connect() as conn:
        active = conn.execute(
            asset_instances.select().where(asset_instances.c.status == "materialized")
        ).all()
        sink_rows = conn.execute(vector_sink.select()).all()
    assert active == []
    assert sink_rows == []


def test_doctor_reports_and_fixes_expired_locks_and_orphan_files(tmp_path: Path) -> None:
    project = write_project(tmp_path)
    manifest, repo = setup_repo(project)
    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshot_sources(manifest.sources),
        operations=plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources)),
    )

    orphan = manifest.artifacts_path / "chunks" / "orphan.json"
    orphan.parent.mkdir(parents=True, exist_ok=True)
    orphan.write_text("{}", encoding="utf-8")
    timestamp = now()
    with repo.begin() as conn:
        conn.execute(
            insert(apply_locks).values(
                project_id=manifest.context.project_id,
                tenant_id=manifest.context.tenant_id,
                run_id="expired-run",
                acquired_at=timestamp - timedelta(hours=2),
                heartbeat_at=timestamp - timedelta(hours=2),
                expires_at=timestamp - timedelta(hours=1),
            )
        )

    result = run_doctor(manifest=manifest, repo=repo)
    assert {issue.code for issue in result.issues} >= {"expired_lock", "orphan_file"}

    fixed = run_doctor(manifest=manifest, repo=repo, fix=True)
    assert fixed.fixed >= 2
    assert not orphan.exists()
    with repo.engine.connect() as conn:
        locks = conn.execute(select(apply_locks)).all()
    assert locks == []


def test_doctor_detects_corrupt_fanout_payload(tmp_path: Path) -> None:
    project = write_project(tmp_path, max_chars=25)
    manifest, repo = setup_repo(project)
    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshot_sources(manifest.sources),
        operations=plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources)),
    )

    with repo.engine.connect() as conn:
        row = conn.execute(
            asset_instances.select().where(asset_instances.c.asset_name == "chunks")
        ).mappings().first()
    assert row is not None
    manifest_ref, item_ref = row["output_location"].removeprefix("artifact://").split("#item=")
    fanout_manifest_path = manifest.artifacts_path / manifest_ref
    fanout_manifest = json.loads(fanout_manifest_path.read_text(encoding="utf-8"))
    item = fanout_manifest["items"][int(item_ref)]
    payload_path = fanout_manifest_path.parent.parent / item["payload"]["path"]
    lines = payload_path.read_text(encoding="utf-8").splitlines()
    record = json.loads(lines[item["payload"]["record"]])
    record["data"] = f"{record['data']} corrupted"
    lines[item["payload"]["record"]] = json.dumps(record, sort_keys=True)
    payload_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = run_doctor(manifest=manifest, repo=repo)
    messages = [issue.message for issue in result.issues if issue.code == "broken_artifact"]
    assert any("payload file hash mismatch" in message for message in messages)
    assert any("content hash mismatch" in message for message in messages)


def test_doctor_fix_missing_manifest_allows_rebuild(tmp_path: Path) -> None:
    project = write_project(tmp_path, max_chars=25)
    manifest, repo = setup_repo(project)
    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshot_sources(manifest.sources),
        operations=plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources)),
    )

    with repo.engine.connect() as conn:
        row = conn.execute(
            asset_instances.select().where(
                asset_instances.c.asset_name == "chunks",
                asset_instances.c.instance_key.like("%#chunk:0000"),
            )
        ).mappings().first()
    assert row is not None
    manifest_ref, _item_ref = row["output_location"].removeprefix("artifact://").split("#item=")
    fanout_manifest_path = manifest.artifacts_path / manifest_ref
    fanout_manifest_path.unlink()

    result = run_doctor(manifest=manifest, repo=repo)
    assert any("fanout manifest is missing" in issue.message for issue in result.issues)

    fixed = run_doctor(manifest=manifest, repo=repo, fix=True)
    assert fixed.fixed > 0
    operations = plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources))
    assert [operation.asset_name for operation in operations] == ["chunks", "embeddings", "sink"]

    rebuild = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshot_sources(manifest.sources),
        operations=operations,
    )
    assert rebuild["failed"] == 0
    assert plan(manifest, repo.snapshot(), snapshot_sources(manifest.sources)) == []
