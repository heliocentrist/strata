from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from conftest import write_project
from sqlalchemy import select

from strata.api import apply_project, plan_project
from strata.core.collections import ArtifactPayload, ArtifactWrite, CollectionWriteContext
from strata.core.config import load_manifest, state_path_from_url
from strata.core.operations import OperationInput, OperationOutput
from strata.core.planning import plan
from strata.execution.apply import apply_operations
from strata.executors.local import InlineOperationRunner
from strata.executors.protocols import OperationInvocation, OperationWindow, OperationWindowResult
from strata.executors.registry import get_operation_runner, registered_operation_runners
from strata.plugins.builtins.collections import LocalJsonArtifactCollection
from strata.plugins.protocols import AdapterMetadata
from strata.plugins.registry import register_artifact_collection, register_operation
from strata.sources.registry import snapshot_sources
from strata.state.connection import bootstrap, connect_state
from strata.state.repository import StateRepository
from strata.state.schema import asset_instances


def test_builtin_executor_registry_exposes_local_modes() -> None:
    assert "local_single_thread" in registered_operation_runners()
    assert "local_threaded" in registered_operation_runners()


def test_threaded_runner_executes_operation_invocations(tmp_path: Path) -> None:
    class EchoOperation:
        def run(
            self,
            inputs: list[OperationInput],
            config: dict[str, Any],
        ) -> list[OperationOutput]:
            _ = config
            return [
                OperationOutput(
                    instance_key=inputs[0].instance_key,
                    data=inputs[0].data,
                    output_location=f"memory://{inputs[0].instance_key}",
                )
            ]

    register_operation(
        "test_echo_runner",
        EchoOperation(),
        metadata=AdapterMetadata(
            name="test_echo_runner",
            kind="operation",
            source="test",
        ),
    )
    runner = get_operation_runner("local_threaded", {"max_workers": 2})
    results = asyncio.run(
        runner.run_window(
            OperationWindow(
                window_id="test-window",
                asset_name="echoes",
                artifact_root=tmp_path,
                artifact_collection="local_json",
                transform_version="test_echo_runner@0.1.0",
                config_hash="cfg",
                determinism="deterministic",
                invocations=[
                    OperationInvocation(
                        invocation_id=f"test-{index}",
                        operation_name="test_echo_runner",
                        inputs=[
                            OperationInput(
                                role="input",
                                asset_name="raw",
                                instance_key=f"item-{index}",
                                data=index,
                            )
                        ],
                        config={},
                    )
                    for index in range(3)
                ],
            )
        )
    )

    assert [[output.data for output in group] for group in results.output_groups] == [
        [0],
        [1],
        [2],
    ]


def test_apply_runner_contract_only_requires_run_window(tmp_path: Path) -> None:
    class WindowOnlyRunner:
        def __init__(self) -> None:
            self.window_count = 0

        async def run_window(self, window: OperationWindow) -> OperationWindowResult:
            self.window_count += 1
            return await InlineOperationRunner().run_window(window)

    project = write_project(tmp_path)
    manifest = load_manifest(project)
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    repo = StateRepository(engine, manifest.context)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    runner = WindowOnlyRunner()

    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
        runner=runner,
    )

    assert result["failed"] == 0
    assert runner.window_count > 0


def test_artifact_reads_use_upstream_collection(tmp_path: Path) -> None:
    class CountingCollection(LocalJsonArtifactCollection):
        def __init__(self) -> None:
            self.read_many_calls = 0
            self.max_refs_per_call = 0

        def read_many(self, root_path: Path, refs: list[str]) -> list[dict[str, Any]]:
            self.read_many_calls += 1
            self.max_refs_per_call = max(self.max_refs_per_call, len(refs))
            return super().read_many(root_path, refs)

    collection = CountingCollection()
    register_artifact_collection("test_counting_json", collection)
    project = write_project(tmp_path, max_chars=18)
    project.write_text(
        project.read_text(encoding="utf-8").replace(
            "  chunks:\n"
            "    input: parsed\n"
            "    operation: fixed_token_chunker\n"
            "    version: fixed_token_chunker@0.1.0\n"
            "    config:\n"
            "      output_label: chunk\n"
            "      max_chars: 18\n"
            "      overlap_chars: 5\n",
            "  chunks:\n"
            "    input: parsed\n"
            "    operation: fixed_token_chunker\n"
            "    version: fixed_token_chunker@0.1.0\n"
            "    artifact_strategy:\n"
            "      type: test_counting_json\n"
            "    config:\n"
            "      output_label: chunk\n"
            "      max_chars: 18\n"
            "      overlap_chars: 5\n",
        ),
        encoding="utf-8",
    )
    manifest = load_manifest(project)
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    repo = StateRepository(engine, manifest.context)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)

    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )

    assert result["failed"] == 0
    assert collection.read_many_calls > 0
    assert collection.max_refs_per_call > 1


def test_apply_submits_runner_windows(tmp_path: Path) -> None:
    class RecordingRunner:
        def __init__(self) -> None:
            self.window_sizes: list[int] = []
            self.window_ids: list[str] = []
            self.invocation_ids: list[str] = []
            self.windows: list[tuple[str, tuple[str, ...]]] = []
            self.artifact_input_locations: list[str] = []

        async def run_window(self, window: OperationWindow) -> OperationWindowResult:
            self.window_ids.append(window.window_id)
            self.invocation_ids.extend(
                invocation.invocation_id for invocation in window.invocations
            )
            self.window_sizes.append(len(window.invocations))
            if window.invocations:
                self.windows.append(
                    (
                        window.invocations[0].operation_name,
                        tuple(
                            input_item.instance_key
                            for invocation in window.invocations
                            for input_item in invocation.inputs
                        ),
                    )
                )
                self.artifact_input_locations.extend(
                    str(input_item.artifact_location)
                    for invocation in window.invocations
                    for input_item in invocation.inputs
                    if input_item.artifact_location is not None
                )
            return await InlineOperationRunner().run_window(window)

    project = write_project(tmp_path, max_chars=1000)
    (tmp_path / "docs" / "b.md").write_text(
        "Beta document for Strata.\nThis file creates another one-chunk input.\n",
        encoding="utf-8",
    )
    (tmp_path / "docs" / "c.md").write_text(
        "Gamma document for Strata.\nThis forces a second window for the layer.\n",
        encoding="utf-8",
    )
    raw = project.read_text(encoding="utf-8")
    project.write_text(
        raw.replace(
            "artifacts:\n  path: ./.strata/artifacts\n",
            "artifacts:\n"
            "  path: ./.strata/artifacts\n\n"
            "execution:\n"
            "  config:\n"
            "    window_size: 2\n",
        ).replace(
            "  embeddings:\n"
            "    input: chunks\n"
            "    operation: fake_embedding\n"
            "    version: fake_embedding@0.1.0\n",
            "  embeddings:\n"
            "    input: chunks\n"
            "    operation: fake_embedding\n"
            "    version: fake_embedding@0.1.0\n"
            "    execution:\n"
            "      window_size: 2\n",
        ),
        encoding="utf-8",
    )
    manifest = load_manifest(project)
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    repo = StateRepository(engine, manifest.context)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    runner = RecordingRunner()

    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
        runner=runner,
    )

    assert result["failed"] == 0
    assert max(runner.window_sizes) <= 2
    assert 2 in runner.window_sizes
    assert len(runner.window_ids) == len(set(runner.window_ids))
    assert len(runner.invocation_ids) == len(set(runner.invocation_ids))
    assert (
        "fake_embedding",
        ("a.md#chunk:0000", "b.md#chunk:0000"),
    ) in runner.windows
    assert runner.artifact_input_locations
    assert all(runner.artifact_input_locations)
    assert any(
        location.startswith("artifact://") for location in runner.artifact_input_locations
    )


def test_local_json_fanout_commit_is_idempotent_by_window_id(tmp_path: Path) -> None:
    collection = LocalJsonArtifactCollection()
    context = CollectionWriteContext(
        root_path=tmp_path,
        asset_name="chunks",
        partition_key="partition-a",
        window_id="win-fixed",
    )
    items = [
        ArtifactWrite(
            instance_key="doc.md#chunk:0000",
            input_fingerprint="fingerprint-0",
            content_hash="content-0",
            payload=ArtifactPayload(data="alpha", metadata={"ordinal": 0}),
        ),
        ArtifactWrite(
            instance_key="doc.md#chunk:0001",
            input_fingerprint="fingerprint-1",
            content_hash="content-1",
            payload=ArtifactPayload(data="beta", metadata={"ordinal": 1}),
        ),
    ]

    first = collection.write_many(context, items)
    second = collection.write_many(context, items)

    assert [item.output_ref for item in first] == [item.output_ref for item in second]
    manifest_path = tmp_path / "chunks" / "partition-a" / "manifests" / "win-fixed.json"
    payload_path = tmp_path / "chunks" / "partition-a" / "payloads" / "win-fixed.jsonl"
    assert manifest_path.exists()
    assert payload_path.exists()
    manifest_doc = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_doc["window_id"] == "win-fixed"
    assert "created_at" not in manifest_doc


def test_window_artifact_outputs_share_partition_manifest(tmp_path: Path) -> None:
    project = write_project(tmp_path, max_chars=18)
    manifest = load_manifest(project)
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    repo = StateRepository(engine, manifest.context)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)

    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
    )

    assert result["failed"] == 0
    with repo.engine.connect() as conn:
        rows = conn.execute(
            select(asset_instances.c.output_location).where(
                asset_instances.c.asset_name == "embeddings",
                asset_instances.c.status == "materialized",
            )
        ).all()
    manifest_refs = {row[0].split("#item=", 1)[0] for row in rows}
    assert len(rows) > 1
    assert len(manifest_refs) == 1


def test_apply_project_uses_configured_threaded_executor(tmp_path: Path) -> None:
    project = write_project(tmp_path)
    docs = tmp_path / "docs"
    (docs / "b.md").write_text(
        "Beta document for Strata.\nThis gives the threaded executor another source item.\n",
        encoding="utf-8",
    )
    raw = project.read_text(encoding="utf-8")
    project.write_text(
        raw.replace(
            "artifacts:\n  path: ./.strata/artifacts\n",
            (
                "artifacts:\n"
                "  path: ./.strata/artifacts\n\n"
                "execution:\n"
                "  executor: local_threaded\n"
                "  config:\n"
                "    max_workers: 2\n"
            ),
        ),
        encoding="utf-8",
    )

    manifest = load_manifest(project)
    assert manifest.execution.executor == "local_threaded"

    result = apply_project(project)

    assert result["failed"] == 0
    assert result["built"] > 0
    assert plan_project(project) == []
