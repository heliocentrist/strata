from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlalchemy import select

from strata.core.config import load_manifest, state_path_from_url
from strata.core.hashing import sha256_text
from strata.core.operations import OperationInput, OperationOutput
from strata.core.planning import plan
from strata.execution.apply import apply_operations
from strata.executors.registry import get_operation_runner
from strata.plugins.protocols import AdapterMetadata
from strata.plugins.registry import register_operation
from strata.sources.registry import snapshot_sources
from strata.state.connection import bootstrap, connect_state
from strata.state.repository import StateRepository
from strata.state.schema import asset_instances, vector_sink


class LineFanoutOperation:
    def run(
        self,
        inputs: list[OperationInput],
        config: dict[str, Any],
    ) -> list[OperationOutput]:
        label = str(config.get("output_label") or "line")
        outputs: list[OperationOutput] = []
        for input_item in inputs:
            lines = [
                line.strip()
                for line in str(input_item.data).splitlines()
                if line.strip()
            ]
            for index, line in enumerate(lines):
                outputs.append(
                    OperationOutput(
                        instance_key=f"{input_item.instance_key}#{label}:{index:04d}",
                        data=line,
                        parent_input_ids=[input_item.input_id],
                        content_hash=sha256_text(line),
                        metadata={"ordinal": index},
                    )
                )
        return outputs


class WordFanoutOperation:
    def run(
        self,
        inputs: list[OperationInput],
        config: dict[str, Any],
    ) -> list[OperationOutput]:
        label = str(config.get("output_label") or "word")
        outputs: list[OperationOutput] = []
        for input_item in inputs:
            words = [word.strip(".,:;!?") for word in str(input_item.data).split()]
            for index, word in enumerate(word for word in words if word):
                outputs.append(
                    OperationOutput(
                        instance_key=f"{input_item.instance_key}#{label}:{index:04d}",
                        data=word,
                        parent_input_ids=[input_item.input_id],
                        content_hash=sha256_text(word),
                        metadata={"ordinal": index},
                    )
                )
        return outputs


class SuffixMapOperation:
    def run(
        self,
        inputs: list[OperationInput],
        config: dict[str, Any],
    ) -> list[OperationOutput]:
        suffix = str(config["suffix"])
        return [
            OperationOutput(
                instance_key=input_item.instance_key,
                data=f"{input_item.data}{suffix}",
                parent_input_ids=[input_item.input_id],
                content_hash=sha256_text(f"{input_item.data}{suffix}"),
            )
            for input_item in inputs
        ]


class EmptyFanoutOperation:
    def run(
        self,
        inputs: list[OperationInput],
        config: dict[str, Any],
    ) -> list[OperationOutput]:
        _ = inputs, config
        return []


def test_multiple_fanouts_sink_and_threaded_runner(tmp_path: Path) -> None:
    _register_shape_operations()
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "a.md").write_text("alpha beta\ngamma delta\n", encoding="utf-8")
    (docs / "b.md").write_text("red blue\ngreen gold\n", encoding="utf-8")
    project = tmp_path / "strata.yml"
    project.write_text(
        """
project_id: multi_fanout
tenant_id: default

state:
  url: sqlite:///./.strata/state.db

artifacts:
  path: ./.strata/artifacts

execution:
  executor: local_threaded
  config:
    max_workers: 4
    window_size: 2

sources:
  docs:
    type: local_files
    path: ./docs
    include: ["**/*.md"]

pipeline:
  raw:
    source: docs
    operation: markdown_noop
    version: markdown_noop@0.1.0

  paragraphs:
    input: raw
    operation: test_line_fanout
    version: test_line_fanout@0.1.0
    config:
      output_label: paragraph

  tokens:
    input: paragraphs
    operation: test_word_fanout
    version: test_word_fanout@0.1.0
    execution:
      inputs_per_call: 3
    config:
      output_label: token

  token_vectors:
    input: tokens
    operation: fake_embedding
    version: fake_embedding@0.1.0
    execution:
      inputs_per_call: 3
    config:
      dimensions: 4

  search:
    input: token_vectors
    operation: local_sqlite_vector_sink
    version: local_sqlite_vector_sink@0.1.0
""",
        encoding="utf-8",
    )
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)

    operations = plan(manifest, repo.snapshot(), snapshots)
    assert [operation.asset_name for operation in operations] == [
        "raw",
        "paragraphs",
        "tokens",
        "token_vectors",
        "search",
    ]
    assert [operation.scope.item_key for operation in operations] == [None] * 5
    assert [operation.scope.item_keys for operation in operations] == [["a.md", "b.md"]] * 5

    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=operations,
        runner=get_operation_runner(manifest.execution.executor, manifest.execution.config),
    )

    assert result["failed"] == 0
    assert result["built"] == 30
    cached_plan = plan(
        manifest,
        repo.snapshot(),
        snapshot_sources(manifest.sources, root=manifest.root),
    )
    assert cached_plan == []
    assert _asset_counts(repo) == {
        "paragraphs": 4,
        "raw": 2,
        "search": 8,
        "token_vectors": 8,
        "tokens": 8,
    }
    with repo.engine.connect() as conn:
        sink_rows = conn.execute(select(vector_sink)).mappings().all()
        token_rows = conn.execute(
            select(asset_instances).where(asset_instances.c.asset_name == "tokens")
        ).mappings().all()
    assert len(sink_rows) == 8
    assert all(row["embedding_json"].startswith("[") for row in sink_rows)
    assert all("#paragraph:" in row["instance_key"] for row in token_rows)
    assert all("#token:" in row["instance_key"] for row in token_rows)


def test_complex_pipeline_rebuilds_only_changed_source_scope(tmp_path: Path) -> None:
    _register_shape_operations()
    project = _write_multi_fanout_project(tmp_path)
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)

    first = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
        runner=get_operation_runner(manifest.execution.executor, manifest.execution.config),
    )
    assert first["failed"] == 0

    (tmp_path / "docs" / "a.md").write_text(
        "alpha beta changed\ngamma delta\n",
        encoding="utf-8",
    )
    changed_snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    operations = plan(manifest, repo.snapshot(), changed_snapshots)

    assert [operation.asset_name for operation in operations] == [
        "raw",
        "paragraphs",
        "tokens",
        "token_vectors",
        "search",
    ]
    assert [operation.reason for operation in operations] == ["source_changed"] * 5
    assert [operation.scope.item_keys for operation in operations] == [["a.md"]] * 5

    second = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=changed_snapshots,
        operations=operations,
        runner=get_operation_runner(manifest.execution.executor, manifest.execution.config),
    )

    assert second["failed"] == 0
    assert plan(
        manifest,
        repo.snapshot(),
        snapshot_sources(manifest.sources, root=manifest.root),
    ) == []


def test_delete_propagates_through_nested_fanouts_and_sink(tmp_path: Path) -> None:
    _register_shape_operations()
    project = _write_multi_fanout_project(tmp_path)
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=plan(manifest, repo.snapshot(), snapshots),
        runner=get_operation_runner(manifest.execution.executor, manifest.execution.config),
    )

    (tmp_path / "docs" / "a.md").unlink()
    delete_snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    operations = plan(manifest, repo.snapshot(), delete_snapshots)
    delete_scopes = [
        (operation.op_type, operation.asset_name, operation.scope.item_key)
        for operation in operations
    ]
    assert delete_scopes == [("delete_scope", "search", "a.md")]

    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=delete_snapshots,
        operations=operations,
    )

    assert result["failed"] == 0
    with repo.engine.connect() as conn:
        active_instances = conn.execute(
            select(asset_instances.c.instance_key).where(
                asset_instances.c.status == "materialized"
            )
        ).scalars().all()
        sink_rows = conn.execute(select(vector_sink.c.instance_key)).scalars().all()
    assert not any(str(instance_key).startswith("a.md") for instance_key in active_instances)
    assert any(str(instance_key).startswith("b.md") for instance_key in active_instances)
    assert not any(str(instance_key).startswith("a.md") for instance_key in sink_rows)
    assert any(str(instance_key).startswith("b.md") for instance_key in sink_rows)


def test_complex_pipeline_selectors_preserve_generic_shape(tmp_path: Path) -> None:
    _register_shape_operations()
    project = _write_multi_fanout_project(tmp_path)
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)

    token_downstream = plan(manifest, repo.snapshot(), snapshots, selection="tokens+")
    assert [operation.asset_name for operation in token_downstream] == [
        "tokens",
        "token_vectors",
        "search",
    ]
    assert [operation.scope.item_keys for operation in token_downstream] == [
        ["a.md", "b.md"]
    ] * 3

    token_with_upstream = plan(manifest, repo.snapshot(), snapshots, selection="+tokens+")
    assert [operation.asset_name for operation in token_with_upstream] == [
        "raw",
        "paragraphs",
        "tokens",
        "token_vectors",
        "search",
    ]

    source_selection = plan(manifest, repo.snapshot(), snapshots, selection="source:docs+")
    assert [operation.asset_name for operation in source_selection] == [
        "raw",
        "paragraphs",
        "tokens",
        "token_vectors",
        "search",
    ]
    assert {operation.scope.source_name for operation in source_selection} == {"docs"}


def test_object_manifest_source_uses_same_generic_pipeline_path(tmp_path: Path) -> None:
    _register_shape_operations()
    objects = tmp_path / "objects"
    objects.mkdir()
    (objects / "a.md").write_text("alpha beta\ngamma delta\n", encoding="utf-8")
    (objects / "b.md").write_text("red blue\ngreen gold\n", encoding="utf-8")
    source_manifest = tmp_path / "source.json"
    source_manifest.write_text(
        json.dumps(
            {
                "mode": "authoritative_snapshot",
                "connection_id": "fixture",
                "items": [
                    {
                        "item_key": "remote/a.md",
                        "object_uri": str(objects / "a.md"),
                        "content_hash": "a-v1",
                    },
                    {
                        "item_key": "remote/b.md",
                        "object_uri": str(objects / "b.md"),
                        "content_hash": "b-v1",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    project = _write_multi_fanout_project(
        tmp_path,
        source_type="object_manifest",
        manifest_uri=str(source_manifest),
    )
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)

    operations = plan(manifest, repo.snapshot(), snapshots)
    assert [operation.scope.item_keys for operation in operations] == [
        ["remote/a.md", "remote/b.md"]
    ] * 5
    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=operations,
        runner=get_operation_runner(manifest.execution.executor, manifest.execution.config),
    )

    assert result["failed"] == 0
    assert _asset_counts(repo) == {
        "paragraphs": 4,
        "raw": 2,
        "search": 8,
        "token_vectors": 8,
        "tokens": 8,
    }
    assert plan(
        manifest,
        repo.snapshot(),
        snapshot_sources(manifest.sources, root=manifest.root),
    ) == []


def test_terminal_empty_fanout_apply_succeeds_without_outputs(tmp_path: Path) -> None:
    _register_shape_operations()
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "a.md").write_text("alpha", encoding="utf-8")
    (docs / "b.md").write_text("bravo", encoding="utf-8")
    project = tmp_path / "strata.yml"
    project.write_text(
        """
project_id: empty_fanout
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
  raw:
    source: docs
    operation: markdown_noop
    version: markdown_noop@0.1.0

  filtered:
    input: raw
    operation: test_empty_fanout
    version: test_empty_fanout@0.1.0
""",
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

    assert result["failed"] == 0
    assert _asset_counts(repo) == {"raw": 2}


def test_multiple_independent_sources_are_grouped_separately(tmp_path: Path) -> None:
    _register_shape_operations()
    docs = tmp_path / "docs"
    notes = tmp_path / "notes"
    docs.mkdir()
    notes.mkdir()
    (docs / "a.md").write_text("alpha beta\n", encoding="utf-8")
    (notes / "n.md").write_text("north south\n", encoding="utf-8")
    project = tmp_path / "strata.yml"
    project.write_text(
        """
project_id: multi_source
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
  notes:
    type: local_files
    path: ./notes
    include: ["**/*.md"]

pipeline:
  docs_raw:
    source: docs
    operation: markdown_noop
    version: markdown_noop@0.1.0

  docs_tokens:
    input: docs_raw
    operation: test_word_fanout
    version: test_word_fanout@0.1.0
    config:
      output_label: token

  notes_raw:
    source: notes
    operation: markdown_noop
    version: markdown_noop@0.1.0

  notes_tokens:
    input: notes_raw
    operation: test_word_fanout
    version: test_word_fanout@0.1.0
    config:
      output_label: token
""",
        encoding="utf-8",
    )
    manifest, repo = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)

    operations = plan(manifest, repo.snapshot(), snapshots)
    assert [
        (operation.asset_name, operation.scope.source_name, operation.scope.item_keys)
        for operation in operations
    ] == [
        ("docs_raw", "docs", ["a.md"]),
        ("docs_tokens", "docs", ["a.md"]),
        ("notes_raw", "notes", ["n.md"]),
        ("notes_tokens", "notes", ["n.md"]),
    ]
    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=operations,
    )

    assert result["failed"] == 0
    assert _asset_counts(repo) == {
        "docs_raw": 1,
        "docs_tokens": 2,
        "notes_raw": 1,
        "notes_tokens": 2,
    }


def test_runner_parity_for_single_thread_and_threaded(tmp_path: Path) -> None:
    _register_shape_operations()
    single_project = _write_runner_parity_project(tmp_path / "single", "local_single_thread")
    threaded_project = _write_runner_parity_project(tmp_path / "threaded", "local_threaded")

    single_manifest, single_repo = _repo(single_project)
    threaded_manifest, threaded_repo = _repo(threaded_project)
    single_result = apply_operations(
        manifest=single_manifest,
        repo=single_repo,
        source_snapshots=snapshot_sources(single_manifest.sources, root=single_manifest.root),
        operations=plan(
            single_manifest,
            single_repo.snapshot(),
            snapshot_sources(single_manifest.sources, root=single_manifest.root),
        ),
        runner=get_operation_runner(
            single_manifest.execution.executor,
            single_manifest.execution.config,
        ),
    )
    threaded_result = apply_operations(
        manifest=threaded_manifest,
        repo=threaded_repo,
        source_snapshots=snapshot_sources(threaded_manifest.sources, root=threaded_manifest.root),
        operations=plan(
            threaded_manifest,
            threaded_repo.snapshot(),
            snapshot_sources(threaded_manifest.sources, root=threaded_manifest.root),
        ),
        runner=get_operation_runner(
            threaded_manifest.execution.executor,
            threaded_manifest.execution.config,
        ),
    )

    assert single_result["failed"] == threaded_result["failed"] == 0
    assert single_result["built"] == threaded_result["built"] == 18
    assert _asset_counts(single_repo) == _asset_counts(threaded_repo)
    assert _asset_counts(single_repo) == {"raw": 2, "sentences": 4, "words": 12}


def _write_runner_parity_project(root: Path, executor: str) -> Path:
    docs = root / "docs"
    docs.mkdir(parents=True)
    (docs / "a.md").write_text("one two three\nfour five six\n", encoding="utf-8")
    (docs / "b.md").write_text("red blue green\nyellow black white\n", encoding="utf-8")
    project = root / "strata.yml"
    project.write_text(
        f"""
project_id: runner_parity_{executor}
tenant_id: default

state:
  url: sqlite:///./.strata/state.db

artifacts:
  path: ./.strata/artifacts

execution:
  executor: {executor}
  config:
    max_workers: 3
    window_size: 2

sources:
  docs:
    type: local_files
    path: ./docs
    include: ["**/*.md"]

pipeline:
  raw:
    source: docs
    operation: markdown_noop
    version: markdown_noop@0.1.0

  sentences:
    input: raw
    operation: test_line_fanout
    version: test_line_fanout@0.1.0
    config:
      output_label: sentence

  words:
    input: sentences
    operation: test_word_fanout
    version: test_word_fanout@0.1.0
    execution:
      inputs_per_call: 2
    config:
      output_label: word
""",
        encoding="utf-8",
    )
    return project


def _write_multi_fanout_project(
    root: Path,
    *,
    source_type: str = "local_files",
    manifest_uri: str | None = None,
) -> Path:
    if source_type == "local_files":
        docs = root / "docs"
        docs.mkdir()
        (docs / "a.md").write_text("alpha beta\ngamma delta\n", encoding="utf-8")
        (docs / "b.md").write_text("red blue\ngreen gold\n", encoding="utf-8")
        source_yaml = """
  docs:
    type: local_files
    path: ./docs
    include: ["**/*.md"]
"""
    else:
        source_yaml = f"""
  docs:
    type: object_manifest
    manifest_uri: {json.dumps(manifest_uri)}
"""
    project = root / "strata.yml"
    project.write_text(
        f"""
project_id: multi_fanout
tenant_id: default

state:
  url: sqlite:///./.strata/state.db

artifacts:
  path: ./.strata/artifacts

execution:
  executor: local_threaded
  config:
    max_workers: 4
    window_size: 2

sources:
{source_yaml}
pipeline:
  raw:
    source: docs
    operation: markdown_noop
    version: markdown_noop@0.1.0

  paragraphs:
    input: raw
    operation: test_line_fanout
    version: test_line_fanout@0.1.0
    config:
      output_label: paragraph

  tokens:
    input: paragraphs
    operation: test_word_fanout
    version: test_word_fanout@0.1.0
    execution:
      inputs_per_call: 3
    config:
      output_label: token

  token_vectors:
    input: tokens
    operation: fake_embedding
    version: fake_embedding@0.1.0
    execution:
      inputs_per_call: 3
    config:
      dimensions: 4

  search:
    input: token_vectors
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


def _asset_counts(repo: StateRepository) -> dict[str, int]:
    with repo.engine.connect() as conn:
        rows = conn.execute(
            select(asset_instances.c.asset_name, asset_instances.c.id).where(
                asset_instances.c.status == "materialized"
            )
        ).all()
    counts: dict[str, int] = {}
    for asset_name, _instance_id in rows:
        counts[str(asset_name)] = counts.get(str(asset_name), 0) + 1
    return dict(sorted(counts.items()))


def _register_shape_operations() -> None:
    for name, operation in {
        "test_line_fanout": LineFanoutOperation(),
        "test_word_fanout": WordFanoutOperation(),
        "test_suffix_map": SuffixMapOperation(),
        "test_empty_fanout": EmptyFanoutOperation(),
    }.items():
        register_operation(
            name,
            operation,
            metadata=AdapterMetadata(name=name, kind="operation", source="test"),
        )
