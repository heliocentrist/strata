from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, cast

from strata.core.collections import ArtifactPayload, ArtifactWrite
from strata.core.hashing import config_hash, hash_canonical, input_fingerprint, sha256_text
from strata.core.models import (
    AssetInstanceCommit,
    ChunkRef,
    EmbeddingRef,
    HybridChunkEmbeddingItem,
    Manifest,
    MaterializedArtifact,
    Operation,
    OperationItemStatus,
    SourceItem,
    SourceRef,
    SourceSnapshot,
)
from strata.execution.artifacts import (
    artifact_source,
    read_artifact,
    write_many_artifacts,
    write_one_artifact,
)
from strata.executors.protocols import ApplyResult
from strata.plugins.registry import get_chunker, get_embedder, get_parser, get_sink
from strata.state.repository import StateRepository

ASSET_EXECUTION_ORDER = ("parsed", "chunks", "embeddings", "sink")


@dataclass(frozen=True)
class LocalExecutor:
    max_workers: int = 1

    def apply(
        self,
        *,
        manifest: Manifest,
        repo: StateRepository,
        source_snapshots: dict[str, SourceSnapshot],
        operations: list[Operation],
        config: dict[str, Any] | None = None,
    ) -> ApplyResult:
        configured_workers = int((config or {}).get("max_workers") or self.max_workers)
        return apply_operations(
            manifest=manifest,
            repo=repo,
            source_snapshots=source_snapshots,
            operations=operations,
            max_workers=configured_workers,
        )


def register_builtin_executors() -> None:
    from strata.executors.registry import register_executor

    register_executor("local_single_thread", LocalExecutor(max_workers=1))
    register_executor("local_threaded", LocalExecutor(max_workers=4))


@dataclass(frozen=True)
class _FanoutWorkItem:
    op_item_id: str
    instance_key: str
    input_fingerprint: str
    content_hash: str | None
    metadata: dict[str, Any]
    cached: MaterializedArtifact | None
    upstream_id: str


def apply_operations(
    *,
    manifest: Manifest,
    repo: StateRepository,
    source_snapshots: dict[str, SourceSnapshot],
    operations: list[Operation],
    max_workers: int = 1,
) -> ApplyResult:
    run_id = repo.create_run(manifest.manifest_hash)
    repo.acquire_lock(run_id)
    counts: dict[str, int] = {"built": 0, "reused": 0, "deleted": 0, "failed": 0}
    failed = False
    try:
        operation_run_ids = {
            operation.op_id: repo.create_operation_run(run_id, operation)
            for operation in operations
        }
        for batch in _operation_batches(operations):
            if max_workers <= 1 or len(batch) == 1:
                for operation in batch:
                    operation_counts, operation_failed = _run_operation(
                        manifest=manifest,
                        repo=repo,
                        source_snapshots=source_snapshots,
                        run_id=run_id,
                        operation_run_id=operation_run_ids[operation.op_id],
                        operation=operation,
                    )
                    _merge_counts(counts, operation_counts)
                    failed = failed or operation_failed
            else:
                with ThreadPoolExecutor(max_workers=max_workers) as pool:
                    futures = [
                        pool.submit(
                            _run_operation,
                            manifest=manifest,
                            repo=repo,
                            source_snapshots=source_snapshots,
                            run_id=run_id,
                            operation_run_id=operation_run_ids[operation.op_id],
                            operation=operation,
                        )
                        for operation in batch
                    ]
                    for future in as_completed(futures):
                        operation_counts, operation_failed = future.result()
                        _merge_counts(counts, operation_counts)
                        failed = failed or operation_failed
        for snapshot in source_snapshots.values():
            repo.update_source_checkpoint(
                snapshot.source_name,
                connection_id=snapshot.connection_id,
                scope_hash_value=snapshot.scope_hash,
                cursor_token={"scan_marker": snapshot.scan_marker},
            )
        repo.finish_run(run_id, "failed" if failed else "succeeded")
    finally:
        repo.release_lock()
    return ApplyResult(run_id=run_id, **counts)


def _run_operation(
    *,
    manifest: Manifest,
    repo: StateRepository,
    source_snapshots: dict[str, SourceSnapshot],
    run_id: str,
    operation_run_id: str,
    operation: Operation,
) -> tuple[dict[str, int], bool]:
    counts: dict[str, int] = {"built": 0, "reused": 0, "deleted": 0, "failed": 0}
    repo.update_operation_run(operation_run_id, "running")
    try:
        if operation.op_type == "delete_scope":
            counts["deleted"] += _execute_delete(repo, run_id, operation_run_id, operation)
        elif operation.asset_name == "parsed":
            _execute_parsed(
                manifest,
                repo,
                source_snapshots,
                run_id,
                operation_run_id,
                operation,
                counts,
            )
        elif operation.asset_name == "chunks":
            _execute_chunks(manifest, repo, run_id, operation_run_id, operation, counts)
        elif operation.asset_name == "embeddings":
            _execute_embeddings(manifest, repo, run_id, operation_run_id, operation, counts)
        elif operation.asset_name == "sink":
            _execute_sink(manifest, repo, run_id, operation_run_id, operation, counts)
        else:
            raise ValueError(f"unsupported asset: {operation.asset_name}")
        repo.update_operation_run(operation_run_id, "succeeded")
        return counts, False
    except Exception as exc:  # keep other operations inspectable
        counts["failed"] += 1
        repo.update_operation_run(operation_run_id, "failed", str(exc))
        return counts, True


def _operation_batches(operations: list[Operation]) -> list[list[Operation]]:
    deletes = [operation for operation in operations if operation.op_type == "delete_scope"]
    batches: list[list[Operation]] = []
    if deletes:
        batches.append(deletes)
    for asset_name in ASSET_EXECUTION_ORDER:
        batch = [
            operation
            for operation in operations
            if operation.op_type == "build_scope" and operation.asset_name == asset_name
        ]
        if batch:
            batches.append(batch)
    known = {operation.op_id for operation in deletes}
    for batch in batches:
        known.update(operation.op_id for operation in batch)
    leftovers = [operation for operation in operations if operation.op_id not in known]
    if leftovers:
        batches.append(leftovers)
    return batches


def _merge_counts(target: dict[str, int], source: dict[str, int]) -> None:
    for key, value in source.items():
        target[key] = target.get(key, 0) + value


def _source_item(
    source_snapshots: dict[str, SourceSnapshot], source_name: str, item_key: str
) -> SourceItem:
    for item in source_snapshots[source_name].items:
        if item.item_key == item_key:
            return item
    raise KeyError(f"source item not found in snapshot: {source_name}/{item_key}")


def _transform_id(manifest: Manifest, repo: StateRepository, asset_name: str, cfg_hash: str) -> str:
    asset = manifest.assets[asset_name]
    return repo.upsert_transform(
        project_id=manifest.context.project_id,
        transform_id=asset.transform or asset.parser or asset.type or asset.name,
        version=asset.version,
        config_json=json.dumps(asset.config, sort_keys=True),
        config_hash_value=cfg_hash,
        determinism=asset.determinism.value,
    )


def _execute_parsed(
    manifest: Manifest,
    repo: StateRepository,
    source_snapshots: dict[str, SourceSnapshot],
    run_id: str,
    operation_run_id: str,
    operation: Operation,
    counts: dict[str, int],
) -> None:
    source_name = operation.scope.source_name or ""
    item_key = operation.scope.item_key or ""
    item = _source_item(source_snapshots, source_name, item_key)
    asset = manifest.assets["parsed"]
    cfg_hash = config_hash(asset.config)
    fingerprint = input_fingerprint(
        transform_version=asset.version,
        config_hash_value=cfg_hash,
        determinism=asset.determinism.value,
        instance_key=item_key,
        source_content_hash=item.content_hash,
    )
    op_item_id = repo.create_operation_item(
        run_id=run_id,
        operation_run_id=operation_run_id,
        asset_name="parsed",
        item_key=item_key,
        instance_key=item_key,
        input_fingerprint_value=fingerprint,
        status="running",
        metadata_value=item.metadata,
    )
    cached = repo.find_materialized("parsed", item_key, fingerprint)
    if cached:
        repo.upsert_source_state(source_name, item_key, item.content_hash)
        repo.update_operation_item(op_item_id, "skipped")
        counts["reused"] += 1
        return
    try:
        if item.path is None:
            raise ValueError(
                "source item has no local path; streaming object parsing is not implemented yet: "
                f"{source_name}/{item_key}"
            )
        text = get_parser(asset.parser or "auto").parse(item.path)
        text_hash = sha256_text(text)
        write_result = write_one_artifact(
            manifest=manifest,
            asset_name="parsed",
            item=ArtifactWrite(
                instance_key=item_key,
                input_fingerprint=fingerprint,
                content_hash=text_hash,
                payload=ArtifactPayload(
                    data=text,
                    metadata={"source_path": str(item.path), "source_uri": item.uri},
                ),
            ),
        )
        transform_id = _transform_id(manifest, repo, "parsed", cfg_hash)
        repo.write_asset_instance(
            asset_name="parsed",
            instance_key=item_key,
            input_fingerprint_value=fingerprint,
            output_location=write_result.output_ref,
            output_hash=write_result.output_hash,
            content_hash=text_hash,
            transform_id=transform_id,
            materialization_strategy=asset.materialization_strategy,
            metadata_value={"source_item_key": item_key, "source_path": str(item.path)},
        )
        repo.upsert_source_state(source_name, item_key, item.content_hash)
        repo.update_operation_item(op_item_id, "succeeded")
        counts["built"] += 1
    except Exception as exc:
        repo.update_operation_item(op_item_id, "failed", error=str(exc))
        raise


def _execute_chunks(
    manifest: Manifest,
    repo: StateRepository,
    run_id: str,
    operation_run_id: str,
    operation: Operation,
    counts: dict[str, int],
) -> None:
    item_key = operation.scope.item_key or ""
    parsed = repo.latest_materialized("parsed", item_key)
    if not parsed:
        raise ValueError(f"missing parsed artifact for {item_key}")
    parsed_payload = read_artifact(manifest, parsed.output_location)
    text = str(parsed_payload["data"])
    parsed_source = artifact_source(parsed_payload)
    asset = manifest.assets["chunks"]
    cfg = {"max_chars": 1200, "overlap_chars": 120, **asset.config}
    cfg_hash = config_hash(asset.config)
    _ = parsed_source
    chunks = get_chunker(asset.transform or asset.name).chunk(text, cfg)
    transform_id = _transform_id(manifest, repo, "chunks", cfg_hash)
    artifact_writes: list[ArtifactWrite] = []
    work_items: list[_FanoutWorkItem] = []
    for index, chunk_text in enumerate(chunks):
        instance_key = f"{item_key}#chunk:{index:04d}"
        chunk_hash = sha256_text(chunk_text)
        fingerprint = input_fingerprint(
            transform_version=asset.version,
            config_hash_value=cfg_hash,
            determinism=asset.determinism.value,
            instance_key=instance_key,
            upstream_fingerprints=[parsed.input_fingerprint],
        )
        op_item_id = repo.create_operation_item(
            run_id=run_id,
            operation_run_id=operation_run_id,
            asset_name="chunks",
            item_key=instance_key,
            instance_key=instance_key,
            input_fingerprint_value=fingerprint,
            status="running",
        )
        cached = repo.find_materialized("chunks", instance_key, fingerprint)
        artifact_writes.append(
            ArtifactWrite(
                instance_key=instance_key,
                input_fingerprint=fingerprint,
                content_hash=chunk_hash,
                payload=ArtifactPayload(
                    data=chunk_text,
                    metadata={"ordinal": index},
                ),
            )
        )
        work_items.append(
            _FanoutWorkItem(
                op_item_id=op_item_id,
                instance_key=instance_key,
                input_fingerprint=fingerprint,
                content_hash=chunk_hash,
                metadata={"source_item_key": item_key, "ordinal": index},
                cached=cached,
                upstream_id=parsed.id,
            )
        )
        if cached:
            counts["reused"] += 1
        else:
            counts["built"] += 1

    write_results = write_many_artifacts(
        manifest=manifest,
        asset_name="chunks",
        partition_key=parsed.input_fingerprint,
        items=artifact_writes,
    )
    repo.commit_asset_instances(
        [
            AssetInstanceCommit(
                asset_name="chunks",
                instance_key=work_item.instance_key,
                input_fingerprint=work_item.input_fingerprint,
                output_location=write_results[index].output_ref,
                output_hash=write_results[index].output_hash,
                content_hash=work_item.content_hash,
                transform_id=transform_id,
                materialization_strategy=asset.materialization_strategy,
                metadata=work_item.metadata,
                upstream_id=work_item.upstream_id,
                operation_item_id=work_item.op_item_id,
                operation_status=(
                    OperationItemStatus.SKIPPED
                    if work_item.cached
                    else OperationItemStatus.SUCCEEDED
                ),
            )
            for index, work_item in enumerate(work_items)
        ]
    )


def _execute_embeddings(
    manifest: Manifest,
    repo: StateRepository,
    run_id: str,
    operation_run_id: str,
    operation: Operation,
    counts: dict[str, int],
) -> None:
    item_key = operation.scope.item_key or ""
    chunks = repo.latest_materialized_by_prefix("chunks", f"{item_key}#chunk:")
    if not chunks:
        raise ValueError(f"missing chunks for {item_key}")
    asset = manifest.assets["embeddings"]
    cfg = {"dimensions": 16, **asset.config}
    cfg_hash = config_hash(asset.config)
    transform_id = _transform_id(manifest, repo, "embeddings", cfg_hash)
    parsed = repo.latest_materialized("parsed", item_key)
    if not parsed:
        raise ValueError(f"missing parsed artifact for {item_key}")
    parsed_source = artifact_source(read_artifact(manifest, parsed.output_location))
    _ = parsed_source
    artifact_writes: list[ArtifactWrite] = []
    work_items: list[_FanoutWorkItem] = []
    for chunk in chunks:
        chunk_payload = read_artifact(manifest, chunk.output_location)
        chunk_text = str(chunk_payload["data"])
        fingerprint = input_fingerprint(
            transform_version=asset.version,
            config_hash_value=cfg_hash,
            determinism=asset.determinism.value,
            instance_key=chunk.instance_key,
            upstream_fingerprints=[chunk.input_fingerprint],
        )
        op_item_id = repo.create_operation_item(
            run_id=run_id,
            operation_run_id=operation_run_id,
            asset_name="embeddings",
            item_key=chunk.instance_key,
            instance_key=chunk.instance_key,
            input_fingerprint_value=fingerprint,
            status="running",
        )
        cached = repo.find_materialized("embeddings", chunk.instance_key, fingerprint)
        embedding = get_embedder(asset.transform or asset.name).embed(chunk_text, cfg)
        embedding_hash = hash_canonical(embedding)
        artifact_writes.append(
            ArtifactWrite(
                instance_key=chunk.instance_key,
                input_fingerprint=fingerprint,
                content_hash=embedding_hash,
                payload=ArtifactPayload(
                    data=embedding,
                    metadata={"chunk_instance_key": chunk.instance_key},
                ),
            )
        )
        work_items.append(
            _FanoutWorkItem(
                op_item_id=op_item_id,
                instance_key=chunk.instance_key,
                input_fingerprint=fingerprint,
                content_hash=embedding_hash,
                metadata={"source_item_key": item_key, "chunk_instance_key": chunk.instance_key},
                cached=cached,
                upstream_id=chunk.id,
            )
        )
        if cached:
            counts["reused"] += 1
        else:
            counts["built"] += 1

    write_results = write_many_artifacts(
        manifest=manifest,
        asset_name="embeddings",
        partition_key=parsed.input_fingerprint,
        items=artifact_writes,
    )
    repo.commit_asset_instances(
        [
            AssetInstanceCommit(
                asset_name="embeddings",
                instance_key=work_item.instance_key,
                input_fingerprint=work_item.input_fingerprint,
                output_location=write_results[index].output_ref,
                output_hash=write_results[index].output_hash,
                content_hash=work_item.content_hash,
                transform_id=transform_id,
                materialization_strategy=asset.materialization_strategy,
                metadata=work_item.metadata,
                upstream_id=work_item.upstream_id,
                operation_item_id=work_item.op_item_id,
                operation_status=(
                    OperationItemStatus.SKIPPED
                    if work_item.cached
                    else OperationItemStatus.SUCCEEDED
                ),
            )
            for index, work_item in enumerate(work_items)
        ]
    )


def _execute_sink(
    manifest: Manifest,
    repo: StateRepository,
    run_id: str,
    operation_run_id: str,
    operation: Operation,
    counts: dict[str, int],
) -> None:
    item_key = operation.scope.item_key or ""
    embeddings = repo.latest_materialized_by_prefix("embeddings", f"{item_key}#chunk:")
    if not embeddings:
        raise ValueError(f"missing embeddings for {item_key}")
    asset = manifest.assets["sink"]
    cfg_hash = config_hash(asset.config)
    transform_id = _transform_id(manifest, repo, "sink", cfg_hash)
    for embedding_artifact in embeddings:
        embedding_payload = read_artifact(manifest, embedding_artifact.output_location)
        embedding = embedding_payload["data"]
        embedding_source = artifact_source(embedding_payload)
        chunk_key = embedding_artifact.instance_key
        chunk = next(iter(repo.upstream_artifacts(embedding_artifact.id, "chunks")), None)
        if chunk is None:
            chunk = repo.latest_materialized("chunks", chunk_key)
        if chunk is None:
            raise ValueError(f"missing chunk artifact for {chunk_key}")
        chunk_payload = read_artifact(manifest, chunk.output_location)
        chunk_text = str(chunk_payload["data"])
        fingerprint = input_fingerprint(
            transform_version=asset.version,
            config_hash_value=cfg_hash,
            determinism=asset.determinism.value,
            instance_key=chunk_key,
            upstream_fingerprints=[chunk.input_fingerprint, embedding_artifact.input_fingerprint],
        )
        op_item_id = repo.create_operation_item(
            run_id=run_id,
            operation_run_id=operation_run_id,
            asset_name="sink",
            item_key=chunk_key,
            instance_key=chunk_key,
            input_fingerprint_value=fingerprint,
            status="running",
        )
        cached = repo.find_materialized("sink", chunk_key, fingerprint)
        if cached:
            repo.update_operation_item(op_item_id, "skipped")
            counts["reused"] += 1
            continue
        sink = get_sink(asset.type or asset.name)
        source_ref = SourceRef(
            name=cast(str | None, embedding_source.get("name")),
            item_key=item_key,
            content_hash=cast(str | None, embedding_source.get("content_hash")),
            metadata=dict(chunk_payload.get("metadata") or {}),
        )
        hybrid_item = HybridChunkEmbeddingItem(
            context=manifest.context,
            source=source_ref,
            chunk=ChunkRef(
                instance_key=chunk.instance_key,
                fingerprint=chunk.input_fingerprint,
                content_hash=chunk.content_hash,
                text=chunk_text,
                metadata=chunk.metadata,
            ),
            embedding=EmbeddingRef(
                instance_key=embedding_artifact.instance_key,
                fingerprint=embedding_artifact.input_fingerprint,
                content_hash=embedding_artifact.content_hash,
                vector=cast(list[float], embedding),
                metadata=embedding_artifact.metadata,
            ),
            document_id=chunk_key,
            metadata={
                "source_item_key": item_key,
                "chunk_instance_key": chunk_key,
            },
        )
        sink.write(
            repo=repo,
            instance_key=chunk_key,
            embedding_fingerprint=embedding_artifact.input_fingerprint,
            source_item_key=item_key,
            chunk_text=chunk_text,
            embedding=cast(list[float], embedding),
        )
        sink_result = sink.write_hybrid(item=hybrid_item, config=asset.config)
        sink_instance = repo.write_asset_instance(
            asset_name="sink",
            instance_key=chunk_key,
            input_fingerprint_value=fingerprint,
            output_location=sink_result.output_location,
            output_hash=sink_result.output_hash,
            content_hash=None,
            transform_id=transform_id,
            materialization_strategy="sink",
            metadata_value={
                "source_name": embedding_source.get("name"),
                "source_item_key": item_key,
                "source_content_hash": embedding_source.get("content_hash"),
                "chunk_instance_key": chunk_key,
            },
        )
        repo.write_lineage(chunk.id, sink_instance.id)
        repo.write_lineage(embedding_artifact.id, sink_instance.id)
        repo.update_operation_item(op_item_id, "succeeded")
        counts["built"] += 1


def _execute_delete(
    repo: StateRepository,
    run_id: str,
    operation_run_id: str,
    operation: Operation,
) -> int:
    item_key = operation.scope.item_key or ""
    op_item_id = repo.create_operation_item(
        run_id=run_id,
        operation_run_id=operation_run_id,
        asset_name=operation.asset_name,
        item_key=item_key,
        status="running",
    )
    repo.mark_source_deleted(operation.scope.source_name or "", item_key)
    deleted = repo.mark_lineage_deleted_for_source(item_key)
    repo.update_operation_item(op_item_id, "deleted")
    return deleted
