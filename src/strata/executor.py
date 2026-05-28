from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from strata.hashing import config_hash, hash_canonical, input_fingerprint, sha256_text
from strata.models import (
    ArtifactEnvelope,
    ArtifactInputs,
    ArtifactOutput,
    ArtifactSource,
    ArtifactTransform,
    ArtifactUpstream,
    AssetInstanceCommit,
    FanoutManifest,
    FanoutManifestItem,
    FanoutManifestParent,
    FanoutManifestPayload,
    Manifest,
    MaterializedArtifact,
    Operation,
    OperationItemStatus,
    SourceItem,
    SourceSnapshot,
)
from strata.plugins import get_chunker, get_embedder, get_parser, get_sink
from strata.state import StateRepository
from strata.transforms import MissingPdfParserError, artifact_payload

ARTIFACT_URI_PREFIX = "artifact://"


class ApplyResult(dict[str, Any]):
    pass


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
        for operation in operations:
            operation_run_id = operation_run_ids[operation.op_id]
            repo.update_operation_run(operation_run_id, "running")
            try:
                if operation.op_type == "delete_scope":
                    deleted = _execute_delete(repo, run_id, operation_run_id, operation)
                    counts["deleted"] += deleted
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
            except Exception as exc:  # keep other operations inspectable
                failed = True
                counts["failed"] += 1
                repo.update_operation_run(operation_run_id, "failed", str(exc))
        for snapshot in source_snapshots.values():
            repo.update_source_checkpoint(
                snapshot.source_name,
                connection_id="local",
                scope_hash_value=snapshot.scope_hash,
                cursor_token={"scan_marker": snapshot.scan_marker},
            )
        repo.finish_run(run_id, "failed" if failed else "succeeded")
    finally:
        repo.release_lock()
    return ApplyResult(run_id=run_id, **counts)


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


def _artifact_path(manifest: Manifest, asset_name: str, fingerprint: str) -> Path:
    return manifest.artifacts_path / asset_name / f"{fingerprint}.json"


def _fanout_dir(manifest: Manifest, asset_name: str, parent_fingerprint: str) -> Path:
    return manifest.artifacts_path / asset_name / parent_fingerprint


def _fanout_item_uri(
    asset_name: str, parent_fingerprint: str, manifest_name: str, item: int
) -> str:
    return (
        f"{ARTIFACT_URI_PREFIX}{asset_name}/{parent_fingerprint}/"
        f"manifests/{manifest_name}#item={item}"
    )


def _write_artifact(path: Path, payload: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return sha256_text(payload)


def _write_immutable_text(path: Path, payload: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        if existing != payload:
            raise ValueError(f"immutable artifact already exists with different content: {path}")
    else:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
    return sha256_text(payload)


def _write_fanout_manifest(
    *,
    manifest: Manifest,
    asset_name: str,
    parent: MaterializedArtifact,
    transform: ArtifactTransform,
    source: ArtifactSource,
    upstreams: list[ArtifactUpstream],
    items: list[FanoutManifestItem],
) -> str:
    created_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest_doc = FanoutManifest(
        created_at=created_at,
        asset_name=asset_name,
        parent=FanoutManifestParent(
            asset_name=parent.asset_name,
            instance_key=parent.instance_key,
            input_fingerprint=parent.input_fingerprint,
        ),
        transform=transform,
        source=source,
        upstreams=upstreams,
        items=items,
    )
    hash_payload = manifest_doc.model_dump(mode="json")
    hash_payload.pop("manifest_hash", None)
    manifest_hash = hash_canonical(hash_payload)
    manifest_doc = manifest_doc.model_copy(update={"manifest_hash": manifest_hash})
    manifest_name = f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{manifest_hash[:12]}.json"
    manifest_path = (
        _fanout_dir(manifest, asset_name, parent.input_fingerprint) / "manifests" / manifest_name
    )
    _write_immutable_text(manifest_path, manifest_doc.model_dump_json(indent=2))
    return manifest_name


def _read_artifact(manifest: Manifest, location: str | None) -> dict[str, Any]:
    if not location:
        raise ValueError("artifact has no output location")
    if location.startswith(ARTIFACT_URI_PREFIX):
        return _read_fanout_record(manifest, location)
    return cast(dict[str, Any], json.loads(Path(location).read_text(encoding="utf-8")))


def _read_fanout_record(manifest: Manifest, location: str) -> dict[str, Any]:
    uri = location.removeprefix(ARTIFACT_URI_PREFIX)
    if "#item=" not in uri:
        raise ValueError(f"invalid artifact URI: {location}")
    manifest_ref, item_ref = uri.split("#item=", 1)
    item_number = int(item_ref)
    manifest_path = manifest.artifacts_path / manifest_ref
    manifest_doc = cast(
        dict[str, Any], json.loads(manifest_path.read_text(encoding="utf-8"))
    )
    try:
        item = cast(dict[str, Any], manifest_doc["items"][item_number])
    except IndexError as exc:
        raise ValueError(f"artifact URI item does not exist: {location}") from exc
    payload = cast(dict[str, Any], item["payload"])
    record_number = int(payload["record"])
    payload_path = manifest_path.parent.parent / str(payload["path"])
    lines = payload_path.read_text(encoding="utf-8").splitlines()
    try:
        record = cast(dict[str, Any], json.loads(lines[record_number]))
    except IndexError as exc:
        raise ValueError(f"artifact URI record does not exist: {location}") from exc
    return {
        "artifact": {
            "schema_version": manifest_doc["schema_version"],
            "asset_name": manifest_doc["asset_name"],
            "instance_key": item["instance_key"],
            "input_fingerprint": item["input_fingerprint"],
            "transform": manifest_doc["transform"],
            "source": manifest_doc.get("source", {}),
            "inputs": {"upstreams": manifest_doc.get("upstreams", [])},
            "output": {"content_hash": item.get("content_hash")},
        },
        "data": record["data"],
        "metadata": item.get("metadata", {}),
    }


def _artifact_source(payload: dict[str, Any]) -> dict[str, Any]:
    source = payload.get("artifact", {}).get("source", {})
    if not isinstance(source, dict):
        return {}
    return source


def _artifact_identity(
    *,
    asset_name: str,
    instance_key: str,
    input_fingerprint_value: str,
    transform_version: str,
    config_hash_value: str,
    content_hash: str | None = None,
    upstreams: list[ArtifactUpstream] | None = None,
    source_name: str | None = None,
    source_item_key: str | None = None,
) -> ArtifactEnvelope:
    return ArtifactEnvelope(
        asset_name=asset_name,
        instance_key=instance_key,
        input_fingerprint=input_fingerprint_value,
        transform=ArtifactTransform(
            version=transform_version,
            config_hash=config_hash_value,
        ),
        source=ArtifactSource(name=source_name, item_key=source_item_key),
        inputs=ArtifactInputs(upstreams=upstreams or []),
        output=ArtifactOutput(content_hash=content_hash),
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
        text = get_parser(asset.parser or "auto").parse(item.path)
        text_hash = sha256_text(text)
        payload = artifact_payload(
            text,
            {"source_path": str(item.path)},
            _artifact_identity(
                asset_name="parsed",
                instance_key=item_key,
                input_fingerprint_value=fingerprint,
                transform_version=asset.version,
                config_hash_value=cfg_hash,
                content_hash=text_hash,
                source_name=source_name,
                source_item_key=item_key,
            ),
        )
        path = _artifact_path(manifest, "parsed", fingerprint)
        output_hash = _write_artifact(path, payload)
        transform_id = _transform_id(manifest, repo, "parsed", cfg_hash)
        repo.write_asset_instance(
            asset_name="parsed",
            instance_key=item_key,
            input_fingerprint_value=fingerprint,
            output_location=str(path),
            output_hash=output_hash,
            content_hash=text_hash,
            transform_id=transform_id,
            materialization_strategy=asset.materialization_strategy,
            metadata_value={"source_item_key": item_key, "source_path": str(item.path)},
        )
        repo.upsert_source_state(source_name, item_key, item.content_hash)
        repo.update_operation_item(op_item_id, "succeeded")
        counts["built"] += 1
    except MissingPdfParserError as exc:
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
    parsed_payload = _read_artifact(manifest, parsed.output_location)
    text = str(parsed_payload["data"])
    parsed_source = _artifact_source(parsed_payload)
    asset = manifest.assets["chunks"]
    cfg = {"max_chars": 1200, "overlap_chars": 120, **asset.config}
    cfg_hash = config_hash(asset.config)
    transform = ArtifactTransform(version=asset.version, config_hash=cfg_hash)
    source = ArtifactSource(
        name=cast(str | None, parsed_source.get("name")),
        item_key=item_key,
    )
    chunks = get_chunker(asset.transform or asset.name).chunk(text, cfg)
    transform_id = _transform_id(manifest, repo, "chunks", cfg_hash)
    fanout_items: list[FanoutManifestItem] = []
    payload_records: list[str] = []
    work_items: list[_FanoutWorkItem] = []
    upstreams = [
        ArtifactUpstream(
            asset_name=parsed.asset_name,
            instance_key=parsed.instance_key,
            input_fingerprint=parsed.input_fingerprint,
        )
    ]
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
        payload_records.append(chunk_text)
        fanout_items.append(
            FanoutManifestItem(
                instance_key=instance_key,
                input_fingerprint=fingerprint,
                content_hash=chunk_hash,
                payload=FanoutManifestPayload(
                    path="",
                    format="jsonl",
                    record=index,
                    file_hash="",
                ),
                metadata={"ordinal": index},
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
    payload_text = "".join(
        f"{json.dumps({'data': record}, ensure_ascii=False, sort_keys=True)}\n"
        for record in payload_records
    )
    payload_hash = sha256_text(payload_text)
    payload_path = f"payloads/{payload_hash[:16]}.jsonl"
    payload_file = _fanout_dir(manifest, "chunks", parsed.input_fingerprint) / payload_path
    record_hashes = [
        sha256_text(json.dumps({"data": record}, ensure_ascii=False, sort_keys=True))
        for record in payload_records
    ]
    _write_immutable_text(payload_file, payload_text)
    fanout_items = [
        item.model_copy(
            update={
                "payload": item.payload.model_copy(
                    update={"path": payload_path, "file_hash": payload_hash}
                )
            }
        )
        for item in fanout_items
    ]
    manifest_name = _write_fanout_manifest(
        manifest=manifest,
        asset_name="chunks",
        parent=parsed,
        transform=transform,
        source=source,
        upstreams=upstreams,
        items=fanout_items,
    )
    repo.commit_asset_instances(
        [
            AssetInstanceCommit(
                asset_name="chunks",
                instance_key=work_item.instance_key,
                input_fingerprint=work_item.input_fingerprint,
                output_location=_fanout_item_uri(
                    "chunks", parsed.input_fingerprint, manifest_name, index
                ),
                output_hash=record_hashes[index],
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
    transform = ArtifactTransform(version=asset.version, config_hash=cfg_hash)
    transform_id = _transform_id(manifest, repo, "embeddings", cfg_hash)
    parsed = repo.latest_materialized("parsed", item_key)
    if not parsed:
        raise ValueError(f"missing parsed artifact for {item_key}")
    parsed_source = _artifact_source(_read_artifact(manifest, parsed.output_location))
    source = ArtifactSource(name=cast(str | None, parsed_source.get("name")), item_key=item_key)
    fanout_items: list[FanoutManifestItem] = []
    payload_records: list[list[float]] = []
    work_items: list[_FanoutWorkItem] = []
    upstreams = [
        ArtifactUpstream(
            asset_name=chunk.asset_name,
            instance_key=chunk.instance_key,
            input_fingerprint=chunk.input_fingerprint,
        )
        for chunk in chunks
    ]
    for chunk in chunks:
        chunk_payload = _read_artifact(manifest, chunk.output_location)
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
        record = int(chunk.metadata.get("ordinal", len(fanout_items)))
        payload_records.append(embedding)
        fanout_items.append(
            FanoutManifestItem(
                instance_key=chunk.instance_key,
                input_fingerprint=fingerprint,
                content_hash=embedding_hash,
                payload=FanoutManifestPayload(
                    path="",
                    format="jsonl",
                    record=record,
                    file_hash="",
                ),
                metadata={"chunk_instance_key": chunk.instance_key},
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
    payload_text = "".join(
        f"{json.dumps({'data': record}, ensure_ascii=False, sort_keys=True)}\n"
        for record in payload_records
    )
    payload_hash = sha256_text(payload_text)
    payload_path = f"payloads/{payload_hash[:16]}.jsonl"
    payload_file = _fanout_dir(manifest, "embeddings", parsed.input_fingerprint) / payload_path
    record_hashes = [
        sha256_text(json.dumps({"data": record}, ensure_ascii=False, sort_keys=True))
        for record in payload_records
    ]
    _write_immutable_text(payload_file, payload_text)
    fanout_items = [
        item.model_copy(
            update={
                "payload": item.payload.model_copy(
                    update={"path": payload_path, "file_hash": payload_hash}
                )
            }
        )
        for item in fanout_items
    ]
    manifest_name = _write_fanout_manifest(
        manifest=manifest,
        asset_name="embeddings",
        parent=parsed,
        transform=transform,
        source=source,
        upstreams=upstreams,
        items=fanout_items,
    )
    repo.commit_asset_instances(
        [
            AssetInstanceCommit(
                asset_name="embeddings",
                instance_key=work_item.instance_key,
                input_fingerprint=work_item.input_fingerprint,
                output_location=_fanout_item_uri(
                    "embeddings", parsed.input_fingerprint, manifest_name, index
                ),
                output_hash=record_hashes[index],
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
        embedding_payload = _read_artifact(manifest, embedding_artifact.output_location)
        embedding = embedding_payload["data"]
        embedding_source = _artifact_source(embedding_payload)
        chunk_key = embedding_artifact.instance_key
        chunk = repo.latest_materialized("chunks", chunk_key)
        chunk_payload = _read_artifact(manifest, chunk.output_location if chunk else None)
        chunk_text = str(chunk_payload["data"])
        fingerprint = input_fingerprint(
            transform_version=asset.version,
            config_hash_value=cfg_hash,
            determinism=asset.determinism.value,
            instance_key=chunk_key,
            upstream_fingerprints=[embedding_artifact.input_fingerprint],
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
        get_sink(asset.type or asset.name).write(
            repo=repo,
            instance_key=chunk_key,
            embedding_fingerprint=embedding_artifact.input_fingerprint,
            source_item_key=item_key,
            chunk_text=chunk_text,
            embedding=embedding,
        )
        sink_instance = repo.write_asset_instance(
            asset_name="sink",
            instance_key=chunk_key,
            input_fingerprint_value=fingerprint,
            output_location=f"sqlite://local_vector_sink/{chunk_key}",
            output_hash=hash_canonical({"chunk": chunk_key, "embedding": embedding}),
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
