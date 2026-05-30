from __future__ import annotations

from strata.core.hashing import config_hash, input_fingerprint
from strata.core.models import (
    CurrentState,
    Manifest,
    Operation,
    OperationScope,
    SourceSnapshot,
    SourceSnapshotMode,
)
from strata.core.selectors import parse_selection


def plan(
    manifest: Manifest,
    current_state: CurrentState,
    source_snapshots: dict[str, SourceSnapshot],
    selection: str | None = None,
) -> list[Operation]:
    selected = parse_selection(manifest, selection)
    operations: list[Operation] = []

    source_items = {
        (snapshot.source_name, item.item_key): item
        for snapshot in source_snapshots.values()
        for item in snapshot.items
    }
    authoritative_sources = {
        snapshot.source_name
        for snapshot in source_snapshots.values()
        if snapshot.mode == SourceSnapshotMode.AUTHORITATIVE
    }

    for source_name, item_key in sorted(source_items):
        if selected.source_names is not None and source_name not in selected.source_names:
            continue
        item = source_items[(source_name, item_key)]
        if item.deleted:
            if "sink" in selected.assets:
                operations.append(
                    _operation(
                        manifest,
                        op_type="delete_scope",
                        asset_name="sink",
                        item_key=item_key,
                        source_name=source_name,
                        reason="source_deleted",
                    )
                )
            continue
        old_hash = current_state.source_hashes.get((source_name, item_key))
        if old_hash is None:
            root_reason = "new_source"
        elif old_hash != item.content_hash:
            root_reason = "source_changed"
        else:
            root_reason = ""

        parsed_asset = manifest.assets["parsed"]
        parsed_fingerprint = input_fingerprint(
            transform_version=parsed_asset.version,
            config_hash_value=config_hash(parsed_asset.config),
            determinism=parsed_asset.determinism.value,
            instance_key=item_key,
            source_content_hash=item.content_hash,
        )
        parsed_current = ("parsed", item_key, parsed_fingerprint) in current_state.materialized
        parsed_reason = root_reason or ("" if parsed_current else "missing_cached_instance")

        first_chunk_key = f"{item_key}#chunk:0000"
        chunks_asset = manifest.assets["chunks"]
        chunks_fingerprint = input_fingerprint(
            transform_version=chunks_asset.version,
            config_hash_value=config_hash(chunks_asset.config),
            determinism=chunks_asset.determinism.value,
            instance_key=first_chunk_key,
            upstream_fingerprints=[parsed_fingerprint],
        )
        chunks_current = (
            "chunks",
            first_chunk_key,
            chunks_fingerprint,
        ) in current_state.materialized
        chunks_reason = root_reason or ("" if chunks_current else "config_or_cache_changed")

        embeddings_asset = manifest.assets["embeddings"]
        embeddings_fingerprint = input_fingerprint(
            transform_version=embeddings_asset.version,
            config_hash_value=config_hash(embeddings_asset.config),
            determinism=embeddings_asset.determinism.value,
            instance_key=first_chunk_key,
            upstream_fingerprints=[chunks_fingerprint],
        )
        embeddings_current = (
            "embeddings",
            first_chunk_key,
            embeddings_fingerprint,
        ) in current_state.materialized
        embeddings_reason = root_reason or chunks_reason or (
            "" if embeddings_current else "config_or_cache_changed"
        )

        sink_asset = manifest.assets["sink"]
        sink_fingerprint = input_fingerprint(
            transform_version=sink_asset.version,
            config_hash_value=config_hash(sink_asset.config),
            determinism=sink_asset.determinism.value,
            instance_key=first_chunk_key,
            upstream_fingerprints=[chunks_fingerprint, embeddings_fingerprint],
        )
        sink_current = ("sink", first_chunk_key, sink_fingerprint) in current_state.materialized
        sink_reason = root_reason or embeddings_reason or (
            "" if sink_current else "config_or_cache_changed"
        )

        if parsed_reason and "parsed" in selected.assets:
            operations.append(
                _operation(
                    manifest,
                    op_type="build_scope",
                    asset_name="parsed",
                    item_key=item_key,
                    source_name=source_name,
                    reason=parsed_reason,
                    count=1,
                )
            )
        if chunks_reason and "chunks" in selected.assets:
            operations.append(
                _operation(
                    manifest,
                    op_type="build_scope",
                    asset_name="chunks",
                    item_key=item_key,
                    source_name=source_name,
                    reason=chunks_reason,
                )
            )
        if embeddings_reason and "embeddings" in selected.assets:
            operations.append(
                _operation(
                    manifest,
                    op_type="build_scope",
                    asset_name="embeddings",
                    item_key=item_key,
                    source_name=source_name,
                    reason=embeddings_reason,
                )
            )
        if sink_reason and "sink" in selected.assets:
            operations.append(
                _operation(
                    manifest,
                    op_type="build_scope",
                    asset_name="sink",
                    item_key=item_key,
                    source_name=source_name,
                    reason=sink_reason,
                )
            )

    current_source_keys = set(current_state.source_hashes)
    observed_keys = {key for key, item in source_items.items() if not item.deleted}
    for source_name, item_key in sorted(current_source_keys - observed_keys):
        if source_name not in authoritative_sources:
            continue
        if selected.source_names is not None and source_name not in selected.source_names:
            continue
        if "sink" in selected.assets:
            operations.append(
                _operation(
                    manifest,
                    op_type="delete_scope",
                    asset_name="sink",
                    item_key=item_key,
                    source_name=source_name,
                    reason="source_deleted",
                )
            )

    if not operations:
        return []
    return _with_dependencies(operations)


def _operation(
    manifest: Manifest,
    *,
    op_type: str,
    asset_name: str,
    item_key: str,
    source_name: str,
    reason: str,
    count: int | None = None,
) -> Operation:
    safe_item = item_key.replace("/", "_").replace("\\", "_")
    op_id = f"{op_type}:{asset_name}:{safe_item}"
    return Operation(
        op_id=op_id,
        op_type=op_type,  # type: ignore[arg-type]
        asset_name=asset_name,
        project_id=manifest.context.project_id,
        tenant_id=manifest.context.tenant_id,
        scope=OperationScope(source_name=source_name, item_key=item_key),
        reason=reason,
        estimated_instance_count=count,
    )


def _with_dependencies(operations: list[Operation]) -> list[Operation]:
    by_item: dict[str, list[Operation]] = {}
    for operation in operations:
        by_item.setdefault(operation.scope.item_key or "", []).append(operation)

    order = {"parsed": 0, "chunks": 1, "embeddings": 2, "sink": 3}
    sorted_ops: list[Operation] = []
    for item_key in sorted(by_item):
        item_ops = sorted(by_item[item_key], key=lambda op: order.get(op.asset_name, 99))
        previous: Operation | None = None
        for operation in item_ops:
            if previous and operation.op_type == "build_scope":
                operation.depends_on.append(previous.op_id)
            sorted_ops.append(operation)
            previous = operation
    return sorted_ops
