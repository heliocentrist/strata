from __future__ import annotations

from dataclasses import dataclass

from strata.core.hashing import config_hash, input_fingerprint
from strata.core.models import (
    AssetSpec,
    CurrentState,
    Manifest,
    Operation,
    OperationScope,
    SourceSnapshot,
    SourceSnapshotMode,
)
from strata.core.selectors import parse_selection


@dataclass(frozen=True)
class _ExpectedInstance:
    instance_key: str
    input_fingerprint: str


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
            operations.extend(
                _delete_operations_for_source(manifest, selected.assets, source_name, item_key)
            )
            continue

        old_hash = current_state.source_hashes.get((source_name, item_key))
        if old_hash is None:
            root_reason = "new_source"
        elif old_hash != item.content_hash:
            root_reason = "source_changed"
        else:
            root_reason = ""

        expected: dict[str, _ExpectedInstance] = {}
        reasons: dict[str, str] = {}
        for asset_name in manifest.asset_order:
            asset = manifest.assets[asset_name]
            if not _asset_applies_to_source(manifest, asset, source_name):
                continue
            expected_instance = _expected_instance(asset, item_key, item.content_hash, expected)
            expected[asset_name] = expected_instance
            upstream_reason = _upstream_reason(asset, reasons)
            current = (
                asset_name,
                expected_instance.instance_key,
                expected_instance.input_fingerprint,
            ) in current_state.materialized
            reason = root_reason or upstream_reason or ("" if current else _missing_reason(asset))
            reasons[asset_name] = reason
            if reason and asset_name in selected.assets:
                operations.append(
                    _operation(
                        manifest,
                        op_type="build_scope",
                        asset_name=asset_name,
                        item_key=item_key,
                        source_name=source_name,
                        reason=reason,
                        count=1 if asset.source else None,
                    )
                )

    current_source_keys = set(current_state.source_hashes)
    observed_keys = {key for key, item in source_items.items() if not item.deleted}
    for source_name, item_key in sorted(current_source_keys - observed_keys):
        if source_name not in authoritative_sources:
            continue
        if selected.source_names is not None and source_name not in selected.source_names:
            continue
        operations.extend(
            _delete_operations_for_source(manifest, selected.assets, source_name, item_key)
        )

    if not operations:
        return []
    return _with_dependencies(manifest, operations)


def _asset_applies_to_source(manifest: Manifest, asset: AssetSpec, source_name: str) -> bool:
    if asset.source:
        return asset.source == source_name
    upstream_names = _asset_dependencies(asset)
    return any(
        _asset_applies_to_source(manifest, manifest.assets[name], source_name)
        for name in upstream_names
    )


def _expected_instance(
    asset: AssetSpec,
    source_item_key: str,
    source_content_hash: str,
    expected: dict[str, _ExpectedInstance],
) -> _ExpectedInstance:
    cfg_hash = config_hash(asset.config)
    if asset.source:
        instance_key = source_item_key
        fingerprint = input_fingerprint(
            transform_version=asset.version,
            config_hash_value=cfg_hash,
            determinism=asset.determinism.value,
            instance_key=instance_key,
            source_content_hash=source_content_hash,
        )
        return _ExpectedInstance(instance_key=instance_key, input_fingerprint=fingerprint)

    upstreams = [expected[name] for name in _asset_dependencies(asset)]
    if not upstreams:
        raise ValueError(f"asset {asset.name} has no expected upstream instance")
    output_label = asset.config.get("output_label")
    if isinstance(output_label, str) and output_label:
        instance_key = _fanout_instance_key(source_item_key, output_label, 0)
    else:
        instance_key = upstreams[0].instance_key
    fingerprint = input_fingerprint(
        transform_version=asset.version,
        config_hash_value=cfg_hash,
        determinism=asset.determinism.value,
        instance_key=instance_key,
        upstream_fingerprints=[upstream.input_fingerprint for upstream in upstreams],
    )
    return _ExpectedInstance(instance_key=instance_key, input_fingerprint=fingerprint)


def _asset_dependencies(asset: AssetSpec) -> list[str]:
    if asset.inputs:
        return list(dict(sorted(asset.inputs.items())).values())
    if asset.input:
        return [asset.input]
    return []


def _upstream_reason(asset: AssetSpec, reasons: dict[str, str]) -> str:
    for upstream_name in _asset_dependencies(asset):
        reason = reasons.get(upstream_name, "")
        if reason:
            return reason
    return ""


def _missing_reason(asset: AssetSpec) -> str:
    if asset.source:
        return "missing_cached_instance"
    return "config_or_cache_changed"


def _delete_operations_for_source(
    manifest: Manifest, selected_assets: set[str], source_name: str, item_key: str
) -> list[Operation]:
    return [
        _operation(
            manifest,
            op_type="delete_scope",
            asset_name=asset.name,
            item_key=item_key,
            source_name=source_name,
            reason="source_deleted",
        )
        for asset in _terminal_assets(manifest)
        if asset.name in selected_assets
    ]


def _terminal_assets(manifest: Manifest) -> list[AssetSpec]:
    depended_on = {
        dependency
        for asset in manifest.assets.values()
        for dependency in _asset_dependencies(asset)
    }
    terminals = [asset for asset in manifest.assets.values() if asset.name not in depended_on]
    sink_terminals = [asset for asset in terminals if asset.kind == "sink"]
    return sink_terminals or terminals


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


def _with_dependencies(manifest: Manifest, operations: list[Operation]) -> list[Operation]:
    by_item: dict[str, list[Operation]] = {}
    for operation in operations:
        by_item.setdefault(operation.scope.item_key or "", []).append(operation)

    order = {asset_name: index for index, asset_name in enumerate(manifest.asset_order)}
    sorted_ops: list[Operation] = []
    for item_key in sorted(by_item):
        item_ops = sorted(by_item[item_key], key=lambda op: order.get(op.asset_name, 9999))
        previous: Operation | None = None
        for operation in item_ops:
            if previous and operation.op_type == "build_scope":
                operation.depends_on.append(previous.op_id)
            sorted_ops.append(operation)
            previous = operation
    return sorted_ops


def _fanout_instance_key(source_item_key: str, label: str, ordinal: int) -> str:
    return f"{source_item_key}#{label}:{ordinal:04d}"
