from __future__ import annotations

from collections.abc import Iterator

from strata.core.models import (
    AssetSpec,
    Manifest,
    MaterializedArtifact,
    Operation,
    SourceItem,
    SourceSnapshot,
)
from strata.core.operations import OperationInput
from strata.core.scopes import asset_scope_fingerprint
from strata.execution.types import InputGroup
from strata.state.repository import StateRepository


def build_input_groups(
    manifest: Manifest,
    repo: StateRepository,
    source_snapshots: dict[str, SourceSnapshot],
    operation: Operation,
    asset: AssetSpec,
    operation_run_id: str,
) -> Iterator[InputGroup]:
    for item_key in operation_item_keys(operation):
        yield from _build_input_groups_for_item(
            manifest,
            repo,
            source_snapshots,
            operation,
            asset,
            operation_run_id,
            item_key,
        )


def operation_item_keys(operation: Operation) -> list[str]:
    if operation.scope.item_keys:
        return operation.scope.item_keys
    if operation.scope.item_key is not None:
        return [operation.scope.item_key]
    return []


def operation_scope(operation: Operation) -> str:
    if operation.scope.item_key is not None:
        return operation.scope.item_key
    if operation.scope.item_keys:
        if len(operation.scope.item_keys) == 1:
            return operation.scope.item_keys[0]
        source_name = operation.scope.source_name or "source"
        return f"{source_name}:{len(operation.scope.item_keys)} items"
    return operation.scope.upstream_instance_key or ""


def group_source_identity(group: InputGroup) -> tuple[str | None, str | None]:
    if group.source_name is not None and group.source_item is not None:
        return group.source_name, group.source_item.item_key
    for upstream in group.upstreams:
        if upstream.source_name is not None and upstream.source_item_key is not None:
            return upstream.source_name, upstream.source_item_key
    return group.source_name, group.item_key or None


def source_roots_for_asset(manifest: Manifest, asset: AssetSpec) -> list[str]:
    if asset.source:
        return [asset.name]
    roots: list[str] = []
    for dependency in _asset_dependencies(asset):
        roots.extend(source_roots_for_asset(manifest, manifest.assets[dependency]))
    return roots


def _build_input_groups_for_item(
    manifest: Manifest,
    repo: StateRepository,
    source_snapshots: dict[str, SourceSnapshot],
    operation: Operation,
    asset: AssetSpec,
    operation_run_id: str,
    item_key: str,
) -> Iterator[InputGroup]:
    if asset.source:
        source_name = operation.scope.source_name or asset.source
        item = _source_item(source_snapshots, source_name, item_key)
        yield (
            InputGroup(
                item_key=item_key,
                default_instance_key=item_key,
                inputs=[_source_operation_input(source_name, item)],
                upstreams=[],
                operation_id=operation.op_id,
                operation_run_id=operation_run_id,
                source_name=source_name,
                source_item=item,
            )
        )
        return
    if asset.input:
        source_name = operation.scope.source_name or _source_name_for_asset(manifest, asset)
        source_content_hash = _source_item(
            source_snapshots,
            source_name,
            item_key,
        ).content_hash
        upstreams = _scope_artifacts(
            manifest,
            repo,
            asset.input,
            source_name,
            item_key,
            source_content_hash,
        )
        if not upstreams:
            raise ValueError(f"missing {asset.input} artifacts for {item_key}")
        partition_key = _partition_key_for_scope(
            manifest,
            repo,
            asset,
            source_name,
            item_key,
            source_content_hash,
            upstreams,
        )
        for upstream in upstreams:
            yield _artifact_group(
                artifact=upstream,
                item_key=item_key,
                operation_id=operation.op_id,
                operation_run_id=operation_run_id,
                partition_key=partition_key,
            )
        return
    raise ValueError(f"asset {asset.name} must define source or input")


def _source_item(
    source_snapshots: dict[str, SourceSnapshot], source_name: str, item_key: str
) -> SourceItem:
    for item in source_snapshots[source_name].items:
        if item.item_key == item_key:
            return item
    raise KeyError(f"source item not found in snapshot: {source_name}/{item_key}")


def _scope_artifacts(
    manifest: Manifest,
    repo: StateRepository,
    asset_name: str,
    source_name: str | None,
    source_item_key: str,
    source_content_hash: str,
) -> list[MaterializedArtifact]:
    if source_name is None:
        raise ValueError(
            f"cannot resolve {asset_name} artifacts without a source name for {source_item_key}"
        )
    return repo.materialized_for_source_scope(
        asset_name,
        source_name,
        source_item_key,
        scope_fingerprint=asset_scope_fingerprint(
            manifest,
            asset_name,
            source_name=source_name,
            source_item_key=source_item_key,
            source_content_hash=source_content_hash,
        ),
    )


def _asset_dependencies(asset: AssetSpec) -> list[str]:
    if asset.input:
        return [asset.input]
    return []


def _partition_key_for_scope(
    manifest: Manifest,
    repo: StateRepository,
    asset: AssetSpec,
    source_name: str | None,
    item_key: str,
    source_content_hash: str,
    fallback_artifacts: list[MaterializedArtifact],
) -> str:
    if source_name is None:
        return fallback_artifacts[0].input_fingerprint
    for root_name in source_roots_for_asset(manifest, asset):
        roots = repo.materialized_for_source_scope(
            root_name,
            source_name,
            item_key,
            scope_fingerprint=asset_scope_fingerprint(
                manifest,
                root_name,
                source_name=source_name,
                source_item_key=item_key,
                source_content_hash=source_content_hash,
            ),
        )
        if roots:
            return roots[0].input_fingerprint
    return fallback_artifacts[0].input_fingerprint


def _source_name_for_asset(manifest: Manifest, asset: AssetSpec) -> str:
    roots = source_roots_for_asset(manifest, asset)
    if len(roots) != 1:
        raise ValueError(f"cannot infer source for asset {asset.name}")
    source = manifest.assets[roots[0]].source
    if source is None:
        raise ValueError(f"source root {roots[0]} has no source")
    return source


def _source_operation_input(source_name: str, item: SourceItem) -> OperationInput:
    return OperationInput(
        role="source",
        asset_name=None,
        instance_key=item.item_key,
        input_id=f"source:{source_name}:{item.item_key}:{item.content_hash}",
        metadata=item.metadata,
        content_hash=item.content_hash,
        source_name=source_name,
        source_content_hash=item.content_hash,
        path=item.path,
        uri=item.uri,
        partition_key=item.content_hash,
    )


def _artifact_operation_input(
    role: str,
    artifact: MaterializedArtifact,
    partition_key: str | None,
) -> OperationInput:
    return OperationInput(
        role=role,
        asset_name=artifact.asset_name,
        instance_key=artifact.instance_key,
        input_id=f"artifact:{artifact.id}",
        metadata=artifact.metadata,
        input_fingerprint=artifact.input_fingerprint,
        content_hash=artifact.content_hash,
        source_name=artifact.source_name,
        artifact_location=artifact.output_location,
        artifact_collection=artifact.artifact_collection,
        partition_key=partition_key or artifact.input_fingerprint,
    )


def _artifact_group(
    *,
    artifact: MaterializedArtifact,
    item_key: str,
    operation_id: str,
    operation_run_id: str,
    partition_key: str | None,
) -> InputGroup:
    return InputGroup(
        item_key=item_key,
        default_instance_key=artifact.instance_key,
        inputs=[_artifact_operation_input("input", artifact, partition_key)],
        upstreams=[artifact],
        operation_id=operation_id,
        operation_run_id=operation_run_id,
        partition_key=partition_key,
        source_name=artifact.source_name,
    )
