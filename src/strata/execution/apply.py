from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from strata.core.collections import ArtifactPayload, ArtifactWrite
from strata.core.hashing import config_hash, hash_canonical, input_fingerprint, sha256_text
from strata.core.models import (
    AssetInstanceCommit,
    AssetSpec,
    Manifest,
    MaterializedArtifact,
    Operation,
    OperationItemStatus,
    SourceItem,
    SourceSnapshot,
)
from strata.core.operations import OperationInput, OperationOutput
from strata.execution.artifacts import read_artifact, write_many_artifacts, write_one_artifact
from strata.executors.local import InlineOperationRunner
from strata.executors.protocols import ApplyResult, OperationInvocation, OperationRunner
from strata.state.repository import StateRepository


@dataclass(frozen=True)
class _InputGroup:
    item_key: str
    default_instance_key: str
    inputs: list[OperationInput]
    upstreams: list[MaterializedArtifact]
    operation_run_id: str
    partition_key: str | None = None
    source_name: str | None = None
    source_item: SourceItem | None = None


@dataclass(frozen=True)
class _PendingArtifactOutput:
    op_item_id: str
    instance_key: str
    input_fingerprint: str
    content_hash: str | None
    metadata: dict[str, Any]
    payload: ArtifactPayload
    group: _InputGroup


@dataclass(frozen=True)
class _InputBatch:
    groups: list[_InputGroup]
    inputs: list[OperationInput]


@dataclass(frozen=True)
class _InvocationWindowItem:
    invocation: OperationInvocation
    input_batch: _InputBatch


def apply_operations(
    *,
    manifest: Manifest,
    repo: StateRepository,
    source_snapshots: dict[str, SourceSnapshot],
    operations: list[Operation],
    runner: OperationRunner | None = None,
) -> ApplyResult:
    return asyncio.run(
        apply_operations_async(
            manifest=manifest,
            repo=repo,
            source_snapshots=source_snapshots,
            operations=operations,
            runner=runner,
        )
    )


async def apply_operations_async(
    *,
    manifest: Manifest,
    repo: StateRepository,
    source_snapshots: dict[str, SourceSnapshot],
    operations: list[Operation],
    runner: OperationRunner | None = None,
) -> ApplyResult:
    operation_runner = runner or InlineOperationRunner()
    run_id = repo.create_run(manifest.manifest_hash)
    repo.acquire_lock(run_id)
    counts: dict[str, int] = {"built": 0, "reused": 0, "deleted": 0, "failed": 0}
    failed = False
    try:
        operation_run_ids = {
            operation.op_id: repo.create_operation_run(run_id, operation)
            for operation in operations
        }
        for layer in _operation_layers(manifest, operations):
            if _is_build_layer(layer):
                operation_counts, operation_failed = await _run_build_layer(
                    manifest=manifest,
                    repo=repo,
                    source_snapshots=source_snapshots,
                    run_id=run_id,
                    operation_run_ids=operation_run_ids,
                    operations=layer,
                    runner=operation_runner,
                )
                _merge_counts(counts, operation_counts)
                failed = failed or operation_failed
                continue

            for operation in layer:
                operation_counts, operation_failed = await _run_operation(
                    manifest=manifest,
                    repo=repo,
                    source_snapshots=source_snapshots,
                    run_id=run_id,
                    operation_run_id=operation_run_ids[operation.op_id],
                    operation=operation,
                    runner=operation_runner,
                )
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


async def _run_operation(
    *,
    manifest: Manifest,
    repo: StateRepository,
    source_snapshots: dict[str, SourceSnapshot],
    run_id: str,
    operation_run_id: str,
    operation: Operation,
    runner: OperationRunner,
) -> tuple[dict[str, int], bool]:
    counts: dict[str, int] = {"built": 0, "reused": 0, "deleted": 0, "failed": 0}
    repo.update_operation_run(operation_run_id, "running")
    try:
        if operation.op_type == "delete_scope":
            counts["deleted"] += _execute_delete(repo, run_id, operation_run_id, operation)
        else:
            asset = manifest.assets[operation.asset_name]
            await _execute_flatmap_operation(
                manifest=manifest,
                repo=repo,
                source_snapshots=source_snapshots,
                run_id=run_id,
                operation_run_id=operation_run_id,
                operation=operation,
                asset=asset,
                counts=counts,
                runner=runner,
            )
        repo.update_operation_run(operation_run_id, "succeeded")
        return counts, False
    except Exception as exc:  # keep other operations inspectable
        counts["failed"] += 1
        repo.update_operation_run(operation_run_id, "failed", str(exc))
        return counts, True


async def _run_build_layer(
    *,
    manifest: Manifest,
    repo: StateRepository,
    source_snapshots: dict[str, SourceSnapshot],
    run_id: str,
    operation_run_ids: dict[str, str],
    operations: list[Operation],
    runner: OperationRunner,
) -> tuple[dict[str, int], bool]:
    counts: dict[str, int] = {"built": 0, "reused": 0, "deleted": 0, "failed": 0}
    if not operations:
        return counts, False

    asset_names = {operation.asset_name for operation in operations}
    if len(asset_names) != 1:
        raise ValueError("build layers must contain operations for exactly one asset")
    asset = manifest.assets[operations[0].asset_name]
    transform_id = _transform_id(manifest, repo, asset.name, config_hash(asset.config))
    groups: list[_InputGroup] = []
    failed = False
    failed_operation_run_ids: set[str] = set()

    for operation in operations:
        operation_run_id = operation_run_ids[operation.op_id]
        repo.update_operation_run(operation_run_id, "running")
        try:
            operation_groups = _build_input_groups(
                manifest,
                repo,
                source_snapshots,
                operation,
                asset,
                operation_run_id,
            )
            if not operation_groups:
                raise ValueError(
                    f"no inputs found for {asset.name}/{operation.scope.item_key}"
                )
            groups.extend(operation_groups)
        except Exception as exc:
            counts["failed"] += 1
            failed = True
            failed_operation_run_ids.add(operation_run_id)
            repo.update_operation_run(operation_run_id, "failed", str(exc))

    if not groups:
        return counts, failed

    try:
        for window in _invocation_windows(
            manifest=manifest,
            repo=repo,
            operation_id=f"layer:{asset.name}",
            asset=asset,
            groups=groups,
        ):
            output_groups = await runner.run_many([item.invocation for item in window])
            _commit_output_window(
                manifest=manifest,
                repo=repo,
                run_id=run_id,
                asset=asset,
                transform_id=transform_id,
                window=window,
                output_groups=output_groups,
                counts=counts,
            )
    except Exception as exc:
        failed = True
        failed_operation_run_ids.update(group.operation_run_id for group in groups)
        for operation_run_id in failed_operation_run_ids:
            repo.update_operation_run(operation_run_id, "failed", str(exc))
        counts["failed"] += len(failed_operation_run_ids)
        return counts, True

    for operation in operations:
        operation_run_id = operation_run_ids[operation.op_id]
        if operation_run_id not in failed_operation_run_ids:
            repo.update_operation_run(operation_run_id, "succeeded")
    return counts, failed


def _operation_layers(manifest: Manifest, operations: list[Operation]) -> list[list[Operation]]:
    deletes = [operation for operation in operations if operation.op_type == "delete_scope"]
    batches: list[list[Operation]] = []
    if deletes:
        batches.append(deletes)
    for asset_name in manifest.asset_order:
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


def _is_build_layer(layer: list[Operation]) -> bool:
    return bool(layer) and all(operation.op_type == "build_scope" for operation in layer)


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


def _scope_artifacts(
    manifest: Manifest,
    repo: StateRepository,
    asset_name: str,
    source_item_key: str,
) -> list[MaterializedArtifact]:
    direct = repo.latest_materialized(asset_name, source_item_key)
    if direct:
        return [direct]

    artifacts_by_id: dict[str, MaterializedArtifact] = {}
    for root_name in _source_roots_for_asset(manifest, manifest.assets[asset_name]):
        root = repo.latest_materialized(root_name, source_item_key)
        if root is None:
            continue
        if root.asset_name == asset_name:
            artifacts_by_id[root.id] = root
            continue
        for artifact in repo.materialized_descendants(root.id, asset_name=asset_name):
            artifacts_by_id[artifact.id] = artifact
    return sorted(
        artifacts_by_id.values(),
        key=lambda artifact: artifact.instance_key,
    )


def _asset_dependencies(asset: AssetSpec) -> list[str]:
    if asset.inputs:
        return list(dict(sorted(asset.inputs.items())).values())
    if asset.input:
        return [asset.input]
    return []


def _partition_key_for_scope(
    manifest: Manifest,
    repo: StateRepository,
    asset: AssetSpec,
    item_key: str,
    fallback_artifacts: list[MaterializedArtifact],
) -> str:
    for root_name in _source_roots_for_asset(manifest, asset):
        root = repo.latest_materialized(root_name, item_key)
        if root:
            return root.input_fingerprint
    return fallback_artifacts[0].input_fingerprint


def _source_roots_for_asset(manifest: Manifest, asset: AssetSpec) -> list[str]:
    if asset.source:
        return [asset.name]
    roots: list[str] = []
    for dependency in _asset_dependencies(asset):
        roots.extend(_source_roots_for_asset(manifest, manifest.assets[dependency]))
    return roots


def _fanout_instance_key(source_item_key: str, asset_name: str, ordinal: int) -> str:
    return f"{source_item_key}#{_fanout_label(asset_name)}:{ordinal:04d}"


def _fanout_label(asset_name: str) -> str:
    return asset_name[:-1] if asset_name.endswith("s") and len(asset_name) > 1 else asset_name


def _transform_id(manifest: Manifest, repo: StateRepository, asset_name: str, cfg_hash: str) -> str:
    asset = manifest.assets[asset_name]
    return repo.upsert_transform(
        project_id=manifest.context.project_id,
        transform_id=asset.operation_name,
        version=asset.version,
        config_json=json.dumps(asset.config, sort_keys=True),
        config_hash_value=cfg_hash,
        determinism=asset.determinism.value,
    )


def _plugin_config(asset: AssetSpec, repo: StateRepository | None = None) -> dict[str, Any]:
    config = dict(asset.config)
    if repo is not None:
        config["_strata_repo"] = repo
    return config


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
    )


def _artifact_operation_input(
    role: str,
    artifact: MaterializedArtifact,
    payload: dict[str, Any],
) -> OperationInput:
    return OperationInput(
        role=role,
        asset_name=artifact.asset_name,
        instance_key=artifact.instance_key,
        input_id=f"artifact:{artifact.id}",
        data=payload.get("data"),
        metadata={**artifact.metadata, **dict(payload.get("metadata") or {})},
        input_fingerprint=artifact.input_fingerprint,
        content_hash=artifact.content_hash,
    )


def _output_content_hash(output: OperationOutput) -> str | None:
    if output.content_hash is not None:
        return output.content_hash
    if output.data is None:
        return None
    if isinstance(output.data, str):
        return sha256_text(output.data)
    return hash_canonical(output.data)


async def _execute_flatmap_operation(
    *,
    manifest: Manifest,
    repo: StateRepository,
    source_snapshots: dict[str, SourceSnapshot],
    run_id: str,
    operation_run_id: str,
    operation: Operation,
    asset: AssetSpec,
    counts: dict[str, int],
    runner: OperationRunner,
) -> None:
    groups = _build_input_groups(
        manifest,
        repo,
        source_snapshots,
        operation,
        asset,
        operation_run_id,
    )
    if not groups:
        raise ValueError(f"no inputs found for {asset.name}/{operation.scope.item_key}")
    transform_id = _transform_id(manifest, repo, asset.name, config_hash(asset.config))
    for window in _invocation_windows(
        manifest=manifest,
        repo=repo,
        operation_id=operation.op_id,
        asset=asset,
        groups=groups,
    ):
        output_groups = await runner.run_many([item.invocation for item in window])
        _commit_output_window(
            manifest=manifest,
            repo=repo,
            run_id=run_id,
            asset=asset,
            transform_id=transform_id,
            window=window,
            output_groups=output_groups,
            counts=counts,
        )


def _invocation_windows(
    *,
    manifest: Manifest,
    repo: StateRepository,
    operation_id: str,
    asset: AssetSpec,
    groups: list[_InputGroup],
) -> Iterator[list[_InvocationWindowItem]]:
    window_size = _window_size(manifest, asset)
    current: list[_InvocationWindowItem] = []
    for index, input_batch in enumerate(_input_batches(manifest, asset, groups)):
        current.append(
            _InvocationWindowItem(
                invocation=OperationInvocation(
                    invocation_id=f"{operation_id}:{index}",
                    operation_name=asset.operation_name,
                    inputs=input_batch.inputs,
                    config=_plugin_config(asset, repo if asset.kind == "sink" else None),
                ),
                input_batch=input_batch,
            )
        )
        if len(current) >= window_size:
            yield current
            current = []
    if current:
        yield current


def _window_size(manifest: Manifest, asset: AssetSpec) -> int:
    value = asset.execution.get(
        "window_size",
        manifest.execution.config.get("window_size", 1000),
    )
    return max(1, int(value))


def _commit_output_window(
    *,
    manifest: Manifest,
    repo: StateRepository,
    run_id: str,
    asset: AssetSpec,
    transform_id: str,
    window: list[_InvocationWindowItem],
    output_groups: list[list[OperationOutput]],
    counts: dict[str, int],
) -> None:
    artifact_outputs: list[_PendingArtifactOutput] = []
    for item, outputs in zip(window, output_groups, strict=True):
        artifact_outputs.extend(
            _commit_output_batch(
                repo=repo,
                run_id=run_id,
                asset=asset,
                transform_id=transform_id,
                input_batch=item.input_batch,
                outputs=outputs,
                counts=counts,
            )
        )
    if artifact_outputs:
        _commit_artifact_outputs_by_partition(
            manifest=manifest,
            repo=repo,
            asset=asset,
            transform_id=transform_id,
            outputs=artifact_outputs,
        )
        seen_groups: set[int] = set()
        for output in artifact_outputs:
            group_id = id(output.group)
            if group_id not in seen_groups:
                seen_groups.add(group_id)
                _update_source_state(repo, output.group)


def _input_batches(
    manifest: Manifest,
    asset: AssetSpec,
    groups: list[_InputGroup],
) -> list[_InputBatch]:
    inputs_per_call = _inputs_per_call(manifest, asset)
    if inputs_per_call <= 1:
        return [_make_input_batch([group]) for group in groups]

    batches: list[_InputBatch] = []
    pending: list[_InputGroup] = []
    pending_input_count = 0
    for group in groups:
        group_input_count = len(group.inputs)
        if group_input_count != 1:
            if pending:
                batches.append(_make_input_batch(pending))
                pending = []
                pending_input_count = 0
            batches.append(_make_input_batch([group]))
            continue
        if pending and pending_input_count + group_input_count > inputs_per_call:
            batches.append(_make_input_batch(pending))
            pending = []
            pending_input_count = 0
        pending.append(group)
        pending_input_count += group_input_count
    if pending:
        batches.append(_make_input_batch(pending))
    return batches


def _make_input_batch(groups: list[_InputGroup]) -> _InputBatch:
    return _InputBatch(
        groups=groups,
        inputs=[input_item for group in groups for input_item in group.inputs],
    )


def _inputs_per_call(manifest: Manifest, asset: AssetSpec) -> int:
    value = asset.execution.get(
        "inputs_per_call",
        manifest.execution.config.get("inputs_per_call", 1),
    )
    return max(1, int(value))


def _build_input_groups(
    manifest: Manifest,
    repo: StateRepository,
    source_snapshots: dict[str, SourceSnapshot],
    operation: Operation,
    asset: AssetSpec,
    operation_run_id: str,
) -> list[_InputGroup]:
    item_key = operation.scope.item_key or ""
    if asset.source:
        source_name = operation.scope.source_name or asset.source
        item = _source_item(source_snapshots, source_name, item_key)
        return [
            _InputGroup(
                item_key=item_key,
                default_instance_key=item_key,
                inputs=[_source_operation_input(source_name, item)],
                upstreams=[],
                operation_run_id=operation_run_id,
                source_name=source_name,
                source_item=item,
            )
        ]
    if asset.input:
        upstreams = _scope_artifacts(manifest, repo, asset.input, item_key)
        if not upstreams:
            raise ValueError(f"missing {asset.input} artifacts for {item_key}")
        partition_key = _partition_key_for_scope(manifest, repo, asset, item_key, upstreams)
        return [
            _artifact_group(
                manifest=manifest,
                role="input",
                artifact=upstream,
                item_key=item_key,
                operation_run_id=operation_run_id,
                partition_key=partition_key,
            )
            for upstream in upstreams
        ]
    if asset.inputs:
        return _build_join_groups(manifest, repo, asset, item_key, operation_run_id)
    raise ValueError(f"asset {asset.name} must define source, input, or inputs")


def _artifact_group(
    *,
    manifest: Manifest,
    role: str,
    artifact: MaterializedArtifact,
    item_key: str,
    operation_run_id: str,
    partition_key: str | None,
) -> _InputGroup:
    payload = read_artifact(manifest, artifact.output_location)
    return _InputGroup(
        item_key=item_key,
        default_instance_key=artifact.instance_key,
        inputs=[_artifact_operation_input(role, artifact, payload)],
        upstreams=[artifact],
        operation_run_id=operation_run_id,
        partition_key=partition_key,
    )


def _build_join_groups(
    manifest: Manifest,
    repo: StateRepository,
    asset: AssetSpec,
    item_key: str,
    operation_run_id: str,
) -> list[_InputGroup]:
    primary_role = str(asset.join.get("primary") or next(iter(asset.inputs)))
    if primary_role not in asset.inputs:
        raise ValueError(
            f"asset {asset.name} join.primary must reference one of its inputs"
        )
    primary_asset_name = asset.inputs[primary_role]
    primary_artifacts = _scope_artifacts(manifest, repo, primary_asset_name, item_key)
    if not primary_artifacts:
        raise ValueError(f"missing {primary_asset_name} artifacts for {item_key}")

    groups: list[_InputGroup] = []
    for primary in primary_artifacts:
        role_artifacts: dict[str, MaterializedArtifact] = {primary_role: primary}
        for role, input_asset_name in asset.inputs.items():
            if role == primary_role:
                continue
            artifact = next(iter(repo.upstream_artifacts(primary.id, input_asset_name)), None)
            if artifact is None:
                artifact = repo.latest_materialized(input_asset_name, primary.instance_key)
            if artifact is None:
                raise ValueError(
                    f"missing {input_asset_name} artifact for {primary.instance_key}"
                )
            role_artifacts[role] = artifact

        inputs = [
            _artifact_operation_input(
                role,
                artifact,
                read_artifact(manifest, artifact.output_location),
            )
            for role, artifact in sorted(role_artifacts.items())
        ]
        upstreams = [artifact for _role, artifact in sorted(role_artifacts.items())]
        groups.append(
            _InputGroup(
                item_key=item_key,
                default_instance_key=primary.instance_key,
                inputs=inputs,
                upstreams=upstreams,
                operation_run_id=operation_run_id,
                partition_key=_partition_key_for_scope(
                    manifest,
                    repo,
                    asset,
                    item_key,
                    upstreams,
                ),
            )
        )
    return groups


def _commit_output_batch(
    *,
    repo: StateRepository,
    run_id: str,
    asset: AssetSpec,
    transform_id: str,
    input_batch: _InputBatch,
    outputs: list[OperationOutput],
    counts: dict[str, int],
) -> list[_PendingArtifactOutput]:
    artifact_outputs: list[_PendingArtifactOutput] = []
    group_output_counts: dict[int, int] = {}
    for output in outputs:
        group = _output_group(input_batch, output)
        group_id = id(group)
        group_output_index = group_output_counts.get(group_id, 0)
        group_output_counts[group_id] = group_output_index + 1
        instance_key = output.instance_key or _default_output_instance_key(
            group,
            asset,
            group_output_index,
        )
        fingerprint = _output_fingerprint(asset, group, instance_key)
        op_item_id = repo.create_operation_item(
            run_id=run_id,
            operation_run_id=group.operation_run_id,
            asset_name=asset.name,
            item_key=instance_key,
            instance_key=instance_key,
            input_fingerprint_value=fingerprint,
            status="running",
        )
        cached = repo.find_materialized(asset.name, instance_key, fingerprint)
        if cached:
            repo.update_operation_item(op_item_id, "skipped")
            _update_source_state(repo, group)
            counts["reused"] += 1
            continue

        if output.output_location is not None:
            _commit_external_output(
                repo=repo,
                asset=asset,
                transform_id=transform_id,
                group=group,
                output=output,
                instance_key=instance_key,
                fingerprint=fingerprint,
                op_item_id=op_item_id,
            )
            _update_source_state(repo, group)
            counts["built"] += 1
            continue

        content_hash = _output_content_hash(output)
        artifact_outputs.append(
            _PendingArtifactOutput(
                op_item_id=op_item_id,
                instance_key=instance_key,
                input_fingerprint=fingerprint,
                content_hash=content_hash,
                metadata=_output_metadata(group, output),
                payload=ArtifactPayload(data=output.data, metadata=output.metadata),
                group=group,
            )
        )
        counts["built"] += 1

    return artifact_outputs


def _output_group(input_batch: _InputBatch, output: OperationOutput) -> _InputGroup:
    if not output.parent_input_ids:
        if len(input_batch.groups) == 1:
            return input_batch.groups[0]
        raise ValueError(
            "batched operation output must define parent_input_ids when an "
            "invocation contains multiple input groups"
        )

    input_to_group = {
        input_item.input_id: group
        for group in input_batch.groups
        for input_item in group.inputs
    }
    groups: list[_InputGroup] = []
    seen_group_ids: set[int] = set()
    for input_id in output.parent_input_ids:
        group = input_to_group.get(input_id)
        if group is None:
            raise ValueError(f"operation output references unknown parent input: {input_id}")
        group_identity = id(group)
        if group_identity not in seen_group_ids:
            seen_group_ids.add(group_identity)
            groups.append(group)
    if len(groups) == 1:
        return groups[0]
    return _merge_output_groups(groups)


def _merge_output_groups(groups: list[_InputGroup]) -> _InputGroup:
    upstreams_by_id: dict[str, MaterializedArtifact] = {}
    for group in groups:
        for upstream in group.upstreams:
            upstreams_by_id[upstream.id] = upstream

    item_keys = {group.item_key for group in groups}
    partition_keys = {group.partition_key for group in groups if group.partition_key is not None}
    first = groups[0]
    same_source_name = all(group.source_name == first.source_name for group in groups)
    same_source_item = all(group.source_item == first.source_item for group in groups)
    return _InputGroup(
        item_key=first.item_key if len(item_keys) == 1 else "",
        default_instance_key=first.default_instance_key,
        inputs=[input_item for group in groups for input_item in group.inputs],
        upstreams=list(upstreams_by_id.values()),
        operation_run_id=first.operation_run_id,
        partition_key=first.partition_key if len(partition_keys) <= 1 else None,
        source_name=first.source_name if same_source_name else None,
        source_item=first.source_item if same_source_item else None,
    )


def _commit_external_output(
    *,
    repo: StateRepository,
    asset: AssetSpec,
    transform_id: str,
    group: _InputGroup,
    output: OperationOutput,
    instance_key: str,
    fingerprint: str,
    op_item_id: str,
) -> None:
    artifact = repo.write_asset_instance(
        asset_name=asset.name,
        instance_key=instance_key,
        input_fingerprint_value=fingerprint,
        output_location=output.output_location,
        output_hash=output.output_hash,
        content_hash=output.content_hash,
        transform_id=transform_id,
        materialization_strategy=asset.materialization_strategy,
        metadata_value=_output_metadata(group, output),
    )
    for upstream in group.upstreams:
        repo.write_lineage(upstream.id, artifact.id)
    repo.update_operation_item(op_item_id, "succeeded")


def _commit_artifact_outputs_by_partition(
    *,
    manifest: Manifest,
    repo: StateRepository,
    asset: AssetSpec,
    transform_id: str,
    outputs: list[_PendingArtifactOutput],
) -> None:
    source_outputs = [output for output in outputs if not output.group.upstreams]
    if source_outputs:
        _commit_artifact_outputs(
            manifest=manifest,
            repo=repo,
            asset=asset,
            transform_id=transform_id,
            outputs=source_outputs,
        )

    outputs_by_partition: dict[str, list[_PendingArtifactOutput]] = {}
    for output in outputs:
        if not output.group.upstreams:
            continue
        outputs_by_partition.setdefault(
            _artifact_output_partition_key(output),
            [],
        ).append(output)

    for partition_outputs in outputs_by_partition.values():
        _commit_artifact_outputs(
            manifest=manifest,
            repo=repo,
            asset=asset,
            transform_id=transform_id,
            outputs=partition_outputs,
        )


def _artifact_output_partition_key(output: _PendingArtifactOutput) -> str:
    return output.group.partition_key or output.group.upstreams[0].input_fingerprint


def _commit_artifact_outputs(
    *,
    manifest: Manifest,
    repo: StateRepository,
    asset: AssetSpec,
    transform_id: str,
    outputs: list[_PendingArtifactOutput],
) -> None:
    if all(not output.group.upstreams for output in outputs):
        for output in outputs:
            write_result = write_one_artifact(
                manifest=manifest,
                asset_name=asset.name,
                item=ArtifactWrite(
                    instance_key=output.instance_key,
                    input_fingerprint=output.input_fingerprint,
                    content_hash=output.content_hash,
                    payload=output.payload,
                ),
            )
            repo.write_asset_instance(
                asset_name=asset.name,
                instance_key=output.instance_key,
                input_fingerprint_value=output.input_fingerprint,
                output_location=write_result.output_ref,
                output_hash=write_result.output_hash,
                content_hash=output.content_hash,
                transform_id=transform_id,
                materialization_strategy=asset.materialization_strategy,
                metadata_value=output.metadata,
            )
            repo.update_operation_item(output.op_item_id, "succeeded")
        return

    partition_key = (
        outputs[0].group.partition_key
        or outputs[0].group.upstreams[0].input_fingerprint
    )
    write_results = write_many_artifacts(
        manifest=manifest,
        asset_name=asset.name,
        partition_key=partition_key,
        items=[
            ArtifactWrite(
                instance_key=output.instance_key,
                input_fingerprint=output.input_fingerprint,
                content_hash=output.content_hash,
                payload=output.payload,
            )
            for output in outputs
        ],
    )
    repo.commit_asset_instances(
        [
            AssetInstanceCommit(
                asset_name=asset.name,
                instance_key=output.instance_key,
                input_fingerprint=output.input_fingerprint,
                output_location=write_results[index].output_ref,
                output_hash=write_results[index].output_hash,
                content_hash=output.content_hash,
                transform_id=transform_id,
                materialization_strategy=asset.materialization_strategy,
                metadata=output.metadata,
                upstream_id=output.group.upstreams[0].id,
                additional_upstream_ids=[
                    upstream.id for upstream in output.group.upstreams[1:]
                ],
                operation_item_id=output.op_item_id,
                operation_status=OperationItemStatus.SUCCEEDED,
            )
            for index, output in enumerate(outputs)
        ]
    )


def _output_fingerprint(asset: AssetSpec, group: _InputGroup, instance_key: str) -> str:
    if group.source_item is not None:
        return input_fingerprint(
            transform_version=asset.version,
            config_hash_value=config_hash(asset.config),
            determinism=asset.determinism.value,
            instance_key=instance_key,
            source_content_hash=group.source_item.content_hash,
        )
    return input_fingerprint(
        transform_version=asset.version,
        config_hash_value=config_hash(asset.config),
        determinism=asset.determinism.value,
        instance_key=instance_key,
        upstream_fingerprints=[upstream.input_fingerprint for upstream in group.upstreams],
    )


def _default_output_instance_key(group: _InputGroup, asset: AssetSpec, index: int) -> str:
    if index == 0:
        return group.default_instance_key
    return _fanout_instance_key(group.default_instance_key, asset.name, index)


def _output_metadata(group: _InputGroup, output: OperationOutput) -> dict[str, Any]:
    metadata = dict(output.metadata)
    metadata.setdefault("source_item_key", group.item_key)
    if group.upstreams:
        metadata.setdefault("upstream_instance_key", group.upstreams[0].instance_key)
    if group.source_name is not None:
        metadata.setdefault("source_name", group.source_name)
    if group.source_item is not None:
        metadata.setdefault("source_content_hash", group.source_item.content_hash)
    return metadata


def _update_source_state(repo: StateRepository, group: _InputGroup) -> None:
    if group.source_name is not None and group.source_item is not None:
        repo.upsert_source_state(
            group.source_name,
            group.source_item.item_key,
            group.source_item.content_hash,
        )


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
