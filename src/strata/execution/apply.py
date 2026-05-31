from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterable, Iterator
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

logger = logging.getLogger(__name__)


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
class _PendingExternalOutput:
    op_item_id: str
    instance_key: str
    input_fingerprint: str
    output_location: str
    output_hash: str | None
    content_hash: str | None
    metadata: dict[str, Any]
    data: Any
    group: _InputGroup


@dataclass(frozen=True)
class _PendingOutputBatch:
    artifacts: list[_PendingArtifactOutput]
    external: list[_PendingExternalOutput]


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
    logger.debug(
        "apply start run_id=%s project=%s tenant=%s operations=%s runner=%s",
        run_id,
        manifest.context.project_id,
        manifest.context.tenant_id,
        len(operations),
        type(operation_runner).__name__,
    )
    try:
        operation_run_ids = {
            operation.op_id: repo.create_operation_run(run_id, operation)
            for operation in operations
        }
        for layer in _operation_layers(manifest, operations):
            logger.debug(
                "apply layer run_id=%s kind=%s operations=%s assets=%s",
                run_id,
                "build" if _is_build_layer(layer) else "mixed",
                len(layer),
                ",".join(sorted({operation.asset_name for operation in layer})),
            )
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
                logger.debug(
                    "apply layer complete run_id=%s counts=%s failed=%s",
                    run_id,
                    operation_counts,
                    operation_failed,
                )
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
                logger.debug(
                    "apply operation complete run_id=%s op_id=%s counts=%s failed=%s",
                    run_id,
                    operation.op_id,
                    operation_counts,
                    operation_failed,
                )
        for snapshot in source_snapshots.values():
            repo.update_source_checkpoint(
                snapshot.source_name,
                connection_id=snapshot.connection_id,
                scope_hash_value=snapshot.scope_hash,
                cursor_token={"scan_marker": snapshot.scan_marker},
            )
        repo.finish_run(run_id, "failed" if failed else "succeeded")
        logger.debug("apply finish run_id=%s counts=%s failed=%s", run_id, counts, failed)
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
    failed = False
    failed_operation_run_ids: set[str] = set()
    active_operation_run_ids: set[str] = set()
    saw_groups = False

    try:
        logger.debug(
            "build layer start run_id=%s asset=%s operations=%s transform_id=%s",
            run_id,
            asset.name,
            len(operations),
            transform_id,
        )
        for window in _invocation_windows(
            manifest=manifest,
            repo=repo,
            operation_id=f"layer:{asset.name}",
            asset=asset,
            groups=_iter_layer_input_groups(
                manifest=manifest,
                repo=repo,
                source_snapshots=source_snapshots,
                operations=operations,
                asset=asset,
                operation_run_ids=operation_run_ids,
                counts=counts,
                failed_operation_run_ids=failed_operation_run_ids,
                active_operation_run_ids=active_operation_run_ids,
            ),
        ):
            saw_groups = True
            logger.debug(
                "build window run_id=%s asset=%s invocations=%s inputs=%s",
                run_id,
                asset.name,
                len(window),
                sum(len(item.input_batch.inputs) for item in window),
            )
            output_groups = await runner.run_many([item.invocation for item in window])
            logger.debug(
                "build window outputs run_id=%s asset=%s output_groups=%s outputs=%s",
                run_id,
                asset.name,
                len(output_groups),
                sum(len(outputs) for outputs in output_groups),
            )
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
        logger.debug(
            "build layer failed run_id=%s asset=%s error=%s",
            run_id,
            asset.name,
            exc,
        )
        failed = True
        failed_operation_run_ids.update(active_operation_run_ids)
        for operation_run_id in failed_operation_run_ids:
            repo.update_operation_run(operation_run_id, "failed", str(exc))
        counts["failed"] += len(failed_operation_run_ids)
        return counts, True

    if failed_operation_run_ids:
        failed = True
    if not saw_groups and not failed:
        failed = True
        counts["failed"] += 1

    for operation in operations:
        operation_run_id = operation_run_ids[operation.op_id]
        if operation_run_id not in failed_operation_run_ids:
            repo.update_operation_run(operation_run_id, "succeeded")
    logger.debug(
        "build layer finish run_id=%s asset=%s counts=%s failed=%s",
        run_id,
        asset.name,
        counts,
        failed,
    )
    return counts, failed


def _iter_layer_input_groups(
    *,
    manifest: Manifest,
    repo: StateRepository,
    source_snapshots: dict[str, SourceSnapshot],
    operations: list[Operation],
    asset: AssetSpec,
    operation_run_ids: dict[str, str],
    counts: dict[str, int],
    failed_operation_run_ids: set[str],
    active_operation_run_ids: set[str],
) -> Iterator[_InputGroup]:
    for operation in operations:
        operation_run_id = operation_run_ids[operation.op_id]
        repo.update_operation_run(operation_run_id, "running")
        operation_group_count = 0
        logger.debug(
            "input groups start asset=%s op_id=%s scope=%s",
            asset.name,
            operation.op_id,
            _operation_scope(operation),
        )
        try:
            for group in _build_input_groups(
                manifest,
                repo,
                source_snapshots,
                operation,
                asset,
                operation_run_id,
            ):
                operation_group_count += 1
                active_operation_run_ids.add(operation_run_id)
                yield group
            logger.debug(
                "input groups finish asset=%s op_id=%s groups=%s",
                asset.name,
                operation.op_id,
                operation_group_count,
            )
            if operation_group_count == 0:
                raise ValueError(f"no inputs found for {asset.name}/{_operation_scope(operation)}")
        except Exception as exc:
            logger.debug(
                "input groups failed asset=%s op_id=%s error=%s",
                asset.name,
                operation.op_id,
                exc,
            )
            counts["failed"] += 1
            failed_operation_run_ids.add(operation_run_id)
            repo.update_operation_run(operation_run_id, "failed", str(exc))


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


def _plugin_config(asset: AssetSpec) -> dict[str, Any]:
    return dict(asset.config)


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
    groups = list(
        _build_input_groups(
            manifest,
            repo,
            source_snapshots,
            operation,
            asset,
            operation_run_id,
        )
    )
    if not groups:
        raise ValueError(f"no inputs found for {asset.name}/{_operation_scope(operation)}")
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
    groups: Iterable[_InputGroup],
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
                    config=_plugin_config(asset),
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
    external_outputs: list[_PendingExternalOutput] = []
    for item, outputs in zip(window, output_groups, strict=True):
        pending = _commit_output_batch(
            repo=repo,
            run_id=run_id,
            asset=asset,
            input_batch=item.input_batch,
            outputs=outputs,
            counts=counts,
        )
        artifact_outputs.extend(pending.artifacts)
        external_outputs.extend(pending.external)
    logger.debug(
        "commit window asset=%s invocations=%s artifacts=%s external=%s",
        asset.name,
        len(window),
        len(artifact_outputs),
        len(external_outputs),
    )
    if external_outputs:
        _commit_external_outputs(
            repo=repo,
            asset=asset,
            transform_id=transform_id,
            outputs=external_outputs,
        )
        _update_source_state_for_groups(repo, [output.group for output in external_outputs])
    if artifact_outputs:
        _commit_artifact_outputs_by_partition(
            manifest=manifest,
            repo=repo,
            asset=asset,
            transform_id=transform_id,
            outputs=artifact_outputs,
        )
        _update_source_state_for_groups(repo, [output.group for output in artifact_outputs])


def _input_batches(
    manifest: Manifest,
    asset: AssetSpec,
    groups: Iterable[_InputGroup],
) -> Iterator[_InputBatch]:
    inputs_per_call = _inputs_per_call(manifest, asset)
    if inputs_per_call <= 1:
        for group in groups:
            yield _make_input_batch([group])
        return

    pending: list[_InputGroup] = []
    pending_input_count = 0
    for group in groups:
        group_input_count = len(group.inputs)
        if group_input_count != 1:
            if pending:
                yield _make_input_batch(pending)
                pending = []
                pending_input_count = 0
            yield _make_input_batch([group])
            continue
        if pending and pending_input_count + group_input_count > inputs_per_call:
            yield _make_input_batch(pending)
            pending = []
            pending_input_count = 0
        pending.append(group)
        pending_input_count += group_input_count
    if pending:
        yield _make_input_batch(pending)


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
) -> Iterator[_InputGroup]:
    for item_key in _operation_item_keys(operation):
        yield from _build_input_groups_for_item(
            manifest,
            repo,
            source_snapshots,
            operation,
            asset,
            operation_run_id,
            item_key,
        )


def _build_input_groups_for_item(
    manifest: Manifest,
    repo: StateRepository,
    source_snapshots: dict[str, SourceSnapshot],
    operation: Operation,
    asset: AssetSpec,
    operation_run_id: str,
    item_key: str,
) -> Iterator[_InputGroup]:
    if asset.source:
        source_name = operation.scope.source_name or asset.source
        item = _source_item(source_snapshots, source_name, item_key)
        yield (
            _InputGroup(
                item_key=item_key,
                default_instance_key=item_key,
                inputs=[_source_operation_input(source_name, item)],
                upstreams=[],
                operation_run_id=operation_run_id,
                source_name=source_name,
                source_item=item,
            )
        )
        return
    if asset.input:
        upstreams = _scope_artifacts(manifest, repo, asset.input, item_key)
        if not upstreams:
            raise ValueError(f"missing {asset.input} artifacts for {item_key}")
        partition_key = _partition_key_for_scope(manifest, repo, asset, item_key, upstreams)
        for upstream in upstreams:
            yield _artifact_group(
                manifest=manifest,
                role="input",
                artifact=upstream,
                item_key=item_key,
                operation_run_id=operation_run_id,
                partition_key=partition_key,
            )
        return
    if asset.inputs:
        yield from _build_join_groups(manifest, repo, asset, item_key, operation_run_id)
        return
    raise ValueError(f"asset {asset.name} must define source, input, or inputs")


def _operation_item_keys(operation: Operation) -> list[str]:
    if operation.scope.item_keys:
        return operation.scope.item_keys
    if operation.scope.item_key is not None:
        return [operation.scope.item_key]
    return []


def _operation_scope(operation: Operation) -> str:
    if operation.scope.item_key is not None:
        return operation.scope.item_key
    if operation.scope.item_keys:
        if len(operation.scope.item_keys) == 1:
            return operation.scope.item_keys[0]
        source_name = operation.scope.source_name or "source"
        return f"{source_name}:{len(operation.scope.item_keys)} items"
    return operation.scope.upstream_instance_key or ""


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
    input_batch: _InputBatch,
    outputs: list[OperationOutput],
    counts: dict[str, int],
) -> _PendingOutputBatch:
    artifact_outputs: list[_PendingArtifactOutput] = []
    external_outputs: list[_PendingExternalOutput] = []
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
            external_outputs.append(
                _PendingExternalOutput(
                    op_item_id=op_item_id,
                    instance_key=instance_key,
                    input_fingerprint=fingerprint,
                    output_location=output.output_location,
                    output_hash=output.output_hash,
                    content_hash=output.content_hash,
                    metadata=_output_metadata(group, output),
                    data=output.data,
                    group=group,
                )
            )
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

    return _PendingOutputBatch(artifacts=artifact_outputs, external=external_outputs)


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


def _commit_external_outputs(
    *,
    repo: StateRepository,
    asset: AssetSpec,
    transform_id: str,
    outputs: list[_PendingExternalOutput],
) -> None:
    if not outputs:
        return
    vector_rows = [
        _vector_sink_row(output)
        for output in outputs
        if str(output.output_location).startswith("sqlite://local_vector_sink/")
    ]
    logger.debug(
        "commit external asset=%s outputs=%s vector_rows=%s",
        asset.name,
        len(outputs),
        len(vector_rows),
    )
    repo.upsert_vectors(vector_rows)
    repo.commit_asset_instances(
        [
            AssetInstanceCommit(
                asset_name=asset.name,
                instance_key=output.instance_key,
                input_fingerprint=output.input_fingerprint,
                output_location=output.output_location,
                output_hash=output.output_hash,
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
            for output in outputs
        ]
    )


def _vector_sink_row(output: _PendingExternalOutput) -> dict[str, Any]:
    if not isinstance(output.data, dict):
        raise ValueError("local vector sink output must carry vector row data")
    return {
        "instance_key": output.data["instance_key"],
        "embedding_fingerprint": output.data["embedding_fingerprint"],
        "source_item_key": output.data["source_item_key"],
        "chunk_text": output.data["chunk_text"],
        "embedding": output.data["embedding"],
    }


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
        logger.debug(
            "commit source artifacts asset=%s outputs=%s",
            asset.name,
            len(source_outputs),
        )
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
        logger.debug(
            "commit partition artifacts asset=%s partition=%s outputs=%s",
            asset.name,
            _artifact_output_partition_key(partition_outputs[0]),
            len(partition_outputs),
        )
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


def _update_source_state_for_groups(
    repo: StateRepository, groups: Iterable[_InputGroup]
) -> None:
    seen_groups: set[int] = set()
    for group in groups:
        group_id = id(group)
        if group_id in seen_groups:
            continue
        seen_groups.add(group_id)
        _update_source_state(repo, group)


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
