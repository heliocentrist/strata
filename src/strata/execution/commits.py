from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from strata.core.collections import ArtifactPayload
from strata.core.hashing import config_hash, hash_canonical, input_fingerprint, sha256_text
from strata.core.models import (
    AssetInstanceCommit,
    AssetSpec,
    Manifest,
    MaterializedArtifact,
    OperationItemStatus,
)
from strata.core.operations import OperationOutput
from strata.execution.inputs import group_source_identity
from strata.execution.types import (
    InputBatch,
    InputGroup,
    InvocationWindowItem,
    PendingArtifactOutput,
    PendingExternalOutput,
    PendingOutputBatch,
)
from strata.execution.windows import asset_artifact_collection
from strata.state.repository import StateRepository

logger = logging.getLogger(__name__)


def commit_output_window(
    *,
    manifest: Manifest,
    repo: StateRepository,
    run_id: str,
    asset: AssetSpec,
    transform_id: str,
    window: list[InvocationWindowItem],
    output_groups: list[list[OperationOutput]],
    counts: dict[str, int],
    completion_counts: dict[tuple[str, str, str], int],
) -> None:
    _ = manifest
    artifact_outputs: list[PendingArtifactOutput] = []
    external_outputs: list[PendingExternalOutput] = []
    for item, outputs in zip(window, output_groups, strict=True):
        pending = _commit_output_batch(
            repo=repo,
            run_id=run_id,
            asset=asset,
            input_batch=item.input_batch,
            outputs=outputs,
            counts=counts,
            completion_counts=completion_counts,
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
        raise ValueError(
            f"runner returned {len(artifact_outputs)} unwritten artifact output(s) "
            f"for asset {asset.name}; runners must write payloads and return locations"
        )


def _commit_output_batch(
    *,
    repo: StateRepository,
    run_id: str,
    asset: AssetSpec,
    input_batch: InputBatch,
    outputs: list[OperationOutput],
    counts: dict[str, int],
    completion_counts: dict[tuple[str, str, str], int],
) -> PendingOutputBatch:
    artifact_outputs: list[PendingArtifactOutput] = []
    external_outputs: list[PendingExternalOutput] = []
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
        scope_fingerprint = _scope_fingerprint(asset, group)
        _record_completion_output(completion_counts, group, scope_fingerprint)
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
                PendingExternalOutput(
                    op_item_id=op_item_id,
                    instance_key=instance_key,
                    input_fingerprint=fingerprint,
                    scope_fingerprint=scope_fingerprint,
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
            PendingArtifactOutput(
                op_item_id=op_item_id,
                instance_key=instance_key,
                input_fingerprint=fingerprint,
                scope_fingerprint=scope_fingerprint,
                content_hash=content_hash,
                metadata=_output_metadata(group, output),
                payload=ArtifactPayload(data=output.data, metadata=output.metadata),
                group=group,
            )
        )
        counts["built"] += 1

    return PendingOutputBatch(artifacts=artifact_outputs, external=external_outputs)


def _output_group(input_batch: InputBatch, output: OperationOutput) -> InputGroup:
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
    groups: list[InputGroup] = []
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


def _merge_output_groups(groups: list[InputGroup]) -> InputGroup:
    upstreams_by_id: dict[str, MaterializedArtifact] = {}
    for group in groups:
        for upstream in group.upstreams:
            upstreams_by_id[upstream.id] = upstream

    item_keys = {group.item_key for group in groups}
    partition_keys = {group.partition_key for group in groups if group.partition_key is not None}
    first = groups[0]
    same_source_name = all(group.source_name == first.source_name for group in groups)
    same_source_item = all(group.source_item == first.source_item for group in groups)
    return InputGroup(
        item_key=first.item_key if len(item_keys) == 1 else "",
        default_instance_key=first.default_instance_key,
        inputs=[input_item for group in groups for input_item in group.inputs],
        upstreams=list(upstreams_by_id.values()),
        operation_run_id=first.operation_run_id,
        partition_key=first.partition_key if len(partition_keys) <= 1 else None,
        source_name=first.source_name if same_source_name else None,
        source_item=first.source_item if same_source_item else None,
        operation_id=",".join(sorted({group.operation_id for group in groups})),
    )


def _commit_external_outputs(
    *,
    repo: StateRepository,
    asset: AssetSpec,
    transform_id: str,
    outputs: list[PendingExternalOutput],
) -> None:
    if not outputs:
        return
    logger.debug(
        "commit external asset=%s outputs=%s",
        asset.name,
        len(outputs),
    )
    source_outputs = [output for output in outputs if not output.group.upstreams]
    deleted_source_scopes: set[tuple[str, str]] = set()
    for output in source_outputs:
        source_name, source_item_key = group_source_identity(output.group)
        if source_name is None or source_item_key is None:
            continue
        source_key = (source_name, source_item_key)
        if source_key not in deleted_source_scopes:
            repo.mark_lineage_deleted_for_source(source_name, source_item_key)
            deleted_source_scopes.add(source_key)
    for output in source_outputs:
        source_name, source_item_key = group_source_identity(output.group)
        repo.write_asset_instance(
            asset_name=asset.name,
            instance_key=output.instance_key,
            input_fingerprint_value=output.input_fingerprint,
            output_location=output.output_location,
            output_hash=output.output_hash,
            content_hash=output.content_hash,
            transform_id=transform_id,
            materialization_strategy=asset.materialization_strategy,
            source_name=source_name,
            source_item_key=source_item_key,
            scope_fingerprint=output.scope_fingerprint,
            artifact_collection=asset_artifact_collection(asset),
            metadata_value=output.metadata,
        )
        repo.update_operation_item(output.op_item_id, "succeeded")

    upstream_outputs = [output for output in outputs if output.group.upstreams]
    if upstream_outputs:
        repo.commit_asset_instances(
            [
                AssetInstanceCommit(
                    asset_name=asset.name,
                    instance_key=output.instance_key,
                    input_fingerprint=output.input_fingerprint,
                    output_location=output.output_location,
                    source_name=group_source_identity(output.group)[0],
                    source_item_key=group_source_identity(output.group)[1],
                    scope_fingerprint=output.scope_fingerprint,
                    artifact_collection=asset_artifact_collection(asset),
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
                for output in upstream_outputs
            ]
        )


def _output_fingerprint(asset: AssetSpec, group: InputGroup, instance_key: str) -> str:
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


def _scope_fingerprint(asset: AssetSpec, group: InputGroup) -> str:
    source_name, source_item_key = group_source_identity(group)
    if source_name is None or source_item_key is None:
        raise ValueError(f"cannot compute source-scope fingerprint for asset {asset.name}")
    if group.source_item is not None:
        return input_fingerprint(
            transform_version=asset.version,
            config_hash_value=config_hash(asset.config),
            determinism=asset.determinism.value,
            instance_key=f"{source_name}:{source_item_key}",
            source_content_hash=group.source_item.content_hash,
        )
    upstream_scopes = [
        upstream.scope_fingerprint
        for upstream in group.upstreams
        if upstream.scope_fingerprint is not None
    ]
    if len(upstream_scopes) != len(group.upstreams):
        raise ValueError(f"cannot compute source-scope fingerprint for asset {asset.name}")
    return input_fingerprint(
        transform_version=asset.version,
        config_hash_value=config_hash(asset.config),
        determinism=asset.determinism.value,
        instance_key=f"{source_name}:{source_item_key}",
        upstream_fingerprints=upstream_scopes,
    )


def _record_completion_output(
    completion_counts: dict[tuple[str, str, str], int],
    group: InputGroup,
    scope_fingerprint: str,
) -> None:
    source_name, source_item_key = group_source_identity(group)
    if source_name is None or source_item_key is None:
        return
    key = (source_name, source_item_key, scope_fingerprint)
    completion_counts[key] = completion_counts.get(key, 0) + 1


def _default_output_instance_key(group: InputGroup, asset: AssetSpec, index: int) -> str:
    if index == 0:
        return group.default_instance_key
    return _fanout_instance_key(group.default_instance_key, asset.name, index)


def _output_metadata(group: InputGroup, output: OperationOutput) -> dict[str, Any]:
    metadata = dict(output.metadata)
    metadata.setdefault("source_item_key", group.item_key)
    if group.upstreams:
        metadata.setdefault("upstream_instance_key", group.upstreams[0].instance_key)
    if group.source_name is not None:
        metadata.setdefault("source_name", group.source_name)
    if group.source_item is not None:
        metadata.setdefault("source_content_hash", group.source_item.content_hash)
    return metadata


def _update_source_state(repo: StateRepository, group: InputGroup) -> None:
    if group.source_name is not None and group.source_item is not None:
        repo.upsert_source_state(
            group.source_name,
            group.source_item.item_key,
            group.source_item.content_hash,
        )


def _update_source_state_for_groups(
    repo: StateRepository, groups: Iterable[InputGroup]
) -> None:
    seen_groups: set[int] = set()
    for group in groups:
        group_id = id(group)
        if group_id in seen_groups:
            continue
        seen_groups.add(group_id)
        _update_source_state(repo, group)


def _output_content_hash(output: OperationOutput) -> str | None:
    if output.content_hash is not None:
        return output.content_hash
    if output.data is None:
        return None
    if isinstance(output.data, str):
        return sha256_text(output.data)
    return hash_canonical(output.data)


def _fanout_instance_key(source_item_key: str, asset_name: str, ordinal: int) -> str:
    return f"{source_item_key}#{_fanout_label(asset_name)}:{ordinal:04d}"


def _fanout_label(asset_name: str) -> str:
    return asset_name[:-1] if asset_name.endswith("s") and len(asset_name) > 1 else asset_name
