from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from strata.core.hashing import config_hash, hash_canonical
from strata.core.models import AssetSpec, Manifest
from strata.core.operations import OperationInput
from strata.execution.types import InputBatch, InputGroup, InvocationWindowItem
from strata.executors.protocols import OperationInvocation, OperationWindow


def invocation_windows(
    *,
    manifest: Manifest,
    asset: AssetSpec,
    groups: Iterable[InputGroup],
) -> Iterator[list[InvocationWindowItem]]:
    size = window_size(manifest, asset)
    current: list[InvocationWindowItem] = []
    for input_batch in input_batches(manifest, asset, groups):
        current.append(
            InvocationWindowItem(
                invocation=OperationInvocation(
                    invocation_id=invocation_id(asset, input_batch),
                    operation_name=asset.operation_name,
                    inputs=input_batch.inputs,
                    config=plugin_config(manifest, asset),
                ),
                input_batch=input_batch,
            )
        )
        if len(current) >= size:
            yield current
            current = []
    if current:
        yield current


def runner_window(
    manifest: Manifest,
    asset: AssetSpec,
    window: list[InvocationWindowItem],
) -> OperationWindow:
    return OperationWindow(
        window_id=window_id(asset, window),
        asset_name=asset.name,
        artifact_root=manifest.artifacts_path,
        artifact_collection=asset_artifact_collection(asset),
        transform_version=asset.version,
        config_hash=config_hash(asset.config),
        determinism=asset.determinism.value,
        invocations=[item.invocation for item in window],
    )


def asset_artifact_collection(asset: AssetSpec) -> str:
    return str(asset.artifact_strategy.get("type") or "local_json")


def plugin_config(manifest: Manifest, asset: AssetSpec) -> dict[str, Any]:
    config = dict(asset.config)
    config.setdefault(
        "_strata",
        {
            "state_url": manifest.state_url,
            "project_root": str(manifest.root),
            "project_id": manifest.context.project_id,
            "tenant_id": manifest.context.tenant_id,
        },
    )
    return config


def input_batches(
    manifest: Manifest,
    asset: AssetSpec,
    groups: Iterable[InputGroup],
) -> Iterator[InputBatch]:
    inputs_per_invocation = inputs_per_call(manifest, asset)
    if inputs_per_invocation <= 1:
        for group in groups:
            yield make_input_batch([group])
        return

    pending: list[InputGroup] = []
    pending_input_count = 0
    for group in groups:
        group_input_count = len(group.inputs)
        if group_input_count != 1:
            if pending:
                yield make_input_batch(pending)
                pending = []
                pending_input_count = 0
            yield make_input_batch([group])
            continue
        if pending and pending_input_count + group_input_count > inputs_per_invocation:
            yield make_input_batch(pending)
            pending = []
            pending_input_count = 0
        pending.append(group)
        pending_input_count += group_input_count
    if pending:
        yield make_input_batch(pending)


def make_input_batch(groups: list[InputGroup]) -> InputBatch:
    return InputBatch(
        groups=groups,
        inputs=[input_item for group in groups for input_item in group.inputs],
    )


def invocation_id(asset: AssetSpec, input_batch: InputBatch) -> str:
    digest = hash_canonical(
        {
            "asset_name": asset.name,
            "operation_name": asset.operation_name,
            "operation_ids": [group.operation_id for group in input_batch.groups],
            "inputs": [_input_identity(input_item) for input_item in input_batch.inputs],
        }
    )
    return f"inv-{digest[:24]}"


def window_id(asset: AssetSpec, window: list[InvocationWindowItem]) -> str:
    digest = hash_canonical(
        {
            "asset_name": asset.name,
            "operation_name": asset.operation_name,
            "invocation_ids": [item.invocation.invocation_id for item in window],
        }
    )
    return f"win-{digest[:24]}"


def _input_identity(input_item: OperationInput) -> dict[str, Any]:
    return {
        "asset_name": input_item.asset_name,
        "instance_key": input_item.instance_key,
        "input_fingerprint": input_item.input_fingerprint,
        "content_hash": input_item.content_hash,
        "source_name": input_item.source_name,
        "source_content_hash": input_item.source_content_hash,
        "partition_key": input_item.partition_key,
    }


def inputs_per_call(manifest: Manifest, asset: AssetSpec) -> int:
    value = asset.execution.get(
        "inputs_per_call",
        manifest.execution.config.get("inputs_per_call", 1),
    )
    return max(1, int(value))


def window_size(manifest: Manifest, asset: AssetSpec) -> int:
    value = asset.execution.get(
        "window_size",
        manifest.execution.config.get("window_size", 1000),
    )
    return max(1, int(value))
