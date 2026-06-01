from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from typing import Any

from strata.core.collections import (
    ArtifactPayload,
    ArtifactWrite,
    ArtifactWriteResult,
    CollectionWriteContext,
)
from strata.core.hashing import hash_canonical, input_fingerprint, sha256_text
from strata.core.operations import OperationInput, OperationOutput
from strata.execution.artifacts import read_artifact_refs
from strata.executors.protocols import (
    OperationInvocation,
    OperationWindow,
    OperationWindowResult,
)
from strata.plugins.registry import get_artifact_collection, get_operation


@dataclass(frozen=True)
class InlineOperationRunner:
    async def _run_invocation(self, invocation: OperationInvocation) -> list[OperationOutput]:
        return get_operation(invocation.operation_name).run(
            invocation.inputs,
            invocation.config,
        )

    async def run_window(self, window: OperationWindow) -> OperationWindowResult:
        input_payloads = _read_window_input_payloads(window)
        return OperationWindowResult(
            output_groups=_write_window_outputs(
                window,
                [
                    await self._run_invocation(
                        _materialize_invocation(invocation, input_payloads)
                    )
                    for invocation in window.invocations
                ],
            )
        )


@dataclass(frozen=True)
class ThreadedOperationRunner:
    max_workers: int = 4

    async def _run_invocation(self, invocation: OperationInvocation) -> list[OperationOutput]:
        return await asyncio.to_thread(self._run_sync, invocation)

    def _run_sync(self, invocation: OperationInvocation) -> list[OperationOutput]:
        return get_operation(invocation.operation_name).run(
            invocation.inputs,
            invocation.config,
        )

    async def _run_invocations(
        self,
        invocations: list[OperationInvocation],
    ) -> list[list[OperationOutput]]:
        return await asyncio.to_thread(self._run_many_sync, invocations)

    async def run_window(self, window: OperationWindow) -> OperationWindowResult:
        input_payloads = _read_window_input_payloads(window)
        invocations = [
            _materialize_invocation(invocation, input_payloads)
            for invocation in window.invocations
        ]
        return OperationWindowResult(
            output_groups=_write_window_outputs(window, await self._run_invocations(invocations))
        )

    def _run_many_sync(
        self,
        invocations: list[OperationInvocation],
    ) -> list[list[OperationOutput]]:
        if not invocations:
            return []
        results: list[list[OperationOutput] | None] = [None] * len(invocations)
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(self._run_sync, invocation): index
                for index, invocation in enumerate(invocations)
            }
            for future in as_completed(futures):
                results[futures[future]] = future.result()
        return [result or [] for result in results]


def register_builtin_operation_runners() -> None:
    from strata.executors.registry import register_operation_runner

    register_operation_runner(
        "local_single_thread",
        lambda config: InlineOperationRunner(),
    )
    register_operation_runner(
        "local_threaded",
        lambda config: ThreadedOperationRunner(
            max_workers=int((config or {}).get("max_workers") or 4)
        ),
    )


register_builtin_executors = register_builtin_operation_runners


def _materialize_invocation(
    invocation: OperationInvocation,
    input_payloads: dict[str, dict[str, Any]],
) -> OperationInvocation:
    return replace(
        invocation,
        inputs=[
            _materialize_input(input_item, input_payloads)
            for input_item in invocation.inputs
        ],
    )


def _materialize_input(
    input_item: OperationInput,
    input_payloads: dict[str, dict[str, Any]],
) -> OperationInput:
    if input_item.artifact_location is None:
        return input_item
    payload = input_payloads[input_item.input_id]
    return replace(
        input_item,
        data=payload.get("data"),
        metadata={**input_item.metadata, **dict(payload.get("metadata") or {})},
    )


def _read_window_input_payloads(window: OperationWindow) -> dict[str, dict[str, Any]]:
    input_refs = [
        (
            input_item.input_id,
            input_item.artifact_collection or "local_json",
            input_item.artifact_location,
        )
        for invocation in window.invocations
        for input_item in invocation.inputs
        if input_item.artifact_location is not None
    ]
    if not input_refs:
        return {}
    payloads = read_artifact_refs(
        window.artifact_root,
        [
            (collection_name, str(location))
            for _input_id, collection_name, location in input_refs
            if location is not None
        ],
    )
    return {
        input_id: payload
        for (input_id, _collection_name, _location), payload in zip(
            input_refs, payloads, strict=True
        )
    }


def _write_window_outputs(
    window: OperationWindow,
    output_groups: list[list[OperationOutput]],
) -> list[list[OperationOutput]]:
    if all(
        output.output_location is not None
        for outputs in output_groups
        for output in outputs
    ):
        return output_groups

    rewritten = [list(outputs) for outputs in output_groups]
    prepared: list[
        tuple[int, int, list[OperationInput], OperationOutput, ArtifactWrite]
    ] = []
    for group_index, (invocation, outputs) in enumerate(
        zip(window.invocations, output_groups, strict=True)
    ):
        input_lookup = {
            input_item.input_id: input_item
            for input_item in invocation.inputs
            if input_item.input_id
        }
        for output_index, output in enumerate(outputs):
            if output.output_location is not None:
                continue
            _original_index, parent_inputs, prepared_output, write = (
                _prepare_artifact_output(
                    window,
                    invocation,
                    input_lookup,
                    output_index,
                    output,
                )
            )
            prepared.append(
                (group_index, output_index, parent_inputs, prepared_output, write)
            )

    collection = get_artifact_collection(window.artifact_collection)
    for group_index, output_index, _parent_inputs, output, write in [
        item for item in prepared if _is_source_parent(item[2])
    ]:
        result = collection.write_one(
            CollectionWriteContext(
                root_path=window.artifact_root,
                asset_name=window.asset_name,
                window_id=window.window_id,
            ),
            write,
        )
        rewritten[group_index][output_index] = _located_output(output, result)

    by_partition: dict[
        str,
        list[tuple[int, int, list[OperationInput], OperationOutput, ArtifactWrite]],
    ] = {}
    for item in prepared:
        if _is_source_parent(item[2]):
            continue
        partition_key = _partition_key(item[2])
        by_partition.setdefault(partition_key, []).append(item)

    for partition_key, items in by_partition.items():
        results = collection.write_many(
            CollectionWriteContext(
                root_path=window.artifact_root,
                asset_name=window.asset_name,
                partition_key=partition_key,
                window_id=window.window_id,
            ),
            [item[4] for item in items],
        )
        for item, result in zip(items, results, strict=True):
            rewritten[item[0]][item[1]] = _located_output(item[3], result)

    return rewritten


def _prepare_artifact_output(
    window: OperationWindow,
    invocation: OperationInvocation,
    input_lookup: dict[str, OperationInput],
    output_index: int,
    output: OperationOutput,
) -> tuple[int, list[OperationInput], OperationOutput, ArtifactWrite]:
    parent_inputs = _parent_inputs(invocation, input_lookup, output)
    instance_key = output.instance_key or _default_instance_key(parent_inputs, output_index)
    content_hash = _output_content_hash(output)
    fingerprint = _output_fingerprint(window, parent_inputs, instance_key)
    return (
        output_index,
        parent_inputs,
        output,
        ArtifactWrite(
            instance_key=instance_key,
            input_fingerprint=fingerprint,
            content_hash=content_hash,
            payload=ArtifactPayload(data=output.data, metadata=output.metadata),
        ),
    )


def _parent_inputs(
    invocation: OperationInvocation,
    input_lookup: dict[str, OperationInput],
    output: OperationOutput,
) -> list[OperationInput]:
    if not output.parent_input_ids:
        return invocation.inputs
    parents = [input_lookup[input_id] for input_id in output.parent_input_ids]
    if not parents:
        raise ValueError("operation output has no parent inputs")
    return parents


def _output_fingerprint(
    window: OperationWindow,
    parent_inputs: list[OperationInput],
    instance_key: str,
) -> str:
    if _is_source_parent(parent_inputs):
        return input_fingerprint(
            transform_version=window.transform_version,
            config_hash_value=window.config_hash,
            determinism=window.determinism,
            instance_key=instance_key,
            source_content_hash=str(parent_inputs[0].source_content_hash),
        )
    return input_fingerprint(
        transform_version=window.transform_version,
        config_hash_value=window.config_hash,
        determinism=window.determinism,
        instance_key=instance_key,
        upstream_fingerprints=[
            str(parent.input_fingerprint) for parent in parent_inputs
        ],
    )


def _is_source_parent(parent_inputs: list[OperationInput]) -> bool:
    return len(parent_inputs) == 1 and parent_inputs[0].source_content_hash is not None


def _partition_key(parent_inputs: list[OperationInput]) -> str:
    for input_item in parent_inputs:
        if input_item.partition_key:
            return input_item.partition_key
    if parent_inputs[0].input_fingerprint:
        return parent_inputs[0].input_fingerprint
    raise ValueError("artifact output has no partition key")


def _default_instance_key(parent_inputs: list[OperationInput], output_index: int) -> str:
    if output_index == 0:
        return parent_inputs[0].instance_key
    return f"{parent_inputs[0].instance_key}#output:{output_index:04d}"


def _output_content_hash(output: OperationOutput) -> str | None:
    if output.content_hash is not None:
        return output.content_hash
    if output.data is None:
        return None
    if isinstance(output.data, str):
        return sha256_text(output.data)
    return hash_canonical(output.data)


def _located_output(
    output: OperationOutput,
    result: ArtifactWriteResult,
) -> OperationOutput:
    return replace(
        output,
        data=None,
        output_location=result.output_ref,
        output_hash=result.output_hash,
        content_hash=result.content_hash,
        instance_key=result.instance_key,
    )
