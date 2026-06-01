from __future__ import annotations

from strata.core.models import Manifest, Operation


def operation_layers(manifest: Manifest, operations: list[Operation]) -> list[list[Operation]]:
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


def is_build_layer(layer: list[Operation]) -> bool:
    return bool(layer) and all(operation.op_type == "build_scope" for operation in layer)


def merge_counts(target: dict[str, int], source: dict[str, int]) -> None:
    for key, value in source.items():
        target[key] = target.get(key, 0) + value
