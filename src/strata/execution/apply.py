from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterator

from strata.core.hashing import config_hash
from strata.core.models import (
    AssetSpec,
    Manifest,
    Operation,
    SourceSnapshot,
)
from strata.execution.commits import commit_output_window
from strata.execution.deletes import execute_delete
from strata.execution.inputs import (
    build_input_groups,
    operation_scope,
)
from strata.execution.layers import is_build_layer, merge_counts, operation_layers
from strata.execution.types import (
    InputGroup,
)
from strata.execution.windows import (
    invocation_windows,
    runner_window,
)
from strata.executors.local import InlineOperationRunner
from strata.executors.protocols import (
    ApplyResult,
    OperationRunner,
)
from strata.state.repository import StateRepository, new_id

logger = logging.getLogger(__name__)


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
    run_id = new_id()
    lock_acquired = False
    run_created = False
    counts: dict[str, int] = {"built": 0, "reused": 0, "deleted": 0, "failed": 0}
    failed = False
    try:
        repo.acquire_lock(run_id)
        lock_acquired = True
        repo.create_run(manifest.manifest_hash, run_id=run_id)
        run_created = True
        logger.debug(
            "apply start run_id=%s project=%s tenant=%s operations=%s runner=%s",
            run_id,
            manifest.context.project_id,
            manifest.context.tenant_id,
            len(operations),
            type(operation_runner).__name__,
        )
        operation_run_ids = {
            operation.op_id: repo.create_operation_run(run_id, operation)
            for operation in operations
        }
        for layer in operation_layers(manifest, operations):
            logger.debug(
                "apply layer run_id=%s kind=%s operations=%s assets=%s",
                run_id,
                "build" if is_build_layer(layer) else "mixed",
                len(layer),
                ",".join(sorted({operation.asset_name for operation in layer})),
            )
            if is_build_layer(layer):
                operation_counts, operation_failed = await _run_build_layer(
                    manifest=manifest,
                    repo=repo,
                    source_snapshots=source_snapshots,
                    run_id=run_id,
                    operation_run_ids=operation_run_ids,
                    operations=layer,
                    runner=operation_runner,
                )
                merge_counts(counts, operation_counts)
                failed = failed or operation_failed
                logger.debug(
                    "apply layer complete run_id=%s counts=%s failed=%s",
                    run_id,
                    operation_counts,
                    operation_failed,
                )
                continue

            for operation in layer:
                operation_counts, operation_failed = _run_delete_operation(
                    repo=repo,
                    run_id=run_id,
                    operation_run_id=operation_run_ids[operation.op_id],
                    operation=operation,
                )
                merge_counts(counts, operation_counts)
                failed = failed or operation_failed
                logger.debug(
                    "apply operation complete run_id=%s op_id=%s counts=%s failed=%s",
                    run_id,
                    operation.op_id,
                    operation_counts,
                    operation_failed,
                )
        if not failed:
            for snapshot in source_snapshots.values():
                repo.update_source_checkpoint(
                    snapshot.source_name,
                    connection_id=snapshot.connection_id,
                    scope_hash_value=snapshot.scope_hash,
                    cursor_token={"scan_marker": snapshot.scan_marker},
                )
        repo.finish_run(run_id, "failed" if failed else "succeeded")
        logger.debug("apply finish run_id=%s counts=%s failed=%s", run_id, counts, failed)
    except Exception:
        logger.exception("apply crashed run_id=%s", run_id)
        if run_created:
            repo.finish_run(run_id, "failed")
        raise
    finally:
        if lock_acquired:
            repo.release_lock(run_id)
    return ApplyResult(run_id=run_id, **counts)


def _run_delete_operation(
    *,
    repo: StateRepository,
    run_id: str,
    operation_run_id: str,
    operation: Operation,
) -> tuple[dict[str, int], bool]:
    counts: dict[str, int] = {"built": 0, "reused": 0, "deleted": 0, "failed": 0}
    repo.update_operation_run(operation_run_id, "running")
    try:
        if operation.op_type != "delete_scope":
            raise ValueError(f"unsupported non-layer operation type: {operation.op_type}")
        counts["deleted"] += execute_delete(repo, run_id, operation_run_id, operation)
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
    completion_counts: dict[tuple[str, str, str], int] = {}
    saw_groups = False

    try:
        logger.debug(
            "build layer start run_id=%s asset=%s operations=%s transform_id=%s",
            run_id,
            asset.name,
            len(operations),
            transform_id,
        )
        for window in invocation_windows(
            manifest=manifest,
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
            window_result = await runner.run_window(
                runner_window(manifest, asset, window)
            )
            logger.debug(
                "build window outputs run_id=%s asset=%s output_groups=%s outputs=%s",
                run_id,
                asset.name,
                len(window_result.output_groups),
                sum(len(outputs) for outputs in window_result.output_groups),
            )
            commit_output_window(
                manifest=manifest,
                repo=repo,
                run_id=run_id,
                asset=asset,
                transform_id=transform_id,
                window=window,
                output_groups=window_result.output_groups,
                counts=counts,
                completion_counts=completion_counts,
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
    if not failed:
        repo.upsert_asset_scope_completions(asset.name, completion_counts)
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
) -> Iterator[InputGroup]:
    for operation in operations:
        operation_run_id = operation_run_ids[operation.op_id]
        repo.update_operation_run(operation_run_id, "running")
        operation_group_count = 0
        logger.debug(
            "input groups start asset=%s op_id=%s scope=%s",
            asset.name,
            operation.op_id,
            operation_scope(operation),
        )
        try:
            for group in build_input_groups(
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
                raise ValueError(f"no inputs found for {asset.name}/{operation_scope(operation)}")
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



