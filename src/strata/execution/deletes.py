from __future__ import annotations

from strata.core.models import Operation
from strata.state.repository import StateRepository


def execute_delete(
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
    source_name = operation.scope.source_name or ""
    repo.mark_source_deleted(source_name, item_key)
    deleted = repo.mark_lineage_deleted_for_source(source_name, item_key)
    repo.update_operation_item(op_item_id, "deleted")
    return deleted
