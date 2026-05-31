from __future__ import annotations

from collections.abc import Sequence

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from strata.core.models import Operation
from strata.tools.doctor import DoctorIssue
from strata.tools.inspect import InspectReport
from strata.tools.testing import TestResult


def print_operations(console: Console, operations: Sequence[Operation]) -> None:
    table = Table(title="Strata Plan")
    table.add_column("Operation")
    table.add_column("Asset")
    table.add_column("Item")
    table.add_column("Reason")
    table.add_column("Count")
    for operation in operations:
        item = _operation_scope_label(operation)
        count = (
            str(operation.estimated_instance_count)
            if operation.estimated_instance_count is not None
            else "unknown"
        )
        table.add_row(operation.op_type, operation.asset_name, item, operation.reason, count)
    console.print(table)
    if not operations:
        console.print("[green]Plan is empty.[/green]")


def _operation_scope_label(operation: Operation) -> str:
    if operation.scope.item_key:
        return operation.scope.item_key
    if operation.scope.item_keys:
        if len(operation.scope.item_keys) == 1:
            return operation.scope.item_keys[0]
        source = operation.scope.source_name or "source"
        return f"{source}: {len(operation.scope.item_keys)} items"
    return operation.scope.upstream_instance_key or ""


def print_doctor(console: Console, issues: Sequence[DoctorIssue], fixed: int, fix: bool) -> None:
    table = Table(title="Strata Doctor")
    table.add_column("Severity")
    table.add_column("Code")
    table.add_column("Fixable")
    table.add_column("Issue")
    for issue in issues:
        table.add_row(
            issue.severity,
            issue.code,
            "yes" if issue.fixable else "no",
            issue.message,
        )
    console.print(table)
    if not issues:
        console.print("[green]No problems found.[/green]")
    elif fix:
        console.print(f"[green]Applied {fixed} fix(es).[/green]")
    else:
        console.print("[yellow]Run with --fix to apply conservative repairs.[/yellow]")


def print_tests(console: Console, results: Sequence[TestResult]) -> None:
    table = Table(title="Strata Tests")
    table.add_column("Status")
    table.add_column("Name")
    table.add_column("Asset")
    table.add_column("Type")
    table.add_column("Message")
    for result in results:
        status = "[green]passed[/green]" if result.status == "passed" else "[red]failed[/red]"
        table.add_row(status, result.name, result.asset, result.test_type, result.message)
    console.print(table)
    if not results:
        console.print("[yellow]No tests configured.[/yellow]")


def print_inspect(console: Console, report: InspectReport) -> None:
    if not report.found or not report.asset:
        console.print(f"[yellow]{report.message or 'No instance found.'}[/yellow]")
        return

    asset = report.asset
    table = Table(title=f"Inspect {asset['asset_name']}/{asset['instance_key']}")
    table.add_column("Field")
    table.add_column("Value")
    table.add_row("status", str(asset["status"]))
    table.add_row("fingerprint", str(asset["input_fingerprint"]))
    table.add_row("content_hash", str(asset["content_hash"] or ""))
    table.add_row("output_hash", str(asset["output_hash"] or ""))
    table.add_row("output_location", str(asset["output_location"] or ""))
    table.add_row("strategy", str(asset["materialization_strategy"]))
    transform = asset["transform"]
    table.add_row("transform", str(transform["version"]))
    table.add_row("config_hash", str(transform["config_hash"]))
    table.add_row("determinism", str(transform["determinism"]))
    console.print(table)

    artifact = report.artifact or {}
    if artifact:
        artifact_table = Table(title="Artifact")
        artifact_table.add_column("Field")
        artifact_table.add_column("Value")
        for field in [
            "readable",
            "kind",
            "length",
            "embedding_dimensions",
            "source_item_key",
            "embedding_fingerprint",
        ]:
            if field in artifact:
                artifact_table.add_row(field, str(artifact[field]))
        if "error" in artifact:
            artifact_table.add_row("error", str(artifact["error"]))
        console.print(artifact_table)
        preview = artifact.get("preview") or artifact.get("chunk_preview")
        if preview:
            console.print(Panel(str(preview), title="Preview"))

    print_lineage_table(console, "Upstreams", report.upstreams or [])
    print_lineage_table(console, "Downstreams", report.downstreams or [])


def print_lineage_table(
    console: Console, title: str, rows: Sequence[dict[str, object]]
) -> None:
    table = Table(title=title)
    table.add_column("Depth", justify="right")
    table.add_column("Asset")
    table.add_column("Instance")
    table.add_column("Status")
    table.add_column("Fingerprint")
    for row in rows:
        table.add_row(
            str(row["depth"]),
            str(row["asset_name"]),
            str(row["instance_key"]),
            str(row["status"]),
            str(row["input_fingerprint"])[:12],
        )
    console.print(table)
