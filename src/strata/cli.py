from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from strata.config import load_manifest, state_path_from_url
from strata.doctor import DoctorIssue, run_doctor
from strata.executor import apply_operations
from strata.models import Manifest, Operation
from strata.planner import plan
from strata.source import snapshot_sources
from strata.state import StateRepository, bootstrap, connect_state

app = typer.Typer(help="Strata Phase 0A local content build system")
console = Console()
ProjectOption = Annotated[
    Path, typer.Option("--project", "-p", help="Path to strata.yml")
]
AssetOption = Annotated[str | None, typer.Option("--asset", help="Minimal Phase 0 asset selector")]
SelectOption = Annotated[
    str | None,
    typer.Option(
        "--select",
        "-s",
        help="Selector such as chunks, chunks+, +embeddings, +chunks+, source:docs+",
    ),
]


def _repo(manifest_path: Path) -> tuple[StateRepository, Manifest]:
    manifest = load_manifest(manifest_path)
    state_path = state_path_from_url(manifest.state_url, manifest.root)
    engine = connect_state(state_path)
    bootstrap(engine)
    repo = StateRepository(engine, manifest.context)
    return repo, manifest


@app.command("compile")
def compile_command(
    project: ProjectOption = Path("strata.yml"),
) -> None:
    manifest = load_manifest(project)
    table = Table(title="Strata Manifest")
    table.add_column("Field")
    table.add_column("Value")
    table.add_row("project_id", manifest.context.project_id)
    table.add_row("tenant_id", manifest.context.tenant_id)
    table.add_row("manifest_hash", manifest.manifest_hash)
    table.add_row("sources", ", ".join(manifest.sources))
    table.add_row("assets", " -> ".join(manifest.asset_order))
    console.print(table)


@app.command("plan")
def plan_command(
    project: ProjectOption = Path("strata.yml"),
    asset: AssetOption = None,
    select: SelectOption = None,
) -> None:
    repo, manifest = _repo(project)
    snapshots = snapshot_sources(manifest.sources)
    operations = plan(manifest, repo.snapshot(), snapshots, _selector(select, asset))
    _print_operations(operations)


@app.command("apply")
def apply_command(
    project: ProjectOption = Path("strata.yml"),
    asset: AssetOption = None,
    select: SelectOption = None,
) -> None:
    repo, manifest = _repo(project)
    snapshots = snapshot_sources(manifest.sources)
    operations = plan(manifest, repo.snapshot(), snapshots, _selector(select, asset))
    _print_operations(operations)
    if not operations:
        console.print("[green]Nothing to apply.[/green]")
        return
    result = apply_operations(
        manifest=manifest,
        repo=repo,
        source_snapshots=snapshots,
        operations=operations,
    )
    console.print(
        "[bold]Apply complete[/bold] "
        f"run_id={result['run_id']} "
        f"built={result['built']} reused={result['reused']} "
        f"deleted={result['deleted']} failed={result['failed']}"
    )


@app.command("progress")
def progress_command(
    run_id: Annotated[str, typer.Option("--run-id", help="Run id from apply")],
    project: ProjectOption = Path("strata.yml"),
) -> None:
    repo, _manifest = _repo(project)
    counts = repo.progress(run_id)
    table = Table(title=f"Progress {run_id}")
    table.add_column("Status")
    table.add_column("Count", justify="right")
    for status, count in counts.items():
        table.add_row(status, str(count))
    console.print(table)


@app.command("lineage")
def lineage_command(
    asset: Annotated[str, typer.Option("--asset", help="Asset name")],
    instance_key: Annotated[str, typer.Option("--instance-key", help="Asset instance key")],
    project: ProjectOption = Path("strata.yml"),
) -> None:
    repo, _manifest = _repo(project)
    rows = repo.lineage(asset, instance_key)
    if not rows:
        console.print("[yellow]No lineage found.[/yellow]")
        return
    table = Table(title=f"Lineage {asset}/{instance_key}")
    table.add_column("Depth", justify="right")
    table.add_column("Asset")
    table.add_column("Instance")
    table.add_column("Status")
    table.add_column("Fingerprint")
    for row in rows:
        table.add_row(
            str(row["depth"]),
            row["asset_name"],
            row["instance_key"],
            row["status"],
            row["fingerprint"][:12],
        )
    console.print(table)


@app.command("doctor")
def doctor_command(
    project: ProjectOption = Path("strata.yml"),
    fix: Annotated[bool, typer.Option("--fix", help="Apply conservative repairs")] = False,
) -> None:
    repo, manifest = _repo(project)
    result = run_doctor(manifest=manifest, repo=repo, fix=fix)
    _print_doctor(result.issues, result.fixed, fix)


def _print_operations(operations: Sequence[Operation]) -> None:
    table = Table(title="Strata Plan")
    table.add_column("Operation")
    table.add_column("Asset")
    table.add_column("Item")
    table.add_column("Reason")
    table.add_column("Count")
    for operation in operations:
        item = operation.scope.item_key or operation.scope.upstream_instance_key or ""
        count = (
            str(operation.estimated_instance_count)
            if operation.estimated_instance_count is not None
            else "unknown"
        )
        table.add_row(operation.op_type, operation.asset_name, item, operation.reason, count)
    console.print(table)
    if not operations:
        console.print("[green]Plan is empty.[/green]")


def _selector(select: str | None, asset: str | None) -> str | None:
    if select and asset:
        raise typer.BadParameter("use either --select or --asset, not both")
    if select:
        return select
    if asset:
        return f"{asset}+"
    return None


def _print_doctor(issues: Sequence[DoctorIssue], fixed: int, fix: bool) -> None:
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


if __name__ == "__main__":
    app()
