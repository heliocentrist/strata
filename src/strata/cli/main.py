from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from strata.cli.render import print_doctor, print_inspect, print_operations, print_tests
from strata.core.config import load_manifest, state_path_from_url
from strata.core.models import Manifest
from strata.core.planning import plan
from strata.executors.local import apply_operations
from strata.plugins.discovery import discover_external_plugins
from strata.sources.registry import snapshot_sources
from strata.state.connection import bootstrap, connect_state
from strata.state.repository import StateRepository
from strata.tools.docs import build_docs_site, clean_docs_site, serve_docs_site
from strata.tools.doctor import run_doctor
from strata.tools.inspect import inspect_instance
from strata.tools.testing import run_tests

app = typer.Typer(help="Strata Phase 0A local content build system")
docs_app = typer.Typer(help="Build or serve the Strata asset browser")
app.add_typer(docs_app, name="docs")
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
    discovery = discover_external_plugins()
    for error in discovery.errors:
        console.print(f"[yellow]Plugin discovery failed: {error}[/yellow]")
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
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    operations = plan(manifest, repo.snapshot(), snapshots, _selector(select, asset))
    print_operations(console, operations)


@app.command("apply")
def apply_command(
    project: ProjectOption = Path("strata.yml"),
    asset: AssetOption = None,
    select: SelectOption = None,
) -> None:
    repo, manifest = _repo(project)
    snapshots = snapshot_sources(manifest.sources, root=manifest.root)
    operations = plan(manifest, repo.snapshot(), snapshots, _selector(select, asset))
    print_operations(console, operations)
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


@app.command("inspect")
def inspect_command(
    asset: Annotated[str, typer.Option("--asset", help="Asset name")],
    instance_key: Annotated[str, typer.Option("--instance-key", help="Asset instance key")],
    project: ProjectOption = Path("strata.yml"),
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON")
    ] = False,
    preview_chars: Annotated[
        int, typer.Option("--preview-chars", help="Maximum artifact preview characters")
    ] = 500,
) -> None:
    repo, manifest = _repo(project)
    report = inspect_instance(
        manifest=manifest,
        repo=repo,
        asset_name=asset,
        instance_key=instance_key,
        preview_chars=preview_chars,
    )
    if json_output:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True, default=str))
        if not report.found:
            raise typer.Exit(1)
        return
    print_inspect(console, report)
    if not report.found:
        raise typer.Exit(1)


@app.command("doctor")
def doctor_command(
    project: ProjectOption = Path("strata.yml"),
    fix: Annotated[bool, typer.Option("--fix", help="Apply conservative repairs")] = False,
) -> None:
    repo, manifest = _repo(project)
    result = run_doctor(manifest=manifest, repo=repo, fix=fix)
    print_doctor(console, result.issues, result.fixed, fix)


@app.command("test")
def test_command(
    project: ProjectOption = Path("strata.yml"),
) -> None:
    repo, manifest = _repo(project)
    results = run_tests(manifest=manifest, repo=repo)
    print_tests(console, results)
    if any(result.status == "failed" for result in results):
        raise typer.Exit(1)


@docs_app.command("build")
def docs_build_command(
    project: ProjectOption = Path("strata.yml"),
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output directory for the static docs site"),
    ] = None,
    clean: Annotated[
        bool, typer.Option("--clean", help="Remove the output directory first")
    ] = False,
) -> None:
    repo, manifest = _repo(project)
    output_path = output or (manifest.root / ".strata" / "docs")
    if clean:
        clean_docs_site(output_path)
    result = build_docs_site(manifest=manifest, repo=repo, output_path=output_path)
    console.print(
        "[green]Docs built[/green] "
        f"path={result.output_path} "
        f"instances={result.instance_count} edges={result.edge_count}"
    )


@docs_app.command("serve")
def docs_serve_command(
    project: ProjectOption = Path("strata.yml"),
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output directory for the static docs site"),
    ] = None,
    host: Annotated[str, typer.Option("--host", help="Host interface")] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port", help="Port to listen on")] = 8765,
    clean: Annotated[
        bool, typer.Option("--clean", help="Remove the output directory first")
    ] = False,
) -> None:
    repo, manifest = _repo(project)
    output_path = output or (manifest.root / ".strata" / "docs")
    if clean:
        clean_docs_site(output_path)
    result = build_docs_site(manifest=manifest, repo=repo, output_path=output_path)
    url = f"http://{host}:{port}"
    console.print(
        "[green]Serving Strata docs[/green] "
        f"url={url} path={result.output_path} "
        f"instances={result.instance_count} edges={result.edge_count}"
    )
    serve_docs_site(directory=result.output_path, host=host, port=port)


def _selector(select: str | None, asset: str | None) -> str | None:
    if select and asset:
        raise typer.BadParameter("use either --select or --asset, not both")
    if select:
        return select
    if asset:
        return f"{asset}+"
    return None


if __name__ == "__main__":
    app()
