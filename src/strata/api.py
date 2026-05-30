from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.engine import Engine

from strata.core.config import load_manifest, state_path_from_url
from strata.core.models import Manifest, Operation, SourceSnapshot
from strata.core.planning import plan
from strata.executors.protocols import ApplyResult
from strata.executors.registry import get_executor
from strata.plugins.discovery import discover_external_plugins
from strata.sources.registry import snapshot_sources
from strata.state.connection import bootstrap, connect_state
from strata.state.repository import StateRepository


@dataclass(frozen=True)
class ProjectHandle:
    manifest: Manifest
    repo: StateRepository
    engine: Engine


def open_project(project: Path | str) -> ProjectHandle:
    discover_external_plugins()
    manifest = load_manifest(Path(project))
    engine = connect_state(state_path_from_url(manifest.state_url, manifest.root))
    bootstrap(engine)
    repo = StateRepository(engine, manifest.context)
    return ProjectHandle(manifest=manifest, repo=repo, engine=engine)


def compile_project(project: Path | str) -> Manifest:
    return load_manifest(Path(project))


def snapshot_project(project: Path | str) -> tuple[ProjectHandle, dict[str, SourceSnapshot]]:
    handle = open_project(project)
    return handle, snapshot_sources(handle.manifest.sources, root=handle.manifest.root)


def plan_project(project: Path | str, selection: str | None = None) -> list[Operation]:
    handle, snapshots = snapshot_project(project)
    return plan(handle.manifest, handle.repo.snapshot(), snapshots, selection)


def apply_project(project: Path | str, selection: str | None = None) -> ApplyResult:
    handle, snapshots = snapshot_project(project)
    operations = plan(handle.manifest, handle.repo.snapshot(), snapshots, selection)
    if not operations:
        return ApplyResult(run_id=None, built=0, reused=0, deleted=0, failed=0)
    executor = get_executor(handle.manifest.execution.executor)
    return executor.apply(
        manifest=handle.manifest,
        repo=handle.repo,
        source_snapshots=snapshots,
        operations=operations,
        config=handle.manifest.execution.config,
    )
