from __future__ import annotations

from pathlib import Path

from conftest import write_project

from strata.api import apply_project, plan_project
from strata.core.config import load_manifest
from strata.executors.registry import registered_executors


def test_builtin_executor_registry_exposes_local_modes() -> None:
    assert "local_single_thread" in registered_executors()
    assert "local_threaded" in registered_executors()


def test_apply_project_uses_configured_threaded_executor(tmp_path: Path) -> None:
    project = write_project(tmp_path)
    docs = tmp_path / "docs"
    (docs / "b.md").write_text(
        "Beta document for Strata.\nThis gives the threaded executor another source item.\n",
        encoding="utf-8",
    )
    raw = project.read_text(encoding="utf-8")
    project.write_text(
        raw.replace(
            "artifacts:\n  path: ./.strata/artifacts\n",
            (
                "artifacts:\n"
                "  path: ./.strata/artifacts\n\n"
                "execution:\n"
                "  executor: local_threaded\n"
                "  config:\n"
                "    max_workers: 2\n"
            ),
        ),
        encoding="utf-8",
    )

    manifest = load_manifest(project)
    assert manifest.execution.executor == "local_threaded"

    result = apply_project(project)

    assert result["failed"] == 0
    assert result["built"] > 0
    assert plan_project(project) == []
