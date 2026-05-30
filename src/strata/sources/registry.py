from __future__ import annotations

from pathlib import Path

from strata.core.models import SourceSnapshot, SourceSpec
from strata.plugins.registry import get_source, register_source
from strata.sources.local_files import LocalFilesSourceAdapter
from strata.sources.object_manifest import ObjectManifestSourceAdapter


def snapshot_sources(
    sources: dict[str, SourceSpec], *, root: Path | None = None
) -> dict[str, SourceSnapshot]:
    return {
        name: get_source(source.type).snapshot(source, root=root or Path.cwd())
        for name, source in sources.items()
    }


def register_builtin_sources() -> None:
    register_source("local_files", LocalFilesSourceAdapter())
    register_source("object_manifest", ObjectManifestSourceAdapter())


register_builtin_sources()
