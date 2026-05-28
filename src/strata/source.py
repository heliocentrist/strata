from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from strata.hashing import hash_file, scope_hash
from strata.models import SourceItem, SourceSnapshot, SourceSpec


def snapshot_local_source(source: SourceSpec) -> SourceSnapshot:
    if source.type != "local_files":
        raise ValueError(f"unsupported source type: {source.type}")
    root = source.path.resolve()
    if not root.exists():
        raise FileNotFoundError(f"local source path does not exist: {root}")

    paths: dict[Path, None] = {}
    for pattern in source.include:
        for path in root.glob(pattern):
            if path.is_file():
                paths[path.resolve()] = None

    items: list[SourceItem] = []
    for path in sorted(paths):
        item_key = path.relative_to(root).as_posix()
        stat = path.stat()
        items.append(
            SourceItem(
                source_name=source.name,
                item_key=item_key,
                path=path,
                content_hash=hash_file(path),
                metadata={
                    "path": str(path),
                    "size": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                    "suffix": path.suffix.lower(),
                },
            )
        )

    resolved_scope = {
        "type": source.type,
        "path": str(root),
        "include": source.include,
    }
    return SourceSnapshot(
        source_name=source.name,
        scope_hash=scope_hash(resolved_scope),
        scan_marker=datetime.now(UTC).isoformat(),
        items=items,
    )


def snapshot_sources(sources: dict[str, SourceSpec]) -> dict[str, SourceSnapshot]:
    return {name: snapshot_local_source(source) for name, source in sources.items()}
