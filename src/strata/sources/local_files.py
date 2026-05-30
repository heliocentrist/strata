from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from strata.core.hashing import hash_file, scope_hash
from strata.core.models import SourceItem, SourceSnapshot, SourceSpec


class LocalFilesSourceAdapter:
    def snapshot(self, source: SourceSpec, *, root: Path) -> SourceSnapshot:
        _ = root
        if source.path is None:
            raise ValueError("local_files source requires path")
        source_root = source.path.resolve()
        if not source_root.exists():
            raise FileNotFoundError(f"local source path does not exist: {source_root}")

        paths: dict[Path, None] = {}
        for pattern in source.include:
            for path in source_root.glob(pattern):
                if path.is_file():
                    paths[path.resolve()] = None

        items: list[SourceItem] = []
        for path in sorted(paths):
            item_key = path.relative_to(source_root).as_posix()
            stat = path.stat()
            uri = str(path)
            items.append(
                SourceItem(
                    source_name=source.name,
                    item_key=item_key,
                    path=path,
                    uri=uri,
                    content_hash=hash_file(path),
                    metadata={
                        "path": str(path),
                        "object_uri": uri,
                        "size": stat.st_size,
                        "mtime_ns": stat.st_mtime_ns,
                        "suffix": path.suffix.lower(),
                    },
                )
            )

        resolved_scope = {
            "type": source.type,
            "path": str(source_root),
            "include": source.include,
        }
        return SourceSnapshot(
            source_name=source.name,
            mode=source.mode,
            connection_id=source.connection_id,
            scope_hash=scope_hash(resolved_scope),
            scan_marker=datetime.now(UTC).isoformat(),
            items=items,
        )
