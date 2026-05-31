from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

from strata.core.hashing import hash_file, scope_hash
from strata.core.models import SourceItem, SourceSnapshot, SourceSnapshotMode, SourceSpec
from strata.storage.object_store import object_store_for_uri


class ObjectStoreSourceAdapter:
    def snapshot(self, source: SourceSpec, *, root: Path) -> SourceSnapshot:
        if not source.uri:
            raise ValueError("object_store source requires uri")

        store = object_store_for_uri(source.uri, root=root)
        source_root = store.resolve_path(source.uri)
        if not source_root.exists():
            raise FileNotFoundError(f"object store source uri does not exist: {source.uri}")
        if not source_root.is_dir():
            raise ValueError(f"object_store source uri must resolve to a directory: {source.uri}")

        paths: dict[Path, None] = {}
        for pattern in source.include:
            for path in source_root.glob(pattern):
                if path.is_file():
                    paths[path.resolve()] = None

        items = [
            _source_item(source, source_root=source_root, path=path)
            for path in sorted(paths)
        ]
        resolved_scope = {
            "type": source.type,
            "uri": str(source_root),
            "include": source.include,
        }
        return SourceSnapshot(
            source_name=source.name,
            mode=SourceSnapshotMode.AUTHORITATIVE,
            connection_id=source.connection_id,
            scope_hash=scope_hash(resolved_scope),
            scan_marker=datetime.now(UTC).isoformat(),
            items=items,
        )


def _source_item(source: SourceSpec, *, source_root: Path, path: Path) -> SourceItem:
    item_key = path.relative_to(source_root).as_posix()
    stat = path.stat()
    uri = _child_uri(source.uri or "", item_key, path)
    return SourceItem(
        source_name=source.name,
        item_key=item_key,
        path=path,
        uri=uri,
        content_hash=hash_file(path),
        metadata={
            "object_uri": uri,
            "path": str(path),
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "suffix": path.suffix.lower(),
        },
    )


def _child_uri(root_uri: str, item_key: str, path: Path) -> str:
    parsed = urlparse(root_uri)
    if parsed.scheme == "file":
        return path.resolve().as_uri()
    if parsed.scheme:
        return f"{root_uri.rstrip('/')}/{item_key}"
    return str(path)
