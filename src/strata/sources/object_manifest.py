from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from strata.core.hashing import scope_hash
from strata.core.models import SourceItem, SourceSnapshot, SourceSnapshotMode, SourceSpec
from strata.storage.object_store import object_store_for_uri


class ObjectManifestSourceAdapter:
    def snapshot(self, source: SourceSpec, *, root: Path) -> SourceSnapshot:
        if not source.manifest_uri:
            raise ValueError("object_manifest source requires manifest_uri")
        store = object_store_for_uri(source.manifest_uri, root=root)
        manifest_text = store.read_text(source.manifest_uri)
        raw = json.loads(manifest_text)
        if not isinstance(raw, dict):
            raise ValueError("object manifest must be a JSON object")

        mode = SourceSnapshotMode(str(raw.get("mode", source.mode.value)))
        connection_id = str(raw.get("connection_id", source.connection_id))
        items = [_object_manifest_item(source.name, item, root=root) for item in _raw_items(raw)]
        resolved_scope: dict[str, Any] = {
            "type": source.type,
            "manifest_uri": source.manifest_uri,
            "mode": mode.value,
            "connection_id": connection_id,
        }
        return SourceSnapshot(
            source_name=source.name,
            mode=mode,
            connection_id=connection_id,
            scope_hash=scope_hash(resolved_scope),
            scan_marker=datetime.now(UTC).isoformat(),
            items=items,
        )


def _raw_items(raw: dict[str, Any]) -> list[dict[str, Any]]:
    items = raw.get("items", [])
    if not isinstance(items, list):
        raise ValueError("object manifest items must be a list")
    output: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("object manifest item must be an object")
        output.append(item)
    return output


def _object_manifest_item(source_name: str, raw: dict[str, Any], *, root: Path) -> SourceItem:
    item_key = str(raw.get("item_key") or "")
    if not item_key:
        raise ValueError("object manifest item requires item_key")
    deleted = bool(raw.get("deleted", False))
    content_hash = str(raw.get("content_hash") or ("deleted:" + item_key if deleted else ""))
    if not content_hash:
        raise ValueError(f"object manifest item requires content_hash: {item_key}")

    object_uri = raw.get("object_uri") or raw.get("uri")
    path: Path | None = None
    if object_uri:
        store = object_store_for_uri(str(object_uri), root=root)
        path = store.resolve_path(str(object_uri))
        if not deleted and not path.exists():
            raise FileNotFoundError(f"object manifest item target is missing: {path}")
    elif not deleted:
        raise ValueError(f"object manifest item requires object_uri unless deleted: {item_key}")

    metadata = dict(raw.get("metadata") or {})
    if object_uri:
        metadata.setdefault("object_uri", str(object_uri))
    return SourceItem(
        source_name=source_name,
        item_key=item_key,
        path=path,
        uri=str(object_uri) if object_uri else None,
        content_hash=content_hash,
        deleted=deleted,
        metadata=metadata,
    )
