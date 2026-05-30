from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path

import yaml

from strata.api import apply_project, plan_project
from strata.core.hashing import hash_file
from strata.executors.local import ApplyResult


@dataclass(frozen=True)
class HostSourceItem:
    item_key: str
    source_file: Path
    source_etag: str | None = None
    title: str | None = None
    source_url: str | None = None


class TestHostApp:
    """Small fixture that mimics a host app staging remote files before Strata runs."""

    __test__ = False

    def __init__(self, root: Path, *, project_id: str = "host-demo", tenant_id: str = "tenant-a"):
        self.root = root
        self.project_id = project_id
        self.tenant_id = tenant_id
        self.store_root = self.root / ".host_store"
        self.raw_root = self.store_root / "raw"
        self.manifest_path = self.store_root / "source-manifest.json"
        self.project_path = self.root / "strata.yml"
        self._items: dict[str, dict[str, object]] = {}

    def stage_batch(self, items: list[HostSourceItem]) -> None:
        self.raw_root.mkdir(parents=True, exist_ok=True)
        for item in items:
            target = self.raw_root / _safe_object_name(item.item_key, item.source_file.suffix)
            shutil.copyfile(item.source_file, target)
            self._items[item.item_key] = {
                "item_key": item.item_key,
                "object_uri": str(target),
                "content_hash": item.source_etag or hash_file(target),
                "metadata": {
                    "title": item.title or item.item_key,
                    "source_url": item.source_url,
                    "staged_path": str(target),
                },
            }

    def mark_deleted(self, item_key: str) -> None:
        self._items[item_key] = {
            "item_key": item_key,
            "deleted": True,
            "content_hash": f"deleted:{item_key}",
            "metadata": {},
        }

    def write_source_manifest(
        self,
        *,
        mode: str = "authoritative_snapshot",
    ) -> Path:
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "mode": mode,
            "connection_id": "test-host",
            "items": [self._items[key] for key in sorted(self._items)],
        }
        self.manifest_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return self.manifest_path

    def write_project(self) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        project = {
            "project_id": self.project_id,
            "tenant_id": self.tenant_id,
            "state": {"url": "sqlite:///./.strata/state.db"},
            "artifacts": {"path": "./.strata/artifacts"},
            "sources": {
                "docs": {
                    "type": "object_manifest",
                    "manifest_uri": str(self.manifest_path),
                }
            },
            "pipeline": {
                "parsed": {
                    "source": "docs",
                    "parser": "liteparse",
                    "version": "parsed@0.1.0",
                },
                "chunks": {
                    "input": "parsed",
                    "transform": "fixed_token_chunker",
                    "version": "fixed_token_chunker@0.1.0",
                    "config": {"max_chars": 80, "overlap_chars": 10},
                },
                "embeddings": {
                    "input": "chunks",
                    "transform": "fake_embedding",
                    "version": "fake_embedding@0.1.0",
                    "config": {"dimensions": 8},
                },
                "sink": {
                    "inputs": {"chunk": "chunks", "embedding": "embeddings"},
                    "type": "local_sqlite_vector_sink",
                    "version": "local_sqlite_vector_sink@0.1.0",
                },
            },
        }
        self.project_path.write_text(yaml.safe_dump(project, sort_keys=False), encoding="utf-8")
        return self.project_path

    def plan_strata(self) -> object:
        return plan_project(self.project_path)

    def run_strata(self) -> ApplyResult:
        return apply_project(self.project_path)


def _safe_object_name(item_key: str, suffix: str) -> str:
    stem = "".join(char if char.isalnum() else "-" for char in item_key).strip("-")
    return f"{stem or 'item'}{suffix}"
