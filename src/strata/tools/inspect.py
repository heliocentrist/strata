from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select

from strata.core.models import Manifest
from strata.execution.artifacts import read_artifact
from strata.state.repository import StateRepository
from strata.state.schema import asset_instances, lineage_edges, transforms, vector_sink


@dataclass(frozen=True)
class InspectReport:
    found: bool
    asset: dict[str, Any] | None = None
    artifact: dict[str, Any] | None = None
    upstreams: list[dict[str, Any]] | None = None
    downstreams: list[dict[str, Any]] | None = None
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "found": self.found,
            "asset": self.asset,
            "artifact": self.artifact,
            "upstreams": self.upstreams or [],
            "downstreams": self.downstreams or [],
            "message": self.message,
        }


def inspect_instance(
    *,
    manifest: Manifest,
    repo: StateRepository,
    asset_name: str,
    instance_key: str,
    preview_chars: int = 500,
) -> InspectReport:
    rows, edge_rows = _state_rows(repo)
    root = _latest_instance(rows, asset_name, instance_key)
    if root is None:
        return InspectReport(
            found=False,
            message=f"No materialized instance found for {asset_name}/{instance_key}",
        )

    by_id = {row["id"]: row for row in rows}
    upstreams = _walk_lineage(
        root_id=str(root["id"]),
        by_id=by_id,
        edge_rows=edge_rows,
        direction="upstream",
    )
    downstreams = _walk_lineage(
        root_id=str(root["id"]),
        by_id=by_id,
        edge_rows=edge_rows,
        direction="downstream",
    )
    artifact = _artifact_summary(
        manifest=manifest,
        repo=repo,
        row=root,
        preview_chars=preview_chars,
    )
    return InspectReport(
        found=True,
        asset=_asset_summary(root),
        artifact=artifact,
        upstreams=[_lineage_summary(row) for row in upstreams],
        downstreams=[_lineage_summary(row) for row in downstreams],
    )


def _state_rows(
    repo: StateRepository,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    with repo.engine.connect() as conn:
        asset_rows = conn.execute(
            select(
                asset_instances,
                transforms.c.transform_id,
                transforms.c.version,
                transforms.c.config_hash,
                transforms.c.config_json,
                transforms.c.determinism,
            )
            .join(transforms, asset_instances.c.transform_id == transforms.c.id)
            .where(
                asset_instances.c.project_id == repo.context.project_id,
                asset_instances.c.tenant_id == repo.context.tenant_id,
            )
        ).mappings().all()
        edges = conn.execute(select(lineage_edges)).mappings().all()
    return [dict(row) for row in asset_rows], [dict(row) for row in edges]


def _latest_instance(
    rows: list[dict[str, Any]], asset_name: str, instance_key: str
) -> dict[str, Any] | None:
    matches = [
        row
        for row in rows
        if row["asset_name"] == asset_name
        and row["instance_key"] == instance_key
        and row["status"] == "materialized"
    ]
    if not matches:
        return None
    return sorted(matches, key=lambda row: str(row["updated_at"]), reverse=True)[0]


def _walk_lineage(
    *,
    root_id: str,
    by_id: dict[str, dict[str, Any]],
    edge_rows: list[dict[str, Any]],
    direction: str,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen = {root_id}

    if direction == "upstream":
        adjacency: dict[str, list[str]] = {}
        source_key = "downstream_asset_instance_id"
        target_key = "upstream_asset_instance_id"
    else:
        adjacency = {}
        source_key = "upstream_asset_instance_id"
        target_key = "downstream_asset_instance_id"

    for edge in edge_rows:
        adjacency.setdefault(str(edge[source_key]), []).append(str(edge[target_key]))

    def visit(instance_id: str, depth: int) -> None:
        for next_id in sorted(adjacency.get(instance_id, [])):
            if next_id in seen or next_id not in by_id:
                continue
            seen.add(next_id)
            row = dict(by_id[next_id])
            row["depth"] = depth
            output.append(row)
            visit(next_id, depth + 1)

    visit(root_id, 1)
    return output


def _asset_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "asset_name": row["asset_name"],
        "instance_key": row["instance_key"],
        "status": row["status"],
        "input_fingerprint": row["input_fingerprint"],
        "content_hash": row["content_hash"],
        "output_hash": row["output_hash"],
        "output_location": row["output_location"],
        "artifact_collection": row["artifact_collection"],
        "materialization_strategy": row["materialization_strategy"],
        "metadata": _json_object(row["metadata_json"]),
        "transform": {
            "id": row["transform_id"],
            "version": row["version"],
            "config_hash": row["config_hash"],
            "config": _json_object(row["config_json"]),
            "determinism": row["determinism"],
        },
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def _lineage_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "depth": row.get("depth", 0),
        "asset_name": row["asset_name"],
        "instance_key": row["instance_key"],
        "status": row["status"],
        "input_fingerprint": row["input_fingerprint"],
        "content_hash": row["content_hash"],
        "transform_version": row["version"],
        "output_location": row["output_location"],
    }


def _artifact_summary(
    *,
    manifest: Manifest,
    repo: StateRepository,
    row: dict[str, Any],
    preview_chars: int,
) -> dict[str, Any]:
    location = str(row["output_location"] or "")
    if location.startswith("sqlite://"):
        return _sink_summary(repo, row, preview_chars)
    try:
        artifact_doc = _read_artifact_doc(
            manifest,
            location,
            str(row.get("artifact_collection") or "local_json"),
        )
    except (OSError, ValueError, KeyError, json.JSONDecodeError, IndexError) as exc:
        return {"readable": False, "error": str(exc)}

    data = artifact_doc.get("data")
    summary: dict[str, Any] = {
        "readable": True,
        "metadata": artifact_doc.get("metadata", {}),
        "source": artifact_doc.get("artifact", {}).get("source", {}),
        "preview": _preview(data, preview_chars),
    }
    if isinstance(data, list):
        summary["kind"] = "list"
        summary["length"] = len(data)
    elif isinstance(data, str):
        summary["kind"] = "text"
        summary["length"] = len(data)
    else:
        summary["kind"] = type(data).__name__
    return summary


def _sink_summary(
    repo: StateRepository, row: dict[str, Any], preview_chars: int
) -> dict[str, Any]:
    with repo.engine.connect() as conn:
        sink_row = conn.execute(
            select(vector_sink).where(
                vector_sink.c.project_id == repo.context.project_id,
                vector_sink.c.tenant_id == repo.context.tenant_id,
                vector_sink.c.instance_key == row["instance_key"],
            )
        ).mappings().first()
    if sink_row is None:
        return {"readable": False, "error": "sink row not found"}
    embedding = json.loads(str(sink_row["embedding_json"]))
    chunk_text = str(sink_row["chunk_text"])
    return {
        "readable": True,
        "kind": "local_sqlite_vector_sink",
        "source_item_key": sink_row["source_item_key"],
        "embedding_fingerprint": sink_row["embedding_fingerprint"],
        "embedding_dimensions": len(embedding),
        "chunk_preview": _preview(chunk_text, preview_chars),
    }


def _read_artifact_doc(
    manifest: Manifest,
    location: str,
    artifact_collection: str,
) -> dict[str, Any]:
    return read_artifact(manifest, location, artifact_collection)


def _preview(data: Any, preview_chars: int) -> str:
    if isinstance(data, list):
        text = json.dumps(data[:8], ensure_ascii=False)
    else:
        text = str(data)
    text = " ".join(text.split())
    if len(text) <= preview_chars:
        return text
    return f"{text[:preview_chars]}..."


def _json_object(raw: Any) -> dict[str, Any]:
    if not raw:
        return {}
    parsed = json.loads(str(raw))
    if isinstance(parsed, dict):
        return parsed
    return {}
