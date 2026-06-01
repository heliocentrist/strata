from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from strata.core.collections import (
    ArtifactWrite,
    ArtifactWriteResult,
    CollectionWriteContext,
)
from strata.core.hashing import hash_canonical, sha256_text
from strata.plugins.registry import register_artifact_collection

ARTIFACT_URI_PREFIX = "artifact://"


class LocalJsonArtifactCollection:
    """Local JSON/JSONL collection used by the prototype executor."""

    def write_one(
        self,
        context: CollectionWriteContext,
        item: ArtifactWrite,
    ) -> ArtifactWriteResult:
        path = context.root_path / context.asset_name / f"{item.input_fingerprint}.json"
        payload = json.dumps(
            {"data": item.payload.data, "metadata": item.payload.metadata},
            ensure_ascii=False,
            sort_keys=True,
        )
        output_hash = _write_text(path, payload)
        return ArtifactWriteResult(
            instance_key=item.instance_key,
            input_fingerprint=item.input_fingerprint,
            output_ref=str(path),
            output_hash=output_hash,
            content_hash=item.content_hash,
        )

    def write_many(
        self,
        context: CollectionWriteContext,
        items: list[ArtifactWrite],
    ) -> list[ArtifactWriteResult]:
        if context.partition_key is None:
            raise ValueError("local_json fanout writes require a partition key")
        if context.window_id is None:
            raise ValueError("local_json fanout writes require a window id")
        partition_key = context.partition_key

        payload_records = [
            json.dumps(
                {"data": item.payload.data},
                ensure_ascii=False,
                sort_keys=True,
            )
            for item in items
        ]
        payload_text = "".join(f"{record}\n" for record in payload_records)
        payload_hash = sha256_text(payload_text)
        payload_path = f"payloads/{context.window_id}.jsonl"
        payload_file = _fanout_dir(context, partition_key) / payload_path
        _write_immutable_text(payload_file, payload_text)

        fanout_items = [
            {
                "instance_key": item.instance_key,
                "input_fingerprint": item.input_fingerprint,
                "content_hash": item.content_hash,
                "payload": {
                    "path": payload_path,
                    "format": "jsonl",
                    "record": index,
                    "file_hash": payload_hash,
                },
                "metadata": item.payload.metadata,
            }
            for index, item in enumerate(items)
        ]
        manifest_name = _write_fanout_manifest(
            context=context,
            partition_key=partition_key,
            window_id=context.window_id,
            items=fanout_items,
        )
        return [
            ArtifactWriteResult(
                instance_key=item.instance_key,
                input_fingerprint=item.input_fingerprint,
                output_ref=_fanout_item_uri(
                    context.asset_name,
                    partition_key,
                    manifest_name,
                    index,
                ),
                output_hash=sha256_text(payload_records[index]),
                content_hash=item.content_hash,
            )
            for index, item in enumerate(items)
        ]

    def read(self, root_path: Path, ref: str) -> dict[str, Any]:
        if ref.startswith(ARTIFACT_URI_PREFIX):
            return _read_fanout_record(root_path, ref)
        return cast(dict[str, Any], json.loads(Path(ref).read_text(encoding="utf-8")))

    def read_many(self, root_path: Path, refs: list[str]) -> list[dict[str, Any]]:
        results: list[dict[str, Any] | None] = [None] * len(refs)
        fanout_by_manifest: dict[str, list[tuple[int, int]]] = {}
        for index, ref in enumerate(refs):
            if not ref.startswith(ARTIFACT_URI_PREFIX):
                results[index] = self.read(root_path, ref)
                continue
            uri = ref.removeprefix(ARTIFACT_URI_PREFIX)
            if "#item=" not in uri:
                raise ValueError(f"invalid artifact URI: {ref}")
            manifest_ref, item_ref = uri.split("#item=", 1)
            fanout_by_manifest.setdefault(manifest_ref, []).append((index, int(item_ref)))

        for manifest_ref, requested_items in fanout_by_manifest.items():
            manifest_path = root_path / manifest_ref
            manifest_doc = cast(
                dict[str, Any], json.loads(manifest_path.read_text(encoding="utf-8"))
            )
            payload_lines_by_path: dict[str, list[str]] = {}
            for result_index, item_number in requested_items:
                try:
                    item = cast(dict[str, Any], manifest_doc["items"][item_number])
                except IndexError as exc:
                    raise ValueError(
                        f"artifact URI item does not exist: {manifest_ref}#item={item_number}"
                    ) from exc
                payload = cast(dict[str, Any], item["payload"])
                payload_path = str(payload["path"])
                lines = payload_lines_by_path.get(payload_path)
                if lines is None:
                    full_payload_path = manifest_path.parent.parent / payload_path
                    lines = full_payload_path.read_text(encoding="utf-8").splitlines()
                    payload_lines_by_path[payload_path] = lines
                record_number = int(payload["record"])
                try:
                    record = cast(dict[str, Any], json.loads(lines[record_number]))
                except IndexError as exc:
                    raise ValueError(
                        f"artifact URI record does not exist: {manifest_ref}#item={item_number}"
                    ) from exc
                results[result_index] = _fanout_payload(manifest_doc, item, record)

        return [cast(dict[str, Any], result) for result in results]


def _fanout_dir(context: CollectionWriteContext, partition_key: str) -> Path:
    return context.root_path / context.asset_name / partition_key


def _fanout_item_uri(
    asset_name: str, partition_key: str, manifest_name: str, item: int
) -> str:
    return (
        f"{ARTIFACT_URI_PREFIX}{asset_name}/{partition_key}/"
        f"manifests/{manifest_name}#item={item}"
    )


def _write_text(path: Path, payload: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")
    return sha256_text(payload)


def _write_immutable_text(path: Path, payload: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        if existing != payload:
            raise ValueError(f"immutable artifact already exists with different content: {path}")
    else:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
    return sha256_text(payload)


def _write_fanout_manifest(
    *,
    context: CollectionWriteContext,
    partition_key: str,
    window_id: str,
    items: list[dict[str, Any]],
) -> str:
    manifest_doc: dict[str, Any] = {
        "schema_version": 1,
        "window_id": window_id,
        "asset_name": context.asset_name,
        "partition_key": partition_key,
        "collection": {"type": "local_json"},
        "items": items,
    }
    manifest_hash = hash_canonical(manifest_doc)
    manifest_doc["manifest_hash"] = manifest_hash
    manifest_name = f"{window_id}.json"
    manifest_path = _fanout_dir(context, partition_key) / "manifests" / manifest_name
    _write_immutable_text(manifest_path, json.dumps(manifest_doc, indent=2, sort_keys=True))
    return manifest_name


def _read_fanout_record(root_path: Path, ref: str) -> dict[str, Any]:
    uri = ref.removeprefix(ARTIFACT_URI_PREFIX)
    if "#item=" not in uri:
        raise ValueError(f"invalid artifact URI: {ref}")
    manifest_ref, item_ref = uri.split("#item=", 1)
    item_number = int(item_ref)
    manifest_path = root_path / manifest_ref
    manifest_doc = cast(dict[str, Any], json.loads(manifest_path.read_text(encoding="utf-8")))
    try:
        item = cast(dict[str, Any], manifest_doc["items"][item_number])
    except IndexError as exc:
        raise ValueError(f"artifact URI item does not exist: {ref}") from exc
    payload = cast(dict[str, Any], item["payload"])
    record_number = int(payload["record"])
    payload_path = manifest_path.parent.parent / str(payload["path"])
    lines = payload_path.read_text(encoding="utf-8").splitlines()
    try:
        record = cast(dict[str, Any], json.loads(lines[record_number]))
    except IndexError as exc:
        raise ValueError(f"artifact URI record does not exist: {ref}") from exc
    return _fanout_payload(manifest_doc, item, record)


def _fanout_payload(
    manifest_doc: dict[str, Any],
    item: dict[str, Any],
    record: dict[str, Any],
) -> dict[str, Any]:
    return {
        "artifact": {
            "schema_version": manifest_doc["schema_version"],
            "asset_name": manifest_doc["asset_name"],
            "instance_key": item["instance_key"],
            "input_fingerprint": item["input_fingerprint"],
            "transform": {},
            "source": {},
            "inputs": {"upstreams": []},
            "output": {"content_hash": item.get("content_hash")},
        },
        "data": record["data"],
        "metadata": item.get("metadata", {}),
    }


def register_builtin_collections() -> None:
    register_artifact_collection("local_json", LocalJsonArtifactCollection())
