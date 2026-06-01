from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from strata.core.collections import (
    ArtifactCollection,
    ArtifactWrite,
    ArtifactWriteResult,
    CollectionWriteContext,
)
from strata.core.models import Manifest
from strata.plugins.registry import get_artifact_collection


def write_one_artifact(
    *,
    manifest: Manifest,
    asset_name: str,
    item: ArtifactWrite,
) -> ArtifactWriteResult:
    collection = _collection_for_asset(manifest, asset_name)
    return collection.write_one(
        CollectionWriteContext(
            root_path=manifest.artifacts_path,
            asset_name=asset_name,
        ),
        item,
    )


def write_many_artifacts(
    *,
    manifest: Manifest,
    asset_name: str,
    partition_key: str,
    items: list[ArtifactWrite],
    window_id: str | None = None,
) -> list[ArtifactWriteResult]:
    collection = _collection_for_asset(manifest, asset_name)
    return collection.write_many(
        CollectionWriteContext(
            root_path=manifest.artifacts_path,
            asset_name=asset_name,
            partition_key=partition_key,
            window_id=window_id,
        ),
        items,
    )


def _collection_for_asset(manifest: Manifest, asset_name: str) -> ArtifactCollection:
    asset = manifest.assets[asset_name]
    collection_type = str(asset.artifact_strategy.get("type") or "local_json")
    return get_artifact_collection(collection_type)


def read_artifact(
    manifest: Manifest,
    location: str | None,
    artifact_collection: str = "local_json",
) -> dict[str, Any]:
    return read_artifact_ref(manifest.artifacts_path, location, artifact_collection)


def read_artifact_ref(
    root_path: Path,
    location: str | None,
    artifact_collection: str = "local_json",
) -> dict[str, Any]:
    if not location:
        raise ValueError("artifact has no output location")
    return get_artifact_collection(artifact_collection).read(root_path, location)


def read_artifact_refs(
    root_path: Path,
    refs: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any] | None] = [None] * len(refs)
    by_collection: dict[str, list[tuple[int, str]]] = {}
    for index, (collection_name, location) in enumerate(refs):
        by_collection.setdefault(collection_name, []).append((index, location))

    for collection_name, collection_refs in by_collection.items():
        collection = get_artifact_collection(collection_name)
        payloads = collection.read_many(
            root_path,
            [location for _index, location in collection_refs],
        )
        for (index, _location), payload in zip(collection_refs, payloads, strict=True):
            results[index] = payload
    return [cast(dict[str, Any], payload) for payload in results]


def artifact_source(payload: dict[str, Any]) -> dict[str, Any]:
    source = payload.get("artifact", {}).get("source", {})
    if not isinstance(source, dict):
        return {}
    return source
