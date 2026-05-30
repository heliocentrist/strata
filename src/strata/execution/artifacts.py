from __future__ import annotations

from typing import Any

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
) -> list[ArtifactWriteResult]:
    collection = _collection_for_asset(manifest, asset_name)
    return collection.write_many(
        CollectionWriteContext(
            root_path=manifest.artifacts_path,
            asset_name=asset_name,
            partition_key=partition_key,
        ),
        items,
    )


def _collection_for_asset(manifest: Manifest, asset_name: str) -> ArtifactCollection:
    asset = manifest.assets[asset_name]
    collection_type = str(asset.artifact_strategy.get("type") or "local_json")
    return get_artifact_collection(collection_type)


def read_artifact(manifest: Manifest, location: str | None) -> dict[str, Any]:
    if not location:
        raise ValueError("artifact has no output location")
    return get_artifact_collection("local_json").read(manifest.artifacts_path, location)


def artifact_source(payload: dict[str, Any]) -> dict[str, Any]:
    source = payload.get("artifact", {}).get("source", {})
    if not isinstance(source, dict):
        return {}
    return source
