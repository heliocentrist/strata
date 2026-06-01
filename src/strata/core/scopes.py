from __future__ import annotations

from strata.core.hashing import config_hash, input_fingerprint
from strata.core.models import Manifest


def asset_scope_fingerprint(
    manifest: Manifest,
    asset_name: str,
    *,
    source_name: str,
    source_item_key: str,
    source_content_hash: str,
) -> str:
    asset = manifest.assets[asset_name]
    if asset.source:
        return input_fingerprint(
            transform_version=asset.version,
            config_hash_value=config_hash(asset.config),
            determinism=asset.determinism.value,
            instance_key=f"{source_name}:{source_item_key}",
            source_content_hash=source_content_hash,
        )
    upstream_scopes = [
        asset_scope_fingerprint(
            manifest,
            upstream_name,
            source_name=source_name,
            source_item_key=source_item_key,
            source_content_hash=source_content_hash,
        )
        for upstream_name in _asset_dependencies(manifest, asset_name)
    ]
    return input_fingerprint(
        transform_version=asset.version,
        config_hash_value=config_hash(asset.config),
        determinism=asset.determinism.value,
        instance_key=f"{source_name}:{source_item_key}",
        upstream_fingerprints=upstream_scopes,
    )


def _asset_dependencies(manifest: Manifest, asset_name: str) -> list[str]:
    asset = manifest.assets[asset_name]
    if asset.input:
        return [asset.input]
    return []
