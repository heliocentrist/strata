from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml

from strata.hashing import hash_canonical
from strata.models import AssetSpec, ExecutionContext, Manifest, SourceSpec

AssetKind = Literal["parsed", "chunks", "embeddings", "sink"]

PIPELINE_KINDS: dict[str, AssetKind] = {
    "parsed": "parsed",
    "chunks": "chunks",
    "embeddings": "embeddings",
    "sink": "sink",
}


def load_manifest(project_file: Path) -> Manifest:
    project_file = project_file.resolve()
    root = project_file.parent
    raw = yaml.safe_load(project_file.read_text(encoding="utf-8")) or {}

    context = ExecutionContext(
        project_id=str(raw.get("project_id", "default")),
        tenant_id=str(raw.get("tenant_id", "default")),
    )

    sources = {}
    for name, spec in raw.get("sources", {}).items():
        source_values = {k: v for k, v in spec.items() if k != "path"}
        sources[name] = SourceSpec(
            name=name,
            path=(root / spec["path"]).resolve(),
            **source_values,
        )
    if not sources:
        raise ValueError("strata.yml must define at least one source")

    raw_pipeline = raw.get("pipeline", {})
    asset_order = [
        name for name in ["parsed", "chunks", "embeddings", "sink"] if name in raw_pipeline
    ]
    if asset_order != list(raw_pipeline.keys()):
        unknown = [name for name in raw_pipeline if name not in PIPELINE_KINDS]
        if unknown:
            raise ValueError(f"unsupported Phase 0 asset(s): {', '.join(unknown)}")

    assets: dict[str, AssetSpec] = {}
    for name in asset_order:
        spec = dict(raw_pipeline[name] or {})
        version = spec.pop("version", None) or f"{name}@0.1.0"
        kind = PIPELINE_KINDS[name]
        assets[name] = AssetSpec(name=name, kind=kind, version=version, **spec)

    required = ["parsed", "chunks", "embeddings", "sink"]
    missing = [name for name in required if name not in assets]
    if missing:
        raise ValueError(f"Phase 0 pipeline requires assets: {', '.join(required)}")
    _validate_pipeline(assets, sources)

    state_url = raw.get("state", {}).get("url", "sqlite:///./.strata/state.db")
    artifacts_path = (root / raw.get("artifacts", {}).get("path", "./.strata/artifacts")).resolve()

    manifest_payload: dict[str, Any] = {
        "project_id": context.project_id,
        "tenant_id": context.tenant_id,
        "sources": {name: spec.model_dump(mode="json") for name, spec in sources.items()},
        "assets": {name: spec.model_dump(mode="json") for name, spec in assets.items()},
    }
    return Manifest(
        context=context,
        root=root,
        state_url=state_url,
        artifacts_path=artifacts_path,
        sources=sources,
        assets=assets,
        asset_order=asset_order,
        manifest_hash=hash_canonical(manifest_payload),
    )


def _validate_pipeline(assets: dict[str, AssetSpec], sources: dict[str, SourceSpec]) -> None:
    parsed = assets["parsed"]
    if not parsed.source or parsed.source not in sources:
        raise ValueError("parsed asset must reference an existing source")
    if assets["chunks"].input != "parsed":
        raise ValueError("chunks.input must be 'parsed'")
    if assets["embeddings"].input != "chunks":
        raise ValueError("embeddings.input must be 'chunks'")
    if assets["sink"].input != "embeddings":
        raise ValueError("sink.input must be 'embeddings'")


def state_path_from_url(state_url: str, root: Path) -> Path:
    prefix = "sqlite:///"
    if not state_url.startswith(prefix):
        raise ValueError("Phase 0 only supports sqlite:/// state URLs")
    raw_path = state_url[len(prefix) :]
    path = Path(raw_path)
    if not path.is_absolute():
        path = root / path
    return path.resolve()
