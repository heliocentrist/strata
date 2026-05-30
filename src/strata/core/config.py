from __future__ import annotations

from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import yaml

from strata.core.hashing import hash_canonical
from strata.core.models import AssetSpec, ExecutionContext, Manifest, SourceSpec, TestSpec

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
        source_values = dict(spec)
        if "path" in source_values:
            source_values["path"] = (root / source_values["path"]).resolve()
        if "manifest_uri" in source_values:
            source_values["manifest_uri"] = _resolve_project_uri(
                str(source_values["manifest_uri"]), root
            )
        sources[name] = SourceSpec(name=name, **source_values)
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
    tests = _parse_tests(raw.get("tests", []), assets)

    manifest_payload: dict[str, Any] = {
        "project_id": context.project_id,
        "tenant_id": context.tenant_id,
        "sources": {name: spec.model_dump(mode="json") for name, spec in sources.items()},
        "assets": {name: spec.model_dump(mode="json") for name, spec in assets.items()},
        "tests": [spec.model_dump(mode="json") for spec in tests],
    }
    return Manifest(
        context=context,
        root=root,
        state_url=state_url,
        artifacts_path=artifacts_path,
        sources=sources,
        assets=assets,
        asset_order=asset_order,
        tests=tests,
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
    sink = assets["sink"]
    if sink.inputs:
        if sink.inputs.get("chunk") != "chunks" or sink.inputs.get("embedding") != "embeddings":
            raise ValueError("sink.inputs must map chunk: chunks and embedding: embeddings")
    elif sink.input != "embeddings":
        raise ValueError("sink.input must be 'embeddings'")


def _resolve_project_uri(value: str, root: Path) -> str:
    parsed = urlparse(value)
    if parsed.scheme:
        return value
    path = Path(value)
    if not path.is_absolute():
        path = root / path
    return str(path.resolve())


def _parse_tests(raw_tests: Any, assets: dict[str, AssetSpec]) -> list[TestSpec]:
    if raw_tests is None:
        return []
    if not isinstance(raw_tests, list):
        raise ValueError("tests must be a list")

    tests: list[TestSpec] = []
    for index, raw_test in enumerate(raw_tests):
        if not isinstance(raw_test, dict):
            raise ValueError("each test must be an object")
        asset = str(raw_test.get("asset", ""))
        test_type = str(raw_test.get("type", ""))
        if asset not in assets:
            raise ValueError(f"test {index} references unknown asset: {asset}")
        if not test_type:
            raise ValueError(f"test {index} must define type")
        tests.append(
            TestSpec(
                name=str(raw_test.get("name") or f"{asset}:{test_type}"),
                asset=asset,
                type=test_type,
                config=dict(raw_test.get("config") or {}),
            )
        )
    return tests


def state_path_from_url(state_url: str, root: Path) -> Path:
    prefix = "sqlite:///"
    if not state_url.startswith(prefix):
        raise ValueError("Phase 0 only supports sqlite:/// state URLs")
    raw_path = state_url[len(prefix) :]
    path = Path(raw_path)
    if not path.is_absolute():
        path = root / path
    return path.resolve()
