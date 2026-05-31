from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from strata.core.hashing import hash_canonical
from strata.core.models import (
    AssetSpec,
    ExecutionContext,
    ExecutionSpec,
    Manifest,
    SourceSpec,
    TestSpec,
)
from strata.plugins.registry import adapter_metadata


def load_manifest(project_file: Path) -> Manifest:
    project_file = project_file.resolve()
    root = project_file.parent
    raw = yaml.safe_load(project_file.read_text(encoding="utf-8")) or {}

    context = ExecutionContext(
        project_id=str(raw.get("project_id", "default")),
        tenant_id=str(raw.get("tenant_id", "default")),
    )
    execution = ExecutionSpec(**dict(raw.get("execution") or {}))

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
    if not isinstance(raw_pipeline, dict) or not raw_pipeline:
        raise ValueError("strata.yml must define at least one pipeline asset")
    asset_order = _topological_asset_order(raw_pipeline)

    assets: dict[str, AssetSpec] = {}
    for name in asset_order:
        spec = dict(raw_pipeline[name] or {})
        if "inputs" in spec or "join" in spec:
            raise ValueError(
                f"asset {name} uses removed multi-input join syntax; "
                "model joins as an upstream operation and use a single input"
            )
        version = spec.pop("version", None) or f"{name}@0.1.0"
        operation_name = _operation_ref(name, spec)
        adapter_metadata("operation", operation_name)
        kind = str(spec.pop("kind", None) or _default_asset_kind(name, spec))
        assets[name] = AssetSpec(
            name=name,
            kind=kind,
            operation_name=operation_name,
            version=version,
            **spec,
        )
    _validate_pipeline(assets, sources)

    state_url = raw.get("state", {}).get("url", "sqlite:///./.strata/state.db")
    artifacts_path = (root / raw.get("artifacts", {}).get("path", "./.strata/artifacts")).resolve()
    tests = _parse_tests(raw.get("tests", []), assets)

    manifest_payload: dict[str, Any] = {
        "project_id": context.project_id,
        "tenant_id": context.tenant_id,
        "execution": execution.model_dump(mode="json"),
        "sources": {name: spec.model_dump(mode="json") for name, spec in sources.items()},
        "assets": {name: spec.model_dump(mode="json") for name, spec in assets.items()},
        "tests": [spec.model_dump(mode="json") for spec in tests],
    }
    return Manifest(
        context=context,
        execution=execution,
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
    source_assets = [asset for asset in assets.values() if asset.source]
    if not source_assets:
        raise ValueError("pipeline must define at least one source-backed asset")
    for asset in assets.values():
        if asset.source and asset.source not in sources:
            raise ValueError(f"asset {asset.name} references unknown source: {asset.source}")
        if asset.input and asset.input not in assets:
            raise ValueError(f"asset {asset.name} references unknown input asset: {asset.input}")
        binding_count = sum(bool(value) for value in (asset.source, asset.input))
        if binding_count != 1:
            raise ValueError(
                f"asset {asset.name} must define exactly one of source or input"
            )


def _operation_ref(asset_name: str, spec: dict[str, Any]) -> str:
    if spec.get("operation"):
        return str(spec.pop("operation"))
    raise ValueError(f"asset {asset_name} must define operation")


def _default_asset_kind(asset_name: str, spec: dict[str, Any]) -> str:
    if spec.get("source"):
        return "parsed"
    return asset_name


def _topological_asset_order(raw_pipeline: dict[str, Any]) -> list[str]:
    ordered: list[str] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visited:
            return
        if name in visiting:
            raise ValueError(f"pipeline contains a dependency cycle at asset: {name}")
        if name not in raw_pipeline:
            raise ValueError(f"pipeline references unknown asset: {name}")
        visiting.add(name)
        spec = dict(raw_pipeline[name] or {})
        if spec.get("input"):
            visit(str(spec["input"]))
        visiting.remove(name)
        visited.add(name)
        ordered.append(name)

    for asset_name in raw_pipeline:
        visit(str(asset_name))
    return ordered


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
