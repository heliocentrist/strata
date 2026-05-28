from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field


class Determinism(StrEnum):
    DETERMINISTIC = "deterministic"
    SEEDED = "seeded"
    NONDETERMINISTIC = "nondeterministic"


class ExecutionContext(BaseModel):
    project_id: str = "default"
    tenant_id: str = "default"


class SourceSpec(BaseModel):
    name: str
    type: Literal["local_files"]
    path: Path
    include: list[str] = Field(default_factory=lambda: ["**/*.txt", "**/*.md", "**/*.pdf"])


class AssetSpec(BaseModel):
    name: str
    kind: Literal["parsed", "chunks", "embeddings", "sink"]
    source: str | None = None
    input: str | None = None
    parser: str | None = None
    transform: str | None = None
    type: str | None = None
    version: str
    config: dict[str, Any] = Field(default_factory=dict)
    determinism: Determinism = Determinism.DETERMINISTIC
    materialization_strategy: str = "content_addressed"


class Manifest(BaseModel):
    context: ExecutionContext
    root: Path
    state_url: str
    artifacts_path: Path
    sources: dict[str, SourceSpec]
    assets: dict[str, AssetSpec]
    asset_order: list[str]
    manifest_hash: str


class SourceItem(BaseModel):
    source_name: str
    item_key: str
    path: Path
    content_hash: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourceSnapshot(BaseModel):
    source_name: str
    scope_hash: str
    scan_marker: str
    items: list[SourceItem]


class CurrentState(BaseModel):
    source_hashes: dict[tuple[str, str], str] = Field(default_factory=dict)
    deleted_sources: set[tuple[str, str]] = Field(default_factory=set)
    materialized: set[tuple[str, str, str]] = Field(default_factory=set)
    failed: set[tuple[str, str]] = Field(default_factory=set)


class OperationScope(BaseModel):
    source_name: str | None = None
    item_key: str | None = None
    upstream_asset_name: str | None = None
    upstream_instance_key: str | None = None


class Operation(BaseModel):
    op_id: str
    op_type: Literal["build_scope", "delete_scope"]
    asset_name: str
    project_id: str
    tenant_id: str
    scope: OperationScope
    reason: str
    depends_on: list[str] = Field(default_factory=list)
    estimated_instance_count: int | None = None
    estimated_cost: dict[str, Any] | None = None


class OperationItemStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    DELETED = "deleted"


class MaterializedArtifact(BaseModel):
    id: str
    asset_name: str
    instance_key: str
    input_fingerprint: str
    output_location: str | None = None
    output_hash: str | None = None
    content_hash: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class AssetInstanceCommit(BaseModel):
    asset_name: str
    instance_key: str
    input_fingerprint: str
    output_location: str
    output_hash: str | None = None
    content_hash: str | None = None
    transform_id: str
    materialization_strategy: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    upstream_id: str
    operation_item_id: str
    operation_status: OperationItemStatus


class ArtifactTransform(BaseModel):
    version: str
    config_hash: str


class ArtifactSource(BaseModel):
    name: str | None = None
    item_key: str | None = None


class ArtifactUpstream(BaseModel):
    asset_name: str
    instance_key: str
    input_fingerprint: str


class ArtifactInputs(BaseModel):
    upstreams: list[ArtifactUpstream] = Field(default_factory=list)


class ArtifactOutput(BaseModel):
    content_hash: str | None = None


class ArtifactEnvelope(BaseModel):
    schema_version: int = 1
    asset_name: str
    instance_key: str
    input_fingerprint: str
    transform: ArtifactTransform
    source: ArtifactSource = Field(default_factory=ArtifactSource)
    inputs: ArtifactInputs = Field(default_factory=ArtifactInputs)
    output: ArtifactOutput = Field(default_factory=ArtifactOutput)


class ArtifactDocument(BaseModel):
    artifact: ArtifactEnvelope
    data: Any
    metadata: dict[str, Any] = Field(default_factory=dict)


class FanoutManifestParent(BaseModel):
    asset_name: str
    instance_key: str
    input_fingerprint: str


class FanoutManifestPayload(BaseModel):
    path: str
    format: Literal["jsonl"]
    record: int
    file_hash: str


class FanoutManifestItem(BaseModel):
    instance_key: str
    input_fingerprint: str
    content_hash: str | None = None
    payload: FanoutManifestPayload
    metadata: dict[str, Any] = Field(default_factory=dict)


class FanoutManifest(BaseModel):
    schema_version: int = 1
    manifest_hash: str = ""
    created_at: str
    asset_name: str
    parent: FanoutManifestParent
    transform: ArtifactTransform
    source: ArtifactSource = Field(default_factory=ArtifactSource)
    upstreams: list[ArtifactUpstream] = Field(default_factory=list)
    items: list[FanoutManifestItem] = Field(default_factory=list)
