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


class ExecutionSpec(BaseModel):
    executor: str = "local_single_thread"
    config: dict[str, Any] = Field(default_factory=dict)


class SourceSnapshotMode(StrEnum):
    AUTHORITATIVE = "authoritative_snapshot"
    INCREMENTAL = "incremental_delta"


class SourceSpec(BaseModel):
    name: str
    type: str
    path: Path | None = None
    manifest_uri: str | None = None
    mode: SourceSnapshotMode = SourceSnapshotMode.AUTHORITATIVE
    connection_id: str = "local"
    include: list[str] = Field(default_factory=lambda: ["**/*.txt", "**/*.md", "**/*.pdf"])


class AssetSpec(BaseModel):
    name: str
    kind: str
    operation_name: str
    source: str | None = None
    input: str | None = None
    inputs: dict[str, str] = Field(default_factory=dict)
    join: dict[str, Any] = Field(default_factory=dict)
    version: str
    config: dict[str, Any] = Field(default_factory=dict)
    execution: dict[str, Any] = Field(default_factory=dict)
    artifact_strategy: dict[str, Any] = Field(default_factory=dict)
    determinism: Determinism = Determinism.DETERMINISTIC
    materialization_strategy: str = "content_addressed"


class TestSpec(BaseModel):
    name: str
    asset: str
    type: str
    config: dict[str, Any] = Field(default_factory=dict)


class Manifest(BaseModel):
    context: ExecutionContext
    execution: ExecutionSpec = Field(default_factory=ExecutionSpec)
    root: Path
    state_url: str
    artifacts_path: Path
    sources: dict[str, SourceSpec]
    assets: dict[str, AssetSpec]
    asset_order: list[str]
    tests: list[TestSpec] = Field(default_factory=list)
    manifest_hash: str


class SourceItem(BaseModel):
    source_name: str
    item_key: str
    path: Path | None = None
    uri: str | None = None
    content_hash: str
    deleted: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourceSnapshot(BaseModel):
    source_name: str
    mode: SourceSnapshotMode = SourceSnapshotMode.AUTHORITATIVE
    connection_id: str = "local"
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
    additional_upstream_ids: list[str] = Field(default_factory=list)
    operation_item_id: str
    operation_status: OperationItemStatus


class SourceRef(BaseModel):
    name: str | None = None
    item_key: str
    content_hash: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ChunkRef(BaseModel):
    asset_name: str = "chunks"
    instance_key: str
    fingerprint: str
    content_hash: str | None = None
    text: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class EmbeddingRef(BaseModel):
    asset_name: str = "embeddings"
    instance_key: str
    fingerprint: str
    content_hash: str | None = None
    vector: list[float]
    metadata: dict[str, Any] = Field(default_factory=dict)


class HybridChunkEmbeddingItem(BaseModel):
    context: ExecutionContext
    source: SourceRef
    chunk: ChunkRef
    embedding: EmbeddingRef
    document_id: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class SinkWriteResult(BaseModel):
    output_location: str
    output_hash: str | None = None
    external_id: str | None = None
