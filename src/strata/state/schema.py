from __future__ import annotations

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)

metadata = MetaData()


transforms = Table(
    "transforms",
    metadata,
    Column("id", String, primary_key=True),
    Column("project_id", String, nullable=False),
    Column("transform_id", String, nullable=False),
    Column("version", String, nullable=False),
    Column("config_json", Text, nullable=False),
    Column("config_hash", String, nullable=False),
    Column("code_hash", String),
    Column("determinism", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("project_id", "transform_id", "version", "config_hash"),
)

asset_instances = Table(
    "asset_instances",
    metadata,
    Column("id", String, primary_key=True),
    Column("project_id", String, nullable=False),
    Column("tenant_id", String, nullable=False),
    Column("asset_name", String, nullable=False),
    Column("instance_key", String, nullable=False),
    Column("source_name", String),
    Column("source_item_key", String),
    Column("scope_fingerprint", String),
    Column("input_fingerprint", String, nullable=False),
    Column("artifact_collection", String, nullable=False),
    Column("output_location", Text),
    Column("output_hash", String),
    Column("content_hash", String),
    Column("transform_id", String, ForeignKey("transforms.id"), nullable=False),
    Column("materialization_strategy", String, nullable=False),
    Column("status", String, nullable=False),
    Column("error", Text),
    Column("metadata_json", Text, nullable=False, default="{}"),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "project_id",
        "tenant_id",
        "asset_name",
        "instance_key",
        "input_fingerprint",
    ),
)
Index(
    "ix_asset_instances_lookup",
    asset_instances.c.project_id,
    asset_instances.c.tenant_id,
    asset_instances.c.asset_name,
    asset_instances.c.instance_key,
    asset_instances.c.status,
)
Index(
    "ix_asset_instances_source_scope",
    asset_instances.c.project_id,
    asset_instances.c.tenant_id,
    asset_instances.c.asset_name,
    asset_instances.c.source_name,
    asset_instances.c.source_item_key,
    asset_instances.c.status,
)

asset_scope_completions = Table(
    "asset_scope_completions",
    metadata,
    Column("project_id", String, nullable=False),
    Column("tenant_id", String, nullable=False),
    Column("asset_name", String, nullable=False),
    Column("source_name", String, nullable=False),
    Column("source_item_key", String, nullable=False),
    Column("scope_fingerprint", String, nullable=False),
    Column("expected_instance_count", String, nullable=False),
    Column("status", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "project_id",
        "tenant_id",
        "asset_name",
        "source_name",
        "source_item_key",
        "scope_fingerprint",
    ),
)
Index(
    "ix_asset_scope_completions_lookup",
    asset_scope_completions.c.project_id,
    asset_scope_completions.c.tenant_id,
    asset_scope_completions.c.asset_name,
    asset_scope_completions.c.source_name,
    asset_scope_completions.c.source_item_key,
    asset_scope_completions.c.status,
)

lineage_edges = Table(
    "lineage_edges",
    metadata,
    Column(
        "downstream_asset_instance_id",
        String,
        ForeignKey("asset_instances.id"),
        nullable=False,
    ),
    Column("upstream_asset_instance_id", String, ForeignKey("asset_instances.id"), nullable=False),
    UniqueConstraint("downstream_asset_instance_id", "upstream_asset_instance_id"),
)

source_state = Table(
    "source_state",
    metadata,
    Column("project_id", String, nullable=False),
    Column("tenant_id", String, nullable=False),
    Column("source_name", String, nullable=False),
    Column("item_key", String, nullable=False),
    Column("source_content_hash", String, nullable=False),
    Column("missing_since", DateTime(timezone=True)),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    Column("deleted_at", DateTime(timezone=True)),
    UniqueConstraint("project_id", "tenant_id", "source_name", "item_key"),
)

source_checkpoints = Table(
    "source_checkpoints",
    metadata,
    Column("id", String, primary_key=True),
    Column("project_id", String, nullable=False),
    Column("tenant_id", String, nullable=False),
    Column("source_name", String, nullable=False),
    Column("connection_id", String, nullable=False),
    Column("scope_hash", String, nullable=False),
    Column("cursor_token", Text, nullable=False),
    Column("cursor_version", String, nullable=False),
    Column("status", String, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("project_id", "tenant_id", "source_name", "connection_id", "scope_hash"),
)

runs = Table(
    "runs",
    metadata,
    Column("id", String, primary_key=True),
    Column("project_id", String, nullable=False),
    Column("tenant_id", String, nullable=False),
    Column("manifest_hash", String, nullable=False),
    Column("status", String, nullable=False),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

operation_runs = Table(
    "operation_runs",
    metadata,
    Column("id", String, primary_key=True),
    Column("run_id", String, ForeignKey("runs.id"), nullable=False),
    Column("project_id", String, nullable=False),
    Column("tenant_id", String, nullable=False),
    Column("op_type", String, nullable=False),
    Column("asset_name", String, nullable=False),
    Column("scope_json", Text, nullable=False),
    Column("reason", String, nullable=False),
    Column("status", String, nullable=False),
    Column("estimated_instance_count", String),
    Column("estimated_cost_json", Text),
    Column("error", Text),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
)

operation_items = Table(
    "operation_items",
    metadata,
    Column("id", String, primary_key=True),
    Column("run_id", String, ForeignKey("runs.id"), nullable=False),
    Column("operation_run_id", String, ForeignKey("operation_runs.id"), nullable=False),
    Column("project_id", String, nullable=False),
    Column("tenant_id", String, nullable=False),
    Column("asset_name", String, nullable=False),
    Column("item_key", String, nullable=False),
    Column("instance_key", String),
    Column("input_fingerprint", String),
    Column("status", String, nullable=False),
    Column("error", Text),
    Column("metadata_json", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("operation_run_id", "item_key"),
)
Index("ix_operation_items_run_status", operation_items.c.run_id, operation_items.c.status)

apply_locks = Table(
    "apply_locks",
    metadata,
    Column("project_id", String, nullable=False),
    Column("tenant_id", String, nullable=False),
    Column("run_id", String, nullable=False),
    Column("acquired_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("project_id", "tenant_id"),
)

vector_sink = Table(
    "local_vector_sink",
    metadata,
    Column("project_id", String, nullable=False),
    Column("tenant_id", String, nullable=False),
    Column("source_name", String, nullable=False),
    Column("source_item_key", String, nullable=False),
    Column("instance_key", String, nullable=False),
    Column("embedding_fingerprint", String, nullable=False),
    Column("chunk_text", Text, nullable=False),
    Column("embedding_json", Text, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "project_id",
        "tenant_id",
        "source_name",
        "instance_key",
        "embedding_fingerprint",
    ),
)

