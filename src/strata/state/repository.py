from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, insert, or_, select, update
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

from strata.core.models import (
    AssetInstanceCommit,
    CurrentState,
    ExecutionContext,
    MaterializedArtifact,
    Operation,
)
from strata.state.schema import (
    apply_locks,
    asset_instances,
    lineage_edges,
    operation_items,
    operation_runs,
    runs,
    source_checkpoints,
    source_state,
    transforms,
    vector_sink,
)


def now() -> datetime:
    return datetime.now(UTC)


def new_id() -> str:
    return str(uuid.uuid4())


class StateRepository:
    def __init__(self, engine: Engine, context: ExecutionContext):
        self.engine = engine
        self.context = context

    @contextmanager
    def begin(self) -> Iterator[Connection]:
        with self.engine.begin() as conn:
            yield conn

    def snapshot(self) -> CurrentState:
        with self.engine.connect() as conn:
            source_rows = conn.execute(
                select(source_state).where(
                    source_state.c.project_id == self.context.project_id,
                    source_state.c.tenant_id == self.context.tenant_id,
                )
            ).mappings()
            asset_rows = conn.execute(
                select(asset_instances).where(
                    asset_instances.c.project_id == self.context.project_id,
                    asset_instances.c.tenant_id == self.context.tenant_id,
                )
            ).mappings()
            state = CurrentState()
            for row in source_rows:
                key = (row["source_name"], row["item_key"])
                if row["deleted_at"] is None:
                    state.source_hashes[key] = row["source_content_hash"]
                else:
                    state.deleted_sources.add(key)
            for row in asset_rows:
                key2 = (row["asset_name"], row["instance_key"])
                if row["status"] == "materialized":
                    state.materialized.add((*key2, row["input_fingerprint"]))
                elif row["status"] == "failed":
                    state.failed.add(key2)
            return state

    def acquire_lock(self, run_id: str, ttl_seconds: int = 3600) -> None:
        timestamp = now()
        expires_at = timestamp + timedelta(seconds=ttl_seconds)
        with self.begin() as conn:
            expired = conn.execute(
                select(apply_locks).where(
                    apply_locks.c.project_id == self.context.project_id,
                    apply_locks.c.tenant_id == self.context.tenant_id,
                    apply_locks.c.expires_at < timestamp,
                )
            ).first()
            if expired:
                conn.execute(
                    delete(apply_locks).where(
                        apply_locks.c.project_id == self.context.project_id,
                        apply_locks.c.tenant_id == self.context.tenant_id,
                    )
                )
            existing = conn.execute(
                select(apply_locks).where(
                    apply_locks.c.project_id == self.context.project_id,
                    apply_locks.c.tenant_id == self.context.tenant_id,
                )
            ).first()
            if existing:
                raise RuntimeError(
                    f"apply already running for {self.context.project_id}/{self.context.tenant_id}"
                )
            conn.execute(
                insert(apply_locks).values(
                    project_id=self.context.project_id,
                    tenant_id=self.context.tenant_id,
                    run_id=run_id,
                    acquired_at=timestamp,
                    heartbeat_at=timestamp,
                    expires_at=expires_at,
                )
            )

    def release_lock(self) -> None:
        with self.begin() as conn:
            conn.execute(
                delete(apply_locks).where(
                    apply_locks.c.project_id == self.context.project_id,
                    apply_locks.c.tenant_id == self.context.tenant_id,
                )
            )

    def create_run(self, manifest_hash: str) -> str:
        run_id = new_id()
        timestamp = now()
        with self.begin() as conn:
            conn.execute(
                insert(runs).values(
                    id=run_id,
                    project_id=self.context.project_id,
                    tenant_id=self.context.tenant_id,
                    manifest_hash=manifest_hash,
                    status="running",
                    started_at=timestamp,
                    created_at=timestamp,
                )
            )
        return run_id

    def finish_run(self, run_id: str, status: str) -> None:
        with self.begin() as conn:
            conn.execute(
                update(runs).where(runs.c.id == run_id).values(status=status, finished_at=now())
            )

    def create_operation_run(self, run_id: str, operation: Operation) -> str:
        operation_run_id = new_id()
        with self.begin() as conn:
            conn.execute(
                insert(operation_runs).values(
                    id=operation_run_id,
                    run_id=run_id,
                    project_id=self.context.project_id,
                    tenant_id=self.context.tenant_id,
                    op_type=operation.op_type,
                    asset_name=operation.asset_name,
                    scope_json=operation.scope.model_dump_json(),
                    reason=operation.reason,
                    status="pending",
                    estimated_instance_count=(
                        str(operation.estimated_instance_count)
                        if operation.estimated_instance_count is not None
                        else None
                    ),
                    estimated_cost_json=json.dumps(operation.estimated_cost)
                    if operation.estimated_cost
                    else None,
                )
            )
        return operation_run_id

    def update_operation_run(
        self, operation_run_id: str, status: str, error: str | None = None
    ) -> None:
        timestamp = now()
        values: dict[str, Any] = {"status": status}
        if status == "running":
            values["started_at"] = timestamp
        if status in {"succeeded", "failed", "skipped"}:
            values["finished_at"] = timestamp
        if error:
            values["error"] = error
        with self.begin() as conn:
            conn.execute(
                update(operation_runs)
                .where(operation_runs.c.id == operation_run_id)
                .values(**values)
            )

    def create_operation_item(
        self,
        *,
        run_id: str,
        operation_run_id: str,
        asset_name: str,
        item_key: str,
        instance_key: str | None = None,
        input_fingerprint_value: str | None = None,
        status: str = "pending",
        metadata_value: dict[str, Any] | None = None,
    ) -> str:
        item_id = new_id()
        timestamp = now()
        with self.begin() as conn:
            conn.execute(
                insert(operation_items).values(
                    id=item_id,
                    run_id=run_id,
                    operation_run_id=operation_run_id,
                    project_id=self.context.project_id,
                    tenant_id=self.context.tenant_id,
                    asset_name=asset_name,
                    item_key=item_key,
                    instance_key=instance_key,
                    input_fingerprint=input_fingerprint_value,
                    status=status,
                    metadata_json=json.dumps(metadata_value or {}),
                    created_at=timestamp,
                    updated_at=timestamp,
                )
            )
        return item_id

    def update_operation_item(
        self,
        item_id: str,
        status: str,
        *,
        instance_key: str | None = None,
        input_fingerprint_value: str | None = None,
        error: str | None = None,
    ) -> None:
        values: dict[str, Any] = {"status": status, "updated_at": now()}
        if instance_key is not None:
            values["instance_key"] = instance_key
        if input_fingerprint_value is not None:
            values["input_fingerprint"] = input_fingerprint_value
        if error is not None:
            values["error"] = error
        with self.begin() as conn:
            conn.execute(
                update(operation_items)
                .where(operation_items.c.id == item_id)
                .values(**values)
            )

    def progress(self, run_id: str) -> dict[str, int]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(operation_items.c.status).where(operation_items.c.run_id == run_id)
            ).all()
        counts = {
            "total": len(rows),
            "pending": 0,
            "running": 0,
            "succeeded": 0,
            "failed": 0,
            "skipped": 0,
            "deleted": 0,
        }
        for (status,) in rows:
            counts[status] = counts.get(status, 0) + 1
        return counts

    def upsert_transform(
        self,
        *,
        project_id: str,
        transform_id: str,
        version: str,
        config_json: str,
        config_hash_value: str,
        determinism: str,
    ) -> str:
        try:
            with self.begin() as conn:
                row = self._find_transform(
                    conn,
                    project_id=project_id,
                    transform_id=transform_id,
                    version=version,
                    config_hash_value=config_hash_value,
                )
                if row:
                    return str(row[0])
                transform_pk = new_id()
                conn.execute(
                    insert(transforms).values(
                        id=transform_pk,
                        project_id=project_id,
                        transform_id=transform_id,
                        version=version,
                        config_json=config_json,
                        config_hash=config_hash_value,
                        determinism=determinism,
                        created_at=now(),
                    )
                )
                return transform_pk
        except IntegrityError:
            with self.engine.connect() as conn:
                row = self._find_transform(
                    conn,
                    project_id=project_id,
                    transform_id=transform_id,
                    version=version,
                    config_hash_value=config_hash_value,
                )
                if row:
                    return str(row[0])
            raise

    def _find_transform(
        self,
        conn: Connection,
        *,
        project_id: str,
        transform_id: str,
        version: str,
        config_hash_value: str,
    ) -> Any:
        return conn.execute(
            select(transforms.c.id).where(
                transforms.c.project_id == project_id,
                transforms.c.transform_id == transform_id,
                transforms.c.version == version,
                transforms.c.config_hash == config_hash_value,
            )
        ).first()

    def find_materialized(
        self, asset_name: str, instance_key: str, input_fingerprint_value: str
    ) -> MaterializedArtifact | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                select(asset_instances).where(
                    asset_instances.c.project_id == self.context.project_id,
                    asset_instances.c.tenant_id == self.context.tenant_id,
                    asset_instances.c.asset_name == asset_name,
                    asset_instances.c.instance_key == instance_key,
                    asset_instances.c.input_fingerprint == input_fingerprint_value,
                    asset_instances.c.status == "materialized",
                )
            ).mappings().first()
        if not row:
            return None
        return MaterializedArtifact(
            id=row["id"],
            asset_name=row["asset_name"],
            instance_key=row["instance_key"],
            input_fingerprint=row["input_fingerprint"],
            output_location=row["output_location"],
            output_hash=row["output_hash"],
            content_hash=row["content_hash"],
            metadata=json.loads(row["metadata_json"] or "{}"),
        )

    def latest_materialized(
        self, asset_name: str, instance_key: str
    ) -> MaterializedArtifact | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                select(asset_instances)
                .where(
                    asset_instances.c.project_id == self.context.project_id,
                    asset_instances.c.tenant_id == self.context.tenant_id,
                    asset_instances.c.asset_name == asset_name,
                    asset_instances.c.instance_key == instance_key,
                    asset_instances.c.status == "materialized",
                )
                .order_by(asset_instances.c.updated_at.desc())
            ).mappings().first()
        if not row:
            return None
        return MaterializedArtifact(
            id=row["id"],
            asset_name=row["asset_name"],
            instance_key=row["instance_key"],
            input_fingerprint=row["input_fingerprint"],
            output_location=row["output_location"],
            output_hash=row["output_hash"],
            content_hash=row["content_hash"],
            metadata=json.loads(row["metadata_json"] or "{}"),
        )

    def latest_materialized_by_prefix(
        self, asset_name: str, instance_key_prefix: str
    ) -> list[MaterializedArtifact]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(asset_instances)
                .where(
                    asset_instances.c.project_id == self.context.project_id,
                    asset_instances.c.tenant_id == self.context.tenant_id,
                    asset_instances.c.asset_name == asset_name,
                    asset_instances.c.instance_key.like(f"{instance_key_prefix}%"),
                    asset_instances.c.status == "materialized",
                )
                .order_by(asset_instances.c.instance_key, asset_instances.c.updated_at.desc())
            ).mappings().all()
        latest: dict[str, Any] = {}
        for row in rows:
            latest.setdefault(row["instance_key"], row)
        return [
            MaterializedArtifact(
                id=row["id"],
                asset_name=row["asset_name"],
                instance_key=row["instance_key"],
                input_fingerprint=row["input_fingerprint"],
                output_location=row["output_location"],
                output_hash=row["output_hash"],
                content_hash=row["content_hash"],
                metadata=json.loads(row["metadata_json"] or "{}"),
            )
            for row in latest.values()
        ]

    def write_asset_instance(
        self,
        *,
        asset_name: str,
        instance_key: str,
        input_fingerprint_value: str,
        output_location: str | None,
        output_hash: str | None,
        content_hash: str | None,
        transform_id: str,
        materialization_strategy: str,
        metadata_value: dict[str, Any] | None = None,
    ) -> MaterializedArtifact:
        timestamp = now()
        with self.begin() as conn:
            artifact = self._upsert_asset_instance(
                conn,
                timestamp=timestamp,
                asset_name=asset_name,
                instance_key=instance_key,
                input_fingerprint_value=input_fingerprint_value,
                output_location=output_location,
                output_hash=output_hash,
                content_hash=content_hash,
                transform_id=transform_id,
                materialization_strategy=materialization_strategy,
                metadata_value=metadata_value or {},
            )
        return artifact

    def commit_asset_instances(
        self, items: list[AssetInstanceCommit]
    ) -> list[MaterializedArtifact]:
        timestamp = now()
        artifacts: list[MaterializedArtifact] = []
        with self.begin() as conn:
            for item in items:
                artifact = self._upsert_asset_instance(
                    conn,
                    timestamp=timestamp,
                    asset_name=item.asset_name,
                    instance_key=item.instance_key,
                    input_fingerprint_value=item.input_fingerprint,
                    output_location=item.output_location,
                    output_hash=item.output_hash,
                    content_hash=item.content_hash,
                    transform_id=item.transform_id,
                    materialization_strategy=item.materialization_strategy,
                    metadata_value=item.metadata,
                )
                artifacts.append(artifact)
                self._write_lineage(conn, item.upstream_id, artifact.id)
                for upstream_id in item.additional_upstream_ids:
                    self._write_lineage(conn, upstream_id, artifact.id)
                conn.execute(
                    update(operation_items)
                    .where(operation_items.c.id == item.operation_item_id)
                    .values(
                        status=item.operation_status.value,
                        instance_key=item.instance_key,
                        input_fingerprint=item.input_fingerprint,
                        updated_at=timestamp,
                    )
                )
        return artifacts

    def _upsert_asset_instance(
        self,
        conn: Connection,
        *,
        timestamp: datetime,
        asset_name: str,
        instance_key: str,
        input_fingerprint_value: str,
        output_location: str | None,
        output_hash: str | None,
        content_hash: str | None,
        transform_id: str,
        materialization_strategy: str,
        metadata_value: dict[str, Any],
    ) -> MaterializedArtifact:
        row = conn.execute(
            select(asset_instances).where(
                asset_instances.c.project_id == self.context.project_id,
                asset_instances.c.tenant_id == self.context.tenant_id,
                asset_instances.c.asset_name == asset_name,
                asset_instances.c.instance_key == instance_key,
                asset_instances.c.input_fingerprint == input_fingerprint_value,
            )
        ).mappings().first()
        values = {
            "output_location": output_location,
            "output_hash": output_hash,
            "content_hash": content_hash,
            "transform_id": transform_id,
            "materialization_strategy": materialization_strategy,
            "status": "materialized",
            "error": None,
            "metadata_json": json.dumps(metadata_value),
            "updated_at": timestamp,
        }
        if row:
            conn.execute(
                update(asset_instances).where(asset_instances.c.id == row["id"]).values(**values)
            )
            instance_id = str(row["id"])
        else:
            instance_id = new_id()
            conn.execute(
                insert(asset_instances).values(
                    id=instance_id,
                    project_id=self.context.project_id,
                    tenant_id=self.context.tenant_id,
                    asset_name=asset_name,
                    instance_key=instance_key,
                    input_fingerprint=input_fingerprint_value,
                    created_at=timestamp,
                    **values,
                )
            )
        return MaterializedArtifact(
            id=instance_id,
            asset_name=asset_name,
            instance_key=instance_key,
            input_fingerprint=input_fingerprint_value,
            output_location=output_location,
            output_hash=output_hash,
            content_hash=content_hash,
            metadata=metadata_value,
        )

    def update_asset_instance_location(
        self,
        artifact: MaterializedArtifact,
        *,
        output_location: str,
        output_hash: str | None,
    ) -> MaterializedArtifact:
        timestamp = now()
        with self.begin() as conn:
            conn.execute(
                update(asset_instances)
                .where(asset_instances.c.id == artifact.id)
                .values(
                    output_location=output_location,
                    output_hash=output_hash,
                    updated_at=timestamp,
                )
            )
        return artifact.model_copy(
            update={"output_location": output_location, "output_hash": output_hash}
        )

    def write_lineage(self, upstream_id: str, downstream_id: str) -> None:
        with self.begin() as conn:
            self._write_lineage(conn, upstream_id, downstream_id)

    def upstream_artifacts(
        self, downstream_id: str, asset_name: str | None = None
    ) -> list[MaterializedArtifact]:
        upstream = asset_instances.alias("upstream")
        query = (
            select(upstream)
            .select_from(
                lineage_edges.join(
                    upstream,
                    lineage_edges.c.upstream_asset_instance_id == upstream.c.id,
                )
            )
            .where(
                lineage_edges.c.downstream_asset_instance_id == downstream_id,
                upstream.c.project_id == self.context.project_id,
                upstream.c.tenant_id == self.context.tenant_id,
                upstream.c.status == "materialized",
            )
            .order_by(upstream.c.asset_name, upstream.c.instance_key)
        )
        if asset_name is not None:
            query = query.where(upstream.c.asset_name == asset_name)
        with self.engine.connect() as conn:
            rows = conn.execute(query).mappings().all()
        return [
            MaterializedArtifact(
                id=row["id"],
                asset_name=row["asset_name"],
                instance_key=row["instance_key"],
                input_fingerprint=row["input_fingerprint"],
                output_location=row["output_location"],
                output_hash=row["output_hash"],
                content_hash=row["content_hash"],
                metadata=json.loads(row["metadata_json"] or "{}"),
            )
            for row in rows
        ]

    def _write_lineage(self, conn: Connection, upstream_id: str, downstream_id: str) -> None:
        exists = conn.execute(
            select(lineage_edges).where(
                lineage_edges.c.upstream_asset_instance_id == upstream_id,
                lineage_edges.c.downstream_asset_instance_id == downstream_id,
            )
        ).first()
        if not exists:
            conn.execute(
                insert(lineage_edges).values(
                    upstream_asset_instance_id=upstream_id,
                    downstream_asset_instance_id=downstream_id,
                )
            )

    def upsert_source_state(self, source_name: str, item_key: str, content_hash: str) -> None:
        timestamp = now()
        with self.begin() as conn:
            row = conn.execute(
                select(source_state).where(
                    source_state.c.project_id == self.context.project_id,
                    source_state.c.tenant_id == self.context.tenant_id,
                    source_state.c.source_name == source_name,
                    source_state.c.item_key == item_key,
                )
            ).first()
            values = {
                "source_content_hash": content_hash,
                "missing_since": None,
                "last_seen_at": timestamp,
                "deleted_at": None,
            }
            if row:
                conn.execute(
                    update(source_state)
                    .where(
                        source_state.c.project_id == self.context.project_id,
                        source_state.c.tenant_id == self.context.tenant_id,
                        source_state.c.source_name == source_name,
                        source_state.c.item_key == item_key,
                    )
                    .values(**values)
                )
            else:
                conn.execute(
                    insert(source_state).values(
                        project_id=self.context.project_id,
                        tenant_id=self.context.tenant_id,
                        source_name=source_name,
                        item_key=item_key,
                        **values,
                    )
                )

    def mark_source_deleted(self, source_name: str, item_key: str) -> None:
        with self.begin() as conn:
            conn.execute(
                update(source_state)
                .where(
                    source_state.c.project_id == self.context.project_id,
                    source_state.c.tenant_id == self.context.tenant_id,
                    source_state.c.source_name == source_name,
                    source_state.c.item_key == item_key,
                )
                .values(deleted_at=now())
            )

    def mark_lineage_deleted_for_source(self, source_item_key: str) -> int:
        like_prefix = f"{source_item_key}#%"
        with self.begin() as conn:
            result = conn.execute(
                update(asset_instances)
                .where(
                    asset_instances.c.project_id == self.context.project_id,
                    asset_instances.c.tenant_id == self.context.tenant_id,
                    asset_instances.c.status == "materialized",
                    or_(
                        asset_instances.c.instance_key.op("GLOB")(f"{source_item_key}#*"),
                        asset_instances.c.instance_key == source_item_key,
                    ),
                )
                .values(status="deleted", updated_at=now())
            )
            conn.execute(
                delete(vector_sink).where(
                    vector_sink.c.project_id == self.context.project_id,
                    vector_sink.c.tenant_id == self.context.tenant_id,
                    or_(
                        vector_sink.c.instance_key == source_item_key,
                        vector_sink.c.instance_key.like(like_prefix),
                    ),
                )
            )
            return int(result.rowcount or 0)

    def update_source_checkpoint(
        self,
        source_name: str,
        connection_id: str,
        scope_hash_value: str,
        cursor_token: dict[str, Any],
    ) -> None:
        timestamp = now()
        token = json.dumps(cursor_token)
        with self.begin() as conn:
            row = conn.execute(
                select(source_checkpoints).where(
                    source_checkpoints.c.project_id == self.context.project_id,
                    source_checkpoints.c.tenant_id == self.context.tenant_id,
                    source_checkpoints.c.source_name == source_name,
                    source_checkpoints.c.connection_id == connection_id,
                    source_checkpoints.c.scope_hash == scope_hash_value,
                )
            ).first()
            if row:
                conn.execute(
                    update(source_checkpoints)
                    .where(source_checkpoints.c.id == row._mapping["id"])
                    .values(cursor_token=token, status="active", updated_at=timestamp)
                )
            else:
                conn.execute(
                    insert(source_checkpoints).values(
                        id=new_id(),
                        project_id=self.context.project_id,
                        tenant_id=self.context.tenant_id,
                        source_name=source_name,
                        connection_id=connection_id,
                        scope_hash=scope_hash_value,
                        cursor_token=token,
                        cursor_version="local-scan-v1",
                        status="active",
                        updated_at=timestamp,
                        created_at=timestamp,
                    )
                )

    def upsert_vector(
        self,
        *,
        instance_key: str,
        embedding_fingerprint: str,
        source_item_key: str,
        chunk_text: str,
        embedding: list[float],
    ) -> None:
        timestamp = now()
        with self.begin() as conn:
            row = conn.execute(
                select(vector_sink).where(
                    vector_sink.c.project_id == self.context.project_id,
                    vector_sink.c.tenant_id == self.context.tenant_id,
                    vector_sink.c.instance_key == instance_key,
                    vector_sink.c.embedding_fingerprint == embedding_fingerprint,
                )
            ).first()
            values = {
                "source_item_key": source_item_key,
                "chunk_text": chunk_text,
                "embedding_json": json.dumps(embedding),
                "updated_at": timestamp,
            }
            if row:
                conn.execute(
                    update(vector_sink)
                    .where(
                        vector_sink.c.project_id == self.context.project_id,
                        vector_sink.c.tenant_id == self.context.tenant_id,
                        vector_sink.c.instance_key == instance_key,
                        vector_sink.c.embedding_fingerprint == embedding_fingerprint,
                    )
                    .values(**values)
                )
            else:
                conn.execute(
                    insert(vector_sink).values(
                        project_id=self.context.project_id,
                        tenant_id=self.context.tenant_id,
                        instance_key=instance_key,
                        embedding_fingerprint=embedding_fingerprint,
                        **values,
                    )
                )

    def lineage(self, asset_name: str, instance_key: str) -> list[dict[str, Any]]:
        root = self.latest_materialized(asset_name, instance_key)
        if not root:
            return []
        with self.engine.connect() as conn:
            rows = conn.execute(select(asset_instances)).mappings().all()
            by_id = {row["id"]: row for row in rows}
            edges = conn.execute(select(lineage_edges)).mappings().all()
        upstream_by_downstream: dict[str, list[str]] = {}
        for edge in edges:
            upstream_by_downstream.setdefault(edge["downstream_asset_instance_id"], []).append(
                edge["upstream_asset_instance_id"]
            )

        output: list[dict[str, Any]] = []
        seen: set[str] = set()

        def visit(instance_id: str, depth: int) -> None:
            if instance_id in seen:
                return
            seen.add(instance_id)
            row = by_id[instance_id]
            output.append(
                {
                    "depth": depth,
                    "asset_name": row["asset_name"],
                    "instance_key": row["instance_key"],
                    "fingerprint": row["input_fingerprint"],
                    "status": row["status"],
                    "output_location": row["output_location"],
                }
            )
            for upstream in upstream_by_downstream.get(instance_id, []):
                visit(upstream, depth + 1)

        visit(root.id, 0)
        return output

