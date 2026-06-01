from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from sqlalchemy import delete, select, update

from strata.core.hashing import hash_canonical, sha256_text
from strata.core.models import Manifest
from strata.state.repository import StateRepository, now
from strata.state.schema import (
    apply_locks,
    asset_instances,
    operation_items,
    operation_runs,
    runs,
)

ARTIFACT_URI_PREFIX = "artifact://"


@dataclass(frozen=True)
class DoctorIssue:
    code: str
    severity: str
    message: str
    fixable: bool = False
    path: str | None = None


@dataclass(frozen=True)
class DoctorResult:
    issues: list[DoctorIssue]
    fixed: int = 0


def run_doctor(*, manifest: Manifest, repo: StateRepository, fix: bool = False) -> DoctorResult:
    issues: list[DoctorIssue] = []
    fixed = 0

    issues.extend(_lock_issues(repo))
    issues.extend(_running_run_issues(repo))
    artifact_issues, expected_files = _artifact_issues(manifest, repo)
    issues.extend(artifact_issues)
    issues.extend(_orphan_file_issues(manifest, expected_files))

    if fix:
        fixed += _fix_apply_locks(repo)
        fixed += _fix_abandoned_runs(repo)
        fixed += _fix_broken_asset_instances(repo, artifact_issues)
        fixed += _fix_orphan_files(manifest, issues)

    return DoctorResult(issues=issues, fixed=fixed)


def _lock_issues(repo: StateRepository) -> list[DoctorIssue]:
    issues: list[DoctorIssue] = []
    for row in _context_rows(repo, apply_locks):
        acquired_at = row["acquired_at"]
        acquired = (
            acquired_at.isoformat() if isinstance(acquired_at, datetime) else str(acquired_at)
        )
        issues.append(
            DoctorIssue(
                code="apply_lock",
                severity="warning",
                message=f"Apply lock is present for run {row['run_id']} since {acquired}",
                fixable=True,
            )
        )
    return issues


def _running_run_issues(repo: StateRepository) -> list[DoctorIssue]:
    lock_rows = _context_rows(repo, apply_locks)
    active_lock_run_ids = {str(row["run_id"]) for row in lock_rows}
    issues: list[DoctorIssue] = []
    for row in _context_rows(repo, runs):
        if row["status"] != "running":
            continue
        if str(row["id"]) in active_lock_run_ids:
            issues.append(
                DoctorIssue(
                    code="active_run",
                    severity="warning",
                    message=f"Run {row['id']} is still marked running and has an active lock",
                )
            )
        else:
            issues.append(
                DoctorIssue(
                    code="abandoned_run",
                    severity="error",
                    message=f"Run {row['id']} is marked running without an active lock",
                    fixable=True,
                )
            )
    return issues


def _artifact_issues(
    manifest: Manifest, repo: StateRepository
) -> tuple[list[DoctorIssue], set[Path]]:
    issues: list[DoctorIssue] = []
    expected_files: set[Path] = set()
    for row in _context_rows(repo, asset_instances):
        if row["status"] != "materialized":
            continue
        location = row["output_location"]
        if not location or str(location).startswith("sqlite://"):
            continue
        if str(location).startswith(ARTIFACT_URI_PREFIX):
            issues.extend(_artifact_uri_issues(manifest, row, expected_files))
        else:
            path = Path(str(location))
            expected_files.add(path)
            if not path.exists():
                issues.append(_broken_artifact_issue(row, f"artifact file is missing: {path}"))
    return issues, expected_files


def _artifact_uri_issues(
    manifest: Manifest, row: dict[str, Any], expected_files: set[Path]
) -> list[DoctorIssue]:
    location = str(row["output_location"])
    issues: list[DoctorIssue] = []
    parsed = _parse_artifact_uri(location)
    if parsed is None:
        return [_broken_artifact_issue(row, f"invalid artifact URI: {location}")]

    manifest_ref, item_index = parsed
    manifest_path = manifest.artifacts_path / manifest_ref
    expected_files.add(manifest_path)

    if not manifest_path.exists():
        return [_broken_artifact_issue(row, f"fanout manifest is missing: {manifest_path}")]

    try:
        manifest_doc = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [_broken_artifact_issue(row, f"artifact manifest cannot be read: {exc}")]
    issues.extend(_manifest_hash_issues(row, manifest_path, manifest_doc))

    item = _manifest_item_for_row(manifest_doc, row, item_index)
    if item is None:
        return [
            _broken_artifact_issue(
                row,
                "manifest item is missing for "
                f"{row['asset_name']}/{row['instance_key']} item {item_index}",
            )
        ]

    payload = cast(dict[str, Any], item.get("payload", {}))
    payload_path = manifest_path.parent.parent / str(payload.get("path", ""))
    expected_files.add(payload_path)
    if not payload_path.exists():
        return [_broken_artifact_issue(row, f"payload file is missing: {payload_path}")]
    try:
        payload_text = payload_path.read_text(encoding="utf-8")
        lines = payload_text.splitlines()
    except OSError as exc:
        return [_broken_artifact_issue(row, f"artifact payload cannot be read: {exc}")]
    if payload.get("file_hash") != sha256_text(payload_text):
        issues.append(_broken_artifact_issue(row, f"payload file hash mismatch: {payload_path}"))

    record = int(payload.get("record", -1))
    if record < 0 or record >= len(lines):
        return [_broken_artifact_issue(row, f"payload record {record} is missing: {payload_path}")]
    try:
        record_doc = json.loads(lines[record])
    except json.JSONDecodeError as exc:
        return [_broken_artifact_issue(row, f"payload record {record} is invalid: {exc}")]
    issues.extend(_content_hash_issues(row, item, record_doc))
    return issues


def _manifest_hash_issues(
    row: dict[str, Any], manifest_path: Path, manifest_doc: dict[str, Any]
) -> list[DoctorIssue]:
    manifest_hash = manifest_doc.get("manifest_hash")
    if not isinstance(manifest_hash, str) or not manifest_hash:
        return [
            _broken_artifact_issue(
                row, f"fanout manifest has no manifest_hash: {manifest_path}"
            )
        ]
    hash_payload = dict(manifest_doc)
    hash_payload.pop("manifest_hash", None)
    actual_hash = hash_canonical(hash_payload)
    if actual_hash != manifest_hash:
        return [_broken_artifact_issue(row, f"fanout manifest hash mismatch: {manifest_path}")]
    window_id = manifest_doc.get("window_id")
    if not isinstance(window_id, str) or not window_id:
        return [
            _broken_artifact_issue(
                row, f"fanout manifest has no window_id: {manifest_path}"
            )
        ]
    if manifest_path.stem != window_id:
        return [
            _broken_artifact_issue(
                row, f"fanout manifest filename does not match window_id: {manifest_path}"
            )
        ]
    return []


def _content_hash_issues(
    row: dict[str, Any], item: dict[str, Any], record_doc: dict[str, Any]
) -> list[DoctorIssue]:
    expected = item.get("content_hash")
    if not isinstance(expected, str) or "data" not in record_doc:
        return []
    data = record_doc["data"]
    if row["asset_name"] == "chunks":
        actual = sha256_text(str(data))
    elif row["asset_name"] == "embeddings":
        actual = hash_canonical(data)
    else:
        return []
    if actual != expected:
        return [
            _broken_artifact_issue(
                row,
                f"content hash mismatch for {row['asset_name']}/{row['instance_key']}",
            )
        ]
    return []


def _manifest_item_for_row(
    manifest_doc: dict[str, Any], row: dict[str, Any], item_index: int
) -> dict[str, Any] | None:
    items = manifest_doc.get("items", [])
    if not isinstance(items, list) or item_index >= len(items):
        return None
    item = items[item_index]
    if not isinstance(item, dict):
        return None
    if (
        item.get("instance_key") == row["instance_key"]
        and item.get("input_fingerprint") == row["input_fingerprint"]
    ):
        return cast(dict[str, Any], item)
    return None


def _orphan_file_issues(manifest: Manifest, expected_files: set[Path]) -> list[DoctorIssue]:
    if not manifest.artifacts_path.exists():
        return []
    normalized_expected = {path.resolve() for path in expected_files}
    issues: list[DoctorIssue] = []
    for path in manifest.artifacts_path.rglob("*"):
        if not path.is_file():
            continue
        resolved = path.resolve()
        if resolved not in normalized_expected:
            issues.append(
                DoctorIssue(
                    code="orphan_file",
                    severity="warning",
                    message=f"Artifact file is not referenced by materialized state: {path}",
                    fixable=True,
                    path=str(path),
                )
            )
    return issues


def _fix_apply_locks(repo: StateRepository) -> int:
    with repo.begin() as conn:
        result = conn.execute(
            delete(apply_locks).where(
                apply_locks.c.project_id == repo.context.project_id,
                apply_locks.c.tenant_id == repo.context.tenant_id,
            )
        )
    return int(result.rowcount or 0)


def _fix_abandoned_runs(repo: StateRepository) -> int:
    timestamp = now()
    lock_rows = _context_rows(repo, apply_locks)
    active_lock_run_ids = {str(row["run_id"]) for row in lock_rows}
    stale_run_ids = [
        str(row["id"])
        for row in _context_rows(repo, runs)
        if row["status"] == "running" and str(row["id"]) not in active_lock_run_ids
    ]
    if not stale_run_ids:
        return 0

    with repo.begin() as conn:
        conn.execute(
            update(runs)
            .where(runs.c.id.in_(stale_run_ids))
            .values(status="failed", finished_at=timestamp)
        )
        conn.execute(
            update(operation_runs)
            .where(
                operation_runs.c.run_id.in_(stale_run_ids),
                operation_runs.c.status.in_(["pending", "running"]),
            )
            .values(status="failed", finished_at=timestamp, error="interrupted run")
        )
        conn.execute(
            update(operation_items)
            .where(
                operation_items.c.run_id.in_(stale_run_ids),
                operation_items.c.status.in_(["pending", "running"]),
            )
            .values(status="failed", updated_at=timestamp, error="interrupted run")
        )
    return len(stale_run_ids)


def _fix_broken_asset_instances(
    repo: StateRepository, artifact_issues: list[DoctorIssue]
) -> int:
    fingerprints = _broken_fingerprints(artifact_issues)
    if not fingerprints:
        return 0
    timestamp = now()
    fixed = 0
    with repo.begin() as conn:
        for asset_name, instance_key, input_fingerprint in fingerprints:
            result = conn.execute(
                update(asset_instances)
                .where(
                    asset_instances.c.project_id == repo.context.project_id,
                    asset_instances.c.tenant_id == repo.context.tenant_id,
                    asset_instances.c.asset_name == asset_name,
                    asset_instances.c.instance_key == instance_key,
                    asset_instances.c.input_fingerprint == input_fingerprint,
                    asset_instances.c.status == "materialized",
                )
                .values(
                    status="failed",
                    error="doctor: artifact target missing or invalid",
                    updated_at=timestamp,
                )
            )
            fixed += int(result.rowcount or 0)
    return fixed


def _fix_orphan_files(manifest: Manifest, issues: list[DoctorIssue]) -> int:
    fixed = 0
    for issue in issues:
        if issue.code not in {"orphan_file", "staging_dir"} or not issue.path:
            continue
        path = Path(issue.path)
        if not _is_inside(path, manifest.artifacts_path) or not path.exists():
            continue
        if path.is_file():
            path.unlink()
            fixed += 1
    fixed += _remove_empty_dirs(manifest.artifacts_path)
    return fixed


def _remove_empty_dirs(root: Path) -> int:
    if not root.exists():
        return 0
    fixed = 0
    for path in sorted((path for path in root.rglob("*") if path.is_dir()), reverse=True):
        try:
            path.rmdir()
            fixed += 1
        except OSError:
            pass
    return fixed


def _broken_artifact_issue(row: dict[str, Any], message: str) -> DoctorIssue:
    return DoctorIssue(
        code="broken_artifact",
        severity="error",
        message=message,
        fixable=True,
        path=f"{row['asset_name']}|{row['instance_key']}|{row['input_fingerprint']}",
    )


def _broken_fingerprints(issues: list[DoctorIssue]) -> set[tuple[str, str, str]]:
    output: set[tuple[str, str, str]] = set()
    for issue in issues:
        if issue.code != "broken_artifact" or not issue.path:
            continue
        asset_name, instance_key, input_fingerprint = issue.path.split("|", 2)
        output.add((asset_name, instance_key, input_fingerprint))
    return output


def _parse_artifact_uri(location: str) -> tuple[Path, int] | None:
    uri = location.removeprefix(ARTIFACT_URI_PREFIX)
    if "#item=" not in uri:
        return None
    manifest_ref, item_ref = uri.split("#item=", 1)
    try:
        item = int(item_ref)
    except ValueError:
        return None
    return Path(manifest_ref), item


def _context_rows(repo: StateRepository, table: Any) -> list[dict[str, Any]]:
    with repo.engine.connect() as conn:
        rows = conn.execute(
            select(table).where(
                table.c.project_id == repo.context.project_id,
                table.c.tenant_id == repo.context.tenant_id,
            )
        ).mappings().all()
    return [dict(row) for row in rows]


def _is_inside(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True
