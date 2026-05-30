from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from sqlalchemy import select

from strata.core.models import Manifest, TestSpec
from strata.state.repository import StateRepository
from strata.state.schema import asset_instances

ARTIFACT_URI_PREFIX = "artifact://"


@dataclass(frozen=True)
class TestResult:
    name: str
    asset: str
    test_type: str
    status: str
    message: str


def run_tests(*, manifest: Manifest, repo: StateRepository) -> list[TestResult]:
    return [_run_test(manifest, repo, test) for test in manifest.tests]


def _run_test(manifest: Manifest, repo: StateRepository, test: TestSpec) -> TestResult:
    rows = _materialized_rows(repo, test.asset)
    if test.type == "asset_materialized":
        return _test_asset_materialized(test, rows)
    if test.type == "not_empty":
        return _test_not_empty(manifest, test, rows)
    if test.type == "embedding_dimensions":
        return _test_embedding_dimensions(manifest, test, rows)
    if test.type == "source_item_key_present":
        return _test_source_item_key_present(test, rows)
    return TestResult(
        name=test.name,
        asset=test.asset,
        test_type=test.type,
        status="failed",
        message=f"unknown test type: {test.type}",
    )


def _test_asset_materialized(test: TestSpec, rows: list[dict[str, Any]]) -> TestResult:
    min_count = int(test.config.get("min_count", 1))
    actual = len(rows)
    if actual >= min_count:
        return _passed(test, f"{actual} materialized instance(s)")
    return _failed(test, f"expected at least {min_count} materialized instance(s), found {actual}")


def _test_not_empty(
    manifest: Manifest, test: TestSpec, rows: list[dict[str, Any]]
) -> TestResult:
    if not rows:
        return _failed(test, "no materialized instances")
    empty: list[str] = []
    unreadable: list[str] = []
    for row in rows:
        try:
            data = _artifact_data(manifest, str(row["output_location"]))
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            unreadable.append(f"{row['instance_key']}: {exc}")
            continue
        if data is None or data == "" or data == []:
            empty.append(str(row["instance_key"]))
    if unreadable:
        return _failed(test, f"{len(unreadable)} artifact(s) could not be read")
    if empty:
        return _failed(test, f"{len(empty)} empty artifact(s): {', '.join(empty[:3])}")
    return _passed(test, f"{len(rows)} non-empty artifact(s)")


def _test_embedding_dimensions(
    manifest: Manifest, test: TestSpec, rows: list[dict[str, Any]]
) -> TestResult:
    expected = int(
        test.config.get(
            "dimensions",
            manifest.assets[test.asset].config.get("dimensions", 0),
        )
    )
    if expected <= 0:
        return _failed(test, "expected dimensions must be configured")
    if not rows:
        return _failed(test, "no materialized instances")

    wrong: list[str] = []
    unreadable: list[str] = []
    for row in rows:
        try:
            data = _artifact_data(manifest, str(row["output_location"]))
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            unreadable.append(f"{row['instance_key']}: {exc}")
            continue
        if not isinstance(data, list) or len(data) != expected:
            wrong.append(str(row["instance_key"]))
    if unreadable:
        return _failed(test, f"{len(unreadable)} embedding artifact(s) could not be read")
    if wrong:
        return _failed(
            test,
            f"{len(wrong)} embedding artifact(s) did not have {expected} dimensions",
        )
    return _passed(test, f"{len(rows)} embedding artifact(s) have {expected} dimensions")


def _test_source_item_key_present(test: TestSpec, rows: list[dict[str, Any]]) -> TestResult:
    if not rows:
        return _failed(test, "no materialized instances")
    missing = [
        str(row["instance_key"])
        for row in rows
        if not json.loads(str(row["metadata_json"] or "{}")).get("source_item_key")
    ]
    if missing:
        return _failed(
            test,
            f"{len(missing)} instance(s) missing source_item_key: {', '.join(missing[:3])}",
        )
    return _passed(test, f"{len(rows)} instance(s) include source_item_key")


def _materialized_rows(repo: StateRepository, asset_name: str) -> list[dict[str, Any]]:
    with repo.engine.connect() as conn:
        rows = conn.execute(
            select(asset_instances).where(
                asset_instances.c.project_id == repo.context.project_id,
                asset_instances.c.tenant_id == repo.context.tenant_id,
                asset_instances.c.asset_name == asset_name,
                asset_instances.c.status == "materialized",
            )
        ).mappings().all()
    return [dict(row) for row in rows]


def _artifact_data(manifest: Manifest, location: str) -> Any:
    if location.startswith(ARTIFACT_URI_PREFIX):
        return _fanout_artifact_data(manifest, location)
    if location.startswith("sqlite://"):
        raise ValueError("sink artifact data is not stored as a JSON artifact")
    doc = cast(dict[str, Any], json.loads(Path(location).read_text(encoding="utf-8")))
    return doc["data"]


def _fanout_artifact_data(manifest: Manifest, location: str) -> Any:
    uri = location.removeprefix(ARTIFACT_URI_PREFIX)
    if "#item=" not in uri:
        raise ValueError(f"invalid artifact URI: {location}")
    manifest_ref, item_ref = uri.split("#item=", 1)
    item_number = int(item_ref)
    manifest_path = manifest.artifacts_path / manifest_ref
    manifest_doc = cast(
        dict[str, Any], json.loads(manifest_path.read_text(encoding="utf-8"))
    )
    item = cast(dict[str, Any], manifest_doc["items"][item_number])
    payload = cast(dict[str, Any], item["payload"])
    payload_path = manifest_path.parent.parent / str(payload["path"])
    record_number = int(payload["record"])
    record = json.loads(payload_path.read_text(encoding="utf-8").splitlines()[record_number])
    return record["data"]


def _passed(test: TestSpec, message: str) -> TestResult:
    return TestResult(
        name=test.name,
        asset=test.asset,
        test_type=test.type,
        status="passed",
        message=message,
    )


def _failed(test: TestSpec, message: str) -> TestResult:
    return TestResult(
        name=test.name,
        asset=test.asset,
        test_type=test.type,
        status="failed",
        message=message,
    )
