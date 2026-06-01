from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from strata.core.collections import ArtifactPayload
from strata.core.models import MaterializedArtifact, SourceItem
from strata.core.operations import OperationInput
from strata.executors.protocols import OperationInvocation


@dataclass(frozen=True)
class InputGroup:
    item_key: str
    default_instance_key: str
    inputs: list[OperationInput]
    upstreams: list[MaterializedArtifact]
    operation_id: str
    operation_run_id: str
    partition_key: str | None = None
    source_name: str | None = None
    source_item: SourceItem | None = None


@dataclass(frozen=True)
class PendingArtifactOutput:
    op_item_id: str
    instance_key: str
    input_fingerprint: str
    scope_fingerprint: str
    content_hash: str | None
    metadata: dict[str, Any]
    payload: ArtifactPayload
    group: InputGroup


@dataclass(frozen=True)
class PendingExternalOutput:
    op_item_id: str
    instance_key: str
    input_fingerprint: str
    scope_fingerprint: str
    output_location: str
    output_hash: str | None
    content_hash: str | None
    metadata: dict[str, Any]
    data: Any
    group: InputGroup


@dataclass(frozen=True)
class PendingOutputBatch:
    artifacts: list[PendingArtifactOutput]
    external: list[PendingExternalOutput]


@dataclass(frozen=True)
class InputBatch:
    groups: list[InputGroup]
    inputs: list[OperationInput]


@dataclass(frozen=True)
class InvocationWindowItem:
    invocation: OperationInvocation
    input_batch: InputBatch
