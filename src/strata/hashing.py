from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

FINGERPRINT_ALGORITHM_VERSION = 1


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def hash_canonical(value: Any) -> str:
    return sha256_text(canonical_json(value))


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def config_hash(config: dict[str, Any] | None) -> str:
    return hash_canonical(config or {})


def scope_hash(config: dict[str, Any]) -> str:
    return hash_canonical(config)


def input_fingerprint(
    *,
    transform_version: str,
    config_hash_value: str,
    determinism: str = "deterministic",
    instance_key: str | None = None,
    content_hash: str | None = None,
    source_content_hash: str | None = None,
    upstream_fingerprints: list[str] | None = None,
) -> str:
    return hash_canonical(
        {
            "algorithm_version": FINGERPRINT_ALGORITHM_VERSION,
            "instance_key": instance_key,
            "content_hash": content_hash,
            "source_content_hash": source_content_hash,
            "upstream_fingerprints": sorted(upstream_fingerprints or []),
            "transform_version": transform_version,
            "config_hash": config_hash_value,
            "determinism": determinism,
        }
    )
