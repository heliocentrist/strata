from __future__ import annotations

from strata.core.hashing import config_hash, input_fingerprint


def test_config_hash_is_canonical() -> None:
    assert config_hash({"b": 2, "a": 1}) == config_hash({"a": 1, "b": 2})


def test_upstream_fingerprints_are_sorted() -> None:
    left = input_fingerprint(
        transform_version="x@1",
        config_hash_value=config_hash({}),
        upstream_fingerprints=["b", "a"],
    )
    right = input_fingerprint(
        transform_version="x@1",
        config_hash_value=config_hash({}),
        upstream_fingerprints=["a", "b"],
    )
    assert left == right


def test_transform_version_changes_fingerprint() -> None:
    first = input_fingerprint(
        transform_version="x@1",
        config_hash_value=config_hash({}),
        source_content_hash="abc",
    )
    second = input_fingerprint(
        transform_version="x@2",
        config_hash_value=config_hash({}),
        source_content_hash="abc",
    )
    assert first != second
