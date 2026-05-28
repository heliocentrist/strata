from __future__ import annotations

from pathlib import Path

import pytest
from conftest import write_project

from strata.config import load_manifest
from strata.selectors import parse_selection


def test_asset_selector_shapes(tmp_path: Path) -> None:
    manifest = load_manifest(write_project(tmp_path))

    assert parse_selection(manifest, "chunks").assets == {"chunks"}
    assert parse_selection(manifest, "chunks+").assets == {"chunks", "embeddings", "sink"}
    assert parse_selection(manifest, "+embeddings").assets == {"parsed", "chunks", "embeddings"}
    assert parse_selection(manifest, "+chunks+").assets == {
        "parsed",
        "chunks",
        "embeddings",
        "sink",
    }


def test_source_selector(tmp_path: Path) -> None:
    manifest = load_manifest(write_project(tmp_path))

    selection = parse_selection(manifest, "source:docs+")
    assert selection.assets == {"parsed", "chunks", "embeddings", "sink"}
    assert selection.source_names == {"docs"}


def test_invalid_selector(tmp_path: Path) -> None:
    manifest = load_manifest(write_project(tmp_path))

    with pytest.raises(ValueError, match="unknown asset selector"):
        parse_selection(manifest, "missing+")

    with pytest.raises(ValueError, match="unknown source selector"):
        parse_selection(manifest, "source:missing+")
