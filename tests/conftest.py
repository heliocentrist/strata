from __future__ import annotations

from pathlib import Path


def write_project(root: Path, *, max_chars: int = 40) -> Path:
    docs = root / "docs"
    docs.mkdir()
    (docs / "a.md").write_text(
        "Alpha document for Strata.\nThis file has enough text to create chunks.\n",
        encoding="utf-8",
    )
    project = root / "strata.yml"
    project.write_text(
        f"""
project_id: default
tenant_id: default

state:
  url: sqlite:///./.strata/state.db

artifacts:
  path: ./.strata/artifacts

sources:
  docs:
    type: local_files
    path: ./docs
    include: ["**/*.txt", "**/*.md", "**/*.pdf"]

pipeline:
  parsed:
    source: docs
    operation: markdown_noop
    version: markdown_noop@0.1.0

  chunks:
    input: parsed
    operation: fixed_token_chunker
    version: fixed_token_chunker@0.1.0
    config:
      output_label: chunk
      max_chars: {max_chars}
      overlap_chars: 5

  embeddings:
    input: chunks
    operation: fake_embedding
    version: fake_embedding@0.1.0
    config:
      dimensions: 8

  sink:
    input: embeddings
    operation: local_sqlite_vector_sink
    version: sink@0.1.0

tests:
  - name: chunks_not_empty
    asset: chunks
    type: not_empty

  - name: embeddings_have_expected_dimensions
    asset: embeddings
    type: embedding_dimensions

  - name: chunks_keep_source_identity
    asset: chunks
    type: source_item_key_present
""",
        encoding="utf-8",
    )
    return project
