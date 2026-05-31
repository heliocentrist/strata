# Strata

Strata is a content build system for RAG, knowledge-base, and agent-memory pipelines.
It tracks source changes, computes stable fingerprints, records lineage, caches
materialized artifacts, and rebuilds only the parts of a pipeline affected by a source,
configuration, or transform change.

This repository is an early prototype. The current implementation is focused on local
development and validation: local files or object-store-like folders as sources, SQLite
state, JSON/JSONL artifact storage, pluggable operations, local runners, and a small CLI.

## What It Does

Strata lets you define a pipeline such as:

```yaml
sources:
  docs:
    type: local_files
    path: ./docs
    include: ["**/*.md", "**/*.txt", "**/*.pdf"]

pipeline:
  parsed:
    source: docs
    operation: liteparse
    version: liteparse@0.1.0

  chunks:
    input: parsed
    operation: fixed_token_chunker
    version: fixed_token_chunker@0.1.0

  embeddings:
    input: chunks
    operation: fake_embedding
    version: fake_embedding@0.1.0

  sink:
    input: embeddings
    operation: local_sqlite_vector_sink
    version: local_sqlite_vector_sink@0.1.0
```

Then you can inspect what would run, apply the changes, and browse lineage/state.

## Quick Start

Install dependencies with `uv`:

```bash
uv sync --extra dev
```

Run the included demo:

```bash
uv run strata compile -p demo/strata.yml
uv run strata plan -p demo/strata.yml
uv run strata apply -p demo/strata.yml
uv run strata test -p demo/strata.yml
```

Useful CLI commands:

```bash
uv run strata inspect -p demo/strata.yml --asset embeddings --instance-key <key>
uv run strata lineage -p demo/strata.yml --asset embeddings --instance-key <key>
uv run strata doctor -p demo/strata.yml
uv run strata docs serve -p demo/strata.yml
```

## Development

Run the test suite and checks:

```bash
uv run --extra dev pytest
uv run --extra dev ruff check .
uv run --extra dev mypy src
```

## Status

Strata is not production-ready yet. The core APIs, storage layout, plugin contracts, and
runner model are still evolving as the first real integration use cases are worked
through. The goal is to keep the core small and make sources, operations, artifact
collections, sinks, and execution runners pluggable.

