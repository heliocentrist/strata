# Strata

Strata is a prototype content build system for RAG, knowledge-base, and agent-memory
pipelines. It tracks source changes, computes stable fingerprints, records lineage,
caches materialized artifacts, and rebuilds only the affected parts of a pipeline.

The current release is aimed at local development and integration experiments. It uses
SQLite for state, local JSON/JSONL artifact collections, pluggable source/operation/sink
adapters, and local execution runners.

## What It Looks Like

```yaml
sources:
  docs:
    type: local_files
    path: ./docs
    include: ["**/*.md", "**/*.txt"]

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

## Quick Start

Install with `uv`:

```bash
uv sync --extra dev
```

Run the local files example:

```bash
uv run strata compile -p examples/pipelines/local_files/strata.yml
uv run strata plan -p examples/pipelines/local_files/strata.yml
uv run strata apply -p examples/pipelines/local_files/strata.yml
uv run strata test -p examples/pipelines/local_files/strata.yml
```

Try other examples:

```bash
uv run strata apply -p examples/pipelines/object_store/strata.yml
uv run strata apply -p examples/pipelines/threaded_runner/strata.yml
```

Useful commands:

```bash
uv run strata inspect -p examples/pipelines/local_files/strata.yml --asset chunks --instance-key alpha.md#chunk:0000
uv run strata lineage -p examples/pipelines/local_files/strata.yml --asset chunks --instance-key alpha.md#chunk:0000
uv run strata doctor -p examples/pipelines/local_files/strata.yml
uv run strata docs serve -p examples/pipelines/local_files/strata.yml
```

## Project Layout

- `src/strata/core`: manifests, models, hashing, selectors, and planning primitives.
- `src/strata/execution`: apply orchestration, input binding, windows, commits, and deletes.
- `src/strata/plugins`: plugin registry and built-in operation/collection adapters.
- `src/strata/sources`: built-in source adapters.
- `src/strata/executors`: local runner implementations.
- `src/strata/tools`: inspect, doctor, docs, and test helpers.
- `examples`: runnable pipeline and host-application examples.
- `docs`: architecture notes and implementation plan.

## Development

Run checks:

```bash
uv run --extra dev pytest
uv run --extra dev ruff check .
uv run --extra dev mypy src tests
```

## Status

Strata is not production-ready. The core contracts are still evolving around durable
execution, artifact storage, external sinks, and plugin discovery. The immediate goal is
to keep the core small while making sources, operations, artifact collections, sinks, and
execution runners pluggable.

## License

MIT
