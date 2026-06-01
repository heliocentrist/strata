# Local Files Pipeline

This example indexes markdown and text files from a local `docs/` folder.

```bash
uv run strata compile -p examples/pipelines/local_files/strata.yml
uv run strata plan -p examples/pipelines/local_files/strata.yml
uv run strata apply -p examples/pipelines/local_files/strata.yml
uv run strata test -p examples/pipelines/local_files/strata.yml
```

The run state and artifacts are written under `examples/pipelines/local_files/.strata/`.
