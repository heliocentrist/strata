# Threaded Runner Pipeline

This example uses the same local file pipeline shape, but configures the local threaded
runner.

```bash
uv run strata apply -p examples/pipelines/threaded_runner/strata.yml
```

The threaded runner executes invocations inside a window with a local thread pool. It is
still a local runner, not a distributed executor.
