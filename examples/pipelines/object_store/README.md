# Object Store Pipeline

This example uses the `object_store` source adapter against a local folder. It behaves
like a small object-store-backed source while remaining runnable on a laptop.

```bash
uv run strata plan -p examples/pipelines/object_store/strata.yml
uv run strata apply -p examples/pipelines/object_store/strata.yml
uv run strata doctor -p examples/pipelines/object_store/strata.yml
```

Modify or delete files under `objects/` and rerun `plan` to see delta detection.
