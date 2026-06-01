# Strata Architecture Decision Log

Status: living document  
Last updated: 2026-05-31

This file records the major architectural decisions made so far and the reasoning
behind them. It is intentionally shorter than the implementation plan. Full design
docs can grow elsewhere later.

## ADR-001: Strata Is A Content Build System, Not A Durable Orchestrator

Status: Accepted

Decision:
Strata owns content pipeline semantics: planning, fingerprints, cache checks, artifact
catalog commits, lineage, source state, progress, and deletes. It does not try to
replace Temporal, Airflow, Dagster, or other durable execution engines.

Grounding:
The core use case is RAG and knowledge-base preparation, where the missing capability is
not scheduling but knowing exactly what changed, why an artifact exists, and what must be
rebuilt. We want Strata to work inside existing host systems rather than force teams to
adopt another orchestrator.

Consequences:
- Execution substrate is pluggable through runners.
- Temporal is a future runner, not the center of the core design.
- Strata must keep apply semantics deterministic and portable across runners.

## ADR-002: Plan Before Apply

Status: Accepted

Decision:
`strata plan` computes what would change without mutating state. `strata apply` builds
from that same model and records execution state.

Grounding:
The system may eventually call paid APIs, parse large documents, and write production
indexes. Users need a quick, honest preview of scope before work starts.

Consequences:
- Planning is allowed to be coarse for fanout assets because exact child instances may
not be knowable before execution.
- Plan output must distinguish known counts from unknown fanout.
- Apply is responsible for expanding coarse operations into exact instances.

## ADR-003: The Planner Is Pure

Status: Accepted

Decision:
The planner is a pure function over a compiled manifest, current state snapshot, source
snapshot, and selector. It does not enumerate sources, read artifacts, call plugins, or
write state.

Grounding:
Keeping planning pure makes it testable, fast, and explainable. Source enumeration and
state reads are impure setup steps outside the planner.

Consequences:
- Source adapters produce `SourceSnapshot` before planning.
- The planner can be regression-tested with in-memory fixtures.
- Fanout exactness remains an apply-time concern.

## ADR-004: The Manifest Is The Runtime Contract

Status: Accepted

Decision:
The YAML project compiles to a `Manifest` containing project identity, sources, assets,
operation names, versions, config, artifact strategy, execution settings, and graph
order. Runtime code operates on the manifest, not raw YAML.

Grounding:
We need one stable contract shared by CLI, library users, planner, apply runtime, tests,
inspect, and docs.

Consequences:
- YAML is the only project format for now.
- Python decorators and richer project authoring can compile to the same manifest later.
- Manifest hashing gives runs a stable description of what was executed.

## ADR-005: Project And Tenant Are First-Class State Dimensions

Status: Accepted

Decision:
All state is scoped by `project_id` and `tenant_id`. A tenant can have multiple projects.

Grounding:
The expected real-world workload is SaaS knowledge-base ingest. Even in local prototype
mode, the data model should not assume one global project.

Consequences:
- Tables include project and tenant identifiers.
- Locks are project/tenant scoped.
- Future production state backends can use these columns for isolation and indexing.

## ADR-006: Discoverable Sources Are The Default UX

Status: Accepted

Decision:
Users should be able to point Strata at a local folder or object-store-like folder and
let Strata enumerate items, compute source content hashes, and detect full-load and
delta behavior. Source manifests remain available for host-staged integrations.

Grounding:
Requiring users to provide a source manifest hurts usability. The simple path should be
"point at documents and run plan/apply." Host systems can still stage a manifest when
the original source cannot be listed directly.

Consequences:
- Source adapters are pluggable.
- Current built-ins include local files, object manifests, and local object-store-like
  folders.
- Large production sources will need streaming or paged enumeration later.

## ADR-007: Source State And Source Checkpoints Are Separate

Status: Accepted

Decision:
`source_state` stores per-item facts: item key, source content hash, last seen time, and
delete status. `source_checkpoints` stores connector-level cursors for a source scope.

Grounding:
Per-item change detection and connector cursor advancement solve different problems.
For example, an S3/listable source can compare items, while SharePoint-style APIs may
also provide a delta token.

Consequences:
- Delta behavior can be based on item hashes even when a checkpoint is absent.
- Checkpoint commit semantics need reliability hardening: cursors should not advance
  after failed processing.

## ADR-008: Stable Instance Keys, Content Hashes Separately

Status: Accepted

Decision:
Asset instances use stable logical keys such as source item keys and derived chunk keys.
`content_hash` is stored separately for output reuse/debugging. We do not optimize
identity around embedding reuse yet.

Grounding:
Stable keys are easier to inspect, delete, and reason about. Content-addressed reuse
can still be added with separate output hashes later.

Consequences:
- Instance keys remain human-readable.
- Fingerprints still include transform version, config hash, determinism, source hash,
  and upstream fingerprints.
- Cross-source or cross-project output deduplication is deferred.

## ADR-009: Database Is The Catalog And Source Of Truth

Status: Accepted

Decision:
The state DB is the authoritative catalog for runs, operations, source state,
materialized asset instances, and lineage. Artifact files store payloads; they are not
the source of truth for what is committed.

Grounding:
We need reliable queries for plan/apply/inspect/docs without scanning the artifact
store. This mirrors table-format/catalog thinking: files are data, catalog commits make
them live.

Consequences:
- Orphan files are possible after crashes and should be handled by `doctor`.
- Lineage queries are DB-backed.
- Existing demo DBs may need to be dropped when schema changes until migrations exist.

## ADR-010: Artifacts Are Written As Immutable Payloads Plus Manifests

Status: Accepted, storage format provisional

Decision:
The current local artifact collection writes immutable JSON/JSONL payload files and
manifest files. The DB stores output locations pointing to specific artifact records.

Grounding:
One file per chunk or embedding creates a small-files problem. Immutable payload files
plus manifests are closer to an Iceberg-style model without committing to a full table
format yet.

Consequences:
- Current local implementation is good enough for prototype testing.
- Reads are currently inefficient for individual records and need batched/indexed
  materialization before large datasets.
- Parquet or another existing table/storage format remains a future candidate.

## ADR-011: Artifact Collections Are Pluggable

Status: Accepted, implementation incomplete

Decision:
Physical artifact layout is behind an `ArtifactCollection` interface. The core should
not hard-code JSON files, Parquet files, object-store paths, or compaction behavior.

Grounding:
Different deployments will want different storage strategies: one file per item for
debugging, JSONL bundles for prototype fanout, Parquet for production-scale assets, or
external stores for sinks.

Consequences:
- Built-in `local_json` is only one implementation.
- Reads must dispatch through the correct collection type. The current read path still
  assumes local JSON in places and should be fixed.

## ADR-012: Operation Plugins Use One Many-To-Many Shape

Status: Accepted

Decision:
Each asset names one operation plugin. A plugin receives `list[OperationInput]` and
returns `list[OperationOutput]`. One-to-one, one-to-many, many-to-one, and many-to-many
work all use this shape.

Grounding:
Special parser/chunker/embedder/sink interfaces made the core too domain-specific.
The core should not know that an asset is "chunks" or "embeddings"; it should only bind
inputs, run an operation, and commit outputs.

Consequences:
- Outputs can include `parent_input_ids` so batched calls still produce correct lineage.
- Plugin authors have one mental model.
- Join/grouping semantics are still immature and should be revisited before adding
  complex multi-input operations.

## ADR-013: Pipeline Assets Currently Have One Upstream Input

Status: Provisional

Decision:
The current manifest model allows each asset to declare either a source or a single
upstream asset input.

Grounding:
This keeps the current core simple while we validate generic operation execution,
fanout, lineage, and source-scoped rebuild behavior. Hybrid search sink behavior is
modeled by carrying chunk text and metadata forward through embeddings, then having the
sink consume the enriched embedding asset.

Consequences:
- Complex joins are deferred.
- This may need to change once we have a clearer grouping/join contract.
- The operation plugin shape can support many inputs, but manifest-level DAG semantics
  are currently narrower.

## ADR-014: Apply Semantics Belong To The Runtime, Not The Runner

Status: Accepted

Decision:
The Strata runtime owns dependency layers, input binding, cache checks, fingerprinting,
state commits, lineage, source state, checkpoints, and delete semantics. Runners only
decide where bounded windows of plugin invocations execute.

Grounding:
Local, threaded, and future Temporal execution must share the same cache and lineage
semantics. If each runner implements apply semantics, behavior will diverge.

Consequences:
- Built-in runners are `local_single_thread` and `local_threaded`.
- Temporal should implement the runner boundary, not a separate apply engine.
- The current runner/artifact-write boundary is still being refined.

## ADR-015: Execute By Asset Layers, Then Windows

Status: Accepted

Decision:
Apply executes one asset layer at a time in manifest topological order. Within a layer,
the runtime groups input batches and forms runner windows. `inputs_per_call` controls
how many operation inputs a plugin call can receive; `window_size` controls how many
invocations are handed to the runner before results are committed.

Grounding:
This maps cleanly to local execution and future Temporal activity scheduling while
preserving commit points between assets.

Consequences:
- Large embedding workloads can be batched at plugin-call and runner-window levels.
- Future streaming apply should avoid collecting all input groups into memory.
- Layer fusing is deferred as an optimization.

## ADR-016: Source Identity Is Stored On Asset Instances

Status: Accepted

Decision:
Every materialized asset instance should carry `source_name` and `source_item_key` when
it derives from a source item.

Grounding:
Downstream rebuilds and deletes need to find all artifacts for one source scope without
scanning the whole lineage graph. This also prevents ambiguity when multiple sources use
the same item key.

Consequences:
- Normal apply lookup can use source-scoped lineage from the latest root artifact.
- Delete handling should move fully to source-scoped deletes; prefix-based deletes are
  a known remaining reliability issue.

## ADR-017: Sinks Are Operations, But Commit Semantics Need Hardening

Status: Provisional

Decision:
Sinks are currently modeled as operation plugins that consume an upstream asset and
return output descriptors. The local SQLite vector sink writes sink rows itself.

Grounding:
This made the pipeline fully generic quickly: sink is just another asset operation.
It also allowed hybrid search indexing to consume enriched embedding records in one
linear pipeline.

Consequences:
- Sink writes must be idempotent.
- Current sink writes can happen before Strata commits asset state, so crash semantics
  need improvement before external systems like Elasticsearch.
- Future design should likely separate sink output intents from sink commit.

## ADR-018: Selectors Are Dbt-Inspired

Status: Accepted

Decision:
Selectors operate over asset/source graph concepts, with forms like `chunks`,
`chunks+`, `+embeddings`, `+chunks+`, and `source:docs+`.

Grounding:
Target users need to rebuild/debug slices of the DAG without writing custom scripts.
The dbt selector model is a familiar precedent.

Consequences:
- Selector behavior belongs in core, not plugins.
- More selector syntax can be added later, but the first version stays minimal.

## ADR-019: CLI And Library Are Both First-Class

Status: Accepted

Decision:
Strata should work as a CLI for local development and inspection, and as a library that
host applications can call from their own ingest workflows.

Grounding:
The first real integration direction is a host app that downloads/stages source content
and calls Strata as part of its ingest logic. At the same time, developers need a CLI to
inspect a project created by that host app.

Consequences:
- CLI commands are thin wrappers over importable functions.
- Project/state identity must be explicit enough for external host apps.
- Host test fixtures are part of the regression strategy.

## ADR-020: External Plugins Are A Later Extension Point

Status: Accepted

Decision:
Built-in plugins cover the prototype, but external plugin discovery is part of the
architecture. Sources, operations, artifact collections, and runners should be
registered behind narrow interfaces.

Grounding:
Production use will need real parsers, embedding providers, object stores, vector/search
stores, and durable runners. These should not bloat core.

Consequences:
- Plugin API versioning matters.
- Config schemas and dependency reporting should become more explicit.
- External discovery exists but should be hardened before open-source plugin authorship.

## ADR-021: Reliability Hardening Comes Before Temporal

Status: Accepted

Decision:
Before implementing a Temporal runner, we should harden core reliability semantics:
lock/run lifecycle, checkpoint advancement, source-scoped deletes, artifact collection
reads, sink commit semantics, and apply modularity.

Grounding:
Temporal will amplify unclear boundaries. If the local runtime has ambiguous commit and
retry semantics, a Temporal runner will inherit those problems.

Consequences:
- The next architecture work should fix control-plane reliability first.
- `apply.py` should be split into smaller services before adding durable execution.
- Regression tests should include crash/retry and partial fanout cases.

