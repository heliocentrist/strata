# Strata — Implementation Plan

*A build system for context: content-addressed, lineage-aware preparation of documents and other raw content for RAG pipelines, agent memory, and knowledge bases.*

**Status:** Phase 0A implemented; Phase 0B reliability hardening implemented; Phase 1A selectors, Phase 1B generic operation plugins, and pluggable local operation runners implemented; Phase 2A tests and external operation discovery implemented; host-staged ingest foundations partially implemented
**Author:** —
**Last updated:** May 2026

---

## 1. One-paragraph summary

Strata is to unstructured content what dbt is to SQL transformations: a build system that lets you define a DAG of typed assets (parsed documents, chunks, embeddings, summaries, entities, memory episodes), tracks lineage at per-document and per-chunk granularity, caches every artifact by a content-addressed fingerprint derived from `source_hash + transform_version + config_hash`, and reprocesses only what actually changed when a source document, a transform, or a config value is updated. It brings the dbt developer workflow — `plan`, `apply`, `test`, `docs`, and a node-selector syntax — to a domain that today is served only by ad-hoc scripts and pipeline runners with no real notion of incremental, surgical reprocessing.

### 1.1 Implementation checkpoint — May 2026

Phase 0A now exists as a working local fixture. The package includes `compile`, `plan`, `apply`, `progress`, `lineage`, `doctor`, `test`, `inspect`, and `docs`; SQLite state; local file and object-manifest sources; pipeline-configured operation plugins; deterministic chunking; fake deterministic embeddings; a local SQLite sink; operation progress; lineage; deletes; apply locks; and tests.

The artifact store design evolved during implementation. Fanout assets now use an Iceberg-inspired immutable snapshot model: payload JSONL files are immutable, fanout manifests are immutable and content-addressed, and the database is the catalog that commits asset instances to a specific manifest item. The physical artifact layout is behind an artifact collection plugin (`local_json`) rather than hard-coded in the core.

Apply semantics now live in the Strata runtime, not in executor plugins. Execution substrate is selected through an operation-runner registry. The built-in runners are `local_single_thread` and `local_threaded`; both share the same Strata cache/lineage/artifact commit semantics, while `local_threaded` parallelizes plugin invocations where the runtime can safely batch them.

---

## 2. Problem statement

Teams building RAG systems, agent memory, and knowledge bases all run the same shape of pipeline: ingest raw content, parse it, chunk it, enrich it, embed it, index it, expose retrieval. Today this is built one of two ways:

1. **Ad-hoc scripts and notebooks** — imperative, no lineage, no incrementality. Changing a chunker parameter means re-running everything and praying.
2. **Pipeline runners** (Temporal, Airflow, Prefect, LlamaIndex IngestionPipeline) — these handle execution and sometimes source-level dedup, but none track *why* an artifact exists, *which version* of *which transform* produced it, or which downstream artifacts a config change invalidates.

The concrete pains that result:

- **No reproducibility.** "Which chunker version produced this chunk?" is usually unanswerable.
- **No surgical reprocessing.** Changing a chunk size, swapping an embedder, or adding an enrichment step forces a full rebuild. At scale this is slow and expensive (embeddings and LLM enrichment cost real money).
- **No retrieval-quality debugging.** When a customer complains about a bad search result, there is no lineage trail from the retrieved chunk back through every transform to the source byte range.
- **No separation of concerns.** "What to compute" is tangled into imperative execution code, so the logic is hard to test, reason about, or reuse.

The market research (conducted separately) confirms no existing tool combines (a) content-addressed caching, (b) selective per-chunk invalidation on transform/config change, and (c) dbt-style developer ergonomics for unstructured content. The wedge is real, narrow, and unoccupied.

---

## 3. Goals and non-goals

### 3.1 Goals

1. **Lineage as a first-class artifact.** Every materialized asset records its full provenance: source URI and content hash, the transform and version that produced it, the config used, and the upstream artifacts it derived from. Lineage is queryable and browsable.
2. **Content-addressed, surgical reprocessing.** A change to a source document, a transform version, or a config value invalidates exactly the affected downstream artifacts and nothing else. `plan` shows the diff before `apply` executes it.
3. **dbt-grade developer ergonomics.** A declarative project, a compiled manifest, a node-selector grammar (`state:modified+`, `+asset`, `source:foo+`), plan-before-apply, asset-level tests, and a lineage/docs site.
4. **Substrate-agnostic.** Bring your own parser (LiteParse, LlamaParse, Reducto, Docling, Unstructured, Marker), embedding model, vector store, object store, and metadata DB. Strata orchestrates; it does not reimplement these.
5. **Execution-engine-agnostic.** Strata computes *what* to do and owns the apply semantics; a pluggable operation runner decides where plugin invocations run. Local runners ship first; Temporal, Dagster, and Prefect runners follow. Strata must be embeddable inside an existing orchestrator, not a replacement for one.
6. **Batch first, streaming-ready.** Batch corpus ingest is the v1 workload. The source and state abstractions must not preclude streaming ingest (agent memory) later, even though streaming execution is out of scope for v1.
7. **Multi-tenant-capable.** The data model must support per-tenant isolation of assets and lineage from the start, since the canonical early workloads are multi-tenant SaaS KB systems.

### 3.2 Non-goals (explicitly out of scope)

1. **Not a parser.** Strata never reimplements document parsing or OCR. It wraps and version-pins external parsers.
2. **Not an embedding model or vector store.** These are sinks and dependencies, not Strata's concern.
3. **Not an agent framework or LLM orchestration layer.** Strata prepares context; it does not run agents, manage prompts at runtime, or compete with LangChain/LlamaIndex agent abstractions.
4. **Not a durable execution engine.** Strata does not reimplement Temporal. Durable scheduling of plugin invocations can be delegated to an operation runner, while Strata still owns cache checks, artifact commits, lineage, and state transitions.
5. **Not a standalone hosted platform (yet).** v1 is an open-source library. A hosted control plane is a later-phase consideration, not part of the core design.
6. **Not a general-purpose data orchestrator.** Strata is deliberately domain-specific to content/context preparation. Pachyderm's general-purpose content-addressed pipelines never crossed the AI chasm; domain specificity is the bet.

---

## 4. Design principles

These are the load-bearing commitments. When a design decision is unclear, these resolve it.

1. **The planner is a pure function.** `plan(manifest, current_state, source_snapshot, selection) -> operations` performs no I/O, talks to no external system, and is fully deterministic given its inputs. Snapshot collection happens before the planner; execution happens after it. The planner is quick and honest: for fanout transforms such as chunking, it emits coarse scoped operations instead of pretending to know exact future child instances.

2. **Identity is content-addressed, not location-addressed.** An artifact's identity is the hash of its inputs (source content + upstream fingerprints + transform version + config hash), never its storage location or a timestamp. Two runs that should produce the same artifact produce the same fingerprint and reuse the cache. This is what makes reprocessing surgical.

3. **The manifest is the contract.** The project compiles to a manifest — a complete, serialized description of the asset graph, transforms, configs, and their hashes. Every command (`plan`, `apply`, `test`, `docs`, selection) operates on the manifest, never on raw project files. The manifest is the interface between the parser, the planner, the runner, and the docs site.

4. **State is portable and Strata-owned.** Lineage and asset instance state live in a Strata-owned logical schema with foreign keys pointing *out* to host systems (`project_id`, `tenant_id`, connection IDs) but no foreign keys pointing *in*. Strata's state can be lifted out of any host system without untangling it.

5. **Transforms declare their version explicitly.** Code-hash-based invalidation is too brittle (a comment change should not invalidate a million embeddings) and dependency-hash-based invalidation is worse. Developers declare a `version` string on each transform; bumping it is a deliberate act. Config hashing *is* automatic. The combination is sane by default and debuggable.

6. **Determinism is declared, not assumed.** Each transform is marked `deterministic`, `seeded`, or `nondeterministic`. Deterministic and seeded transforms reuse cache by fingerprint. Nondeterministic transforms also reuse cache by default to avoid surprise spend, but plans label them clearly and later rerun/invalidate commands support refresh and evaluation workflows.

7. **Asset is the user-facing unit; instance is the runtime unit.** Users define and select assets (`chunks`, `embeddings`) exactly as in dbt. The runner operates on asset *instances* scoped by `(project_id, tenant_id, asset_name, instance_key)` and tracks fingerprints per instance. The two-layer model (logical asset DAG, runtime instance DAG) is an implementation detail users rarely confront.

8. **Plan before apply, always.** No command mutates state or spends money without first producing a plan the user (or calling system) can inspect: what is stale, why, known counts, unknown fanout, and estimated cost where honest. This is Terraform's model adapted for content pipelines where some exact child instances are unknowable before execution.

9. **Opinionated core, pluggable edges.** The core (manifest, planner, fingerprinting, state model, selector grammar, apply semantics) is small and opinionated. The edges (sources, operations, artifact collections, operation runners, state backends, object stores) are plugins behind narrow interfaces. Resist growing the core.

10. **Cheap to be wrong.** Every phase delivers standalone value so that stopping at any phase is still a win. The first internal workload benefits before any framework abstraction is formalized.

---

## 5. Core concepts and data model

### 5.1 Concepts

- **Project** — a Strata project with one manifest, one logical DAG, and one state namespace. A tenant can have many projects.
- **Tenant** — the host application's customer/account boundary. Phase 0 supports tenant-scoped state, but not full hosted multi-tenant product features such as RBAC or quotas.
- **Execution context** — the explicit scope for every plan/apply: `(project_id, tenant_id)`. Both are required and default to `"default"` for local and single-tenant usage.
- **Source** — an external content root (a SharePoint connection, an S3 prefix, a Notion workspace, a stream of conversation turns). Has a `mode` (`batch` | `stream` | `hybrid`) and emits content items with a stable identity and a content hash.
- **Source checkpoint** — connector-level cursor state for a source scope, such as a SharePoint delta token, S3 continuation/version marker, or webhook cursor. It is scoped by `(project_id, tenant_id, source_name, connection_id, scope_hash)` and is separate from per-item `source_state`.
- **Asset** — a logical node in the DAG (e.g. `parsed`, `chunks`, `embeddings`, `summaries`). Defined by a transform, its inputs (other assets or sources), its config, its materialization strategy, and its determinism class.
- **Transform** — the computation an asset performs, plus an explicit version string. Wraps a parser, chunker, embedder, LLM call, etc.
- **Materialization strategy** — a manifest-level asset setting describing how an asset's output is persisted (`ephemeral`, `snapshot`, `incremental`, `content_addressed`, `sink`, later `decaying` and `versioned`).
- **Asset instance** — a concrete runtime state record for an asset at `(project_id, tenant_id, asset_name, instance_key)`. For Phase 0 fanout assets, instance keys prefer stable logical keys such as `parent_instance_key + ordinal`; chunk content hashes are stored separately as metadata for future embedding reuse.
- **Fingerprint** — the content-addressed identity of an asset instance: canonical JSON + SHA-256 over source/upstream identity, transform version, and config hash.
- **Operation** — the planner/executor contract. Phase 0 operations are typed coarse scopes, primarily `build_scope` and `delete_scope`, not necessarily exact instance-level work.
- **Operation item** — an apply-time expanded work item under an operation. Operation items provide durable progress, retry, and resume state after a coarse operation expands into many source items, chunks, embeddings, or deletes.
- **Sink** — a terminal materialization target (vector store upsert, metadata DB write, search index feed).
- **Retriever** *(post-v1)* — a versioned, lineage-tracked read path (hybrid search config, reranker, similarity threshold). Treated as an artifact whose changes invalidate downstream evaluation results.
- **Manifest** — the compiled, serialized representation of the whole project graph.
- **Fanout artifact manifest** — an immutable per-parent artifact snapshot for fanout assets such as chunks and embeddings. It records transform/config/source/upstream metadata plus item-to-payload mappings. The database points materialized `asset_instances` at a specific manifest item.

### 5.2 State schema (logical)

A Strata-owned schema, portable across host databases. SQLite ships first for local/dev and Phase 0; Postgres is the production target. All project-scoped state includes required `project_id` and `tenant_id`, both defaulting to `"default"`.

```
strata.transforms
  id
  project_id              -- required, default "default"
  transform_id            -- logical name, e.g. "chunk"
  version                 -- declared, e.g. "semantic_chunk@0.4.1"
  config_json
  config_hash
  code_hash               -- optional, advisory only
  determinism             -- deterministic | seeded | nondeterministic
  created_at

  unique(project_id, transform_id, version, config_hash)

strata.asset_instances
  id
  project_id              -- required, default "default"
  tenant_id               -- required, default "default"
  asset_name
  instance_key            -- stable logical key, e.g. doc id or parent key + ordinal
  input_fingerprint       -- content-addressed identity of this asset instance
  output_location         -- URI: s3://..., pinecone://index/id, pg://table/pk
  output_hash             -- hash of produced content
  content_hash            -- optional asset content hash, e.g. normalized chunk text hash
  transform_id            -- FK to strata.transforms
  materialization_strategy -- content_addressed | incremental | snapshot | sink | ...
  status                  -- materialized | running | failed | stale | deleted | delete_failed
  error                   -- nullable
  created_at
  updated_at

  unique(project_id, tenant_id, asset_name, instance_key, input_fingerprint)

strata.lineage_edges
  downstream_asset_instance_id   -- FK
  upstream_asset_instance_id     -- FK

  unique(downstream_asset_instance_id, upstream_asset_instance_id)

strata.source_state
  project_id              -- required, default "default"
  tenant_id               -- required, default "default"
  source_name
  item_key                -- e.g. sharepoint doc id
  source_content_hash     -- or etag/version token
  missing_since           -- nullable quarantine marker for non-authoritative snapshots
  last_seen_at
  deleted_at              -- confirmed source-side removal

  unique(project_id, tenant_id, source_name, item_key)

strata.source_checkpoints
  id
  project_id              -- required, default "default"
  tenant_id               -- required, default "default"
  source_name
  connection_id           -- host/source connection id; "local" for local sources
  scope_hash              -- hash of resolved source scope/config
  cursor_token            -- connector cursor, e.g. SharePoint delta token
  cursor_version          -- connector-defined cursor schema/version
  status                  -- active | stale | invalid
  updated_at
  created_at

  unique(project_id, tenant_id, source_name, connection_id, scope_hash)

strata.runs
  id
  project_id
  tenant_id
  manifest_hash
  status                  -- planned | running | succeeded | failed | canceled
  started_at
  finished_at
  created_at

strata.operation_runs
  id
  run_id
  project_id              -- copied for query convenience
  tenant_id               -- copied for query convenience
  op_type                 -- build_scope | delete_scope
  asset_name
  scope_json
  reason
  status                  -- pending | running | succeeded | failed | skipped
  estimated_instance_count
  estimated_cost_json
  error
  started_at
  finished_at

strata.operation_items
  id
  run_id
  operation_run_id
  project_id
  tenant_id
  asset_name
  item_key                -- source item key or asset instance key
  instance_key            -- nullable until known for some fanout work
  input_fingerprint       -- nullable until computable
  status                  -- pending | running | succeeded | failed | skipped | deleted
  error
  metadata_json           -- connector/asset metadata needed for resume/progress
  created_at
  updated_at

  unique(operation_run_id, item_key)

strata.apply_locks
  project_id
  tenant_id
  run_id
  acquired_at
  heartbeat_at
  expires_at

  unique(project_id, tenant_id)
```

**Invariants:**

- An `asset_instances` row with `status = materialized` and a matching `input_fingerprint` means the artifact at `output_location` is current and reusable. The planner skips any instance whose expected fingerprint matches a materialized row.
- A produced asset instance only becomes reusable after its output has been written and state has committed with `status = materialized`.
- For fanout assets, payload files and manifest files are immutable. The write order is payload first, manifest second, database catalog commit last. A crash before the database commit leaves only orphan files, which `strata doctor` can report and clean.
- `lineage_edges` form a DAG; cycles are a compile-time error.
- No host table holds a foreign key *into* `strata.*`. Strata depends on the host; the host does not depend on Strata's internal IDs.
- There can be at most one active `apply` per `(project_id, tenant_id)`. Different project/tenant scopes may run concurrently.
- Confirmed source deletes remove sink rows immediately, mark downstream asset instances `deleted`, and retain tombstoned state and lineage for debugging/audit. Artifact blob cleanup is a later garbage-collection concern.
- Connector cursors live in `source_checkpoints`, not in `source_state`. Per-item hashes/deletes live in `source_state`.
- `operation_runs` track coarse planned operations. `operation_items` track apply-expanded work for progress, retry, and resume. Final reusable artifacts still live in `asset_instances`.
- Source scope/config changes produce a new `scope_hash`, invalidate the old source checkpoint, and can emit delete operations for previously tracked items that are no longer in scope.

### 5.3 Fingerprint algorithm

Fingerprints use canonical JSON + SHA-256 with an explicit algorithm version. The conceptual formula remains:

```
fingerprint(instance) =
  sha256(canonical_json({
    "algorithm_version": 1,
    "source_content_hash": "... or null",
    "upstream_fingerprints": ["..."],
    "transform_version": "...",
    "config_hash": "...",
    "determinism": "deterministic | seeded | nondeterministic"
  }))
```

Rules:

- `fingerprint_algorithm_version = 1`.
- Serialization is canonical JSON encoded as UTF-8.
- Object keys are sorted.
- Insignificant whitespace is omitted.
- Nulls are explicit.
- Lists preserve order unless explicitly defined otherwise.
- `upstream_fingerprints` are sorted lexicographically before hashing.
- Source content hashes use SHA-256 over raw bytes.
- Config hashes use the same canonical JSON + SHA-256 rules over resolved config.
- Referenced files in config, such as prompt templates, are represented by path plus content SHA-256.
- For root assets, the source term is the source item's content hash.
- For derived assets, the upstream term is the sorted list of upstream fingerprints.
- `transform.version` is the developer-declared string.
- `code_hash` is advisory and never the sole invalidation trigger.
- For seeded transforms, the seed must be part of config and therefore part of `config_hash`.

This is the single most important algorithm in the system and the first thing to implement and test in isolation.

---

## 6. Architecture

### 6.1 Layered view

```
┌─────────────────────────────────────────────────────────────┐
│  Surface layer                                                │
│  - Python decorator API (@asset, @source, @transform)        │
│  - Declarative YAML project (compiles to same IR)            │
│  - CLI: strata plan | apply | test | docs | ls               │
├─────────────────────────────────────────────────────────────┤
│  Compilation layer                                            │
│  - Project parser → manifest (the contract)                  │
│  - Selector grammar resolver (state:modified+, +asset, ...)  │
├─────────────────────────────────────────────────────────────┤
│  Planning layer  ★ PURE, NO I/O ★                            │
│  - plan(manifest, current_state, source_snapshot, selection) │
│      -> ordered typed operations (coarse when needed)        │
│  - staleness detection via fingerprint diff                  │
│  - cost estimation                                           │
├─────────────────────────────────────────────────────────────┤
│  Execution layer                                              │
│  - Executor interface (local | temporal | dagster | prefect) │
│  - runs operations in topological order                      │
│  - expand fanout, compute fingerprints, check cache, run,    │
│    write asset instance state + lineage                      │
├─────────────────────────────────────────────────────────────┤
│  Plugin layer (narrow interfaces)                            │
│  - Sources (sharepoint, s3, notion, gdrive, stream)          │
│  - Parsers (liteparse, llamaparse, docling, unstructured)    │
│  - Transforms (chunkers, embedders, enrichers)               │
│  - Sinks (pinecone, weaviate, qdrant, pgvector, opensearch)  │
│  - State backends (sqlite, postgres)                         │
│  - Artifact stores (local fs, s3)                            │
└─────────────────────────────────────────────────────────────┘
```

### 6.2 The critical boundary

The planning layer is pure and sits between compilation and execution. It receives the manifest, a snapshot of current Strata state (from the state backend), a snapshot of source state (from source plugins), and the resolved selector. It returns typed operations. It performs no I/O itself; the impure work of *gathering* those snapshots happens above it and the impure work of *executing* operations happens below it.

This is what allows: exhaustive unit testing of planning logic with no infrastructure; swapping executors without touching planning; and eventual extraction of Strata from any host system.

For transforms whose exact outputs are not knowable before execution, the planner emits coarse operations such as "build chunks for parsed/docA". It does not run the operation plugin to discover exact output instances. During `apply`, the runtime binds input groups from `source` or a single upstream `input`, invokes the plugin through the selected operation runner, flatmaps returned outputs into exact asset instances, and records lineage.

Important boundary correction: Strata's apply semantics are not pluggable executor behavior. The runtime always owns dependency layers, input binding, cache checks, fingerprinting, artifact writes, state commits, lineage, operation item progress, source state, checkpoints, and delete semantics. The pluggable boundary is the async `OperationRunner`, which decides where bounded windows of `OperationPlugin.run(...)` invocations execute: inline, in a local thread pool, or later as Temporal/Dagster activities.

The executable plugin contract is now operation-shaped rather than parser/chunker/embedder/sink-shaped. A pipeline asset names one `operation`; the operation receives a list of `OperationInput` objects and returns a list of `OperationOutput` objects. This means one-to-one, one-to-many, many-to-one, and many-to-many work all use the same call shape. Each input has a stable `input_id`; outputs may declare `parent_input_ids` so batched plugin calls can still commit precise fingerprints and lineage. The runtime is responsible for grouping inputs from the DAG, reading/writing artifact collections, computing fingerprints, cache checks, and recording lineage. Plugins are responsible for domain work only.

```python
class OperationPlugin(Protocol):
    def run(
        self,
        inputs: list[OperationInput],
        config: dict[str, Any],
    ) -> list[OperationOutput]:
        ...
```

An operation may validly return an empty output list. This is treated as a successful no-output execution, not as a failed operation. Phase 0 does not write cache markers for empty output groups, so such work may be repeated by future plans. That is acceptable for the prototype; an explicit empty-output marker can be added later if repeated filters become expensive.

Assets may opt into larger plugin calls with `execution.inputs_per_call`. The minimum is `1`. This lets an embedding operation receive many chunk inputs in one sync plugin call while the runtime still commits each returned embedding as its own asset instance. Separately, `execution.window_size` controls how many invocation batches the runtime hands to the async runner before waiting and committing results. Windows are formed across the whole asset dependency layer, not only within a single source-scoped planned operation, so small inputs from many source items can still fill efficient runner windows. Artifact outputs returned by a window are grouped by partition before writing, so the local artifact collection writes one payload/manifest pair per window/partition instead of one pair per output item. Runners may do their own lower-level batching, concurrency, and activity scheduling inside each window.

The intended runner IO contract is location-based for all runners, not Temporal-specific. The runtime builds partition-aligned windows and passes artifact locations or manifest pointers to the runner rather than large payloads. A runner reads whole input files for the partitions assigned to that window, runs operation plugins, writes immutable output payload files and data manifests for the window/partition, and returns output descriptors: manifest URI, item URI, record ranges/counts, hashes, logical output ids, and per-item status. The Strata runtime is the control plane: it does not need data-plane object store access during normal apply. It trusts runner-reported descriptors, commits `asset_instances` and lineage in the DB, updates operation progress, and stamps the window as done. Data manifests and payload files are therefore written artifacts, not commits; a crash after runner writes but before runtime DB commit leaves orphan files for `doctor` to report or clean. `doctor` is allowed to be a special case that has object-store access for deep repair/verification.

Layer fusing is a future optimization, not part of the current contract. Today the runtime executes one asset layer at a time and commits between layers. Later, compatible adjacent layers may be fused into one runner window to reduce storage IO, but this must preserve the same external cache, lineage, manifest, and retry semantics.

Pipeline YAML is therefore operation-centric:

```yaml
pipeline:
  parsed:
    source: docs
    operation: liteparse

  chunks:
    input: parsed
    operation: fixed_token_chunker
    config:
      output_label: chunk

  embeddings:
    input: chunks
    operation: fake_embedding
    execution:
      inputs_per_call: 128
      window_size: 1000

  sink:
    input: embeddings
    operation: local_sqlite_vector_sink
```

### 6.3 Surface API sketch (Python)

```python
from strata import asset, source, Config
from strata.transforms import liteparse, semantic_chunk, embed

@source(connector="sharepoint", mode="batch")
class Docs: ...

@asset(inputs=[Docs])
def parsed(doc) -> Document:
    return liteparse(doc.bytes, version="2.0", ocr=True)

class ChunkCfg(Config):
    max_tokens: int = 512
    overlap: int = 50

@asset(inputs=[parsed], config=ChunkCfg)
def chunks(doc, cfg) -> list[Chunk]:
    return semantic_chunk(doc, **cfg.dict())

@asset(inputs=[chunks])
def embeddings(chunk) -> Embedding:
    return embed(chunk.text, model="bge-m3", version="1.5")

@asset(inputs=[chunks], deterministic=False, cache="aggressive")
def summaries(chunk) -> str:
    return llm_summarize(chunk.text, prompt="prompts/sum.j2",
                         model="claude-haiku-4.5")
```

### 6.4 CLI sketch

```
strata compile                      # build the manifest
strata plan                         # show stale instances + cost, no mutation
strata plan -s chunks+              # scope to chunks and downstream
strata apply                        # execute the plan
strata apply -s state:modified+     # rebuild changed nodes and descendants
strata test -s embeddings           # run asset-level assertions
strata inspect --asset embeddings --instance-key docs/a.md#chunk:0000
strata docs build                   # generate searchable static asset browser
strata docs serve                   # lineage + DAG browser
strata ls -s source:docs+           # list selected assets
strata invalidate -s chunks         # force-stale an asset (rare)
```

### 6.5 Operation model

Phase 0 uses typed coarse operations as the planner/executor contract:

```
Operation
  op_id
  op_type                 -- build_scope | delete_scope
  asset_name
  project_id
  tenant_id
  scope                   -- SourceItemScope | AssetInputScope
  reason                  -- source_changed | config_changed | transform_version_changed | source_deleted | missing_cached_instance | previous_failure | forced_selection
  depends_on              -- operation ids
  estimated_instance_count
  estimated_cost
```

Initial scopes:

```
SourceItemScope
  source_name
  item_key

AssetInputScope
  upstream_asset_name
  upstream_instance_key
```

Exact instance-level work is an execution detail. The operation model can later grow exact `build_instance` operations for assets whose output instances are fully knowable during planning.

During `apply`, coarse operations may expand into durable `operation_items`. These are not final artifacts. They are per-run work records used for progress, retry, and resume. For example, a SharePoint source operation can create one operation item per enumerated file, and a chunking operation can create one operation item per produced chunk before final `asset_instances` are committed.

### 6.6 Command semantics

`strata plan`:

1. Loads project config.
2. Compiles the project to a manifest.
3. Resolves `ExecutionContext(project_id, tenant_id)`, defaulting both to `"default"`.
4. Reads current Strata state for that context, including source checkpoints.
5. Reads source snapshots outside the pure planner boundary using the relevant source checkpoint/scope state.
6. Calls `plan(manifest, current_state, source_snapshot, selection)`.
7. Validates the graph and selector.
8. Compares source snapshots, source scope hashes, transform versions, config hashes, fingerprints, failures, and delete markers.
9. Emits topologically ordered `build_scope` and `delete_scope` operations.
10. Marks fanout counts as estimated or unknown when exact counts are not knowable.
11. Labels nondeterministic operations clearly while still treating them as cacheable by default.
12. Prints the plan and writes no state, no artifacts, and no sink output.

`strata apply`:

1. Loads and compiles the project.
2. Resolves `ExecutionContext(project_id, tenant_id)`.
3. Acquires the `apply_locks` row for that exact `(project_id, tenant_id)`.
4. Creates a `runs` row.
5. Builds the same plan as `strata plan`.
6. Persists planned operations to `operation_runs`.
7. Executes operations in topological order.
8. Re-checks cache before running each operation against latest committed state.
9. Binds input groups, runs operation plugins, flatmaps returned outputs, and writes `operation_items` for durable progress/resume.
10. Fetches short-lived source access material, such as download URLs, just in time during execution rather than storing it in source snapshots.
11. Computes fingerprints using canonical JSON + SHA-256.
12. Writes outputs before marking asset instances `materialized`.
13. Writes `asset_instances` and `lineage_edges`.
14. Updates source checkpoints only after the relevant enumeration/checkpoint-producing phase succeeds.
15. For confirmed deletes, removes sink rows immediately and retains tombstoned asset instances and lineage.
16. Records failures per operation item/instance and keeps successful asset instances.
17. Marks the run `succeeded` or `failed`.
18. Releases the apply lock.

### 6.7 Real sync pipeline pressure test

The SharePoint/KVS sync pipeline is the reference Phase 0B validation target after the local fixture. It shows where Strata ends and the host application begins:

- Host application owns OAuth, credentials, user connections, source scope UI, per-user access policy, Temporal scheduling/cancellation, and search authorization.
- Strata owns source item state, source checkpoints, content fingerprints, parse/chunk/embed lineage, incremental rebuild decisions, delete propagation into sinks, and content-preparation progress/audit state.
- `org_id` maps naturally to `tenant_id`.
- `knowledge_base_id` maps naturally to `project_id`.
- `connection_id` maps to source connection identity.
- `source_unique_id` maps to source `item_key`.
- `etag` or equivalent source version token maps to `source_content_hash`.
- Host access records, such as `DocumentAccess`, remain outside Strata. They may reference Strata/KVS outputs, but Strata does not own authorization policy.

Design notes from this pressure test:

- Two-pass sync (`enumerate -> ingest`) validates the plan/apply split. Enumeration/source snapshotting can discover source items and counts without parsing/chunking/embedding.
- Durable expanded work items are required for progress and resume. A coarse operation may represent thousands of files or chunks; `operation_items` are the durable per-run status rows.
- Source checkpoints must be connector-level state. Delta tokens, continuation cursors, webhook cursors, and scope hashes do not belong on individual source item rows.
- Short-lived source access material, such as SharePoint download URLs, must be fetched just in time during `apply`, not stored in source snapshots or fingerprints.
- Scope changes are config changes. They create a new `scope_hash`, invalidate the old checkpoint, and can emit deletes for items that are no longer in scope.
- Physical sink cleanup may need host/reference policy. Strata can emit and execute sink deletes, but a host may defer blob/vector garbage collection while other users or projects still reference the same underlying content.

### 6.8 Artifact commit model

Phase 0B uses an Iceberg-inspired local artifact commit model for fanout assets.

Fanout payloads are immutable JSONL files:

```
.strata/artifacts/chunks/{parent_fingerprint}/payloads/{payload_hash_prefix}.jsonl
.strata/artifacts/embeddings/{parent_fingerprint}/payloads/{payload_hash_prefix}.jsonl
```

Fanout manifests are immutable, timestamped, and content-addressed:

```
.strata/artifacts/chunks/{parent_fingerprint}/manifests/{YYYYMMDDTHHMMSSZ}-{manifest_hash_12}.json
```

The manifest contains `manifest_hash`, `created_at`, asset identity, parent identity, transform/config metadata, source metadata, upstream references, payload file hashes, and item records. Item records contain the stable `instance_key`, `input_fingerprint`, `content_hash`, payload file path, payload format, and record number.

The database is the catalog. `asset_instances.output_location` points to a specific manifest item:

```
artifact://chunks/{parent_fingerprint}/manifests/{timestamp}-{manifest_hash_12}.json#item=4
```

The write order is:

1. Compute all fanout payload records and item metadata.
2. Write immutable payload file.
3. Write immutable fanout manifest.
4. Commit or update `asset_instances`, `lineage_edges`, and operation item status in state.

This avoids mutable fanout files. A crash before step 4 leaves orphan payload/manifest files but no reusable state. A crash after step 4 leaves database rows pointing to already-written immutable files. `strata doctor` validates manifest hashes, payload hashes, item content hashes, and missing or orphaned files.

### 6.9 Host-staged object ingest sub-plan

The first production-shaped integration should assume Strata is embedded inside a host ingestion service, not running as the top-level orchestrator. The host owns external credentials, upstream enumeration, download URL freshness, scheduling, cancellation, and user access policy. Strata owns the content-preparation DAG once raw files have been staged durably.

The intended shape is:

```
host connector/enumerator
  -> stage raw files into object storage
  -> write a source object manifest
  -> call Strata as a library
  -> inspect/debug with Strata CLI/docs by project id/name
```

This keeps short-lived source URLs out of Strata and gives Strata a durable, replayable source snapshot.

#### 6.9.1 Source adapters become pluggable

Sources should use the same plugin model as parsers, chunkers, embedders, and sinks:

```python
class SourceAdapter(Protocol):
    def snapshot(self, spec: SourceSpec) -> SourceSnapshot:
        ...
```

Built-in source adapters:

- `local_files` — current fixture behavior. Strata owns enumeration by scanning a local path.
- `object_manifest` — host-staged ingest behavior. Strata reads a manifest of already-staged objects.

Later source adapters may include `s3_prefix`, `gcs_prefix`, `http_manifest`, or connector-specific metadata sources, but those should remain plugins rather than core logic.

`SourceSnapshot` needs explicit source semantics:

```python
class SourceSnapshotMode(StrEnum):
    AUTHORITATIVE = "authoritative_snapshot"
    INCREMENTAL = "incremental_delta"
```

Planner rules:

- `authoritative_snapshot`: absent previously-known source items are treated as deletes.
- `incremental_delta`: absent items mean unknown/no-op; deletes require explicit `deleted: true` items.

`SourceItem` should support:

```python
item_key: str                  # stable source identity
content_hash: str              # source version token, e.g. etag or object version
uri: str                       # staged object URI
metadata: dict                 # file name, source path, web URL, MIME type, etc.
deleted: bool = False
```

#### 6.9.2 Object manifest source format

The source manifest is an input manifest, separate from Strata artifact/fanout manifests.

Example:

```json
{
  "schema_version": 1,
  "mode": "authoritative_snapshot",
  "source_name": "staged_documents",
  "connection_id": "conn_123",
  "items": [
    {
      "item_key": "source:drive:item",
      "content_hash": "etag-or-object-version",
      "object_uri": "s3://bucket/raw/source-drive-item.pdf",
      "metadata": {
        "file_name": "Report.pdf",
        "source_path": "/Documents/Reports/Report.pdf",
        "source_web_url": "https://source.example/Report.pdf",
        "mime_type": "application/pdf"
      }
    }
  ]
}
```

Local development should support filesystem paths and `file://` URIs first. S3/GCS support should come through an object storage abstraction rather than direct path handling in parsers.

#### 6.9.3 Pluggable object storage

Artifacts and staged source objects should be accessed through an object storage interface:

```python
class ObjectStore(Protocol):
    def open(self, uri: str) -> BinaryIO:
        ...

    def read_text(self, uri: str) -> str:
        ...

    def write_text(self, uri: str, value: str) -> str:
        ...

    def exists(self, uri: str) -> bool:
        ...
```

Built-ins:

- `local` / `file` for local development and tests.
- `s3` for production object manifests and artifact storage.

The artifact store should continue to be immutable and content-addressed. The object storage abstraction is transport; it must not change fingerprint identity.

#### 6.9.4 Pluggable artifact collections

The core should not know whether an asset is stored as one file per instance, a JSONL bundle, a Parquet shard, or a remote object manifest. It should work with iterable/addressable artifact collections:

```python
class ArtifactCollection(Protocol):
    def write_one(self, context: CollectionWriteContext, item: ArtifactWrite) -> ArtifactWriteResult:
        ...

    def write_many(self, context: CollectionWriteContext, items: list[ArtifactWrite]) -> list[ArtifactWriteResult]:
        ...

    def read(self, root_path: Path, ref: str) -> dict[str, Any]:
        ...
```

The collection owns the physical layout. The state DB stores the returned address/hash. This lets small prototypes use JSON files while production pipelines choose fewer larger files, such as JSONL or Parquet bundles, without changing transform logic.

Initial built-in:

- `local_json` stores single-output artifacts as JSON and fanout artifacts as `payloads/*.jsonl` plus timestamped immutable manifests.

Future built-ins:

- `local_jsonl_bundle` for batching many single-output artifacts into fewer local JSONL files.
- `s3_jsonl_bundle` for object-storage-backed bundles.
- `parquet_bundle` for high-volume parsed/chunk/embedding assets.

Manifests are commit markers for bundled writes: payload files are written first; the manifest is written last and is the addressable record used by state.

**Current implementation:** artifact collection protocols and registry are implemented. `local_json` is registered as the built-in collection. Single-output assets currently use one JSON file per instance; fanout assets use one JSONL payload plus an immutable timestamped manifest per parent/partition. The database stores the collection-returned address and hashes. More compact strategies for high-volume single-output assets, such as JSONL or Parquet bundles, remain deferred.

#### 6.9.5 Enriched single-input sink contracts

Hybrid search indexes usually need one physical document per chunk containing both lexical text and vector embedding. Model this as a linear pipeline: the embedding/enrichment operation carries chunk text and source metadata forward, then the sink consumes that enriched asset as its single input.

```yaml
embedded_chunks:
  input: chunks
  operation: openai_embedding
  version: openai_embedding@0.1.0
  execution:
    inputs_per_call: 128

search_index:
  operation: elasticsearch_hybrid_sink
  version: elasticsearch_hybrid_sink@0.1.0
  input: embedded_chunks
  config:
    index: search_chunks
```

The operation should not crawl Strata internals. Instead:

- The embedding/enrichment operation receives chunks and returns enriched records with vector, text, source metadata, and stable parent lineage.
- The sink receives enriched records through the normal single-input contract.
- The sink writes physical index documents and returns `OperationOutput` values with output location/hash.
- Strata records ordinary lineage edges from chunk to enriched record to sink result.
- If a future use case truly needs joining independent assets, model that join as an explicit upstream operation that produces one enriched output collection.

Delete behavior should be part of the sink contract:

```python
sink.delete_source(context, source_name, source_item_key, config)
```

For search indexes this may be a delete-by-query or a deletion of tracked document IDs. Host access records remain outside Strata.

#### 6.9.6 Library-first execution API

The host should call Strata as regular code inside its existing execution environment first. Executor adapters are later.

Initial API target:

```python
result = strata.apply_project(
    project_id="project_123",
    tenant_id="tenant_123",
    state_url="postgresql://...",
    manifest=manifest,
    selection=None,
)
```

or:

```python
result = strata.apply_staged_manifest(
    project_id="project_123",
    tenant_id="tenant_123",
    source_manifest_uri="s3://.../manifest.json",
    pipeline_template="default_rag",
)
```

The host scheduler can run this in one activity/job initially. Strata remains responsible for idempotency, cache checks, operation item progress, lineage, and partial item failures. A later Temporal/Dagster executor can map Strata operations to native activities when needed.

#### 6.9.7 Project registry and CLI access to host-created projects

Projects created by a host service may not have a local `strata.yml`. Operators still need to inspect them with the CLI and docs tools.

Add a lightweight project registry/catalog:

```
strata.projects
  id
  project_id
  tenant_id
  name
  state_url
  artifact_root_uri
  latest_manifest_json
  latest_manifest_hash
  created_at
  updated_at

  unique(project_id, tenant_id)
  unique(name)                  -- optional/operator-friendly alias
```

Required CLI behavior:

```bash
strata project ls --state-url postgresql://...
strata project show --state-url postgresql://... --project-id project_123 --tenant-id tenant_123
strata docs serve --state-url postgresql://... --project-id project_123 --tenant-id tenant_123
strata inspect --state-url postgresql://... --project-id project_123 --tenant-id tenant_123 --asset chunks --instance-key ...
```

The existing `-p strata.yml` path remains for local projects. The new state/project-id path is for projects created and processed by a host application.

#### 6.9.8 Milestones for host-staged ingest

**Milestone A — Source/object foundations**

- Add source adapter registry.
- Move `local_files` behind the registry.
- Add `object_manifest` source adapter with local/file URI support.
- Add `SourceSnapshot.mode` and `SourceItem.deleted`.
- Update planner delete rules for authoritative vs incremental snapshots.
- Add tests for unchanged, changed, missing-authoritative-delete, missing-incremental-no-op, and explicit incremental delete.

**Current implementation:** source registry, `local_files`, `object_manifest`, snapshot modes, explicit deletes, and authoritative-vs-incremental planner behavior are implemented for local/file URIs.

**Milestone B — Object storage abstraction**

- Add local/file object store.
- Route artifact reads/writes and object manifest reads through the abstraction where practical.
- Add S3 object store support.
- Keep artifact identity independent of storage URI.

**Current implementation:** local/file object store support is implemented for source manifests and staged objects. S3 remains deferred.

**Milestone C — Library execution API**

- Add a stable library entry point for `compile/plan/apply` without Typer.
- Add a host-staged manifest helper.
- Return structured per-source-item results: built, reused, deleted, failed, and sink output references.
- Add tests proving a host can run Strata without a local YAML file.

**Current implementation:** `strata.api` exposes compile/plan/apply helpers, and `strata.host_testing` provides a local host fixture that stages batch inputs and invokes Strata as a library. Per-source-item result summaries and YAML-free host projects remain deferred.

**Milestone D — Hybrid sink contract**

- Keep assets single-input only: every asset defines either `source` or `input`.
- Carry chunk text/source metadata forward in the embedding/enrichment output.
- Update local sink or add a fake hybrid search sink test double that consumes enriched records.
- Record normal lineage from chunk to enriched record to sink result.
- Reject removed named-input/join YAML with a clear compile-time error.

**Current implementation:** named operation inputs and executor-side joins have been removed. The built-in embedding operation preserves chunk context in its output metadata, and the local SQLite vector sink consumes enriched embedding records as a normal single-input sink.

**Milestone E — Search index sink**

- Add an Elasticsearch/OpenSearch hybrid sink adapter.
- Bulk-write one document per chunk containing text, embedding, source metadata, and fingerprint fields.
- Implement delete/deactivate by source item.
- Return stable `output_location` values such as `elasticsearch://index/document_id`.

**Milestone F — Host-created project observability**

- Add project registry/catalog table.
- Persist latest compiled manifest for host-created projects.
- Support CLI/docs/inspect/progress by `--state-url + --project-id + --tenant-id` or `--project-name`.
- Add docs browser support for project registry lookup.

**Milestone G — Operation runner adapter later**

- Keep the first host integration as one host activity/job calling Strata as a library.
- Add activity heartbeats/cancellation checks around Strata progress if needed.
- Only after this is proven, add Temporal/Dagster operation runners that map plugin invocations onto native activities while the Strata runtime keeps cache, lineage, artifact commit, and progress semantics.

#### 6.9.9 Test host fixture

Before integrating with a real host service, build a small test host app fixture that mimics the intended boundary:

```
test host
  -> receives source metadata in batches
  -> stages raw files into local object storage
  -> writes an object_manifest source manifest
  -> calls Strata as an embedded library
  -> inspects results with Strata state/docs/inspect
```

Suggested layout:

```
examples/host_app/
  host_app.py
  fixtures/
  .host_store/
    raw/
    manifests/
  README.md
```

The fixture models the host contract without requiring a real connector, real object store, real search index, or real scheduler.

Host fixture API:

```python
class TestHostApp:
    def stage_batch(self, items: list[HostSourceItem]) -> None:
        ...

    def write_source_manifest(
        self,
        *,
        mode: Literal["authoritative_snapshot", "incremental_delta"],
    ) -> Path:
        ...

    def run_strata(self) -> ApplyResult:
        ...
```

Test scenarios:

- Initial authoritative ingest: stage files across multiple batches, write one complete manifest, run Strata, assert all expected asset/sink rows exist.
- Idempotent rerun: same manifest and content hashes produce an empty plan or full cache reuse.
- One changed file: same item key with a new content hash rebuilds only that file lineage.
- Authoritative delete: previously known item missing from an authoritative manifest emits delete operations and deactivates/removes sink documents.
- Incremental delta: manifest containing only changed items does not delete absent items.
- Explicit delta delete: manifest item with `deleted: true` deletes/tombstones that source lineage.
- Partial failure: one bad staged object fails while successful items remain materialized and retryable.
- Host-created project observability: after a host run, `strata inspect`, `strata progress`, and `strata docs build/serve` can inspect the project by project id/name once the project registry exists.

Implementation should happen in two passes:

1. Use generated local project config while `object_manifest` and source plugins are introduced.
2. Switch to the library-first API and project registry once those exist, removing the need for a temporary YAML file.

---

## 7. Implementation phases

Each phase ships standalone value. Stopping after any phase is a coherent outcome.

### Phase 0 — Fingerprint + state core (minimal local fixture)

**Goal:** Prove the load-bearing primitive in isolation using a small local RAG fixture before integrating real parsers, embedders, or vector stores.

- Implement SQLite state schema: `transforms`, `asset_instances`, `lineage_edges`, `source_state`, `source_checkpoints`, `runs`, `operation_runs`, `operation_items`, and `apply_locks`.
- Include required `project_id` and `tenant_id` on project-scoped state, defaulting both to `"default"`.
- Implement canonical JSON + SHA-256 fingerprinting as a standalone, exhaustively tested function.
- Implement stable logical instance keys for fanout assets. Phase 0 chunks use `parent_instance_key + ordinal`; chunk content hash is stored separately for future embedding reuse.
- Implement pure `plan(manifest, current_state, source_snapshot, selection) -> operations`.
- Implement typed coarse operations: `build_scope` and `delete_scope`.
- Implement a local apply runtime that binds operation inputs, invokes plugins through the selected runner, writes `operation_items` for progress/resume, writes asset instance state, and records lineage.
- Implement source checkpoints and source scope hashing, even if the local file source only uses a simple filesystem scan marker.
- Implement apply locking with at most one active apply per `(project_id, tenant_id)`, while allowing different project/tenant scopes to run concurrently.
- Implement confirmed-delete behavior: remove sink rows immediately, mark downstream asset instances `deleted`, and retain tombstoned lineage.
- Build the Phase 0A fixture:
  - local file source
  - pipeline-configured parser adapter
  - deterministic chunker
  - fake deterministic embedder
  - SQLite state
  - local SQLite/file sink

**Exit criterion:** The local fixture proves all of the following: a single file content change rebuilds only that file lineage; a chunker config change reuses parsed docs and rebuilds chunks/embeddings; a confirmed delete removes sink rows and tombstones lineage; progress is queryable from `operation_items`; lineage is queryable from source to parsed to chunks to embeddings; and apply locking prevents concurrent applies for the same `(project_id, tenant_id)`.

**Current status:** Implemented as Phase 0A. The implemented fixture also includes immutable fanout payload/manifest artifacts and `strata doctor`.

### Phase 0B — Reliability and artifact-store hardening

**Goal:** Make the local build loop resilient enough that crashes produce recoverable state, not ambiguous state.

- Use immutable fanout payload files and immutable fanout manifests for chunks and embeddings.
- Treat SQLite state as the artifact catalog: materialized fanout rows point to a specific manifest item.
- Commit fanout state only after payload and manifest files exist.
- Add `strata doctor` to inspect and optionally fix local reliability issues.
- Detect expired locks and abandoned running runs.
- Detect materialized rows whose artifact URI cannot be resolved.
- Validate manifest filename hash prefixes, manifest hashes, payload file hashes, and item content hashes.
- Report and optionally remove orphan artifact files left by interrupted runs.
- Keep `--fix` conservative: release expired locks, fail abandoned runs, fail broken materializations, and delete orphan files.
- Commit fanout `asset_instances`, `lineage_edges`, and operation item status in one database transaction after immutable payload and manifest files exist.

**Exit criterion:** A simulated interrupted run that leaves orphan payloads/manifests, expired locks, abandoned run rows, or broken artifact references is reported by `strata doctor`; safe issues are fixed by `strata doctor --fix`; a clean project reports no issues.

**Current status:** Implemented. Regression coverage includes expired locks, orphan files, corrupt payloads, missing fanout manifests, doctor repair, and rebuild after repair.

### Phase 1 — The build system core

**Goal:** A real, standalone Strata that a developer can adopt as a library.

- Minimal selector grammar: `asset`, `asset+`, `+asset`, `+asset+`, and `source:x+`.
- Local plugin registry for built-in parser, chunker, embedder, and sink adapters.
- Pipeline-level operation selection, so parser/chunker/embedder/sink behavior is configured in the project manifest instead of by CLI extras.
- Manifest-level operation-runner selection:
  ```yaml
  execution:
    executor: local_threaded
    config:
      max_workers: 4
  ```
- Python decorator surface (`@asset`, `@source`, `Config`).
- Project compilation → manifest.
- Pluggable operation-runner registry with local single-thread and local threaded runners.
- Strata runtime with dependency-safe scheduling, generic input binding, flatmap output materialization, per-instance caching, artifact commits, and lineage.
- Extended selector grammar: `tag:y`, `state:modified+`.
- `strata compile | plan | apply | ls`.
- Built-in operation plugins for one of each: one parser (LiteParse), one chunker, one embedder, one sink (pgvector or Qdrant), plus Postgres state backend after the Phase 0 SQLite backend.

**Exit criterion:** A new user can define a RAG ingest pipeline in <50 lines, run `strata plan` / `strata apply`, change a config, and see surgical reprocessing — all without a host orchestrator.

**Current status:** Phase 1A minimal selectors are implemented. `--select/-s` supports `asset`, `asset+`, `+asset`, `+asset+`, comma-separated asset terms, and `source:x+`. The old `--asset` option remains as a backward-compatible alias for `asset+`.

Phase 1B local plugin registry is implemented around a generic operation interface. The runtime dispatches every executable asset through the named `operation` registry. Built-in operation plugins are registered for `markdown_noop`, `liteparse` (`auto` currently aliases `liteparse`), `fixed_token_chunker`, `fake_embedding`, and `local_sqlite_vector_sink`. `markdown_noop` accepts only `.md` files. `liteparse` reads `.txt` and `.md` directly and parses `.pdf` through LiteParse.

The planner no longer hardcodes the canonical `parsed -> chunks -> embeddings -> sink` asset names. Asset ordering is compiled from the declared DAG, and the planner emits generic `build_scope` / `delete_scope` operations from asset dependencies and current state. Runtime input binding is inferred from the asset declaration shape: `source` or a single upstream `input`. Plugin metadata no longer declares map/fanout/sink shape; every operation is executed through the same flatmap-style `OperationInput` / `OperationOutput` contract. Per-asset `execution.inputs_per_call` lets the runtime pass multiple compatible inputs into one plugin call; outputs use `parent_input_ids` to preserve exact lineage.

Pluggable operation-runner selection is implemented. `ExecutionSpec` is part of the manifest, `strata.yml` supports an `execution:` block, and the CLI/API dispatch through an operation-runner registry. Built-ins:

- `local_single_thread` runs plugin invocations inline.
- `local_threaded` uses a `ThreadPoolExecutor` and `max_workers` from execution config for safely batched plugin invocations.

The runtime still owns apply semantics for both runners: dependency layers, input binding, layer-level runtime windows, cache checks, fingerprints, DB lineage, operation item status, source state, checkpoints, and delete behavior. The runner executes bounded, partition-aligned windows. Runners receive artifact locations/manifest pointers, read whole input files for their assigned partitions, write immutable output payload/data-manifest files, and return descriptors; the runtime commits those descriptors into state. The current local runners implement this boundary in-process, and the local SQLite sink operation owns its external sink write.

### Phase 2 — Ergonomics and trust

**Goal:** The things that make people trust it in production.

- External plugin discovery via Python package entry points, so operation/source/artifact/executor plugins can ship outside the core package.
- Plugin metadata and compatibility checks: plugin type, supported asset kinds, config schema, dependency requirements, and declared Strata API version.
- `strata test` — asset-level assertions (no empty chunks, embedding dims match model, parent_doc_id present, ≥X% of docs produced chunks).
- `strata inspect` — CLI-first per-instance debug view showing fingerprints, transform/config identity, artifact preview, upstream lineage, and downstream sink linkage.
- `strata docs build` / `strata docs serve` — searchable asset browser + per-instance lineage trace (source → parse → chunk → embed) with fingerprints visible. This is the retrieval-quality debugging tool and a key differentiator.
- Declarative YAML surface compiling to the same manifest.
- Partial-failure handling: one instance failing does not fail the run; failed instances are retried on next apply.
- Determinism classes wired into cache behavior.

**Exit criterion:** A team can debug "why is this chunk in the results" via the docs UI, and trust `plan` to tell the truth about rebuild scope before running apply.

**Current status:** Phase 2A is implemented. External operation discovery uses the Python entry point group `strata.operations` and records basic plugin metadata. `strata test` is implemented with declarative YAML tests and built-in assertions for materialized assets, non-empty artifacts, embedding dimensions, and source identity metadata.

Phase 2B is implemented as a CLI-first inspection layer. `strata inspect` assembles state rows, transform metadata, lineage edges, artifact payload previews, embedding dimensions, and local sink linkage into a human-readable or JSON report.

Phase 2C is implemented as a static asset browser. `strata docs build` writes `index.html`, `styles.css`, `app.js`, and `data.json`; `strata docs serve` rebuilds and serves that static site locally. The browser supports asset/status filters, text search across keys/hashes/previews, per-instance identity details, previews, and clickable upstream/downstream lineage.

Future docs enhancement: add an interactive lineage graph view for the selected instance and its neighborhood, so users can visually follow source -> parsed -> chunks -> embeddings -> sinks in addition to the current linked lists.

Richer test plugins and stronger compatibility enforcement remain deferred.

### Phase 3 — Operation runners and scale

**Goal:** Fit into how teams already run things.

- Operation runner adapters: Temporal (durability/retries/scheduling), Dagster (`dagster-strata`), Prefect.
- Temporal runner should map partition-aligned runtime windows onto native workflow/activity boundaries while the Strata runtime preserves fingerprints, lineage, artifact commits, and retryable item state.
- Generalize the current local window contract to remote runners: runner inputs are artifact file/manifest pointers, runner outputs are immutable data-manifest descriptors, and the runtime writes only control-plane DB commits during normal apply.
- Keep windows partition-aligned so separate runner activities do not share input files in the common case.
- Defer layer fusing until the single-layer window contract is stable; fused layers may later reduce storage IO but must keep externally visible cache, lineage, manifest, and retry semantics.
- Threaded local execution can later be refined once manifest assembly and partial fanout retry semantics are explicit.
- Sink operations should support batched writes. The current local sink commits one external output at a time, which is acceptable for the fixture but wrong for Elasticsearch/OpenSearch/vector DB adapters. The sink contract should let a window produce many sink writes and commit them with bulk insert/upsert/delete APIs while still recording one `asset_instance` and lineage set per logical sink output.
- Artifact store abstraction (local fs + S3) so intermediate artifacts (parsed markdown, chunk parquet) are the source of truth and the vector store is a rebuildable downstream sink.
- (To consider) Option to use Iceberg as storage.
- Future artifact storage research:
  - Parquet-backed artifact collections for high-volume parsed/chunk/embedding assets.
  - File-level manifests that point to Parquet/JSONL data files instead of listing every item inline.
  - Operation-run output group manifests under `artifacts/{asset_name}/{partition_hash}/manifests/{timestamp}-{hash}.json`.
  - Optional compaction from many small operation outputs into larger files.
  - Clear output location format for bundled records.
  - Hash/state simplification once DB-as-SSOT plus bundled artifact storage is proven.
  These are deliberately deferred until the artifact storage model is clearer.
- Cost estimation in `plan` for paid parser, embedding, and LLM operations, using plugin-provided estimators where available.
- Per-asset scheduling (re-embed daily, re-summarize weekly) expressed declaratively. Scheduling should be optional, because a lot of use cases will have Strata runs orchestrated by external scheduler.
- Multi-tenant and multi-project operations hardened (isolation, per-tenant/project plan/apply, bulk reprocessing across tenants).

**Exit criterion:** Strata runs as the build-system layer on top of an existing Temporal or Dagster deployment, not as a replacement for it.

**Current status:** Apply semantics live in `strata.execution.apply`. The sync public apply API wraps an async apply runtime. The async operation runner protocol and registry are implemented. `local_single_thread` and `local_threaded` are implemented and selectable from `strata.yml`. The runtime executes dependency layers, forms bounded windows across all source-scoped operations in that layer, hands each window to the runner, and commits each returned window before continuing. Temporal/Dagster/Prefect runners remain the next execution-substrate milestone.

### Phase 4 — Memory and knowledge-base asset types

**Goal:** Realize the unified "context build system" vision, incrementally.

- Streaming source mode (long-running watch; incremental materialization without full re-plan).
- Built-in asset types for memory: `Episode`, `ConsolidatedMemory`, `DecayingChunk`; `decaying` and `versioned` materializations (half-life, consolidation threshold, salience-based forgetting).
- Built-in asset types for KG: `Entity`, `KGTriple`, `Community`; a "GraphRAG with proper incremental updates" recipe (a direct wedge against rebuild-everything graph tools).
- `Retriever` as a versioned artifact; changing a reranker/hybrid-weight invalidates downstream evaluation results.

**Exit criterion:** The same DAG can express a RAG corpus, an agent memory store, and a knowledge graph, with lineage and selective reprocessing across all three.

### Phase 5 — Commercial layer (optional, conditional)

Only if OSS adoption signals demand. A hosted control plane: managed manifest store, multi-team workspaces, scheduled apply, hosted lineage UI, RBAC, audit logs. Architecturally mirrors the dbt → dbt Cloud split. Pricing on seats or asset volume, deliberately not per-page (that is the parser vendors' model). Out of scope until Phase 1–3 have real users.

---

## 8. Locked pre-build decisions

These decisions are locked for Phase 0 and should be treated as the implementation baseline.

1. **Plan is quick, pure, and honest.** The planner does not run transforms. For fanout assets, it emits coarse scoped operations and marks exact child counts as unknown or estimated.

2. **Apply materializes plugin outputs.** The runtime turns coarse operations into input groups, runs operation plugins through the selected runner, flatmaps returned outputs into exact asset instances, and records lineage.

3. **State is scoped by project and tenant.** Every project-scoped state table includes required `project_id` and `tenant_id`, defaulting both to `"default"`. A tenant can have multiple projects.

4. **Apply locks are scoped by `(project_id, tenant_id)`.** Multiple applies may run for different project/tenant scopes, but only one active apply may run for the same scope.

5. **Runtime state table is `asset_instances`.** `materialization_strategy` is a manifest-level asset setting. Runtime rows are asset instance state records with artifact metadata attached for Phase 0.

6. **Instance keys prefer stability over reuse optimization.** Phase 0 fanout keys use stable logical keys such as `parent_instance_key + ordinal`. Chunk content hashes are stored separately as metadata for future embedding reuse.

7. **Fingerprints use canonical JSON + SHA-256.** Fingerprint payloads include `algorithm_version = 1`, sorted object keys, explicit nulls, and lexicographically sorted upstream fingerprints.

8. **Transform versions are developer-declared.** `version` is mandatory. Config hashing is automatic, including content hashes for referenced files. `code_hash` is advisory only.

9. **Deletes are tombstoned in Strata and removed from sinks.** Confirmed source deletes remove sink rows immediately, mark downstream asset instances `deleted`, and retain lineage/tombstones. Non-authoritative missing source items can enter quarantine before confirmed delete.

10. **Operations are typed coarse scopes.** Phase 0 operations are primarily `build_scope` and `delete_scope` with explicit scope, reason, dependencies, estimated count, and estimated cost.

11. **Run and operation history exist from the start.** `runs` and `operation_runs` are part of Phase 0, not later observability work.

12. **Apply-expanded work items exist from the start.** `operation_items` are the durable per-run records for source/input-group/output work discovered during apply. They power progress, retry, and resume, but they are not reusable artifacts.

13. **Source checkpoints are first-class state.** Connector cursors and scope hashes live in `source_checkpoints`. Per-item hashes and delete markers live in `source_state`.

14. **Nondeterministic transforms reuse cache by default.** They are labeled in plan output. Explicit rerun/invalidate commands handle refresh and evaluation workflows later.

15. **Phase 0 validates against a minimal local RAG fixture.** The first workload is local files, text/markdown parsing, deterministic chunking, fake deterministic embeddings, SQLite state, and local sink output.

---

## 9. Risks and mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| A turnkey RAG framework adds config-version-aware invalidation + selectors, narrowing the wedge to ergonomics | Medium | Stay substrate- and executor-neutral; ship selectors and plan/apply faster; lean into memory + KG asset types they will not prioritize |
| Warehouse-native ingestion (Cortex / Mosaic-style) absorbs warehouse-resident workloads | Medium | Lead with non-warehouse data (SharePoint, Drive, Notion, GitHub, S3-of-PDFs) where the warehouse path does not reach |
| "Content-addressed lineage" is too abstract to sell | Medium | Lead with concrete pain narratives ("change the chunker, reprocess 42 docs not 12,000"), never with correctness theory |
| Framework fatigue; developers skeptical of one more pipeline tool | High | Ruthlessly minimal core API; library-first; integrate *into* existing orchestrators rather than replacing them |
| Building the framework before validating the need | Medium | Phase 0 ships value with a minimal fixture first, then a real workload; only formalize the broader abstraction after real usage |
| Over-generalizing from a single first workload | Medium | Keep the host-specific glue out of the core package from day one; maintain a "what I'd do differently as a library" notes file; do not extract before 6–9 months of production use |
| Streaming/batch boundary is hard | Medium | Ship batch first; design the source contract to allow streaming but defer streaming execution to Phase 4 |
| Scope creep into agent runtime / parsing / vector storage | High | Enforce the non-goals; the core stays small; edges are plugins |

---

## 10. Success criteria

**Technical (Phase 1–2):**

- Define a RAG ingest pipeline in under 50 lines of Python.
- A single-document source change reprocesses only that document's lineage, verifiably.
- A chunker config change reprocesses only affected chunks/embeddings, reusing parses from cache.
- Full per-instance lineage trace browsable in the docs UI.
- Planner logic covered by unit tests with zero infrastructure dependencies.

**Adoption (if pursued as an OSS project):**

- A first production workload running on Strata.
- An unsolicited external request to use it (the strongest signal that extraction/OSS release is warranted).
- Early-stage OSS traction comparable to dbt/Dagster/DVC at equivalent maturity.

**Strategic gates (any one triggers a re-think):**

- No clear 5× concrete-pain story over existing turnkey ingestion within 90 days of Phase 1 → ship as a library and stop.
- A turnkey framework ships config-version-aware invalidation + selectors before Strata's Phase 2 → pivot to differentiated memory/KG + executor-integration framing.
- Warehouse vendors ship per-chunk lineage → re-target to non-warehouse data and accept a smaller addressable set.

---

## 11. Appendix — relationship to existing tools

- **dbt** — the ergonomic and conceptual model (manifest, selectors, plan/apply, tests, docs). Strata copies the developer experience; it differs in operating on unstructured content at per-document/per-chunk granularity rather than SQL tables, and in not being warehouse-bound.
- **Dagster** — the closest existing analog (first-class assets, code-versioned data versions, dependency-aware reactivity). Strata goes *below* the asset granularity (per chunk) and targets content specifically. Strata integrates with Dagster as an executor rather than competing.
- **Temporal** — an execution substrate, not a competitor. Strata delegates durability, retries, and scheduling to it via an executor adapter.
- **LlamaIndex IngestionPipeline** — the closest in spirit (per-node+transform caching, source-hash docstore upsert). Strata differs by making invalidation transform-and-config-version-aware, adding the selector grammar, plan/apply, lineage UI, and substrate neutrality.
- **Pachyderm** — proved the general-purpose content-addressed pipeline is commercially hard. Strata's bet is that domain specificity (RAG/memory/KG) changes the adoption curve.
- **Parsers (LiteParse, LlamaParse, Reducto, Docling, Unstructured, Marker)** — plugins, not competitors. Strata version-pins and orchestrates them.
