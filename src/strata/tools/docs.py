# ruff: noqa: E501

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from strata.core.models import Manifest
from strata.state.repository import StateRepository
from strata.tools.inspect import _artifact_summary, _asset_summary, _state_rows


@dataclass(frozen=True)
class DocsBuildResult:
    output_path: Path
    instance_count: int
    edge_count: int


def build_docs_site(
    *,
    manifest: Manifest,
    repo: StateRepository,
    output_path: Path,
    preview_chars: int = 320,
) -> DocsBuildResult:
    output_path.mkdir(parents=True, exist_ok=True)
    rows, edge_rows = _state_rows(repo)
    rows = sorted(rows, key=lambda row: (str(row["asset_name"]), str(row["instance_key"])))
    by_id = {str(row["id"]): row for row in rows}
    edges = [
        {
            "upstream_id": str(edge["upstream_asset_instance_id"]),
            "downstream_id": str(edge["downstream_asset_instance_id"]),
        }
        for edge in edge_rows
        if str(edge["upstream_asset_instance_id"]) in by_id
        and str(edge["downstream_asset_instance_id"]) in by_id
    ]
    instances = [
        _docs_instance(
            manifest=manifest,
            repo=repo,
            row=row,
            edges=edges,
            preview_chars=preview_chars,
        )
        for row in rows
    ]
    data = {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "project": {
            "project_id": manifest.context.project_id,
            "tenant_id": manifest.context.tenant_id,
            "manifest_hash": manifest.manifest_hash,
            "asset_order": manifest.asset_order,
        },
        "counts": _counts(instances),
        "instances": instances,
        "edges": edges,
    }
    (output_path / "data.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_path / "index.html").write_text(_INDEX_HTML, encoding="utf-8", newline="\n")
    (output_path / "styles.css").write_text(_STYLES_CSS, encoding="utf-8", newline="\n")
    (output_path / "app.js").write_text(_APP_JS, encoding="utf-8", newline="\n")
    return DocsBuildResult(
        output_path=output_path,
        instance_count=len(instances),
        edge_count=len(edges),
    )


def serve_docs_site(*, directory: Path, host: str, port: int) -> None:
    handler = partial(SimpleHTTPRequestHandler, directory=str(directory))
    with ThreadingHTTPServer((host, port), handler) as server:
        server.serve_forever()


def clean_docs_site(output_path: Path) -> None:
    if output_path.exists():
        shutil.rmtree(output_path)


def _docs_instance(
    *,
    manifest: Manifest,
    repo: StateRepository,
    row: dict[str, Any],
    edges: list[dict[str, str]],
    preview_chars: int,
) -> dict[str, Any]:
    asset = _asset_summary(row)
    artifact = _artifact_summary(
        manifest=manifest,
        repo=repo,
        row=row,
        preview_chars=preview_chars,
    )
    upstream_ids = [
        edge["upstream_id"] for edge in edges if edge["downstream_id"] == str(row["id"])
    ]
    downstream_ids = [
        edge["downstream_id"] for edge in edges if edge["upstream_id"] == str(row["id"])
    ]
    source_item_key = _source_item_key(asset, artifact)
    instance = {
        "id": str(row["id"]),
        "asset_name": asset["asset_name"],
        "instance_key": asset["instance_key"],
        "status": asset["status"],
        "input_fingerprint": asset["input_fingerprint"],
        "content_hash": asset["content_hash"],
        "output_hash": asset["output_hash"],
        "output_location": asset["output_location"],
        "materialization_strategy": asset["materialization_strategy"],
        "metadata": asset["metadata"],
        "transform": asset["transform"],
        "artifact": artifact,
        "source_item_key": source_item_key,
        "upstream_ids": upstream_ids,
        "downstream_ids": downstream_ids,
        "search_text": "",
    }
    instance["search_text"] = _search_text(instance)
    return instance


def _source_item_key(asset: dict[str, Any], artifact: dict[str, Any]) -> str | None:
    metadata = asset.get("metadata", {})
    if isinstance(metadata, dict) and metadata.get("source_item_key"):
        return str(metadata["source_item_key"])
    source = artifact.get("source", {})
    if isinstance(source, dict) and source.get("item_key"):
        return str(source["item_key"])
    if isinstance(metadata, dict) and metadata.get("chunk_instance_key"):
        chunk_key = str(metadata["chunk_instance_key"])
        return chunk_key.split("#chunk:", 1)[0]
    return None


def _search_text(instance: dict[str, Any]) -> str:
    artifact = instance.get("artifact", {})
    parts = [
        instance.get("asset_name"),
        instance.get("instance_key"),
        instance.get("source_item_key"),
        instance.get("input_fingerprint"),
        instance.get("content_hash"),
        instance.get("transform", {}).get("version"),
        artifact.get("preview"),
        artifact.get("chunk_preview"),
    ]
    return " ".join(str(part) for part in parts if part).lower()


def _counts(instances: list[dict[str, Any]]) -> dict[str, Any]:
    by_asset: dict[str, int] = {}
    by_status: dict[str, int] = {}
    for instance in instances:
        by_asset[str(instance["asset_name"])] = by_asset.get(str(instance["asset_name"]), 0) + 1
        by_status[str(instance["status"])] = by_status.get(str(instance["status"]), 0) + 1
    return {
        "total": len(instances),
        "by_asset": by_asset,
        "by_status": by_status,
    }


_INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Strata Docs</title>
  <link rel="icon" href="data:,">
  <link rel="stylesheet" href="styles.css">
</head>
<body>
  <div class="app-shell">
    <aside class="sidebar">
      <div class="brand">
        <div class="mark">S</div>
        <div>
          <h1>Strata Docs</h1>
          <p id="projectLine">Loading project</p>
        </div>
      </div>
      <div class="search-block">
        <label for="searchInput">Search</label>
        <input id="searchInput" type="search" placeholder="asset, file, hash, preview">
      </div>
      <div>
        <div class="section-title">Assets</div>
        <div id="assetFilters" class="filter-list"></div>
      </div>
      <div>
        <div class="section-title">Status</div>
        <div id="statusFilters" class="filter-list"></div>
      </div>
    </aside>

    <main class="content">
      <header class="summary-strip">
        <div>
          <span class="metric-label">Instances</span>
          <strong id="totalCount">0</strong>
        </div>
        <div>
          <span class="metric-label">Visible</span>
          <strong id="visibleCount">0</strong>
        </div>
        <div>
          <span class="metric-label">Edges</span>
          <strong id="edgeCount">0</strong>
        </div>
        <div class="hash-line" id="manifestHash"></div>
      </header>

      <section class="workspace">
        <div class="list-pane">
          <div class="pane-head">
            <h2>Asset Instances</h2>
            <button id="clearButton" type="button">Clear</button>
          </div>
          <div id="resultList" class="result-list"></div>
        </div>

        <article class="detail-pane">
          <div id="emptyState" class="empty-state">
            <h2>Select an instance</h2>
            <p>Browse assets or search for a filename, chunk key, transform, hash, or text preview.</p>
          </div>
          <div id="detailView" class="detail-view hidden"></div>
        </article>
      </section>
    </main>
  </div>
  <script src="app.js"></script>
</body>
</html>
"""


_STYLES_CSS = r""":root {
  --bg: #f5f3ee;
  --ink: #161616;
  --muted: #66645d;
  --line: #d7d1c3;
  --panel: #fffcf4;
  --panel-2: #ebe5d6;
  --accent: #0f766e;
  --accent-2: #b42318;
  --code: #24211b;
}

* { box-sizing: border-box; }

body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font-family: "Aptos", "Segoe UI", sans-serif;
}

.app-shell {
  display: grid;
  grid-template-columns: 320px minmax(0, 1fr);
  height: 100vh;
  overflow: hidden;
}

.sidebar {
  border-right: 1px solid var(--line);
  padding: 24px;
  background: var(--panel);
  display: flex;
  flex-direction: column;
  gap: 28px;
}

.brand {
  display: grid;
  grid-template-columns: 44px 1fr;
  gap: 12px;
  align-items: center;
}

.mark {
  width: 44px;
  height: 44px;
  border: 2px solid var(--ink);
  display: grid;
  place-items: center;
  font-family: Georgia, serif;
  font-weight: 700;
  font-size: 24px;
}

h1, h2, h3, p { margin: 0; }
h1 { font-size: 20px; }
h2 { font-size: 16px; }
h3 { font-size: 13px; text-transform: uppercase; letter-spacing: 0; color: var(--muted); }
p { color: var(--muted); line-height: 1.45; }

.search-block { display: flex; flex-direction: column; gap: 8px; }
label, .section-title, .metric-label {
  font-size: 11px;
  text-transform: uppercase;
  color: var(--muted);
  font-weight: 700;
}

input {
  height: 40px;
  border: 1px solid var(--line);
  background: #fff;
  color: var(--ink);
  padding: 0 12px;
  font: inherit;
}

.filter-list { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }
button {
  border: 1px solid var(--line);
  background: #fff;
  color: var(--ink);
  min-height: 32px;
  padding: 6px 10px;
  font: inherit;
  cursor: pointer;
}
button.active { background: var(--ink); color: #fff; border-color: var(--ink); }

.content { min-width: 0; min-height: 0; display: flex; flex-direction: column; overflow: hidden; }
.summary-strip {
  min-height: 72px;
  border-bottom: 1px solid var(--line);
  background: var(--panel);
  display: grid;
  grid-template-columns: 120px 120px 120px 1fr;
  gap: 16px;
  align-items: center;
  padding: 12px 24px;
}
.summary-strip > div { min-width: 0; }
.summary-strip strong { display: block; font-size: 24px; line-height: 1; margin-top: 5px; }
.hash-line {
  color: var(--muted);
  font-family: "Cascadia Mono", Consolas, monospace;
  font-size: 12px;
  text-align: right;
  overflow-wrap: anywhere;
}

.workspace {
  display: grid;
  grid-template-columns: minmax(360px, 44%) minmax(0, 1fr);
  height: calc(100vh - 75px);
  min-height: 0;
  flex: none;
  overflow: hidden;
}
.list-pane {
  border-right: 1px solid var(--line);
  min-width: 0;
  min-height: 0;
  height: 100%;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}
.pane-head {
  height: 56px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0 18px;
  border-bottom: 1px solid var(--line);
}
.result-list { overflow: auto; min-height: 0; }
.result-item {
  width: 100%;
  display: grid;
  grid-template-columns: 92px minmax(0, 1fr);
  gap: 12px;
  text-align: left;
  padding: 13px 18px;
  border: 0;
  border-bottom: 1px solid var(--line);
  background: transparent;
}
.result-item:hover, .result-item.selected { background: var(--panel-2); color: var(--ink); }
.asset-chip { color: var(--accent); font-weight: 700; font-size: 12px; }
.instance-title {
  font-family: "Cascadia Mono", Consolas, monospace;
  overflow-wrap: anywhere;
  font-size: 12px;
}
.instance-sub { color: var(--muted); font-size: 12px; margin-top: 4px; }

.detail-pane { min-width: 0; min-height: 0; height: 100%; padding: 24px; overflow: auto; }
.hidden { display: none; }
.empty-state {
  min-height: 300px;
  border: 1px dashed var(--line);
  display: grid;
  place-content: center;
  gap: 10px;
  text-align: center;
}
.detail-view { display: flex; flex-direction: column; gap: 18px; }
.detail-header {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: start;
}
.detail-title {
  font-family: "Cascadia Mono", Consolas, monospace;
  overflow-wrap: anywhere;
  font-size: 18px;
}
.status-pill {
  border: 1px solid var(--accent);
  color: var(--accent);
  padding: 4px 8px;
  font-size: 12px;
}
.kv-grid {
  display: grid;
  grid-template-columns: 160px minmax(0, 1fr);
  border-top: 1px solid var(--line);
}
.kv-grid div {
  border-bottom: 1px solid var(--line);
  padding: 9px 0;
  overflow-wrap: anywhere;
}
.kv-grid div:nth-child(odd) { color: var(--muted); font-size: 12px; }
.preview {
  background: var(--code);
  color: #f7f0df;
  padding: 16px;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  font-family: "Cascadia Mono", Consolas, monospace;
  font-size: 12px;
  line-height: 1.55;
}
.lineage-list { display: flex; flex-direction: column; gap: 8px; }
.lineage-item {
  border: 1px solid var(--line);
  background: var(--panel);
  padding: 10px;
  cursor: pointer;
}
.lineage-item:hover { border-color: var(--accent); }

@media (max-width: 1100px) {
  .app-shell, .workspace { grid-template-columns: 1fr; }
  .app-shell { height: auto; min-height: 100vh; overflow: visible; }
  .content, .workspace { height: auto; overflow: visible; }
  .sidebar, .list-pane { border-right: 0; }
  .result-list { max-height: 440px; }
  .summary-strip { grid-template-columns: 1fr 1fr; }
  .hash-line { grid-column: 1 / -1; text-align: left; }
}
"""


_APP_JS = r"""let docs = null;
let selectedAsset = "all";
let selectedStatus = "all";
let selectedId = null;

const $ = (id) => document.getElementById(id);

fetch("data.json")
  .then((response) => response.json())
  .then((data) => {
    docs = data;
    selectedId = data.instances[0]?.id || null;
    initialize();
    render();
  });

function initialize() {
  $("projectLine").textContent = `${docs.project.project_id} / ${docs.project.tenant_id}`;
  $("totalCount").textContent = docs.counts.total;
  $("edgeCount").textContent = docs.edges.length;
  $("manifestHash").textContent = docs.project.manifest_hash;
  $("searchInput").addEventListener("input", render);
  $("clearButton").addEventListener("click", () => {
    $("searchInput").value = "";
    selectedAsset = "all";
    selectedStatus = "all";
    render();
  });
  renderFilters();
}

function renderFilters() {
  renderFilterList("assetFilters", ["all", ...Object.keys(docs.counts.by_asset)], selectedAsset, (value) => {
    selectedAsset = value;
    render();
  });
  renderFilterList("statusFilters", ["all", ...Object.keys(docs.counts.by_status)], selectedStatus, (value) => {
    selectedStatus = value;
    render();
  });
}

function renderFilterList(targetId, values, selected, onSelect) {
  const target = $(targetId);
  target.innerHTML = "";
  values.forEach((value) => {
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = value;
    button.className = value === selected ? "active" : "";
    button.addEventListener("click", () => onSelect(value));
    target.appendChild(button);
  });
}

function filteredInstances() {
  const query = $("searchInput").value.trim().toLowerCase();
  return docs.instances.filter((instance) => {
    if (selectedAsset !== "all" && instance.asset_name !== selectedAsset) return false;
    if (selectedStatus !== "all" && instance.status !== selectedStatus) return false;
    if (query && !instance.search_text.includes(query)) return false;
    return true;
  });
}

function render() {
  renderFilters();
  const instances = filteredInstances();
  $("visibleCount").textContent = instances.length;
  if (!instances.some((instance) => instance.id === selectedId)) {
    selectedId = instances[0]?.id || null;
  }
  renderList(instances);
  renderDetail();
}

function renderList(instances) {
  const list = $("resultList");
  list.innerHTML = "";
  instances.forEach((instance) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `result-item ${instance.id === selectedId ? "selected" : ""}`;
    button.innerHTML = `
      <span class="asset-chip">${escapeHtml(instance.asset_name)}</span>
      <span>
        <span class="instance-title">${escapeHtml(instance.instance_key)}</span>
        <span class="instance-sub">${escapeHtml(instance.transform.version)} - ${shortHash(instance.input_fingerprint)}</span>
      </span>
    `;
    button.addEventListener("click", () => {
      selectedId = instance.id;
      render();
    });
    list.appendChild(button);
  });
}

function renderDetail() {
  const instance = docs.instances.find((item) => item.id === selectedId);
  $("emptyState").classList.toggle("hidden", Boolean(instance));
  $("detailView").classList.toggle("hidden", !instance);
  if (!instance) return;

  const artifact = instance.artifact || {};
  const upstreams = instance.upstream_ids.map(findById).filter(Boolean);
  const downstreams = instance.downstream_ids.map(findById).filter(Boolean);
  $("detailView").innerHTML = `
    <div class="detail-header">
      <div>
        <h2 class="detail-title">${escapeHtml(instance.instance_key)}</h2>
        <p>${escapeHtml(instance.asset_name)} - ${escapeHtml(instance.transform.version)}</p>
      </div>
      <span class="status-pill">${escapeHtml(instance.status)}</span>
    </div>
    ${kvGrid([
      ["input fingerprint", instance.input_fingerprint],
      ["content hash", instance.content_hash || ""],
      ["output hash", instance.output_hash || ""],
      ["source item", instance.source_item_key || ""],
      ["output location", instance.output_location || ""],
      ["config hash", instance.transform.config_hash],
      ["determinism", instance.transform.determinism],
      ["artifact kind", artifact.kind || ""],
      ["artifact length", artifact.length ?? artifact.embedding_dimensions ?? ""],
    ])}
    ${previewBlock(artifact)}
    ${lineageBlock("Upstreams", upstreams)}
    ${lineageBlock("Downstreams", downstreams)}
  `;
}

function kvGrid(rows) {
  return `<section><h3>Identity</h3><div class="kv-grid">${rows.map(([key, value]) => `
    <div>${escapeHtml(key)}</div><div>${escapeHtml(String(value))}</div>
  `).join("")}</div></section>`;
}

function previewBlock(artifact) {
  const preview = artifact.preview || artifact.chunk_preview || "";
  if (!preview) return "";
  return `<section><h3>Preview</h3><pre class="preview">${escapeHtml(preview)}</pre></section>`;
}

function lineageBlock(title, items) {
  if (!items.length) return `<section><h3>${title}</h3><p>No linked instances.</p></section>`;
  return `<section><h3>${title}</h3><div class="lineage-list">${items.map((item) => `
    <div class="lineage-item" onclick="selectInstance('${item.id}')">
      <strong>${escapeHtml(item.asset_name)}</strong>
      <div class="instance-title">${escapeHtml(item.instance_key)}</div>
      <div class="instance-sub">${escapeHtml(item.transform.version)} - ${shortHash(item.input_fingerprint)}</div>
    </div>
  `).join("")}</div></section>`;
}

function selectInstance(id) {
  selectedId = id;
  render();
}

function findById(id) {
  return docs.instances.find((instance) => instance.id === id);
}

function shortHash(value) {
  return value ? value.slice(0, 12) : "";
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
"""
