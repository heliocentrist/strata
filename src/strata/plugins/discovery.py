from __future__ import annotations

from collections.abc import Sequence
from importlib.metadata import entry_points
from typing import Any

from strata.plugins.protocols import AdapterMetadata, ExternalPluginDiscoveryResult
from strata.plugins.registry import Registry, chunkers, embedders, parsers, sinks, sources

ENTRY_POINT_GROUPS: dict[str, tuple[str, Registry[Any], tuple[str, ...]]] = {
    "strata.sources": ("source", sources, ("source",)),
    "strata.parsers": ("parser", parsers, ("parsed",)),
    "strata.chunkers": ("chunker", chunkers, ("chunks",)),
    "strata.embedders": ("embedding", embedders, ("embeddings",)),
    "strata.sinks": ("sink", sinks, ("sink",)),
}

_external_plugins_discovered = False

__all__ = ["ENTRY_POINT_GROUPS", "discover_external_plugins", "entry_points"]


def discover_external_plugins(*, force: bool = False) -> ExternalPluginDiscoveryResult:
    global _external_plugins_discovered
    if _external_plugins_discovered and not force:
        return ExternalPluginDiscoveryResult()

    loaded: list[AdapterMetadata] = []
    errors: list[str] = []
    discovered = entry_points()
    for group, (kind, registry, supported_asset_kinds) in ENTRY_POINT_GROUPS.items():
        group_entries: Sequence[Any] = discovered.select(group=group)
        for entry_point in group_entries:
            try:
                adapter = entry_point.load()
                if isinstance(adapter, type):
                    adapter = adapter()
                adapter_metadata = AdapterMetadata(
                    name=entry_point.name,
                    kind=kind,
                    source=f"entry_point:{group}:{entry_point.value}",
                    supported_asset_kinds=supported_asset_kinds,
                )
                registry.register(entry_point.name, adapter, metadata=adapter_metadata)
                loaded.append(adapter_metadata)
            except Exception as exc:  # keep discovery best-effort across plugins
                errors.append(f"{group}:{entry_point.name}: {exc}")

    _external_plugins_discovered = True
    return ExternalPluginDiscoveryResult(loaded=loaded, errors=errors)
