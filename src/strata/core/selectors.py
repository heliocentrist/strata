from __future__ import annotations

from pydantic import BaseModel, Field

from strata.core.models import Manifest


class Selection(BaseModel):
    assets: set[str]
    source_names: set[str] | None = None


class SelectorTerm(BaseModel):
    assets: set[str] = Field(default_factory=set)
    source_names: set[str] | None = None


def parse_selection(manifest: Manifest, selector: str | None) -> Selection:
    if selector is None or not selector.strip():
        return Selection(assets=set(manifest.asset_order))

    selected_assets: set[str] = set()
    selected_sources: set[str] | None = None
    for raw_term in selector.split(","):
        term = raw_term.strip()
        if not term:
            continue
        parsed = _parse_term(manifest, term)
        selected_assets.update(parsed.assets)
        if parsed.source_names is not None:
            selected_sources = (selected_sources or set()) | parsed.source_names

    if not selected_assets:
        raise ValueError(f"empty selector: {selector}")
    return Selection(assets=selected_assets, source_names=selected_sources)


def _parse_term(manifest: Manifest, term: str) -> SelectorTerm:
    if term.startswith("source:"):
        return _parse_source_term(manifest, term)
    return SelectorTerm(assets=_expand_asset_selector(manifest, term))


def _parse_source_term(manifest: Manifest, term: str) -> SelectorTerm:
    include_downstream = term.endswith("+")
    source_name = term.removeprefix("source:")
    if include_downstream:
        source_name = source_name[:-1]
    if not source_name:
        raise ValueError(f"invalid source selector: {term}")
    if source_name not in manifest.sources:
        raise ValueError(f"unknown source selector: {source_name}")
    return SelectorTerm(
        assets=set(manifest.asset_order) if include_downstream else {"parsed"},
        source_names={source_name},
    )


def _expand_asset_selector(manifest: Manifest, term: str) -> set[str]:
    include_upstream = term.startswith("+")
    include_downstream = term.endswith("+")
    asset_name = term.strip("+")
    if not asset_name:
        raise ValueError(f"invalid asset selector: {term}")
    if asset_name not in manifest.assets:
        raise ValueError(f"unknown asset selector: {asset_name}")

    index = manifest.asset_order.index(asset_name)
    if include_upstream and include_downstream:
        return set(manifest.asset_order)
    if include_upstream:
        return set(manifest.asset_order[: index + 1])
    if include_downstream:
        return set(manifest.asset_order[index:])
    return {asset_name}
