"""Tests for asset alias lookup and natural-language resolution."""

from __future__ import annotations

from src.collectors.asset_lookup import AssetLookupCollector


def test_asset_lookup_matches_alias_from_sentence():
    collector = AssetLookupCollector({"asset_aliases_file": "config/asset_aliases.yaml"})
    matches = collector.search("分析一下有色金属ETF", limit=5)
    assert matches
    assert matches[0]["symbol"] == "512400"


def test_asset_lookup_resolve_best_returns_unique_match():
    collector = AssetLookupCollector({"asset_aliases_file": "config/asset_aliases.yaml"})
    best = collector.resolve_best("有色金属ETF")
    assert best is not None
    assert best["symbol"] == "512400"
