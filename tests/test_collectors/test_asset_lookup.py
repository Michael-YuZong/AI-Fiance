"""Tests for asset alias lookup and natural-language resolution."""

from __future__ import annotations

import pandas as pd

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


def test_asset_lookup_uses_live_fund_name_search_when_alias_missing(monkeypatch):
    collector = AssetLookupCollector({"asset_aliases_file": "config/asset_aliases.yaml"})

    def fake_cached_call(cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        if cache_key == "asset_lookup:fund_name_em":
            return pd.DataFrame(
                [
                    {"基金代码": "159995", "基金简称": "芯片ETF", "基金类型": "指数型"},
                    {"基金代码": "012345", "基金简称": "芯片联接A", "基金类型": "混合型"},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    matches = collector.search("芯片ETF", limit=5)
    assert matches
    assert matches[0]["symbol"] == "159995"
