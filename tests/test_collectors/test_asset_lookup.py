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


def test_asset_lookup_matches_theme_alias_for_commercial_space():
    collector = AssetLookupCollector({"asset_aliases_file": "config/asset_aliases.yaml"})
    best = collector.resolve_best("商业航天")
    assert best is not None
    assert best["symbol"] == "159218"


def test_asset_lookup_matches_exact_symbol_alias_for_index():
    collector = AssetLookupCollector({"asset_aliases_file": "config/asset_aliases.yaml"})
    best = collector.resolve_best("000300")
    assert best is not None
    assert best["asset_type"] == "cn_index"


def test_asset_lookup_uses_tushare_open_end_fund_search_when_alias_missing(monkeypatch):
    collector = AssetLookupCollector({"asset_aliases_file": "config/asset_aliases.yaml"})

    def fake_fund_basic_snapshot(market: str) -> pd.DataFrame:
        if market == "O":
            return pd.DataFrame(
                [
                    {"ts_code": "012345.OF", "name": "联接A测试", "fund_type": "混合型"},
                    {"ts_code": "021740.OF", "name": "前海开源黄金ETF联接C", "fund_type": "指数型"},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_fund_basic_snapshot", fake_fund_basic_snapshot)
    monkeypatch.setattr(collector, "_ts_call", lambda *args, **kwargs: pd.DataFrame())
    matches = collector.search("联接A测试", limit=5)
    assert matches
    assert matches[0]["symbol"] == "012345"
    assert matches[0]["asset_type"] == "cn_fund"


def test_asset_lookup_keeps_true_tushare_etf_as_cn_etf(monkeypatch):
    collector = AssetLookupCollector({"asset_aliases_file": "config/asset_aliases.yaml"})

    def fake_fund_basic_snapshot(market: str) -> pd.DataFrame:
        if market == "E":
            return pd.DataFrame([{"ts_code": "512660.SH", "name": "国泰中证军工ETF", "fund_type": "指数型-股票"}])
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_fund_basic_snapshot", fake_fund_basic_snapshot)
    monkeypatch.setattr(collector, "_ts_call", lambda *args, **kwargs: pd.DataFrame())
    matches = collector.search("军工ETF", limit=5)
    assert matches
    assert matches[0]["asset_type"] == "cn_etf"
