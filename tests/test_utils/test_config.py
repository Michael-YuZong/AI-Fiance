"""Tests for asset type detection helpers."""

from __future__ import annotations

from src.utils.config import load_config, detect_asset_type


def test_detect_asset_type_prefers_alias_for_index_symbols() -> None:
    config = load_config()
    assert detect_asset_type("000300", config) == "cn_index"


def test_detect_asset_type_uses_fund_heuristic_for_non_etf_codes() -> None:
    config = load_config()
    assert detect_asset_type("022365", config) == "cn_fund"
    assert detect_asset_type("512660", config) == "cn_etf"
