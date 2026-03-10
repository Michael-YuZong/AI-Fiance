"""Tests for assistant routing helpers."""

from __future__ import annotations

from src.commands.assistant import _has_explicit_symbol, _should_resolve_assets


def test_should_resolve_assets_for_nasdaq_keyword() -> None:
    assert _should_resolve_assets("分析一下纳斯达克")


def test_should_resolve_assets_for_space_and_index_keywords() -> None:
    assert _should_resolve_assets("分析一下商业航天")
    assert _should_resolve_assets("分析一下沪深300")
    assert _should_resolve_assets("分析一下中证A500")


def test_has_explicit_symbol_for_open_fund_code() -> None:
    assert _has_explicit_symbol("分析一下基金022365")
