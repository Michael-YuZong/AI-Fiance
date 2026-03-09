"""Tests for assistant command helpers."""

from __future__ import annotations

from src.commands.assistant import _detect_news_sources, _should_resolve_assets


def test_assistant_detects_requested_news_sources():
    sources = _detect_news_sources("帮我写晨报，要有路透和彭博的消息")
    assert "Reuters" in sources
    assert "Bloomberg" in sources


def test_assistant_only_resolves_assets_for_asset_like_requests():
    assert _should_resolve_assets("分析一下有色金属ETF")
    assert not _should_resolve_assets("帮我写今天的晨报 要有路透和彭博的消息")
