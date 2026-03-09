"""Tests for briefing markdown rendering."""

from __future__ import annotations

from src.output.briefing import BriefingRenderer


def test_briefing_renderer_outputs_core_sections():
    payload = {
        "title": "每日晨报",
        "generated_at": "2026-03-09 07:30:00",
        "macro_items": ["中国 PMI 49.0。"],
        "watchlist_rows": [["561380", "2.234", "+1.00%", "+2.00%", "+3.00%", "多头"]],
        "alerts": ["561380 放量。"],
        "portfolio_lines": ["组合市值 100000。"],
    }
    rendered = BriefingRenderer().render(payload)
    assert "# 每日晨报" in rendered
    assert "## Watchlist 概览" in rendered
    assert "561380" in rendered
    assert "组合市值 100000。" in rendered
