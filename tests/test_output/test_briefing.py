"""Tests for briefing markdown rendering."""

from __future__ import annotations

from src.output.briefing import BriefingRenderer


def test_briefing_renderer_outputs_core_sections():
    payload = {
        "title": "每日晨报",
        "generated_at": "2026-03-09 07:30:00",
        "headline_lines": ["主线偏防守。"],
        "macro_items": ["中国 PMI 49.0。"],
        "market_overview_lines": ["watchlist 整体偏中性。"],
        "watchlist_rows": [["561380", "2.234", "+1.00%", "+2.00%", "+3.00%", "多头", "1.20"]],
        "focus_lines": ["561380 趋势偏强。"],
        "rotation_lines": ["电网强于科技。"],
        "alerts": ["561380 放量。"],
        "portfolio_lines": ["组合市值 100000。"],
        "calendar_lines": ["盘前看强弱延续。"],
        "action_lines": ["优先跟踪 561380。"],
    }
    rendered = BriefingRenderer().render(payload)
    assert "# 每日晨报" in rendered
    assert "## 今日主线" in rendered
    assert "## Watchlist 雷达" in rendered
    assert "## 行动建议" in rendered
    assert "561380" in rendered
    assert "组合市值 100000。" in rendered
