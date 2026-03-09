"""Tests for briefing markdown rendering."""

from __future__ import annotations

from src.output.briefing import BriefingRenderer


def test_briefing_renderer_outputs_core_sections():
    payload = {
        "title": "每日晨报",
        "generated_at": "2026-03-09 07:30:00",
        "headline_lines": ["主线偏防守。"],
        "important_event_lines": ["美联储与利率预期: 市场等待 CPI。"],
        "news_lines": ["[能源与地缘] 今日重点看原油与黄金。"],
        "story_lines": ["今天市场更像在交易油价冲击。"],
        "rotation_driver_lines": ["行业轮动靠前: 电力(+3.20%)。"],
        "main_flow_driver_lines": ["全市场主力资金最新为 `净流出` 12.30亿。"],
        "market_pulse_lines": ["A股全市场热度: 涨停 40 家，跌停 3 家。"],
        "lhb_lines": ["机构净买额靠前: 兖矿能源(6.53亿)。"],
        "impact_lines": ["港股科技更容易承压。"],
        "monitor_lines": ["布伦特原油 108.860，1日 +1.00%，5日 +5.00%。"],
        "overnight_lines": ["科技方向偏弱。"],
        "macro_items": ["中国 PMI 49.0。"],
        "market_overview_lines": ["watchlist 整体偏中性。"],
        "flow_lines": ["全球资金流偏防守。"],
        "sentiment_lines": ["561380 情绪中性。"],
        "watchlist_rows": [["561380", "2.234", "+1.00%", "+2.00%", "+3.00%", "多头", "1.20"]],
        "focus_lines": ["561380 趋势偏强。"],
        "rotation_lines": ["电网强于科技。"],
        "alerts": ["561380 放量。"],
        "event_lines": ["09:00 [高] A股盘前检查。"],
        "portfolio_lines": ["组合市值 100000。"],
        "verification_lines": ["先看原油是否继续扩张涨幅。"],
        "calendar_lines": ["盘前看强弱延续。"],
        "action_lines": ["优先跟踪 561380。"],
    }
    rendered = BriefingRenderer().render(payload)
    assert "# 每日晨报" in rendered
    assert "## 今日主线" in rendered
    assert "## 重要催化" in rendered
    assert "## 新闻主线" in rendered
    assert "## 新闻推演" in rendered
    assert "## 板块轮动" in rendered
    assert "## 主力资金流向" in rendered
    assert "## 全市场脉搏" in rendered
    assert "## 龙虎榜与涨停池" in rendered
    assert "## 资产影响" in rendered
    assert "## 关键宏观资产" in rendered
    assert "## 隔夜与主要资产" in rendered
    assert "## 全球资金流代理" in rendered
    assert "## 情绪代理" in rendered
    assert "## Watchlist 雷达" in rendered
    assert "## 今日已知事件" in rendered
    assert "## 今日验证点" in rendered
    assert "## 行动建议" in rendered
    assert "561380" in rendered
    assert "组合市值 100000。" in rendered
