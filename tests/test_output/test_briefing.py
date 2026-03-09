"""Tests for briefing markdown rendering."""

from __future__ import annotations

from src.output.briefing import BriefingRenderer


def test_briefing_renderer_outputs_core_sections():
    payload = {
        "title": "每日晨报",
        "generated_at": "2026-03-09 07:30:00",
        "data_coverage": "中国宏观 | RSS新闻(Reuters/财联社)",
        "missing_sources": "融资融券",
        "headline_lines": ["主线偏防守。"],
        "yesterday_review_lines": ["昨日原油验证点回看: 今天继续强化。"],
        "regime_reason_lines": ["背景 regime 当前判为 `滞涨`，触发依据: PMI 低于 50。"],
        "narrative_validation_lines": ["结论: 当前主线校验通过 3/3 项。"],
        "catalyst_rows": [["油价/地缘", "油价跳升", "原油 -> 通胀预期", "先看防守资产"]],
        "source_quality_lines": ["本次新闻覆盖源: Reuters / 财联社。"],
        "anomaly_lines": ["⚠️ 布伦特原油 5 日 +25.00%，请人工复核。"],
        "rotation_driver_lines": ["行业轮动靠前: 电力(+3.20%)。"],
        "main_flow_driver_lines": ["全市场主力资金最新为 `净流出` 12.30亿。"],
        "lhb_lines": ["机构净买额靠前: 兖矿能源(6.53亿)。"],
        "liquidity_lines": ["南向资金当日净流入约 300.00亿。"],
        "asset_dashboard_rows": [["布伦特原油", "108.860", "+1.00%", "+5.00%", "+10.00%", "冲击", "—"]],
        "macro_items": ["中国 PMI 49.0。"],
        "watchlist_rows": [["561380", "场内价格 2.234", "+1.00%", "+2.00%", "+3.00%", "多头", "RSI 66.0 / ADX 30.0", "偏强"]],
        "watchlist_technical_lines": ["561380: 技术共振偏强。"],
        "alerts": ["561380 放量。"],
        "portfolio_lines": ["组合市值 100000。"],
        "verification_rows": [["原油", "是否继续上冲", "主线强化", "主线降温"]],
        "event_rows": [["09:00", "高", "A股盘前检查", "检查最强最弱方向"]],
        "action_lines": ["今天先按防守优先处理。"],
        "market_pulse_lines": ["A股情绪仍有局部赚钱效应。"],
        "flow_lines": ["国内 vs 海外相对强弱: 国内更稳。"],
        "sentiment_lines": ["561380: 情绪代理中性。"],
    }
    rendered = BriefingRenderer().render(payload)
    assert "# 每日晨报" in rendered
    assert "> **数据覆盖**: 中国宏观 | RSS新闻(Reuters/财联社) | 缺失项: 融资融券" in rendered
    assert "## 0. 昨日验证回顾" in rendered
    assert "## 1. 主线判断与行动" in rendered
    assert "## 2. 资产仪表盘" in rendered
    assert "## 3. 驱动与催化" in rendered
    assert "## 4. 今日验证点" in rendered
    assert "## 5. 组合与持仓" in rendered
    assert "### 1.1 今日主线" in rendered
    assert "### 1.2 今天怎么做" in rendered
    assert "### 2.1 宏观资产" in rendered
    assert "### 2.2 Watchlist" in rendered
    assert "### 3.1 核心事件" in rendered
    assert "### 3.2 今日日历" in rendered
    assert "### 3.3 盘面与资金" in rendered
    assert "### 3.4 新闻覆盖与异常" in rendered
    assert "### 4.1 验证点表" in rendered
    assert "### 4.2 关注提醒" in rendered
    assert "## 附录" in rendered
    assert "<details>" in rendered
    assert "561380" in rendered
    assert "组合市值 100000。" in rendered
