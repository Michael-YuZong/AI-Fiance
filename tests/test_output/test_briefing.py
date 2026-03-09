"""Tests for briefing markdown rendering."""

from __future__ import annotations

from src.output.briefing import BriefingRenderer


def test_briefing_renderer_outputs_core_sections():
    payload = {
        "title": "每日晨报",
        "generated_at": "2026-03-09 07:30:00",
        "headline_lines": ["主线偏防守。"],
        "yesterday_review_lines": ["昨日原油验证点回看: 今天继续强化。"],
        "regime_reason_lines": ["背景 regime 当前判为 `滞涨`，触发依据: PMI 低于 50。"],
        "narrative_validation_lines": ["结论: 当前主线校验通过 3/3 项。"],
        "catalyst_rows": [["油价/地缘", "油价跳升", "原油 -> 通胀预期", "先看防守资产"]],
        "important_event_lines": ["美联储与利率预期: 市场等待 CPI。"],
        "story_lines": ["今天市场更像在交易油价冲击。"],
        "source_quality_lines": ["本次新闻覆盖源: Reuters / 财联社。"],
        "anomaly_lines": ["⚠️ 布伦特原油 5 日 +25.00%，请人工复核。"],
        "rotation_driver_lines": ["行业轮动靠前: 电力(+3.20%)。"],
        "main_flow_driver_lines": ["全市场主力资金最新为 `净流出` 12.30亿。"],
        "lhb_lines": ["机构净买额靠前: 兖矿能源(6.53亿)。"],
        "liquidity_lines": ["南向资金当日净流入约 300.00亿。"],
        "asset_dashboard_rows": [["布伦特原油", "宏观资产", "108.860", "1日 +1.00% / 5日 +5.00%", "冲击", ""]],
        "macro_items": ["中国 PMI 49.0。"],
        "watchlist_rows": [["561380", "场内价格 2.234", "+1.00%", "+2.00%", "+3.00%", "多头", "1.20", "偏强"]],
        "watchlist_technical_lines": ["561380: 技术共振偏强。"],
        "alerts": ["561380 放量。"],
        "event_lines": ["09:00 [高] A股盘前检查。"],
        "portfolio_lines": ["组合市值 100000。"],
        "verification_rows": [["原油", "是否继续上冲", "主线强化", "主线降温"]],
        "verification_lines": ["先看原油是否继续扩张涨幅。"],
        "event_rows": [["09:00", "高", "A股盘前检查", "检查最强最弱方向"]],
        "calendar_lines": ["盘前看强弱延续。"],
        "action_lines": ["今天先按防守优先处理。"],
    }
    rendered = BriefingRenderer().render(payload)
    assert "# 每日晨报" in rendered
    assert "## 昨日验证回顾" in rendered
    assert "## 主线判断" in rendered
    assert "## 资产仪表盘" in rendered
    assert "## 验证与行动" in rendered
    assert "## Watchlist 与组合" in rendered
    assert "### 背景 Regime 依据" in rendered
    assert "### 今天怎么做" in rendered
    assert "### 主线校验" in rendered
    assert "### 驱动与催化" in rendered
    assert "### 重要催化" in rendered
    assert "### 新闻覆盖与异常" in rendered
    assert "### 资产仪表盘" in rendered
    assert "### 市场主线解读" in rendered
    assert "### 盘面与资金" in rendered
    assert "### 龙虎榜与活跃资金" in rendered
    assert "### Watchlist 雷达" in rendered
    assert "### Watchlist 技术指标" in rendered
    assert "### 今日验证点表" in rendered
    assert "### 今日日历" in rendered
    assert "### 今日已知事件" in rendered
    assert "### 今日验证点" in rendered
    assert "### 跟踪清单" in rendered
    assert "561380" in rendered
    assert "组合市值 100000。" in rendered
