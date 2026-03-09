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
        "yesterday_review_rows": [["原油是否冲高回落", "布伦特收盘 < 开盘价", "收 100，+1.0%", "❌"]],
        "yesterday_review_lines": ["昨日原油验证点回看: 今天继续强化。"],
        "domestic_index_rows": [["上证指数", "3300.00", "+0.10%", "4000", "—", "偏强"]],
        "domestic_market_lines": ["全市场成交额: 12000亿，较前日口径暂缺。"],
        "style_rows": [["大盘 vs 小盘", "中证1000 +1.0%", "沪深300 -0.2%", "偏小盘"]],
        "industry_rows": [["1", "电力", "+3.20%", "能源冲击抬升"]],
        "macro_asset_rows": [["布伦特原油", "108.860", "+1.00%", "+5.00%", "+10.00%", "冲击", "—"]],
        "overnight_rows": [["美股", "标普500", "5000.00", "-1.20%", "偏弱"]],
        "watchlist_rows": [["561380", "场内价格 2.234", "+1.00%", "+2.00%", "+3.00%", "多头", "66.0 / 30.0", "偏强"]],
        "core_event_lines": ["**油价跳升**\n  → 原油 -> 通胀预期\n  → 先看防守资产"],
        "market_event_rows": [["20:30", "美国 CPI", "预期 0.3% / 前值 0.2%", "高", "QQQM"]],
        "workflow_event_rows": [["09:00", "盘前检查", "检查最强最弱方向"]],
        "capital_flow_lines": ["主力资金净流出 12.30 亿。"],
        "quality_lines": ["本次新闻覆盖源: Reuters / 财联社。", "⚠️ 布伦特原油 5 日 +25.00%，请人工复核。"],
        "portfolio_lines": ["组合市值 100000。"],
        "portfolio_table_rows": [["561380", "多", "2.10", "2.23", "+6.0%", "电网投资", "持有观察"]],
        "verification_rows": [["1", "原油", "收盘 < 开盘", "主线强化", "主线降温"]],
        "action_lines": ["今天先按防守优先处理。"],
        "appendix_technical_rows": [["561380", "多头", "金叉", "66.0", "上轨", "均线上", "30.0", "高位区"]],
        "appendix_lhb_lines": ["机构净买额靠前: 兖矿能源(6.53亿)。"],
        "appendix_flow_lines": ["国内 vs 海外相对强弱: 国内更稳。", "561380: 情绪代理中性。"],
        "appendix_derivative_lines": ["IF/IC/IM 基差暂不可用。"],
        "appendix_earnings_rows": [["英伟达", "Q4", "+20%", "+25%", "超预期", "毛利率改善", "利好算力链"]],
        "appendix_allocation_rows": [["保守型", "≤40%", "高股息", "—", "维持防守"]],
    }
    rendered = BriefingRenderer().render(payload)
    assert "# 每日晨报" in rendered
    assert "> **数据覆盖**: 中国宏观 | RSS新闻(Reuters/财联社) | 缺失项: 融资融券" in rendered
    assert "## 0. 昨日验证回顾" in rendered
    assert "## 1. 主线判断与行动" in rendered
    assert "## 2. 市场全景" in rendered
    assert "## 3. 驱动与催化" in rendered
    assert "## 4. 今日验证点" in rendered
    assert "## 5. 组合与持仓" in rendered
    assert "### 1.1 今日主线" in rendered
    assert "### 1.2 今天怎么做" in rendered
    assert "### 2.1 国内市场概览" in rendered
    assert "### 2.2 风格与行业" in rendered
    assert "### 2.3 宏观资产" in rendered
    assert "### 2.4 隔夜外盘" in rendered
    assert "### 2.5 Watchlist" in rendered
    assert "### 3.1 核心事件（限3-5条）" in rendered
    assert "### 3.2 今日日历 - 市场事件" in rendered
    assert "### 3.2 今日日历 - 操作提醒" in rendered
    assert "### 3.3 盘面与资金" in rendered
    assert "### 3.4 新闻覆盖与数据质量" in rendered
    assert "### 4.1 验证点表" in rendered
    assert "## 附录（折叠，按需展开）" in rendered
    assert "<details>" in rendered
    assert "561380" in rendered
    assert "组合市值 100000。" in rendered
