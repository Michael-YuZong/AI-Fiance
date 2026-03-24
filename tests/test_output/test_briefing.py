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
        "a_share_watch_rows": [["1", "宁德时代 (300750)", "新能源", "较强机会", "持有优于追高", "首次建仓 ≤3%"]],
        "a_share_watch_lines": ["A 股观察池来自 `Tushare 优先` 的全市场快照；初筛池 `60` 只，完整分析 `8` 只。"],
        "core_event_lines": ["**油价跳升**\n  → 原油 -> 通胀预期\n  → 先看防守资产"],
        "theme_tracking_rows": [["电力/电网", "能源冲击", "防守与政策承接共振", "短线交易 / 中线配置", "油价回落则催化降温"]],
        "theme_tracking_lines": ["与主线一致性: 电力/电网 与 1.1 主线吻合。", "与前日对比: 暂无前一日行业跟踪归档，对比项从本次开始记录。"],
        "market_event_rows": [["20:30", "美国 CPI", "预期 0.3% / 前值 0.2%", "高", "QQQM"]],
        "workflow_event_rows": [["09:00", "盘前检查", "检查最强最弱方向"]],
        "capital_flow_lines": ["主力资金净流出 12.30 亿。"],
        "quality_lines": ["本次新闻覆盖源: Reuters / 财联社。", "⚠️ 布伦特原油 5 日 +25.00%，请人工复核。"],
        "portfolio_lines": ["组合市值 100000。"],
        "portfolio_table_rows": [["561380", "多", "2.10", "2.23", "+6.0%", "电网投资", "持有观察"]],
        "verification_rows": [["1", "原油", "收盘 < 开盘", "主线强化", "主线降温"]],
        "action_lines": [
            "今天先按防守优先处理。",
            "如果今天还要沿 电网ETF(561380) 做新仓/加仓，先按 `波段跟踪（2-6周）` 跑一遍组合预演：`portfolio whatif buy 561380 2.2340 计划金额`。",
        ],
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
    assert "### 2.6 A股全市场观察池（Tushare 初筛）" in rendered
    assert "### 3.1 核心事件（限3-5条）" in rendered
    assert "### 3.2 行业与主题跟踪（限2-4个方向）" in rendered
    assert "### 3.3 今日日历" in rendered
    assert "**市场事件**" in rendered
    assert "**操作提醒**" in rendered
    assert "### 3.4 盘面与资金" in rendered
    assert "### 3.5 新闻覆盖与数据质量" in rendered
    assert "### 4.1 验证点表" in rendered
    assert "## 附录（折叠，按需展开）" in rendered
    assert "<details>" in rendered
    assert "561380" in rendered
    assert "组合市值 100000。" in rendered
    assert "portfolio whatif buy 561380" in rendered


def test_noon_renderer_outputs_core_sections():
    payload = {
        "title": "午间盘中简报",
        "generated_at": "2026-03-10 11:45:00",
        "morning_eval_rows": [["原油冲高回落", "布伦特收盘 < 开盘价", "收 81.23 +0.4%", "❌"]],
        "morning_eval_fallback": "",
        "domestic_index_rows": [["上证指数", "3300.00", "+0.10%", "4000", "—", "偏强"]],
        "domestic_market_lines": ["全市场成交额: 12000亿。"],
        "style_rows": [["大盘 vs 小盘", "中证1000 +1.0%", "沪深300 -0.2%", "偏小盘"]],
        "industry_rows": [["1", "半导体", "+2.50%", "AI算力催化"]],
        "watchlist_rows": [["561380", "2.234", "+1.00%", "+2.00%", "+3.00%", "多头", "66"]],
        "strategy_adjustment_lines": ["晨报主线: 偏防守", "上午验证 1/1 未兑现，需修正。"],
        "afternoon_action_lines": [
            "下午偏防守，减少追高操作。",
            "如果下午还要沿 电网ETF(561380) 做新仓/加仓，先按 `波段跟踪（2-6周）` 跑一遍组合预演：`portfolio whatif buy 561380 2.2340 计划金额`。",
        ],
        "afternoon_verification_rows": [["1", "561380延续", "下午涨幅不回吐超过一半", "主线确认", "谨慎持有"]],
        "afternoon_event_rows": [["14:00", "盘中检查", "关注资金流向"]],
        "portfolio_lines": ["组合市值 100000。"],
        "portfolio_table_rows": [["561380", "多", "2.10", "2.23", "+6.0%", "电网投资", "持有"]],
    }
    rendered = BriefingRenderer().render_noon(payload)
    assert "# 午间盘中简报" in rendered
    assert "## 0. 晨报策略验证" in rendered
    assert "## 1. 上午盘面回顾" in rendered
    assert "### 1.1 指数与成交" in rendered
    assert "### 1.2 风格与行业" in rendered
    assert "### 1.3 Watchlist 表现" in rendered
    assert "## 2. 策略修正" in rendered
    assert "### 2.1 主线修正" in rendered
    assert "### 2.2 下午观察" in rendered
    assert "## 3. 下午看点" in rendered
    assert "### 3.1 下午验证点" in rendered
    assert "### 3.2 操作提醒" in rendered
    assert "## 4. 组合与持仓" in rendered
    assert "561380" in rendered
    assert "portfolio whatif buy 561380" in rendered


def test_evening_renderer_outputs_core_sections():
    payload = {
        "title": "收盘晚报",
        "generated_at": "2026-03-10 16:00:00",
        "full_day_eval_rows": [["原油冲高回落", "布伦特收盘 < 开盘价", "收 80.50 -0.5%", "✅"]],
        "full_day_eval_fallback": "",
        "hit_rate_lines": ["全日验证命中率: 1/1 (100%)。框架精准。"],
        "domestic_index_rows": [["上证指数", "3310.00", "+0.30%", "5000", "+25%", "偏强"]],
        "domestic_market_lines": ["全市场成交额: 15000亿。"],
        "style_rows": [["大盘 vs 小盘", "沪深300 +0.5%", "中证1000 -0.1%", "偏大盘"]],
        "industry_rows": [["1", "人工智能", "+3.80%", "算力催化"]],
        "macro_asset_rows": [["布伦特原油", "80.50", "-0.50%", "+2.00%", "+8.00%", "正常", "—"]],
        "watchlist_rows": [["515070", "1.234", "+2.00%", "+5.00%", "+8.00%", "多头", "72"]],
        "narrative_review_lines": ["晨报主线: 偏防守", "实际驱动: AI算力"],
        "core_event_lines": ["**AI算力政策**\n  → 半导体利好"],
        "capital_flow_lines": ["主力资金净流入 8.50 亿。"],
        "overnight_rows": [["美股", "标普500", "5100.00", "+0.50%", "偏强"]],
        "tomorrow_outlook_lines": ["今日主线 AI算力 的延续性需要明天开盘验证。"],
        "tomorrow_verification_rows": [["1", "人工智能ETF延续", "明日涨幅 > 0", "主线持续", "考虑止盈"]],
        "tomorrow_action_lines": [
            "今日框架有效，明天可延续策略方向。",
            "如果明天还要沿 人工智能ETF(515070) 做新仓/加仓，先按 `短线交易（3-10日）` 跑一遍组合预演：`portfolio whatif buy 515070 1.2340 计划金额`。",
        ],
        "portfolio_lines": ["组合市值 105000。"],
        "portfolio_table_rows": [["515070", "多", "1.15", "1.23", "+7.0%", "AI投资", "持有"]],
        "appendix_technical_rows": [["515070", "多头", "金叉", "72.0", "上轨", "均线上", "28.0", "高位区"]],
        "appendix_lhb_lines": ["机构净买额靠前: 中芯国际(3.20亿)。"],
        "appendix_flow_lines": ["国内 vs 海外: 国内偏强。"],
        "charts": {},
    }
    rendered = BriefingRenderer().render_evening(payload)
    assert "# 收盘晚报" in rendered
    assert "## 0. 全日验证回顾" in rendered
    assert "## 1. 全日市场总结" in rendered
    assert "### 1.1 指数与成交" in rendered
    assert "### 1.2 风格与行业" in rendered
    assert "### 1.3 宏观资产" in rendered
    assert "### 1.4 Watchlist 表现" in rendered
    assert "## 2. 主线复盘" in rendered
    assert "### 2.1 今日主线回顾" in rendered
    assert "### 2.2 核心事件复盘" in rendered
    assert "### 2.3 盘面与资金" in rendered
    assert "## 3. 明日展望" in rendered
    assert "### 3.1 隔夜外盘" in rendered
    assert "### 3.2 明日主线预判" in rendered
    assert "### 3.3 明日验证点（预设）" in rendered
    assert "### 3.4 明日操作建议" in rendered
    assert "## 4. 组合与持仓" in rendered
    assert "## 附录（折叠，按需展开）" in rendered
    assert "515070" in rendered
    assert "portfolio whatif buy 515070" in rendered


def test_market_renderer_outputs_core_sections():
    payload = {
        "title": "全市场行情简报",
        "generated_at": "2026-03-19 16:10:00",
        "data_coverage": "中国宏观 | 全市场快照 | RSS新闻",
        "missing_sources": "跨市场代理部分降级",
        "headline_lines": ["当前主线候选: `利率驱动成长修复`。", "背景 regime 当前判为 `通缩/偏弱`。"],
        "action_lines": ["仓位框架: 当前按常规节奏分批确认。", "如果 A 股观察池扩散不足，就按结构性行情处理。"],
        "macro_items": ["制造业 PMI 50.1，较前值回升。", "当前宏观环境判断: 通缩/偏弱。"],
        "domestic_index_rows": [["上证指数", "3420.00", "+0.20%", "5200", "+3%", "震荡偏强"]],
        "domestic_market_lines": ["全市场成交额: 12800亿。", "watchlist 平均 5 日表现 +1.2%。"],
        "index_signal_rows": [["上证指数", "3420.00", "+0.20%", "偏强修复", "周线金叉", "月线修复", "常态量能", "等待确认"]],
        "index_signal_lines": ["上证指数：偏强修复。"],
        "market_signal_rows": [["市场宽度", "上涨 2800 / 下跌 2100", "分歧中性", "涨跌比 1.33，涨停 52 / 跌停 4"]],
        "market_signal_lines": ["情绪极端指标 `分歧中性`：当前 54/100。"],
        "style_rows": [["大盘 vs 小盘", "沪深300 +0.6%", "中证1000 -0.1%", "偏大盘"]],
        "industry_rows": [["1", "银行", "+1.40%", "红利承接"]],
        "rotation_rows": [["行业", "银行(+1.40%)、电力(+1.20%)", "半导体(-0.80%)", "防守占优，高低切明显"]],
        "rotation_lines": ["行业轮动靠前: 银行(+1.40%)、电力(+1.20%)。"],
        "macro_asset_rows": [["美元指数", "103.200", "-0.20%", "-0.50%", "+1.00%", "中性", "—"]],
        "overnight_rows": [["美股", "标普500", "5100.00", "+0.40%", "偏强"]],
        "watchlist_rows": [["510210", "1.026", "-0.20%", "+0.30%", "+1.00%", "震荡", "44", "宽基修复观察"]],
        "core_event_lines": ["**利率预期回落**\n  → 长久期资产压力缓和"],
        "theme_tracking_rows": [["宽基", "利率预期回落", "指数修复观察", "短线观察", "量能不足"]],
        "theme_tracking_lines": ["与主线一致性: 宽基与利率修复方向一致。"],
        "capital_flow_lines": ["全市场主力资金最新为 `净流入` 23.10亿。"],
        "quality_lines": ["本次新闻覆盖源: Reuters / 财联社。"],
        "a_share_watch_rows": [["1", "宁德时代 (300750)", "新能源", "较强机会", "持有优于追高", "首次建仓 ≤3%"]],
        "a_share_watch_lines": ["A 股观察池来自 `Tushare 优先` 的全市场快照；初筛池 `60` 只，完整分析 `8` 只。"],
        "verification_rows": [["1", "上证指数", "能否收回 MA20", "修复延续", "继续震荡"]],
        "alerts": ["当前没有触发额外强提醒，但市场风格切换仍需持续验证。"],
        "proxy_contract": {
            "market_flow": {
                "interpretation": "黄金相对成长更抗跌，市场风格偏防守。",
                "confidence_label": "中",
                "coverage_summary": "科技/黄金/国内/海外代理样本",
                "limitation": "这是相对强弱代理，不是原始资金流。",
                "downgrade_impact": "更适合辅助判断风格切换，不适合单独下交易结论。",
            },
            "social_sentiment": {
                "covered": 2,
                "total": 2,
                "confidence_labels": {"中": 1, "高": 1},
                "limitation": "这是价格和量能推导出的情绪代理，不是真实社媒抓取。",
                "downgrade_impact": "更适合提示拥挤线索，不适合单独作为买卖信号。",
            },
        },
    }
    rendered = BriefingRenderer().render_market(payload)
    assert "# 全市场行情简报" in rendered
    assert "## 1. 市场结论" in rendered
    assert "### 1.1 今日主线" in rendered
    assert "### 1.2 仓位与执行" in rendered
    assert "## 2. 宏观与市场全景" in rendered
    assert "### 2.1 宏观框架" in rendered
    assert "### 2.2 国内市场概览" in rendered
    assert "### 2.3 核心指数信号" in rendered
    assert "### 2.4 市场宽度与情绪" in rendered
    assert "### 2.5 风格与行业" in rendered
    assert "### 2.6 板块轮动" in rendered
    assert "### 2.7 宏观资产" in rendered
    assert "### 2.8 隔夜外盘" in rendered
    assert "### 2.9 跨市场观察哨" in rendered
    assert "### 2.10 代理信号与限制" in rendered
    assert "## 3. 资金与催化" in rendered
    assert "### 3.1 核心事件" in rendered
    assert "### 3.2 主线跟踪" in rendered
    assert "### 3.3 盘面与资金" in rendered
    assert "## 4. A股观察池" in rendered
    assert "## 5. 今日验证点" in rendered
    assert "### 5.2 风险提醒" in rendered
