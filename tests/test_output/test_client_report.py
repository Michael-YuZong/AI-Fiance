from src.output.client_report import ClientReportRenderer, _fund_profile_sections


def _sample_analysis(symbol: str, name: str, asset_type: str = "cn_stock", rank: int = 3) -> dict:
    return {
        "symbol": symbol,
        "name": name,
        "asset_type": asset_type,
        "generated_at": "2026-03-11 10:00:00",
        "rating": {"rank": rank, "label": "较强机会"},
        "action": {
            "direction": "小仓试仓",
            "entry": "等回踩确认",
            "position": "首次建仓 ≤3%",
            "scaling_plan": "分 2-3 批建仓",
            "stop": "跌破关键支撑离场",
            "target": "先看前高",
            "max_portfolio_exposure": "单标的 ≤6%",
        },
        "narrative": {
            "headline": "这是一个趋势启动的标的。",
            "positives": ["相对强弱仍占优。", "价格下方仍有承接。"],
            "cautions": ["短线动能还需要确认。", "追高盈亏比一般。"],
            "judgment": {"state": "持有优于追高"},
        },
        "dimensions": {
            "technical": {"score": 55, "max_score": 100, "core_signal": "MACD 金叉", "summary": "技术结构偏强", "factors": [{"name": "MACD", "signal": "金叉", "detail": "DIF 站上 DEA", "display_score": "20/20"}]},
            "fundamental": {"score": 70, "max_score": 100, "core_signal": "ROE 20%", "summary": "基本面支撑存在", "factors": [{"name": "ROE", "signal": "20%", "detail": "盈利质量较高", "display_score": "20/20"}]},
            "catalyst": {
                "score": 45,
                "max_score": 100,
                "core_signal": "财报日临近",
                "summary": "有事件驱动",
                "factors": [{"name": "前瞻催化", "signal": "财报日临近", "detail": "未来 14 日内有事件", "display_score": "5/5"}],
                "evidence": [
                    {
                        "layer": "前瞻催化",
                        "title": "Meta scheduled to report earnings on 2026-03-18",
                        "source": "Investor Relations",
                        "link": "https://example.com/earnings",
                        "date": "2026-03-18",
                    }
                ],
            },
            "relative_strength": {"score": 80, "max_score": 100, "core_signal": "跑赢基准", "summary": "资金更愿意先买它", "factors": [{"name": "超额拐点", "signal": "跑赢基准", "detail": "5日/20日超额为正", "display_score": "25/30"}]},
            "risk": {"score": 60, "max_score": 100, "core_signal": "波动中等", "summary": "风险尚可控", "factors": [{"name": "波动率", "signal": "中等", "detail": "20日年化波动适中", "display_score": "10/25"}]},
            "macro": {"score": 30, "max_score": 40, "core_signal": "主线顺风", "summary": "宏观不逆风", "factors": [{"name": "敏感度向量", "signal": "顺风", "detail": "主线顺风", "display_score": "30/40"}]},
            "chips": {"score": 20, "max_score": 100, "core_signal": "一般", "summary": "筹码一般", "factors": [{"name": "资金承接", "signal": "一般", "detail": "ETF 流入一般", "display_score": "10/10"}]},
            "seasonality": {"score": 30, "max_score": 100, "core_signal": "中性", "summary": "季节性一般", "factors": [{"name": "月度胜率", "signal": "中性", "detail": "历史月度胜率一般", "display_score": "30/30"}]},
        },
        "hard_checks": [{"name": "流动性", "status": "✅", "detail": "日均成交充足"}],
        "signal_confidence": {
            "available": True,
            "summary": "同标的近似样本 12 个。",
            "scope": "同标的日线相似场景",
            "sample_count": 12,
            "win_rate_20d": 0.58,
            "avg_return_20d": 0.061,
            "median_return_20d": 0.044,
            "avg_mae_20d": -0.072,
            "stop_hit_rate": 0.33,
            "target_hit_rate": 0.42,
            "confidence_label": "中",
            "confidence_score": 57,
            "sample_dates": ["2026-01-05", "2025-11-21"],
            "reason": "仅使用同标的当时可见的日线量价和技术状态，不重建历史新闻与财报快照。",
        },
    }


def test_render_stock_picks_has_client_table_and_reasoning() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "top": [
            _sample_analysis("300502", "新易盛", "cn_stock", rank=3),
            _sample_analysis("01024.HK", "快手-W", "hk", rank=3),
            _sample_analysis("META", "Meta", "us", rank=3),
        ],
        "watch_positive": [],
    }
    rendered = ClientReportRenderer().render_stock_picks(payload)
    assert "## A股" in rendered
    assert "| 标的 | 技术 | 基本面 | 催化 | 相对强弱 | 风险 | 结论 |" in rendered
    assert "为什么能进正式推荐" in rendered
    assert "## 仓位管理" in rendered


def test_render_stock_picks_detailed_keeps_analysis_but_hides_internal_trace() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "market_label": "全市场",
        "top": [
            _sample_analysis("300502", "新易盛", "cn_stock", rank=3),
            _sample_analysis("01024.HK", "快手-W", "hk", rank=3),
            _sample_analysis("META", "Meta", "us", rank=3),
        ],
        "watch_positive": [],
    }
    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)
    assert "## 今日结论" in rendered
    assert "## A股" in rendered
    assert "八维雷达" in rendered
    assert "催化拆解" in rendered
    assert "催化证据来源" in rendered
    assert "https://example.com/earnings" in rendered
    assert "硬排除检查" in rendered
    assert "风险拆解" in rendered
    assert "样本置信度" in rendered
    assert "这层只反映历史相似量价/技术场景的样本置信度" in rendered
    assert "模型版本" not in rendered
    assert "当日基准版" not in rendered
    assert "本版口径变更" not in rendered
    assert "当前输出角色" not in rendered


def test_render_stock_picks_detailed_explains_coverage_denominator() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "market_label": "全市场",
        "stock_pick_coverage": {
            "note": "本轮新闻/事件覆盖基本正常。",
            "lines": ["A股 结构化事件覆盖 100%（3/3） / 高置信公司新闻覆盖 67%（2/3）"],
        },
        "top": [
            _sample_analysis("300502", "新易盛", "cn_stock", rank=3),
        ],
        "watch_positive": [],
    }
    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)
    assert "覆盖率的分母是当前纳入详细分析的各市场标的" in rendered
    assert "新闻热度更看多源共振" in rendered


def test_render_stock_picks_marks_watch_only_market_as_observe() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "top": [
            _sample_analysis("300502", "新易盛", "cn_stock", rank=3),
            _sample_analysis("01024.HK", "快手-W", "hk", rank=2),
            _sample_analysis("META", "Meta", "us", rank=3),
        ],
        "watch_positive": [{"symbol": "01024.HK", "name": "快手-W"}],
    }
    rendered = ClientReportRenderer().render_stock_picks(payload)
    assert "- 港股暂不正式推荐，优先观察：`快手-W`" in rendered


def test_render_scan_has_reasoning_and_position_management() -> None:
    analysis = _sample_analysis("561380", "电网ETF", "cn_etf", rank=1)
    rendered = ClientReportRenderer().render_scan(analysis)
    assert "## 为什么这么判断" in rendered
    assert "## 硬检查" in rendered
    assert "## 关键证据" in rendered
    assert "https://example.com/earnings" in rendered
    assert "## 当前更合适的动作" in rendered
    assert "## 仓位管理" in rendered
    assert "## 分维度详解" in rendered


def test_render_stock_analysis_uses_stock_analysis_title() -> None:
    analysis = _sample_analysis("META", "Meta", "us", rank=3)
    rendered = ClientReportRenderer().render_stock_analysis(analysis)
    assert "# Meta (META) | 个股详细分析 | 2026-03-11" in rendered.splitlines()[0]
    assert "## 为什么这么判断" in rendered


def test_render_scan_backfills_cautions_when_narrative_is_too_short() -> None:
    analysis = _sample_analysis("159819", "人工智能ETF", "cn_etf", rank=2)
    analysis["narrative"]["cautions"] = ["短线动能还需要确认。"]
    rendered = ClientReportRenderer().render_scan(analysis)
    section = rendered.split("## 现在不适合激进的地方", 1)[1].split("## 当前更合适的动作", 1)[0]
    assert section.count("- ") >= 2


def test_render_scan_includes_notes_as_data_limitations() -> None:
    analysis = _sample_analysis("510880", "红利ETF", "cn_etf", rank=1)
    analysis["notes"] = ["完整日线历史当前不可用，本次先用本地实时快照降级生成分析。"]
    rendered = ClientReportRenderer().render_scan(analysis)
    assert "## 数据限制与说明" in rendered
    assert "本地实时快照降级生成分析" in rendered


def test_render_fund_pick_has_alternatives() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "winner": {
            "name": "前海开源黄金ETF联接C",
            "symbol": "021740",
            "trade_state": "持有优于追高",
            "positives": ["方向对。", "防守属性更顺风。"],
            "dimension_rows": [["技术面", "44/100", "有支撑、没加速"]],
            "action": {
                "direction": "继续持有",
                "entry": "等回撤再看",
                "position": "计划仓位的 1/3 - 1/2",
                "scaling_plan": "确认后再加",
                "stop": "跌破支撑离场",
            },
            "positioning_lines": ["先小仓。"],
        },
        "alternatives": [{"name": "永赢科技智选混合发起C", "symbol": "022365", "cautions": ["节奏不对。"]}],
    }
    rendered = ClientReportRenderer().render_fund_pick(payload)
    assert "今日场外基金推荐" in rendered
    assert "为什么不是另外几只" in rendered


def test_render_etf_pick_has_fund_profile_and_alternatives() -> None:
    analysis = _sample_analysis("159981", "能源化工ETF", "cn_etf", rank=3)
    analysis["fund_profile"] = {
        "overview": {
            "基金类型": "商品型 / 能源化工期货型",
            "基金公司": "建信基金",
            "基金经理": "朱金钰、亢豆",
            "成立日期": "2019-12-13",
            "业绩比较基准": "易盛郑商所能源化工指数A收益率",
        },
        "style_analysis": {},
    }
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "winner": {
            "name": "能源化工ETF",
            "symbol": "159981",
            "trade_state": "观望偏多",
            "positives": ["方向没坏。", "相对强弱还在。", "催化不算弱。"],
            "dimension_rows": [["技术面", "52/100", "方向没坏但不适合追高"]],
            "action": {
                "direction": "观望偏多",
                "entry": "等回踩再看",
                "position": "首次建仓 ≤3%",
                "scaling_plan": "分 2-3 批建仓",
                "stop": "跌破支撑离场",
                "target": "先看前高",
            },
            "positioning_lines": ["先小仓。"],
            "evidence": list(analysis["dimensions"]["catalyst"]["evidence"]),
            "fund_sections": _fund_profile_sections(analysis),
        },
        "alternatives": [{"name": "红利ETF", "symbol": "510880", "cautions": ["今天弹性不如能源主线。"]}],
        "notes": ["当前数据源连接不稳定，已按可用数据降级处理。"],
    }
    rendered = ClientReportRenderer().render_etf_pick(payload)
    assert "今日ETF推荐" in rendered
    assert "## 这只ETF为什么是这个分" in rendered
    assert "## 基金画像" in rendered
    assert "## 为什么不是另外几只" in rendered


def test_render_briefing_includes_macro_leading_section() -> None:
    payload = {
        "generated_at": "2026-03-12 08:30:00",
        "headline_lines": ["今天更像结构性行情。", "风险偏好没有全面回暖。", "更适合先看主线确认。"],
        "action_lines": ["先小仓。", "等确认。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [
            "制造业 PMI 50.1，较前值回升；新订单 50.8、生产 51.7。",
            "PPI 同比 -0.9%，较前值回升；CPI 同比 1.3%，价格环境抬升。",
            "M1-M2 剪刀差 -4.1 个百分点，较前值修复；社融近 3 个月均值约 2.56 万亿元。",
        ],
        "alerts": [],
    }
    rendered = ClientReportRenderer().render_briefing(payload)
    assert "## 宏观领先指标" in rendered
    assert "PPI 同比 -0.9%" in rendered
    assert "M1-M2 剪刀差" in rendered
