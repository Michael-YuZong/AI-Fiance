import pandas as pd
import src.output.client_report as client_report_module

from src.output.client_report import (
    ClientReportRenderer,
    _analysis_provenance_lines,
    _evidence_lines,
    _evidence_lines_with_event_digest,
    _fund_profile_sections,
    _observe_watch_levels,
    _observe_trigger_condition,
    _pick_client_safe_line,
    _proxy_contract_section,
    _scan_dimension_rows,
    _stock_observe_data_gap_text,
    _stock_observe_evidence_hardness_text,
    _tighten_observe_client_markdown,
)


def _sample_analysis(symbol: str, name: str, asset_type: str = "cn_stock", rank: int = 3) -> dict:
    return {
        "symbol": symbol,
        "name": name,
        "asset_type": asset_type,
        "metadata": {"history_source": "akshare", "history_source_label": "AKShare 日线回退"},
        "generated_at": "2026-03-11 10:00:00",
        "rating": {"rank": rank, "label": "较强机会"},
        "action": {
            "direction": "小仓试仓",
            "entry": "等回踩确认",
            "buy_range": "9.850 - 10.000",
            "position": "首次建仓 ≤3%",
            "scaling_plan": "分 2-3 批建仓",
            "trim_range": "11.200 - 11.500",
            "stop": "跌破关键支撑离场",
            "target": "先看前高",
            "max_portfolio_exposure": "单标的 ≤6%",
            "timeframe": "中线配置(1-3月)" if rank >= 3 else "短线交易(1-2周)" if rank >= 2 else "等待更好窗口",
            "horizon": {
                "code": "position_trade" if rank >= 3 else "short_term" if rank >= 2 else "watch",
                "label": "中线配置（1-3月）" if rank >= 3 else "短线交易（3-10日）" if rank >= 2 else "观察期",
                "style": "更像 1-3 个月的分批配置或波段跟踪，不按隔日涨跌去做快进快出。" if rank >= 3 else "更看催化、趋势和执行节奏，适合盯右侧确认和止损，不适合当成长线底仓。" if rank >= 2 else "先看窗口和确认信号，不建议急着把它定义成短线执行仓或长线配置仓。",
                "fit_reason": "基本面、风险收益和趋势至少有两项站得住，更适合按一段完整主线去拿，而不是只博短催化。" if rank >= 3 else "当前更强的是催化、相对强弱和执行节奏，优势主要集中在接下来几个交易日到一两周内。" if rank >= 2 else "当前信号还没共振到足以支撑正式动作，继续观察比仓促出手更重要。",
                "misfit_reason": "现在不适合当成纯隔夜交易，也还没强到可以长期不复核地持有一年以上。" if rank >= 3 else "现在不适合直接当成长线底仓，一旦催化和强势股状态失效要更快处理。" if rank >= 2 else "现在不适合直接按短线执行仓或长线配置仓去理解。",
            },
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
            "summary": "严格口径下保留 12 个非重叠样本。",
            "scope": "同标的日线相似场景（严格非重叠样本）",
            "sample_count": 12,
            "candidate_pool": 31,
            "non_overlapping_count": 12,
            "coverage_months": 7,
            "coverage_span_days": 198,
            "win_rate_20d": 0.58,
            "win_rate_20d_ci_low": 0.31,
            "win_rate_20d_ci_high": 0.81,
            "avg_return_20d": 0.061,
            "median_return_20d": 0.044,
            "median_return_20d_ci_low": -0.012,
            "median_return_20d_ci_high": 0.093,
            "avg_mae_20d": -0.072,
            "stop_hit_rate": 0.33,
            "target_hit_rate": 0.42,
            "sample_quality_label": "中",
            "sample_quality_score": 54,
            "confidence_label": "中",
            "confidence_score": 57,
            "sample_dates": ["2026-01-05", "2025-11-21"],
            "quality_notes": ["20 日胜率的下沿还没站上 50%，统计优势不够硬。"],
            "reason": "仅使用同标的当时可见的日线量价和技术状态，不重建历史新闻与财报快照。",
        },
    }


def test_observe_watch_levels_prefers_near_resistance_over_far_target_for_etf() -> None:
    analysis = {
        "asset_type": "cn_etf",
        "action": {"stop_ref": 1.284, "target_ref": 1.508},
        "dimensions": {
            "technical": {
                "factors": [
                    {
                        "name": "压力位",
                        "factor_id": "j1_resistance_zone",
                        "signal": "上方存在近端压力：近20日高点 1.432（上方 2.6%） / 近60日高点 1.432（上方 2.6%） / 摆动前高 1.432（上方 2.6%）",
                    }
                ]
            }
        },
    }

    line = _observe_watch_levels(analysis)

    assert "1.432" in line
    assert "1.508" not in line


def test_proxy_contract_section_marks_sentiment_as_proxy_evidence_not_high_confidence_fact() -> None:
    lines = _proxy_contract_section(
        {},
        winner={
            "proxy_signals": {
                "social_sentiment": {
                    "aggregate": {
                        "interpretation": "情绪指数 97.5，讨论热度偏高，需防拥挤交易。",
                        "confidence_label": "高",
                        "limitations": ["这是价格和量能行为推导出的情绪代理，不是真实社媒抓取。"],
                    }
                }
            }
        },
    )
    rendered = "\n".join(lines)

    assert "参考价值高，证据层级中等" in rendered
    assert "`高` / 日涨跌" not in rendered


def _attach_second_stage_stock_signals(analysis: dict) -> dict:
    analysis["dimensions"]["technical"]["factors"].append(
        {
            "name": "股票技术面状态",
            "signal": "趋势偏强 / 动能改善（2026-04-03）",
            "detail": "Tushare stk_factor_pro 已确认日度技术状态。",
            "factor_id": "j1_stk_factor_pro",
            "factor_meta": {"degraded": False},
        }
    )
    analysis["dimensions"]["fundamental"]["factors"].append(
        {
            "name": "跨市场比价",
            "signal": "A/H 比价溢价 +18.20%（2026-04-03）",
            "detail": "A/H 溢价处于可观察区间。",
            "factor_id": "j3_ah_comparison",
            "factor_meta": {"degraded": False},
        }
    )
    analysis["dimensions"]["fundamental"]["factors"].append(
        {
            "name": "可转债映射",
            "signal": "新易转债 / 趋势偏强 / 动能改善 / 转股溢价 +9.80%",
            "detail": "发行人对应转债状态偏强。",
            "factor_id": "j4_convertible_bond_proxy",
            "factor_meta": {"degraded": False},
        }
    )
    analysis["market_event_rows"] = [
        [
            "2026-04-03",
            "机构调研：多家机构追问新品放量与订单兑现节奏",
            "机构调研专题",
            "高",
            "基本面",
            "",
            "机构调研",
            "偏利多，机构关注点集中在订单兑现和景气延续。",
        ]
    ]
    return analysis


def _install_fake_thesis_repo(monkeypatch, theses: dict | None = None) -> None:
    theses = theses or {
        "*": {
            "core_assumption": "AI算力景气继续扩散",
            "validation_metric": "公告和订单同步兑现",
            "holding_period": "1-3个月",
            "event_digest_snapshot": {
                "status": "待补充",
                "lead_layer": "行业主题事件",
                "lead_title": "AI算力链热度扩散",
            },
        }
    }

    class _FakeThesisRepo:
        def get(self, symbol):
            return theses.get(symbol, theses.get("*", {}))

    monkeypatch.setattr("src.output.editor_payload.ThesisRepository", lambda: _FakeThesisRepo())


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
    assert "## 先看执行" in rendered
    assert rendered.index("## 先看执行") < rendered.index("## 首页判断")
    assert "| 看不看 | 今天可以做，但只做前排 1-2 只，不把整份名单当全面进攻。 |" in rendered
    assert "| 怎么触发 |" in rendered
    assert "| 哪里建仓 | A股：新易盛 建仓先看 `9.850 - 10.000` 一带承接。；港股：快手-W 建仓先看 `9.850 - 10.000` 一带承接。 |" in rendered
    assert "`9.850 - 10.000`" in rendered
    assert "新易盛" in rendered
    assert "跌破关键支撑离场" in rendered
    assert "组合先只开最接近确认的 1-2 只，不扩散到整份名单。" in rendered
    assert "任一前排票失守自己的失效位就先处理，不为单一观点硬扛。" in rendered
    assert "首屏不写统一止损价" not in rendered
    assert "## 首页判断" in rendered
    assert "### 板块 / 主题认知" in rendered
    assert "## 今日结论" not in rendered
    assert "## 名单结构" in rendered
    assert "| 报告定位 | 推荐稿 |" in rendered
    assert "| 空仓怎么做 |" not in rendered
    assert "| 持仓怎么做 |" not in rendered
    assert "| 首次仓位 | 单票 `2% - 5%` 试仓 |" not in rendered
    assert "| 短线优先 |" in rendered
    assert "| 中线优先 |" in rendered
    assert "## A股" in rendered
    assert "| 标的 | 技术 | 基本面 | 催化 | 相对强弱 | 风险 | 结论 |" in rendered
    assert "### 第一批：核心主线" in rendered
    assert "为什么能进正式推荐" in rendered
    assert "持有周期：中线配置（1-3月）" in rendered
    assert "portfolio whatif buy 300502" in rendered
    assert "为什么按这个周期理解" in rendered
    assert "现在不适合的打法" in rendered
    assert "建议买入区间：9.850 - 10.000" in rendered
    assert "第一减仓位：`11.200` 附近先兑现第一段反弹" in rendered
    assert "第二减仓位：若放量站上 `11.200`，再看 `11.500` 一带是否做第二次减仓。" in rendered
    assert "上修条件：只有放量站上 `11.500` 且催化、相对强弱和量能继续增强，才考虑把目标继续上修到 `先看前高`。" in rendered
    assert "## 仓位管理" in rendered


def test_render_stock_picks_surfaces_three_formal_recommendations() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "创新药 / 科技轮动"},
        "regime": {"current_regime": "recovery"},
        "top": [
            _sample_analysis("300502", "新易盛", "cn_stock", rank=3),
            _sample_analysis("300308", "中际旭创", "cn_stock", rank=3),
            _sample_analysis("600276", "恒瑞医药", "cn_stock", rank=3),
        ],
        "watch_positive": [],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "| 优先推荐 | 中际旭创 (300308) |" in rendered
    assert "| 次优推荐 | 新易盛 (300502) |" in rendered
    assert "| 第三推荐 | 恒瑞医药 (600276) |" in rendered
    assert rendered.count("为什么能进正式推荐") >= 3


def test_pick_client_safe_line_hides_operation_not_permitted_path() -> None:
    line = _pick_client_safe_line("板块驱动数据缺失: [Errno 1] Operation not permitted: '/Users/bilibili/tk.csv'")
    assert "tk.csv" not in line
    assert "/Users/bilibili" not in line
    assert "已按可用数据降级处理" in line


def test_render_scan_and_stock_analysis_surface_index_weekly_and_monthly_horizon_lines() -> None:
    analysis = _sample_analysis("300502", "新易盛", "cn_stock", rank=3)
    analysis["index_topic_bundle"] = {
        "index_snapshot": {"index_name": "人工智能精选", "display_label": "真实指数估值"},
        "history_snapshots": {
            "weekly": {
                "status": "matched",
                "summary": "近 156周 +12.00%，最近一周 +1.10%，修复中，动能改善",
                "trend_label": "修复中",
                "momentum_label": "动能改善",
                "latest_date": "2026-04-02",
            },
            "monthly": {
                "status": "matched",
                "summary": "近 36月 +18.00%，最近一月 +4.20%，趋势偏强，动能偏强",
                "trend_label": "趋势偏强",
                "momentum_label": "动能偏强",
                "latest_date": "2026-04-02",
            },
        },
    }

    rendered = ClientReportRenderer().render_scan(analysis)
    stock_analysis_rendered = ClientReportRenderer().render_stock_analysis(analysis)

    assert "## 周月节奏" not in rendered
    assert "周线：人工智能精选" not in rendered
    assert "月线：人工智能精选" not in rendered
    assert "周月节奏同向偏强" not in rendered
    assert "## 周月节奏" not in stock_analysis_rendered

    etf = _sample_analysis("510300", "沪深300ETF", "cn_etf", rank=3)
    etf["index_topic_bundle"] = analysis["index_topic_bundle"]
    etf_rendered = ClientReportRenderer().render_scan(etf)

    assert "## 周月节奏" in etf_rendered
    assert "周线：人工智能精选" in etf_rendered

    active_fund = _sample_analysis("022365", "永赢科技智选混合发起C", "cn_fund", rank=3)
    active_fund["fund_profile"] = {
        "overview": {"基金类型": "混合型-偏股", "业绩比较基准": "中国战略新兴产业成份指数收益率"},
        "style": {"tags": ["科技主题"]},
    }
    active_fund["index_topic_bundle"] = analysis["index_topic_bundle"]
    active_fund_rendered = ClientReportRenderer().render_scan(active_fund)

    assert "## 周月节奏" not in active_fund_rendered
    assert "周线：人工智能精选" not in active_fund_rendered


def test_render_stock_picks_surface_standard_industry_framework_rows() -> None:
    lead = _sample_analysis("300308", "中际旭创", "cn_stock", rank=3)
    lead["market_event_rows"] = [
        [
            "2026-04-01",
            "标准行业框架：中际旭创 属于 申万二级行业·通信设备（+3.60%）",
            "申万行业框架",
            "高",
            "通信设备",
            "",
            "标准行业归因",
            "偏利多，先按标准行业框架理解。",
        ]
    ]
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "AI算力"},
        "regime": {"current_regime": "recovery"},
        "top": [lead],
        "watch_positive": [],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "标准行业框架：中际旭创 属于 申万二级行业·通信设备（+3.60%）" in rendered
    assert "申万行业框架" in rendered


def test_render_stock_picks_surfaces_portfolio_overlap_summary_in_body() -> None:
    lead = _sample_analysis("300502", "新易盛", "cn_stock", rank=3)
    lead["portfolio_overlap_summary"] = {
        "summary_line": "这条建议和现有组合最重的行业同线，更像同一主线延伸。",
        "style_summary_line": "当前组合风格偏进攻，最重风格是进攻 52.0%。",
    }
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "top": [lead],
        "watch_positive": [],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "和现有持仓怎么配：" in rendered
    assert "同一主线延伸" in rendered
    assert "风格与方向：" in rendered


def test_render_stock_picks_explains_regime_basis_when_reasoning_is_available() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {
            "current_regime": "stagflation",
            "reasoning": [
                "PMI 49.2 低于 50，增长端偏弱。",
                "CPI 2.1% 与油价冲击并存，价格压力还没完全回落。",
            ],
        },
        "top": [_sample_analysis("300502", "新易盛", "cn_stock", rank=3)],
        "watch_positive": [],
    }
    rendered = ClientReportRenderer().render_stock_picks(payload)
    assert "## 宏观判断依据" in rendered
    assert "PMI 49.2 低于 50" in rendered
    assert "当天主线写成 `能源冲击 + 地缘风险`" in rendered


def test_render_stock_picks_detailed_keeps_analysis_but_hides_internal_trace() -> None:
    featured = _sample_analysis("300502", "新易盛", "cn_stock", rank=3)
    featured["visuals"] = {
        "dashboard": "/tmp/dashboard.png",
        "windows": "/tmp/windows.png",
    }
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "market_label": "全市场",
        "top": [
            featured,
            _sample_analysis("01024.HK", "快手-W", "hk", rank=3),
            _sample_analysis("META", "Meta", "us", rank=3),
        ],
        "watch_positive": [],
    }
    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)
    assert "## 先看执行" in rendered
    assert rendered.index("## 先看执行") < rendered.index("## 首页判断")
    assert "| 看不看 | 今天可以做，但只适合先小仓、分批确认。 |" in rendered
    assert "| 怎么触发 |" in rendered
    assert "真正出手要等对应个股复核卡里的介入条件兑现" in rendered
    assert "| 多大仓位 | 单票 `2% - 5%` 试仓 |" in rendered
    assert "## 首页判断" in rendered
    assert "## 今日结论" not in rendered
    assert "## 名单结构" in rendered
    assert "| 中期背景 / 当天主线 | `stagflation` / `能源冲击 + 地缘风险` |" in rendered
    assert "## A股" in rendered
    assert "八维雷达" in rendered
    assert "#### 图表速览" in rendered
    assert "![分析看板](/tmp/dashboard.png)" in rendered
    assert "催化拆解" in rendered
    assert "催化证据来源" in rendered
    assert "证据时点与来源" in rendered
    assert "https://example.com/earnings" in rendered
    assert "硬排除检查" in rendered
    assert "风险拆解" in rendered
    assert "样本置信度" in rendered
    assert "非重叠样本" in rendered
    assert "95%区间" in rendered
    assert "样本质量" in rendered
    assert "持有周期：中线配置（1-3月）" in rendered
    assert "为什么按这个周期理解" in rendered
    assert "不直接替代本次总推荐判断" in rendered
    assert "严格口径会先去掉未来窗口重叠样本" in rendered
    assert "分钟级快照 as_of" in rendered
    assert "盘中快照 as_of" not in rendered
    assert "隔夜交易" not in rendered
    assert "模型版本" not in rendered
    assert "当日基准版" not in rendered
    assert "本版口径变更" not in rendered
    assert "当前输出角色" not in rendered


def test_render_stock_picks_detailed_contextualizes_repeated_formal_execution_lines() -> None:
    first = _sample_analysis("600989", "宝丰能源", "cn_stock", rank=3)
    second = _sample_analysis("300750", "宁德时代", "cn_stock", rank=3)
    third = _sample_analysis("603259", "药明康德", "cn_stock", rank=3)
    first["action"]["entry"] = "等煤化工价差和量能继续确认"
    first["action"]["buy_range"] = "30.075 - 30.299"
    second["action"]["entry"] = "等电池链回踩后承接重新转强"
    second["action"]["buy_range"] = "231.100 - 236.800"
    third["action"]["entry"] = "等创新药情绪和价格强度继续共振"
    third["action"]["buy_range"] = "51.200 - 52.600"
    first["narrative"]["positives"] = ["煤化工景气和盈利弹性还在。"]
    second["narrative"]["positives"] = ["电池龙头的盈利质量和订单韧性更强。"]
    third["narrative"]["positives"] = ["医药龙头的估值和修复节奏更占优。"]
    payload = {
        "generated_at": "2026-04-08 10:00:00",
        "day_theme": {"label": "背景宏观主导"},
        "regime": {"current_regime": "recovery"},
        "market_label": "A股",
        "top": [first, second, third],
        "coverage_analyses": [first, second, third],
        "watch_positive": [],
        "stock_pick_coverage": {
            "note": "当前催化/事件覆盖可直接作为 pre-screen 参考。",
            "lines": ["A股 结构化事件覆盖 67%（2/3） / 高置信公司新闻覆盖 67%（2/3）"],
        },
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "加仓节奏：先按 `30.075 - 30.299` 一带分 2-3 批承接" in rendered
    assert "加仓节奏：先按 `231.100 - 236.800` 一带分 2-3 批承接" in rendered
    assert "加仓节奏：先按 `51.200 - 52.600` 一带分 2-3 批承接" in rendered
    assert "为什么按这个周期理解：基本面、风险收益和趋势至少有两项站得住，更适合按一段完整主线去拿，而不是只博短催化。" not in rendered
    assert "- 加仓节奏：分 2-3 批建仓\n" not in rendered
    assert "**数据完整度：** 当前催化/事件覆盖可直接作为 pre-screen 参考。 分母是进入详细分析的样本，不是全市场扫描池。" in rendered
    assert "相关性/分散度按各市场观察池基准代理" not in rendered


def test_render_stock_picks_detailed_prefers_subject_theme_over_day_theme_for_stock_cards() -> None:
    analysis = _sample_analysis("300274", "阳光电源", "cn_stock", rank=1)
    analysis["action"]["direction"] = "观察为主"
    analysis["action"]["position"] = "暂不出手"
    analysis["action"]["entry"] = "等右侧确认。"
    analysis["metadata"] = {
        "history_source": "akshare",
        "history_source_label": "AKShare 日线回退",
        "sector": "电力设备",
        "industry_framework_label": "光伏主链",
        "chain_nodes": ["光伏主链", "储能", "电网设备"],
    }
    analysis["theme_playbook"] = {
        "key": "solar_mainchain",
        "label": "光伏主链",
        "playbook_level": "theme",
        "hard_sector_label": "电力设备 / 新能源设备",
    }
    payload = {
        "generated_at": "2026-04-05 10:00:00",
        "day_theme": {"label": "背景宏观主导"},
        "regime": {"current_regime": "recovery"},
        "market_label": "A股",
        "top": [analysis],
        "coverage_analyses": [analysis],
        "watch_positive": [analysis],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "| 代表主题 | `光伏主链` |" in rendered
    assert "| 交易分层 |" in rendered
    assert "| 交易分层 | `主线核心` / `主线仓`" not in rendered
    assert "主题定位：当前更接近 `光伏主链` 这条方向" in rendered
    assert "市场背景看 `背景宏观主导`，代表主题先按 `光伏主链` 理解" not in rendered
    assert "背景宏观主导" in rendered


def test_render_stock_picks_detailed_surfaces_semiconductor_trading_role_and_theme_split() -> None:
    analysis = _sample_analysis("588170", "半导体设备ETF", "cn_etf", rank=2)
    analysis["action"]["direction"] = "观察为主"
    analysis["action"]["position"] = "暂不出手"
    analysis["metadata"] = {"sector": "半导体设备", "industry_framework_label": "半导体链"}
    analysis["theme_playbook"] = {
        "key": "semiconductor",
        "label": "半导体",
        "playbook_level": "theme",
        "hard_sector_label": "信息技术",
        "trading_role_label": "主线核心",
        "trading_position_label": "主线仓",
        "trading_role_summary": "这条线已经够资格按主线核心理解，执行上优先按主线仓看待，可分批拿趋势，不用默认压成纯几周波段。",
    }
    payload = {
        "generated_at": "2026-04-11 10:00:00",
        "day_theme": {"label": "硬科技 / AI硬件链"},
        "regime": {"current_regime": "recovery"},
        "market_label": "A股",
        "top": [analysis],
        "coverage_analyses": [analysis],
        "watch_positive": [analysis],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "| 交易分层 | `主线核心` / `主线仓`" in rendered or "| 交易分层 | `主线扩散` / `卫星仓`" in rendered
    assert "不要和软件/应用层提前混成一个大科技桶" in rendered


def test_scan_dimension_rows_append_shared_technical_signal_text() -> None:
    sample = _sample_analysis("300502", "新易盛", "cn_stock", rank=3)
    sample["history"] = pd.DataFrame(
        {
            "date": pd.date_range("2025-10-01", periods=120, freq="B"),
            "open": [1.0 + i * 0.004 for i in range(120)],
            "high": [1.02 + i * 0.004 for i in range(120)],
            "low": [0.98 + i * 0.004 for i in range(120)],
            "close": [1.0 + i * 0.004 for i in range(120)],
            "volume": [8_000_000 + i * 1_000 for i in range(120)],
            "amount": [16_000_000 + i * 2_000 for i in range(120)],
        }
    )
    rows = _scan_dimension_rows(sample)
    technical_row = next(row for row in rows if row[0] == "技术面")
    assert "当前图形标签：" in technical_row[2]


def test_render_scan_surfaces_portfolio_overlap_summary() -> None:
    sample = _sample_analysis("300502", "新易盛", "cn_stock", rank=3)
    sample["portfolio_overlap_summary"] = {
        "summary_line": "这条建议和现有组合最重的行业同线，重复度较高，更像同一主线延伸。",
        "style_summary_line": "当前组合风格偏进攻，最重风格是进攻 52.0%。",
        "style_priority_hint": "如果只是同风格加码，优先级低于补新方向。",
    }

    rendered = ClientReportRenderer().render_scan(sample)

    assert "## 与现有持仓的关系" in rendered
    assert "重复度较高" in rendered
    assert "风格与方向：" in rendered
    assert "组合优先级：" in rendered


def test_observe_trigger_condition_appends_shared_technical_trigger_hint_when_history_exists() -> None:
    sample = _sample_analysis("300502", "新易盛", "cn_stock", rank=1)
    sample["history"] = pd.DataFrame(
        {
            "date": pd.date_range("2025-10-01", periods=120, freq="B"),
            "open": [1.0 + i * 0.004 for i in range(120)],
            "high": [1.02 + i * 0.004 for i in range(120)],
            "low": [0.98 + i * 0.004 for i in range(120)],
            "close": [1.0 + i * 0.004 for i in range(120)],
            "volume": [8_000_000 + i * 1_000 for i in range(120)],
            "amount": [16_000_000 + i * 2_000 for i in range(120)],
        }
    )
    trigger = _observe_trigger_condition(sample, sample["action"]["horizon"], default_text="先等更多确认。")
    assert "技术上先看" in trigger


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
    assert "本轮新闻/事件覆盖基本正常。 分母是进入详细分析的样本，不是全市场扫描池。" in rendered
    assert "热度也更看多源共振，而不是单条提及" not in rendered


def test_render_stock_picks_detailed_backfills_catalyst_and_degraded_history_sections() -> None:
    analysis = _sample_analysis("600989", "宝丰能源", "cn_stock", rank=1)
    analysis["dimensions"]["catalyst"]["evidence"] = []
    analysis["signal_confidence"] = {
        "available": False,
        "reason": "当前用了历史降级快照，不能在低置信历史上继续推导相似样本统计。",
    }
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "market_label": "A股",
        "top": [analysis],
        "watch_positive": [analysis],
    }
    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)
    assert "证据口径：" in rendered
    assert "高置信直连催化" in rendered or "直连情报" in rendered
    assert "下一步怎么盯：" in rendered
    assert "## 历史相似样本验证" in rendered
    assert "非重叠样本" in rendered
    assert "样本质量" in rendered
    assert "关键盯盘价位" in rendered


def test_render_stock_picks_detailed_sanitizes_event_digest_intraday_wording() -> None:
    analysis = _sample_analysis("603259", "药明康德", "cn_stock", rank=1)
    analysis["dimensions"]["catalyst"]["theme_news"] = [
        {
            "layer": "行业主题事件",
            "title": "打板风险提示：药明康德 竞价明显低开",
            "source": "财联社",
            "date": "2026-04-03",
            "freshness_bucket": "fresh",
            "age_days": 0,
            "signal_type": "打板过热",
            "signal_strength": "中",
            "signal_conclusion": "药明康德 打板/情绪风险偏高：竞价明显低开。",
        }
    ]
    payload = {
        "generated_at": "2026-04-03 18:00:00",
        "day_theme": {"label": "成长修复"},
        "regime": {"current_regime": "disinflation"},
        "market_label": "A股",
        "top": [analysis],
        "watch_positive": [],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "竞价明显低开" in rendered
    assert "开局明显低开" not in rendered


def test_render_stock_picks_sanitizes_intraday_wording_in_final_body() -> None:
    analysis = _sample_analysis("603259", "药明康德", "cn_stock", rank=1)
    analysis["dimensions"]["catalyst"]["theme_news"] = [
        {
            "layer": "行业主题事件",
            "title": "打板风险提示：药明康德 竞价明显低开",
            "source": "财联社",
            "date": "2026-04-03",
            "freshness_bucket": "fresh",
            "age_days": 0,
            "signal_type": "打板过热",
            "signal_strength": "中",
            "signal_conclusion": "药明康德 打板/情绪风险偏高：竞价明显低开。",
        }
    ]
    payload = {
        "generated_at": "2026-04-03 18:00:00",
        "day_theme": {"label": "成长修复"},
        "regime": {"current_regime": "disinflation"},
        "market_label": "A股",
        "top": [analysis],
        "watch_positive": [],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "竞价明显低开" in rendered
    assert "开局明显低开" not in rendered


def test_render_stock_picks_detailed_compacts_watch_items() -> None:
    watch_one = _sample_analysis("601857", "中国石油", "cn_stock", rank=1)
    watch_one["action"]["entry"] = "当前缺少完整日线历史，先按观察仓处理；更适合等补齐日线后再确认趋势。"
    watch_one["action"]["buy_range"] = ""
    watch_one["action"]["stop_ref"] = 11.72
    watch_one["action"]["target_ref"] = 12.95
    watch_one["dimensions"]["relative_strength"]["score"] = 15
    watch_two = _sample_analysis("002195", "岩山科技", "cn_stock", rank=1)
    watch_two["action"]["entry"] = "当前缺少完整日线历史，先按观察仓处理；更适合等补齐日线后再确认趋势。"
    watch_two["action"]["buy_range"] = ""
    watch_two["action"]["stop_ref"] = 10.33
    watch_two["action"]["target_ref"] = 11.36
    watch_two["dimensions"]["relative_strength"]["score"] = 22
    watch_three = _sample_analysis("600406", "国电南瑞", "cn_stock", rank=1)
    watch_three["action"]["direction"] = "观察为主"
    watch_three["action"]["position"] = "暂不出手"
    watch_three["action"]["entry"] = "等量价重新同步前先继续跟踪。"
    watch_three["action"]["buy_range"] = ""
    watch_three["action"]["stop_ref"] = 24.18
    watch_three["action"]["target_ref"] = 26.72
    watch_three["metrics"] = {"last_close": 25.1}
    watch_three["dimensions"]["technical"]["score"] = 28
    watch_three["dimensions"]["relative_strength"]["score"] = 31
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "market_label": "A股",
        "top": [watch_one, watch_two, watch_three],
        "watch_positive": [watch_one, watch_two, watch_three],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "### 观察触发器" in rendered
    assert "### 第二批：继续跟踪" in rendered
    assert "### 第二批：低门槛 / 观察替代" in rendered
    assert "#### 低门槛继续跟踪" in rendered
    assert "## 正式动作阈值" not in rendered
    assert "升级条件" in rendered
    assert "## 观察名单复核卡" in rendered
    assert "| 层次 | 标的 | 为什么继续看 | 主要卡点 | 升级条件 | 关键盯盘价位 |" in rendered
    assert "| 标的 | 为什么还没进第一批 | 现在更该看什么 |" in rendered
    assert "关键盯盘价位" in rendered
    assert "八维速览：" in rendered
    assert "硬检查：" in rendered
    assert "### 看好但暂不推荐" not in rendered
    assert "## 代表样本复核卡" in rendered
    assert rendered.count("**先看结论：**") <= 8
    assert "## 仓位纪律" in rendered
    assert "相对强弱：更像主题方向没坏，但个股右侧扩散和价格确认还没完成" in rendered


def test_render_stock_picks_detailed_keeps_followup_sections_when_watch_pool_is_small() -> None:
    watch_one = _sample_analysis("300750", "宁德时代", "cn_stock", rank=1)
    watch_one["action"]["direction"] = "观察为主"
    watch_one["action"]["position"] = "暂不出手"
    watch_one["action"]["entry"] = "先等量价重新同步。"
    watch_one["metrics"] = {"last_close": 245.0}
    watch_two = _sample_analysis("300308", "中际旭创", "cn_stock", rank=1)
    watch_two["action"]["direction"] = "观察为主"
    watch_two["action"]["position"] = "暂不出手"
    watch_two["action"]["entry"] = "先等右侧确认。"
    watch_two["metrics"] = {"last_close": 185.0}
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "AI 算力"},
        "regime": {"current_regime": "recovery"},
        "market_label": "A股",
        "top": [watch_one, watch_two],
        "watch_positive": [watch_one, watch_two],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "### 第二批：继续跟踪" in rendered
    assert "暂时没有更后排但仍值得单列的第二批补充标的" in rendered
    assert "### 第二批：低门槛 / 观察替代" in rendered


def test_render_stock_picks_detailed_downgrades_observe_only_title_and_history_heading() -> None:
    analysis = _sample_analysis("002241", "歌尔股份", "cn_stock", rank=1)
    analysis["action"]["direction"] = "回避"
    analysis["action"]["position"] = "暂不出手"
    analysis["action"]["entry"] = "等回踩确认前先别急着动手。"
    analysis["metrics"] = {"last_close": 23.4}
    analysis["metadata"] = {"sector": "人工智能", "chain_nodes": ["AI算力"]}
    analysis["visuals"] = {
        "dashboard": "/tmp/observe-dashboard.png",
        "windows": "/tmp/observe-windows.png",
    }
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "AI/半导体催化"},
        "regime": {"current_regime": "deflation"},
        "market_label": "A股",
        "top": [analysis],
        "coverage_analyses": [analysis],
        "watch_positive": [analysis],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "# 今日个股观察（详细版） | 2026-03-11" in rendered
    assert "## 首页判断" in rendered
    assert "## 今日结论" not in rendered
    assert "### 第一批：优先观察" in rendered
    assert "#### 图表速览" in rendered
    assert "![分析看板](/tmp/observe-dashboard.png)" in rendered
    assert "### 观察触发器" in rendered
    assert "## 历史相似样本附注" in rendered
    assert "样本给的是边界，不是免确认通行证" in rendered
    assert "## 历史相似样本验证" not in rendered
    assert "#### 低门槛继续跟踪" in rendered
    assert "## 观察名单复核卡" in rendered
    assert "## 代表样本复核卡" in rendered


def test_render_stock_picks_detailed_backfills_observe_visuals_when_payload_keeps_history(monkeypatch) -> None:
    analysis = _sample_analysis("601899", "紫金矿业", "cn_stock", rank=1)
    analysis["action"]["direction"] = "回避"
    analysis["action"]["position"] = "暂不出手"
    analysis["action"]["entry"] = "等回踩确认前先别急着动手。"
    analysis["history"] = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-03-10", "2026-03-11"]),
            "open": [30.0, 30.5],
            "high": [31.0, 31.2],
            "low": [29.8, 30.1],
            "close": [30.8, 31.0],
            "volume": [1000000, 1200000],
        }
    )

    class _FakeRenderer:
        def render(self, item):  # noqa: ANN001
            return {
                "dashboard": f"/tmp/{item['symbol']}_dashboard.png",
                "windows": f"/tmp/{item['symbol']}_windows.png",
            }

    monkeypatch.setattr("src.output.client_report._CHART_RENDERER", None)
    monkeypatch.setattr("src.output.client_report.AnalysisChartRenderer", lambda: _FakeRenderer())

    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "资源防守"},
        "regime": {"current_regime": "deflation"},
        "market_label": "A股",
        "top": [analysis],
        "coverage_analyses": [analysis],
        "watch_positive": [analysis],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "#### 图表速览" in rendered
    assert "![分析看板](/tmp/601899_dashboard.png)" in rendered
    assert "![阶段走势](/tmp/601899_windows.png)" in rendered


def test_pick_client_safe_line_softens_internal_miss_diagnostics() -> None:
    assert _pick_client_safe_line("近 7 日未命中直接政策催化") == "近 7 日直接政策情报偏弱"
    assert _pick_client_safe_line("未命中明确结构化公司事件") == "结构化公司事件暂不突出"
    assert _pick_client_safe_line("未来 14 日未命中直接催化事件") == "未来 14 日前瞻催化窗口暂不突出"
    assert _pick_client_safe_line("近 14 日未命中明确财报/年报事件窗口") == "近 14 日财报/年报窗口暂不突出"
    assert _pick_client_safe_line("当前没有高置信直连证据，摘要判断主要依赖覆盖率、基金画像和现有代理信号。") == "当前前置的一手情报偏少，摘要判断更多参考覆盖率、基金画像和现有代理信号。"
    assert _pick_client_safe_line("未命中高置信直连源") == "当前前置证据以结构化披露和主题线索为主"
    assert _pick_client_safe_line("新鲜情报 0 条") == "新鲜情报偏少"
    assert _pick_client_safe_line("覆盖源 0 个") == "情报覆盖偏窄"
    assert _pick_client_safe_line("眼下更卡在催化面还停在“近 7 日直接政策情报偏弱”。") == "眼下更卡在催化面还缺新增直接情报确认。"
    assert _pick_client_safe_line("在催化面还停在“近 7 日直接政策情报偏弱”改善前，不要把观察仓误解成趋势已经重启。") == "在催化面新增直接情报确认回来前，不要把观察仓误解成趋势已经重启。"
    assert _pick_client_safe_line("眼下更卡在风险特征还停在“当前回撤 13.1%，历史分位 26%”。") == "眼下更卡在风险收益比还不够舒服。"
    assert _pick_client_safe_line("在相对强弱还停在“相对基准 5日 -0.23% / 20日 -0.03%”改善前，不要把观察仓误解成趋势已经重启。") == "在相对强弱重新转强前，不要把观察仓误解成趋势已经重启。"
    assert _pick_client_safe_line("先等季节/日历还停在“同月胜率 0%（3 年样本）”改善，再讨论第二笔。") == "先等时间窗口改善后，再讨论第二笔。"
    assert _pick_client_safe_line("内部覆盖率摘要") == "覆盖率摘要"
    assert _pick_client_safe_line("当前 前置事件先看 `主题事件：价格/排产验证`。。") == "当前更该前置的是 `主题事件：价格/排产验证`。"
    assert _pick_client_safe_line("美股开盘前观察") == "晚间外盘观察"
    assert _pick_client_safe_line("开盘前观察") == "盘前观察"
    assert _pick_client_safe_line("竞价明显低开") == "竞价明显低开"
    assert _pick_client_safe_line("竞价高开且量比放大") == "竞价高开且量比放大"
    assert _pick_client_safe_line("龙虎榜/竞价/涨跌停边界") == "龙虎榜/竞价/涨跌停边界"
    assert _pick_client_safe_line("推荐理由优先按标准指数暴露理解。") == "推荐理由优先按标准指数暴露理解。"


def test_render_stock_picks_detailed_leads_with_structured_event_coverage_explanation() -> None:
    analysis = _sample_analysis("000975", "山金国际", "cn_stock", rank=1)
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "recovery"},
        "market_label": "A股",
        "stock_pick_coverage": {
            "note": "本轮新闻/事件覆盖基本正常。",
            "lines": ["A股 结构化事件覆盖 100%（3/3） / 高置信公司级直连情报覆盖 0%（0/3）"],
        },
        "top": [analysis],
        "coverage_analyses": [analysis],
        "watch_positive": [analysis],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "当前证据更偏结构化事件与公告日历，不是直连情报催化型驱动；`0%` 只代表没命中高置信个股直连情报。" in rendered


def test_render_stock_picks_detailed_marks_sector_filtered_scope_as_same_theme_ranking() -> None:
    analysis = _sample_analysis("000975", "山金国际", "cn_stock", rank=1)
    analysis["action"]["direction"] = "回避"
    analysis["action"]["position"] = "暂不出手"
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "recovery"},
        "market_label": "A股",
        "sector_filter": "黄金",
        "top": [analysis],
        "coverage_analyses": [analysis],
        "watch_positive": [analysis],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "## 今日结论" not in rendered
    assert "| 范围说明 | 当前是 `黄金` 主题内相对排序，不是跨主题分散候选池。 |" in rendered


def test_render_stock_picks_detailed_includes_market_proxy_section() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "market_label": "A股",
        "proxy_contract": {
            "market_flow": {
                "interpretation": "黄金相对成长更抗跌，资金风格偏防守。",
                "confidence_label": "中",
                "coverage_summary": "科技 / 黄金 / 国内 / 海外",
                "limitation": "这是相对强弱代理，不是原始流向数据。",
                "downgrade_impact": "可辅助判断主线切换，但不应单独决定交易动作。",
            },
            "social_sentiment": {
                "covered": 2,
                "total": 2,
                "confidence_labels": {"中": 1, "高": 1},
                "coverage_summary": "2/2 只候选已生成情绪代理",
                "limitation": "这是价格和量能行为推导出的情绪代理，不是真实社媒抓取。",
                "downgrade_impact": "更适合提示拥挤线索，不适合单独作为买卖信号。",
            },
        },
        "top": [_sample_analysis("300502", "新易盛", "cn_stock", rank=3)],
        "watch_positive": [],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "## 市场代理信号" in rendered
    assert "市场风格代理" in rendered
    assert "情绪代理" in rendered
    assert "科技 / 黄金 / 国内 / 海外" in rendered
    assert "真实社媒抓取" in rendered


def test_render_stock_picks_detailed_contextualizes_short_term_risk_on_against_stagflation() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {
            "current_regime": "stagflation",
            "reasoning": ["PMI 低于 50。", "美元偏强且增长承压。"],
        },
        "market_label": "A股",
        "proxy_contract": {
            "market_flow": {
                "interpretation": "成长相对黄金更强，资金风格偏 risk-on。",
                "confidence_label": "中",
                "coverage_summary": "科技 / 黄金 / 国内 / 海外",
                "limitation": "这是相对强弱代理，不是原始流向数据。",
                "downgrade_impact": "可辅助判断主线切换，但不应单独决定交易动作。",
            },
            "social_sentiment": {
                "covered": 1,
                "total": 1,
                "confidence_labels": {"高": 1},
                "coverage_summary": "1/1 只候选已生成情绪代理",
                "limitation": "这是价格和量能行为推导出的情绪代理，不是真实社媒抓取。",
                "downgrade_impact": "更适合提示拥挤线索，不适合单独作为买卖信号。",
            },
        },
        "top": [_sample_analysis("300502", "新易盛", "cn_stock", rank=3)],
        "watch_positive": [],
    }
    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)
    assert "短线风险偏好修复" in rendered
    assert "不等于中期 `stagflation` 背景已经切回成长主导" in rendered


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
    assert "- 港股今天短线先看：`快手-W`；中线暂不单列" in rendered


def test_render_stock_picks_surfaces_affordable_cn_batch_from_coverage_pool() -> None:
    leader = _sample_analysis("300502", "新易盛", "cn_stock", rank=3)
    leader["metrics"] = {"last_close": 122.5}
    affordable = _sample_analysis("002241", "歌尔股份", "cn_stock", rank=2)
    affordable["metrics"] = {"last_close": 23.4}
    affordable["metadata"] = {"sector": "人工智能", "chain_nodes": ["AI算力"]}
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "AI/半导体催化"},
        "regime": {"current_regime": "deflation"},
        "top": [leader],
        "coverage_analyses": [leader, affordable],
        "watch_positive": [{"symbol": "002241", "name": "歌尔股份"}],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "- A股低门槛可执行先看：`歌尔股份`" in rendered
    assert "- A股关联ETF平替先看：`人工智能ETF`" in rendered
    assert "### 第二批：低门槛 / 关联ETF" in rendered
    assert "#### 低门槛可执行" in rendered
    assert "#### 关联ETF平替" in rendered
    assert "2340 元/100股" in rendered
    assert "人工智能ETF (515070)" in rendered


def test_render_stock_picks_downgrades_observe_only_packaging() -> None:
    watch_one = _sample_analysis("002241", "歌尔股份", "cn_stock", rank=1)
    watch_one["action"]["direction"] = "回避"
    watch_one["action"]["position"] = "暂不出手"
    watch_one["action"]["entry"] = "等回踩确认前先别急着动手。"
    watch_one["metrics"] = {"last_close": 23.4}
    watch_one["metadata"] = {"sector": "人工智能", "chain_nodes": ["AI算力"]}
    watch_two = _sample_analysis("300502", "新易盛", "cn_stock", rank=1)
    watch_two["action"]["direction"] = "观察为主"
    watch_two["action"]["position"] = "暂不出手"
    watch_two["action"]["entry"] = "等 MA20 向上前先观察。"
    watch_two["metrics"] = {"last_close": 122.5}
    watch_two["metadata"] = {"sector": "人工智能", "chain_nodes": ["光模块"]}
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "AI/半导体催化"},
        "regime": {"current_regime": "deflation"},
        "top": [watch_one, watch_two],
        "coverage_analyses": [watch_one, watch_two],
        "watch_positive": [watch_one, watch_two],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "# 今日个股观察 | 2026-03-11" in rendered
    assert "今天先不出手更合理" in rendered
    assert "### 第一批：优先观察" in rendered
    assert "### 第二批：低门槛 / 观察替代" in rendered
    assert "#### 关联ETF观察" in rendered
    assert "#### 低门槛继续跟踪" in rendered
    assert "短线先看" not in rendered
    assert "#### 低门槛可执行" not in rendered


def test_render_stock_picks_keeps_observe_packaging_when_strategy_confidence_is_only_watch() -> None:
    analysis = _sample_analysis("601899", "紫金矿业", "cn_stock", rank=1)
    analysis["trade_state"] = "观察为主"
    analysis["narrative"]["judgment"] = {"state": "观察为主"}
    analysis["strategy_background_confidence"] = {
        "status": "watch",
        "label": "观察",
        "reason": "最近只有 `2` 个可验证样本，暂时不够把它当成稳定策略，只能做辅助说明。",
    }
    analysis["dimensions"]["technical"]["score"] = 48
    analysis["dimensions"]["fundamental"]["score"] = 78
    analysis["dimensions"]["catalyst"]["score"] = 64
    analysis["dimensions"]["relative_strength"]["score"] = 72
    payload = {
        "generated_at": "2026-03-29 10:00:00",
        "day_theme": {"label": "资源轮动"},
        "regime": {"current_regime": "recovery"},
        "top": [analysis],
        "coverage_analyses": [analysis],
        "watch_positive": [analysis],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "# 今日个股观察 | 2026-03-29" in rendered
    assert "| 报告定位 | 观察稿 |" in rendered
    assert "### 看好但暂不推荐" not in rendered


def test_render_stock_picks_detailed_watch_triggers_surface_strategy_background_confidence() -> None:
    analysis = _sample_analysis("601899", "紫金矿业", "cn_stock", rank=1)
    analysis["trade_state"] = "观察为主"
    analysis["narrative"]["judgment"] = {"state": "观察为主"}
    analysis["strategy_background_confidence"] = {
        "status": "watch",
        "label": "观察",
        "reason": "最近只有 `2` 个可验证样本，暂时不够把它当成稳定策略，只能做辅助说明。",
    }
    analysis["action"]["entry"] = "等回踩确认前先别急着动手。"
    analysis["action"]["buy_range"] = ""
    analysis["action"]["stop_ref"] = 17.25
    analysis["action"]["target_ref"] = 18.90
    payload = {
        "generated_at": "2026-03-29 10:00:00",
        "day_theme": {"label": "资源轮动"},
        "regime": {"current_regime": "recovery"},
        "market_label": "A股",
        "top": [analysis],
        "coverage_analyses": [analysis],
        "watch_positive": [analysis],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "### 观察触发器" in rendered
    assert "后台验证当前只到观察，这次先只作辅助说明，不单靠它升级动作" in rendered


def test_render_stock_picks_detailed_surfaces_market_event_rows_in_key_evidence() -> None:
    analysis = _sample_analysis("600276", "恒瑞医药", "cn_stock", rank=1)
    analysis["trade_state"] = "观察为主"
    analysis["narrative"]["judgment"] = {"state": "观察为主"}
    analysis["market_event_rows"] = [
        [
            "2026-04-03",
            "卖方共识非当期：恒瑞医药 最新券商金股仍停在 2026-02",
            "卖方共识专题",
            "低",
            "恒瑞医药",
            "",
            "卖方共识观察",
            "卖方月度金股最新停在 2026-02，当前不按本月 fresh 共识处理；最近一次命中 1 家券商推荐。",
        ]
    ]
    payload = {
        "generated_at": "2026-04-03 10:00:00",
        "day_theme": {"label": "创新药"},
        "regime": {"current_regime": "recovery"},
        "market_label": "A股",
        "top": [analysis],
        "coverage_analyses": [analysis],
        "watch_positive": [analysis],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "## 关键证据" in rendered
    assert "卖方共识非当期：恒瑞医药 最新券商金股仍停在 2026-02" in rendered
    assert "信号类型：`卖方共识观察`" in rendered


def test_render_stock_picks_detailed_surfaces_tdx_structure_in_key_evidence() -> None:
    analysis = _sample_analysis("300308", "中际旭创", "cn_stock", rank=1)
    analysis["trade_state"] = "观察为主"
    analysis["narrative"]["judgment"] = {"state": "观察为主"}
    analysis["market_event_rows"] = [
        [
            "2026-04-01",
            "TDX 结构框架：中际旭创 通信设备 / 进攻 / CN（+3.60%）",
            "TDX结构专题",
            "高",
            "通信设备",
            "",
            "标准结构归因",
            "偏利多，`中际旭创` 的标准板块/风格/地区框架已可直接用来解释当前强弱。",
        ]
    ]
    payload = {
        "generated_at": "2026-04-03 10:00:00",
        "day_theme": {"label": "算力"},
        "regime": {"current_regime": "recovery"},
        "market_label": "A股",
        "top": [analysis],
        "coverage_analyses": [analysis],
        "watch_positive": [analysis],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)
    evidence_section = rendered.split("## 关键证据", 1)[1].split("\n## ", 1)[0]

    assert "TDX 结构框架：中际旭创 通信设备 / 进攻 / CN（+3.60%）" in evidence_section
    assert "信号类型：`标准结构归因`" in evidence_section


def test_render_stock_picks_detailed_dedupes_shared_evidence_across_sections() -> None:
    first = _sample_analysis("300274", "阳光电源", "cn_stock", rank=1)
    second = _sample_analysis("300750", "宁德时代", "cn_stock", rank=1)
    shared_evidence = [
        {
            "layer": "结构化事件",
            "title": "AIDC细分黄金赛道，全球巨头订单积压至2030年！A股布局公司出炉",
            "source": "同花顺",
            "date": "2026-04-04",
            "link": "https://example.com/aidc",
        }
    ]
    first["dimensions"]["catalyst"]["evidence"] = list(shared_evidence)
    second["dimensions"]["catalyst"]["evidence"] = list(shared_evidence)
    first["trade_state"] = "观察为主"
    second["trade_state"] = "观察为主"
    first["narrative"]["judgment"] = {"state": "观察为主"}
    second["narrative"]["judgment"] = {"state": "观察为主"}
    payload = {
        "generated_at": "2026-04-05 18:39:15",
        "day_theme": {"label": "AIDC"},
        "regime": {"current_regime": "recovery"},
        "market_label": "A股",
        "top": [first, second],
        "coverage_analyses": [first, second],
        "watch_positive": [first, second],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert rendered.count("AIDC细分黄金赛道，全球巨头订单积压至2030年！A股布局公司出炉") == 3
    assert "同类前置证据已在本报告前文共享区展示，这里不再重复展开。" in rendered


def test_render_stock_picks_detailed_prioritizes_irm_and_broker_over_generic_framework_rows() -> None:
    analysis = _sample_analysis("603259", "药明康德", "cn_stock", rank=1)
    analysis["trade_state"] = "观察为主"
    analysis["narrative"]["judgment"] = {"state": "观察为主"}
    analysis["market_event_rows"] = [
        ["2026-04-03", "标准行业框架：药明康德 属于 申万二级行业·化学制药（-1.20%）", "申万行业框架", "低", "化学制药", "", "行业框架承压", "行业指数仍在回落。"],
        ["2026-04-03", "相关指数框架：创新药（-0.80%）", "相关指数/框架", "低", "创新药", "", "行业/指数映射", "先看相关指数能否止跌。"],
        ["2026-04-03", "卖方共识非当期：药明康德 最新券商金股仍停在 2026-02", "卖方共识专题", "低", "药明康德", "", "卖方共识观察", "最近一次命中 3 家券商推荐。"],
        ["2026-04-03", "互动易确认：公司回复海外订单进展", "互动易/投资者关系", "中", "药明康德", "", "管理层口径确认", "先按补充证据处理，不替代正式公告。"],
    ]
    payload = {
        "generated_at": "2026-04-03 10:00:00",
        "day_theme": {"label": "创新药"},
        "regime": {"current_regime": "recovery"},
        "market_label": "A股",
        "top": [analysis],
        "coverage_analyses": [analysis],
        "watch_positive": [analysis],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)
    evidence_section = rendered.split("## 关键证据", 1)[1].split("\n## ", 1)[0]

    assert evidence_section.index("互动易确认：公司回复海外订单进展") < evidence_section.index("标准行业框架：药明康德 属于 申万二级行业·化学制药")
    assert evidence_section.index("卖方共识非当期：药明康德 最新券商金股仍停在 2026-02") < evidence_section.index("相关指数框架：创新药（-0.80%）")


def test_render_scan_has_reasoning_and_position_management() -> None:
    analysis = _sample_analysis("561380", "电网ETF", "cn_etf", rank=1)
    analysis["dimensions"]["relative_strength"]["benchmark_name"] = "沪深300ETF"
    analysis["dimensions"]["relative_strength"]["benchmark_symbol"] = "510300"
    analysis["narrative"]["validation_points"] = [
        {
            "watch": "动能重启",
            "judge": "MACD 金叉且收盘站回 MA20",
            "bull": "说明趋势确认增强。",
            "bear": "说明仍需继续观察。",
        }
    ]
    rendered = ClientReportRenderer().render_scan(analysis)
    assert "## 先看执行" in rendered
    assert rendered.index("## 先看执行") < rendered.index("## 首页判断")
    assert "| 看不看 | 小仓试仓；持有优于追高 |" in rendered
    assert "| 怎么触发 |" in rendered
    assert "| 多大仓位 | 首次建仓 ≤3% |" in rendered
    assert "| 哪里止损 | 跌破关键支撑离场 |" in rendered
    assert "## 执行摘要" in rendered
    assert "| 当前建议 | 小仓试仓；持有优于追高 |" in rendered
    assert "| 空仓怎么做 |" in rendered
    assert "| 持仓怎么做 |" in rendered
    assert "| 首次仓位 | 首次建仓 ≤3% |" in rendered
    assert "**升级触发器：** 动能重启：MACD 金叉且收盘站回 MA20" in rendered
    assert "## 为什么这么判断" in rendered
    assert "**先看结论：** 这段先看八维里哪几项在支撑，哪几项在拖后腿。" in rendered
    assert "## 硬检查" in rendered
    assert "## 关键证据" in rendered
    assert "https://example.com/earnings" in rendered
    assert "## 证据时点与来源" in rendered
    assert "行情来源" in rendered
    assert "AKShare 日线回退" in rendered
    assert "## 当前更合适的动作" in rendered
    assert "先看动作和触发条件，再决定要不要给它执行优先级。" in rendered
    assert "| 适用时段 |" in rendered
    assert "| 建议买入区间 | 9.850 - 10.000 |" in rendered
    assert "| 第一减仓位 | `11.200` 附近先兑现第一段反弹，别把第一波空间一次性坐回去。 |" in rendered
    assert "| 第二减仓位 | 若放量站上 `11.200`，再看 `11.500` 一带是否做第二次减仓。 |" in rendered
    assert "| 上修条件 | 只有放量站上 `11.500` 且催化、相对强弱和量能继续增强，才考虑把目标继续上修到 `先看前高`。 |" in rendered
    assert "## 仓位管理" in rendered
    assert "## 组合落单前" in rendered
    assert "portfolio whatif buy 561380" in rendered
    assert "## 分维度详解" in rendered
    assert "**历史相似样本附注：**" in rendered
    assert "只作边界附注" in rendered
    assert "| 相对强弱基准 | 沪深300ETF (510300) |" in rendered


def test_render_scan_and_stock_analysis_include_what_changed_section(monkeypatch) -> None:
    _install_fake_thesis_repo(monkeypatch, {"300502": {
        "core_assumption": "800G 光模块放量兑现",
        "validation_metric": "订单和毛利率同步改善",
        "holding_period": "1-3个月",
        "event_digest_snapshot": {
            "status": "待补充",
            "lead_layer": "行业主题事件",
            "lead_detail": "主题事件：主题热度/映射",
            "lead_title": "AI算力链热度扩散",
            "impact_summary": "资金偏好 / 景气",
            "thesis_scope": "待确认",
        },
    }})
    analysis = _sample_analysis("300502", "新易盛", "cn_stock", rank=3)

    rendered = ClientReportRenderer().render_scan(analysis)

    assert "## What Changed" in rendered
    assert "上次怎么看：核心假设是 `800G 光模块放量兑现`" in rendered
    assert "主题事件：主题热度/映射" in rendered
    assert "这次什么变了：事件状态从 `待补充` 升到 `已消化`" in rendered
    assert "当前更该前置的是 `" in rendered
    assert "当前事件理解：" in rendered
    assert "结论变化：`升级`" in rendered
    assert "触发：事件完成消化" in rendered
    assert "状态解释：" in rendered

    stock_analysis_rendered = ClientReportRenderer().render_stock_analysis(analysis)

    assert "## What Changed" in stock_analysis_rendered
    assert "这次什么变了：事件状态从 `待补充` 升到 `已消化`" in stock_analysis_rendered


def test_evidence_lines_prioritize_earnings_and_announcements_over_generic_news() -> None:
    lines = _evidence_lines(
        [
            {
                "layer": "新闻热度",
                "title": "板块讨论热度上升",
                "source": "财联社",
                "date": "2026-03-29",
            },
            {
                "layer": "结构化事件",
                "title": "公司公告：800G 光模块新品发布",
                "source": "证券时报",
                "date": "2026-03-28",
            },
            {
                "layer": "龙头公告/业绩",
                "title": "公司年报：2026Q1 指引上修",
                "source": "Investor Relations",
                "date": "2026-03-29",
            },
        ],
        max_items=3,
        as_of="2026-03-29 10:00:00",
    )
    assert "年报" in lines[0]
    assert "财报摘要：盈利/指引上修" in lines[0]
    assert "更直接影响 `盈利 / 估值`" in lines[0]
    assert "前置理由：" in lines[0]
    assert "新品发布" in lines[1]
    assert "公告类型：产品/新品" in lines[1]
    assert "更直接影响 `景气 / 资金偏好`" in lines[1]
    assert "先观察，因为" in lines[1]
    assert "情报属性：`新鲜情报 / 一手直连 / 结构化披露`" in lines[0]
    assert "来源层级：`官方直连 / 结构化披露`" in lines[0]


def test_evidence_lines_surface_stale_vs_direct_intelligence_tags() -> None:
    lines = _evidence_lines(
        [
            {
                "layer": "主题级关键新闻",
                "title": "板块热度继续发酵",
                "source": "财联社",
                "date": "2026-03-24",
                "freshness_bucket": "stale",
                "age_days": 6,
            },
            {
                "layer": "结构化事件",
                "title": "公司公告：中标国家电网项目",
                "source": "巨潮资讯",
                "date": "2026-03-29",
                "freshness_bucket": "fresh",
                "age_days": 1,
            },
        ],
        max_items=2,
        as_of="2026-03-30 10:00:00",
    )

    assert "情报属性：`新鲜情报 / 一手直连 / 结构化披露`" in lines[0]
    assert "来源层级：`官方直连 / 结构化披露`" in lines[0]
    assert "旧闻回放" in lines[1]
    assert "媒体直连" in lines[1]
    assert "主题级情报" in lines[1]


def test_evidence_lines_with_event_digest_reuses_since_last_review_tags() -> None:
    event_digest = {
        "previous_reviewed_at": "2026-03-30 09:00:00",
        "items": [
            {
                "layer": "行业主题事件",
                "raw_layer": "主题级关键新闻",
                "title": "AI服务器资本开支延续，先进封装设备链情绪回暖",
                "source": "Reuters",
                "configured_source": "Reuters",
                "category": "topic_search",
                "date": "2026-03-29",
                "freshness_bucket": "fresh",
                "age_days": 1,
            }
        ],
    }

    lines = _evidence_lines_with_event_digest(
        [],
        event_digest=event_digest,
        max_items=1,
        as_of="2026-03-31 10:00:00",
    )

    assert "情报属性：`旧闻回放 / 媒体直连 / 搜索回退 / 主题级情报`" in lines[0]
    assert "来源层级：`媒体直连`" in lines[0]
    assert "复查语境：自上次复查（`2026-03-30 09:00:00`）以来" in lines[0]


def test_evidence_lines_with_event_digest_softens_old_structured_disclosure_to_history_baseline() -> None:
    lines = _evidence_lines_with_event_digest(
        [
            {
                "layer": "结构化事件",
                "title": "紫金矿业 披露现金分红预案（每10股派现 0.38 元）",
                "source": "Tushare dividend",
                "date": "2026-03-21",
                "signal_type": "公告类型：分红/回报",
                "signal_strength": "强",
                "signal_conclusion": "偏利多，已开始改写 `估值 / 资金偏好` 这层。",
                "thesis_scope": "thesis变化",
                "impact_summary": "估值 / 资金偏好",
            }
        ],
        event_digest={},
        max_items=1,
        as_of="2026-04-02 22:14:35",
        symbol="601899",
    )

    assert lines
    assert "信号强弱：`中`" in lines[0]
    assert "结论：中性，当前更多是历史基线，不把它直接当成新增催化。" in lines[0]
    assert "当前更像 `历史基线`" in lines[0]


def test_evidence_lines_surface_signal_type_and_strength() -> None:
    lines = _evidence_lines_with_event_digest(
        [
            {
                "layer": "龙头公告/业绩",
                "title": "公司披露一季报预增，利润率改善",
                "source": "CNINFO",
                "date": "2026-03-31",
                "link": "https://example.com/earnings",
            }
        ],
        event_digest={},
        max_items=1,
        as_of="2026-03-31 10:00:00",
        symbol="600519",
    )

    assert lines
    assert "信号类型：" in lines[0]
    assert "信号强弱：" in lines[0]
    assert "结论：" in lines[0]


def test_evidence_lines_with_event_digest_dedupes_same_evidence_across_layer_aliases() -> None:
    lines = _evidence_lines_with_event_digest(
        [
            {
                "layer": "结构化事件",
                "title": "紫金矿业 披露现金分红预案（每10股派现 0.38 元）",
                "source": "Tushare dividend",
                "date": "2026-03-21",
                "link": "https://example.com/dividend",
            }
        ],
        event_digest={
            "items": [
                {
                    "layer": "公告",
                    "title": "紫金矿业 披露现金分红预案（每10股派现 0.38 元）",
                    "source": "Tushare dividend",
                    "date": "2026-03-21",
                    "link": "https://example.com/dividend",
                }
            ]
        },
        max_items=3,
        as_of="2026-04-02 22:14:35",
        symbol="601899",
    )

    assert len(lines) == 1
    assert lines[0].count("紫金矿业 披露现金分红预案") == 1


def test_evidence_lines_with_event_digest_filters_diagnostic_coverage_rows() -> None:
    lines = _evidence_lines_with_event_digest(
        [
            {
                "layer": "新闻",
                "title": "当前可前置的一手情报有限，判断更多参考结构化事件和行业线索。",
                "source": "覆盖率摘要",
            },
            {
                "layer": "公告",
                "title": "基金公司披露季度运作报告",
                "source": "基金公司公告",
                "date": "2026-04-01",
                "link": "https://example.com/fund-report",
            },
        ],
        event_digest={},
        max_items=3,
        as_of="2026-04-01 10:00:00",
    )

    assert len(lines) == 1
    assert "基金公司披露季度运作报告" in lines[0]
    assert "当前可前置的一手情报有限" not in lines[0]


def test_evidence_lines_surface_official_site_search_as_ir_fallback() -> None:
    lines = _evidence_lines(
        [
            {
                "layer": "结构化事件",
                "title": "贵州茅台投资者关系活动记录表更新经营情况",
                "source": "Investor Relations",
                "configured_source": "Investor Relations::search",
                "source_note": "official_site_search",
                "category": "stock_live_intelligence",
                "date": "2026-03-31",
                "freshness_bucket": "fresh",
                "age_days": 0.5,
                "link": "https://ir.kweichowmoutai.com/cmscontent/123.html",
            },
        ],
        max_items=1,
        as_of="2026-03-31 10:00:00",
    )

    assert "情报属性：`新鲜情报 / 结构化披露 / 官网/IR回退 / 搜索回退`" in lines[0]
    assert "来源层级：`结构化披露`" in lines[0]


def test_evidence_lines_add_cninfo_fallback_link_for_structured_disclosure_without_direct_url() -> None:
    lines = _evidence_lines(
        [
            {
                "symbol": "600519",
                "layer": "结构化事件",
                "title": "贵州茅台分红方案：董事会预案；现金分红 27.6",
                "source": "Tushare",
                "configured_source": "Tushare::dividend",
                "source_note": "structured_disclosure",
                "date": "2026-03-31",
                "freshness_bucket": "fresh",
                "age_days": 0.5,
                "link": "",
            },
        ],
        max_items=1,
        as_of="2026-03-31 10:00:00",
    )

    assert "[贵州茅台分红方案：董事会预案；现金分红 27.6](https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600519)" in lines[0]


def test_evidence_lines_use_exchange_homepage_for_tushare_irm_items() -> None:
    lines = _evidence_lines(
        [
            {
                "layer": "结构化事件",
                "title": "贵州茅台互动平台问答：渠道库存与发货节奏；回复称以公开披露为准",
                "source": "Tushare",
                "configured_source": "Tushare::irm_qa_sh",
                "source_note": "structured_disclosure",
                "note": "投资者关系/路演纪要",
                "date": "2026-04-02",
                "freshness_bucket": "fresh",
                "age_days": 0.2,
                "link": "",
            },
        ],
        max_items=1,
        as_of="2026-04-02 18:00:00",
    )

    assert "[贵州茅台互动平台问答：渠道库存与发货节奏；回复称以公开披露为准](https://sns.sseinfo.com/)" in lines[0]
    assert "情报属性：`新鲜情报 / 结构化披露`" in lines[0]


def test_evidence_lines_use_fallback_symbol_for_tushare_prefixed_sources_without_item_symbol() -> None:
    lines = _evidence_lines(
        [
            {
                "layer": "结构化事件",
                "title": "紫金矿业 披露现金分红预案（每10股派现 0.38 元）",
                "source": "Tushare dividend",
                "configured_source": "Tushare dividend",
                "date": "2026-03-21",
                "freshness_bucket": "stale",
                "age_days": 10,
                "link": "",
            },
        ],
        max_items=1,
        as_of="2026-03-31 10:00:00",
        symbol="601899",
    )

    assert "[紫金矿业 披露现金分红预案（每10股派现 0.38 元）](https://www.cninfo.com.cn/new/disclosure/detail?stockCode=601899)" in lines[0]


def test_evidence_lines_prefer_official_direct_over_media_when_event_scores_are_close() -> None:
    lines = _evidence_lines(
        [
            {
                "layer": "结构化事件",
                "title": "公司交流纪要更新经营情况",
                "source": "财联社",
                "configured_source": "财联社",
                "date": "2026-03-31",
                "freshness_bucket": "fresh",
                "age_days": 0,
            },
            {
                "layer": "结构化事件",
                "title": "关于举办投资者关系活动的公告",
                "source": "CNINFO",
                "configured_source": "CNINFO::direct",
                "source_note": "official_direct",
                "note": "投资者关系/路演纪要",
                "date": "2026-03-31",
                "freshness_bucket": "fresh",
                "age_days": 0,
            },
        ],
        max_items=2,
        as_of="2026-03-31 10:00:00",
    )

    assert "关于举办投资者关系活动的公告" in lines[0]
    assert "来源层级：`官方直连 / 结构化披露`" in lines[0]


def test_evidence_lines_sanitize_internal_miss_titles_into_client_language() -> None:
    lines = _evidence_lines(
        [
            {
                "layer": "财报",
                "title": "未命中直接海外映射（海外映射）",
                "source": "Tushare",
                "configured_source": "Tushare::forecast",
                "source_note": "structured_disclosure",
                "date": "2026-03-31",
                "freshness_bucket": "fresh",
                "age_days": 0,
                "link": "",
                "lead_detail": "未命中直接龙头公告",
                "impact_summary": "盈利 / 估值",
                "thesis_scope": "thesis变化",
                "importance_reason": "未命中高置信直连源",
                "importance": "high",
            },
        ],
        max_items=1,
        as_of="2026-03-31 10:00:00",
        symbol="600519",
    )

    assert "未命中直接海外映射" not in lines[0]
    assert "未命中直接龙头公告" not in lines[0]
    assert "未命中高置信直连源" not in lines[0]
    assert "海外映射直连情报偏弱" in lines[0]
    assert "信号类型：" in lines[0]
    assert "事件理解：" in lines[0]


def test_render_scan_key_evidence_merges_event_digest_lead_item_when_raw_evidence_missing() -> None:
    analysis = _sample_analysis("600519", "贵州茅台", "cn_stock", rank=2)
    analysis["dimensions"]["catalyst"]["evidence"] = []
    analysis["dimensions"]["catalyst"]["theme_news"] = [
        {
            "layer": "行业主题事件",
            "title": "白酒渠道反馈显示动销环比改善",
            "source": "财联社",
            "date": "2026-03-29",
            "freshness_bucket": "fresh",
            "age_days": 1,
        }
    ]

    rendered = ClientReportRenderer().render_scan(analysis)

    evidence_section = rendered.split("## 关键证据", 1)[1]
    assert "白酒渠道反馈显示动销环比改善" in evidence_section
    assert "情报属性：`新鲜情报 / 媒体直连 / 主题级情报`" in evidence_section
    assert "来源层级：`媒体直连`" in evidence_section
    assert "复查语境：这是首次跟踪，当前先建立情报基线。" in evidence_section


def test_render_scan_observe_trigger_uses_trading_language_template() -> None:
    analysis = _sample_analysis("601857", "中国石油", "cn_stock", rank=1)
    analysis["action"]["entry"] = "当前缺少完整日线历史，先按观察仓处理；更适合等补齐日线后再确认趋势。"
    analysis["action"]["buy_range"] = ""
    analysis["action"]["stop_ref"] = 7.86
    analysis["action"]["target_ref"] = 8.52
    analysis["dimensions"]["relative_strength"]["score"] = 10
    rendered = ClientReportRenderer().render_scan(analysis)
    assert "| 怎么触发 | 先等补齐日线并确认 MA20 / MA60 拐头，再看相对强弱转正；触发前先别急着给精确买入价。 |" in rendered
    assert "| 触发买点条件 | 先等补齐日线并确认 MA20 / MA60 拐头，再看相对强弱转正。 |" in rendered
    assert "| 关键盯盘价位 | 下沿先看 `7.860` 上方企稳；上沿再看 `8.520` 附近能不能放量突破 |" in rendered
    assert "| 建议买入区间 |" not in rendered


def test_render_scan_softens_observe_only_topline_when_logic_still_survives() -> None:
    analysis = _sample_analysis("159698", "粮食ETF", "cn_etf", rank=0)
    analysis["rating"] = {"rank": 0, "label": "无信号", "stars": "—"}
    analysis["action"]["direction"] = "回避"
    analysis["action"]["position"] = "暂不出手"
    analysis["narrative"]["headline"] = "这是一个中期偏多，但短线仍在整理的标的。"
    analysis["narrative"]["judgment"] = {"direction": "中性偏多", "cycle": "中期(1-3月)", "odds": "低", "state": "持有优于追高"}
    analysis["narrative"]["validation_points"] = [
        {
            "watch": "动能重启",
            "judge": "MACD 金叉且收盘站回 MA20",
            "bull": "说明趋势确认增强。",
            "bear": "说明仍需继续观察。",
        }
    ]
    analysis["dimensions"]["technical"]["score"] = 28
    analysis["dimensions"]["technical"]["summary"] = "技术结构仍偏弱，暂不支持激进介入。"
    analysis["dimensions"]["catalyst"] = {
        "score": 23,
        "max_score": 100,
        "core_signal": "相关头条 7 条 · 覆盖源 4 个",
        "summary": "催化不足，当前更像静态博弈。",
        "factors": [
            {"name": "政策催化", "signal": "近 7 日未命中直接政策催化", "detail": "政策原文和一级媒体优先", "display_score": "0/30"},
            {"name": "产品/跟踪方向催化", "signal": "粮食ETF涨超1%", "detail": "当前命中跟踪基准 / 行业暴露", "display_score": "12/12"},
            {"name": "研报/新闻密度", "signal": "相关头条 7 条", "detail": "一级媒体头条密度", "display_score": "10/10"},
            {"name": "新闻热度", "signal": "覆盖源 4 个", "detail": "覆盖源数量", "display_score": "10/10"},
        ],
    }

    rendered = ClientReportRenderer().render_scan(analysis)

    assert "`中期逻辑未坏，短线暂无信号。`" in rendered
    assert "**升级触发器：** 动能重启：MACD 金叉且收盘站回 MA20" in rendered
    assert "直接催化偏弱，舆情关注度尚可，因此当前更像静态博弈。" in rendered


def test_render_scan_observe_compacts_disclosure_and_tightens_wording() -> None:
    analysis = _sample_analysis("300274", "阳光电源", "cn_stock", rank=0)
    analysis["rating"] = {"rank": 0, "label": "无信号", "stars": "—"}
    analysis["action"]["direction"] = "观察为主"
    analysis["action"]["position"] = "暂不出手"
    analysis["notes"] = [
        "财报窗口情报抓取存在降级，已按可用数据降级处理。",
        "部分结构化源当前缺失，先把缺失披露放在后文。",
    ]
    rendered = ClientReportRenderer().render_scan(analysis)

    def _section_bullets(markdown: str, heading: str) -> list[str]:
        items: list[str] = []
        inside = False
        for raw in markdown.splitlines():
            if raw.strip() == heading:
                inside = True
                continue
            if inside and raw.startswith("## "):
                break
            if inside and raw.strip().startswith("- "):
                items.append(raw.strip())
        return items

    assert len(_section_bullets(rendered, "## 数据限制与说明")) <= 1
    assert "先按辅助线索看，不单独升级动作" not in rendered
    assert "当前更像" not in rendered


def test_render_scan_sanitizes_internal_miss_diagnostics_in_factor_rows() -> None:
    analysis = _sample_analysis("600313", "农发种业", "cn_stock", rank=1)
    analysis["dimensions"]["catalyst"]["factors"] = [
        {"name": "政策催化", "signal": "近 7 日未命中直接政策催化", "detail": "政策原文和一级媒体优先", "display_score": "0/20"},
        {"name": "结构化事件", "signal": "未命中明确结构化公司事件", "detail": "当前未命中结构化公司事件；这里按信息不足处理，不直接等于个股没有催化。", "display_score": "0/15"},
        {"name": "前瞻催化", "signal": "未来 14 日未命中直接催化事件", "detail": "未来财报/发布会/事件窗口已纳入。", "display_score": "0/15"},
    ]
    analysis["dimensions"]["risk"]["factors"] = [
        {"name": "披露窗口", "signal": "近 14 日未命中明确财报/年报事件窗口", "detail": "当前未识别到会明显放大波动的披露窗口。", "display_score": "信息项"}
    ]

    rendered = ClientReportRenderer().render_scan(analysis)

    assert "近 7 日未命中直接政策催化" not in rendered
    assert "未命中明确结构化公司事件" not in rendered
    assert "未来 14 日未命中直接催化事件" not in rendered
    assert "近 14 日未命中明确财报/年报事件窗口" not in rendered
    assert "近 7 日直接政策情报偏弱" in rendered
    assert "结构化公司事件暂不突出" in rendered
    assert "未来 14 日前瞻催化窗口暂不突出" in rendered
    assert "近 14 日财报/年报窗口暂不突出" in rendered


def test_render_scan_sanitizes_provenance_lines_into_client_language() -> None:
    analysis = _sample_analysis("600313", "农发种业", "cn_stock", rank=1)
    analysis["dimensions"]["catalyst"]["evidence"] = []
    analysis["dimensions"]["catalyst"]["coverage"] = {
        "degraded": True,
        "diagnosis": "stale_live_only",
        "news_mode": "proxy",
    }
    analysis["relative_benchmark_name"] = "沪深300ETF"
    analysis["relative_benchmark_symbol"] = "510300"
    analysis["metadata"]["history_source_label"] = "Tushare 日线"

    rendered = "\n".join(_analysis_provenance_lines(analysis))

    assert "未命中高置信直连源" not in rendered
    assert "当前前置证据以结构化披露和主题线索为主" in rendered
    assert "未命中显式日期" not in rendered
    assert "日期未单独披露" in rendered


def test_render_scan_etf_keeps_fund_profile_sections_when_profile_available() -> None:
    analysis = _sample_analysis("563360", "A500ETF华泰柏瑞", "cn_etf", rank=1)
    analysis["fund_profile"] = {
        "overview": {
            "基金类型": "ETF",
            "基金管理人": "华泰柏瑞基金",
            "基金经理人": "张三",
            "成立日期": "2024-10-15",
            "首发规模": "20.00亿",
            "净资产规模": "41.20亿",
            "业绩比较基准": "中证A500指数收益率",
        },
        "style": {
            "tags": ["宽基", "大盘均衡"],
            "positioning": "偏均衡",
            "selection": "指数复制",
            "consistency": "较稳定",
        },
        "asset_allocation": [{"资产类型": "股票", "仓位占比": 96.2}],
        "top_holdings": [{"股票代码": "600519", "股票名称": "贵州茅台", "占净值比例": 3.2, "季度": "2025Q4"}],
        "industry_allocation": [{"行业类别": "食品饮料", "占净值比例": 12.5, "截止时间": "2025Q4"}],
    }

    rendered = ClientReportRenderer().render_scan(analysis)

    assert "## 基金画像" in rendered
    assert "## 基金经理风格分析" in rendered
    assert "### 资产配置" in rendered
    assert "### 前五大持仓" in rendered
    assert "### 行业暴露" in rendered


def test_render_scan_exec_summary_surfaces_theme_boundary_when_subthemes_conflict() -> None:
    analysis = _sample_analysis("560001", "政策主线ETF", "cn_etf", rank=1)
    analysis["action"]["direction"] = "观察为主"
    analysis["action"]["position"] = "暂不出手"
    analysis["narrative"]["judgment"] = {"state": "观察为主"}
    analysis["day_theme"] = {"label": "政策主线"}
    analysis["metadata"] = {"sector": "工业"}
    analysis["notes"] = ["一带一路、新质生产力和中特估一起走强。"]

    rendered = ClientReportRenderer().render_scan(analysis)

    assert "| 主题边界 |" in rendered
    assert "还没拉开" in rendered


def test_render_scan_reasoning_section_surfaces_theme_boundary_explainer() -> None:
    analysis = _sample_analysis("560001", "政策主线ETF", "cn_etf", rank=1)
    analysis["action"]["direction"] = "观察为主"
    analysis["action"]["position"] = "暂不出手"
    analysis["narrative"]["judgment"] = {"state": "观察为主"}
    analysis["day_theme"] = {"label": "政策主线"}
    analysis["metadata"] = {"sector": "工业"}
    analysis["notes"] = ["一带一路、新质生产力和中特估一起走强。"]

    rendered = ClientReportRenderer().render_scan(analysis)

    assert "## 为什么这么判断" in rendered
    assert "主题边界：" in rendered


def test_render_scan_surfaces_strategy_background_confidence_in_exec_summary_and_trigger() -> None:
    analysis = _sample_analysis("600519", "贵州茅台", "cn_stock", rank=1)
    analysis["trade_state"] = "观察为主"
    analysis["narrative"]["judgment"] = {"state": "观察为主"}
    analysis["strategy_background_confidence"] = {
        "status": "watch",
        "label": "观察",
        "reason": "最近只有 `2` 个可验证样本，暂时不够把它当成稳定策略，只能做辅助说明。",
    }

    rendered = ClientReportRenderer().render_scan(analysis)

    assert "| 后台置信度 | 观察：" in rendered
    assert "后台验证当前只到观察，这次先只作辅助说明，不单靠它升级动作" in rendered


def test_render_scan_exec_summary_surfaces_sector_bridge_hint() -> None:
    analysis = _sample_analysis("515230", "软件ETF", "cn_etf", rank=1)
    analysis["action"]["direction"] = "观察为主"
    analysis["action"]["position"] = "暂不出手"
    analysis["narrative"]["judgment"] = {"state": "观察为主"}
    analysis["day_theme"] = {"label": "AI算力"}
    analysis["metadata"] = {"sector": "信息技术"}
    analysis["notes"] = ["服务器、光模块和海外算力资本开支一起走强。"]

    rendered = ClientReportRenderer().render_scan(analysis)

    assert "| 细分观察 |" in rendered
    assert "信息技术" in rendered
    assert "AI算力" in rendered


def test_render_scan_detailed_reasoning_section_surfaces_sector_bridge_explainer() -> None:
    analysis = _sample_analysis("515230", "软件ETF", "cn_etf", rank=1)
    analysis["risks"] = []
    analysis["action"]["direction"] = "观察为主"
    analysis["action"]["position"] = "暂不出手"
    analysis["narrative"] = {
        "headline": "当前更适合按行业层观察，再看细分线索怎么收敛。",
        "judgment": {
            "direction": "中性偏多",
            "cycle": "短期(1-4周)",
            "odds": "低",
            "state": "观察为主",
        },
        "phase": {"label": "震荡整理", "body": "说明方向没坏，但还没形成新的执行共振。"},
        "drivers": {
            "macro": "宏观不逆风。",
            "flow": "资金仍在等更清晰的主线扩散。",
            "relative": "相对强弱有改善线索，但还没完全站稳。",
            "technical": "技术结构仍在修复。",
        },
        "contradiction": "行业层线索还在，但具体下钻到哪条细分线还没完全拉开。",
        "positives": ["方向还在。", "行业层逻辑没有被证伪。"],
        "cautions": ["细分线索仍在轮动。", "右侧确认还没补齐。"],
        "watch_points": ["继续观察服务器、光模块和算力资本开支。"],
        "scenarios": {"base": "先震荡整理。", "bull": "主线收敛后升级。", "bear": "失守支撑后转弱。"},
        "playbook": {"trend": "等确认。", "allocation": "先观察。", "defensive": "别急着追。"},
        "summary_lines": ["当前先按行业层观察。"],
        "risk_points": {
            "fundamental": "景气验证仍要跟踪。",
            "valuation": "高 beta 方向波动仍大。",
            "crowding": "细分轮动时容易来回切换。",
            "external": "海外科技资本开支预期变化会先影响它。",
        },
    }
    analysis["day_theme"] = {"label": "AI算力"}
    analysis["metadata"] = {"sector": "信息技术"}
    analysis["notes"] = ["服务器、光模块和海外算力资本开支一起走强。"]

    rendered = ClientReportRenderer().render_scan_detailed(analysis)

    assert "## 为什么这么判断" in rendered
    assert "细分观察：" in rendered
    assert "AI算力" in rendered


def test_render_scan_surfaces_strong_factor_breakdown() -> None:
    analysis = _sample_analysis("588200", "科创芯片ETF", "cn_etf", rank=2)
    analysis["dimensions"]["technical"]["factors"] = [
        {
            "name": "假突破识别",
            "signal": "看涨假突破：日内触及近期高点但收盘回落，多头未能守住突破位",
            "detail": "假突破是多空双方试探失败的信号。",
            "display_score": "0/8",
            "awarded": 0,
            "max": 8,
            "factor_id": "j1_false_break",
        }
    ]
    rendered = ClientReportRenderer().render_scan(analysis)
    assert "## 关键强因子拆解" in rendered
    assert "假突破识别（价量结构）" in rendered
    assert "关键位没有站稳时，更容易出现冲高回落或跌破后反抽" in rendered


def test_render_scan_surfaces_fundamental_and_macro_headwinds_in_strong_factor_breakdown() -> None:
    analysis = _sample_analysis("600519", "贵州茅台", "cn_stock", rank=2)
    analysis["dimensions"]["technical"]["factors"] = []
    analysis["dimensions"]["fundamental"]["factors"] = [
        {
            "name": "杠杆压力",
            "signal": "资产负债率 68.0%（较高杠杆，需关注偿债压力）",
            "detail": "资产负债率代理财务杠杆水平；数据源：季报（T+45 天 lag），报告期 2025-09-30。",
            "display_score": "-4/10",
            "awarded": -4,
            "max": 10,
            "factor_id": "j4_leverage",
        }
    ]
    analysis["dimensions"]["macro"]["factors"] = [
        {
            "name": "信用脉冲",
            "signal": "M1-M2 剪刀差 -7.2pct，信用脉冲收缩",
            "detail": "更偏中期环境因子，主要影响资金扩张、订单兑现和风险偏好。",
            "display_score": "-6/15",
            "awarded": -6,
            "max": 15,
            "factor_id": "m1_credit_impulse",
        }
    ]

    rendered = ClientReportRenderer().render_scan(analysis)
    assert "## 关键强因子拆解" in rendered
    assert "杠杆压力（质量/盈利）" in rendered
    assert "信用脉冲（宏观/风格）" in rendered
    assert "这层决定的是基本面质量够不够支撑持有" in rendered
    assert "宏观层回答的是外部环境有没有帮它抬估值" in rendered


def test_render_scan_strong_factor_breakdown_keeps_family_diversity() -> None:
    analysis = _sample_analysis("600519", "贵州茅台", "cn_stock", rank=2)
    analysis["dimensions"]["technical"]["factors"] = [
        {
            "name": "压力位",
            "signal": "上方存在近端压力：前高仍未消化",
            "detail": "上方近端压力会直接影响反弹空间和加速概率。",
            "display_score": "-8/15",
            "awarded": -8,
            "max": 15,
            "factor_id": "j1_resistance_zone",
        },
        {
            "name": "支撑结构",
            "signal": "支撑失效：反抽未能站回关键位",
            "detail": "支撑失效后的分流很重要。",
            "display_score": "-8/8",
            "awarded": -8,
            "max": 8,
            "factor_id": "j1_support_setup",
        },
        {
            "name": "假突破识别",
            "signal": "看涨假突破：日内触及高点但收盘回落",
            "detail": "假突破是多空双方试探失败的信号。",
            "display_score": "0/8",
            "awarded": 0,
            "max": 8,
            "factor_id": "j1_false_break",
        },
        {
            "name": "压缩启动",
            "signal": "情绪追价区：波动已扩张阶段出现放量上涨",
            "detail": "压缩后放量启动才是更干净的 setup。",
            "display_score": "-6/10",
            "awarded": -6,
            "max": 10,
            "factor_id": "j1_compression_breakout",
        },
    ]
    analysis["dimensions"]["fundamental"]["factors"] = [
        {
            "name": "PEG 代理",
            "signal": "PEG 约 3.20",
            "detail": "用真实指数 PE 除以个股增速代理，回答'增长是否已经被定价'。",
            "display_score": "-6/10",
            "awarded": -6,
            "max": 10,
            "factor_id": "j4_peg",
        }
    ]
    analysis["dimensions"]["macro"]["factors"] = [
        {
            "name": "信用脉冲",
            "signal": "M1-M2 剪刀差 -7.2pct，信用脉冲收缩",
            "detail": "更偏中期环境因子，主要影响资金扩张、订单兑现和风险偏好。",
            "display_score": "-6/15",
            "awarded": -6,
            "max": 15,
            "factor_id": "m1_credit_impulse",
        }
    ]

    rendered = ClientReportRenderer().render_scan(analysis)
    strong_factor_section = rendered.split("## 关键强因子拆解", 1)[1].split("## 证据时点与来源", 1)[0]
    assert strong_factor_section.count("（价量结构）") <= 2
    assert "PEG 代理（质量/盈利）" in strong_factor_section
    assert "信用脉冲（宏观/风格）" in strong_factor_section


def test_render_scan_detailed_reuses_internal_structure() -> None:
    analysis = _sample_analysis("588200", "科创芯片ETF", "cn_etf", rank=2)
    analysis["risks"] = []
    analysis["narrative"] = {
        "headline": "这是一个趋势修复中的标的。",
        "judgment": {
            "direction": "偏多但仍需确认",
            "cycle": "短线到波段",
            "odds": "中性偏正",
            "state": "持有优于追高",
        },
        "phase": {"label": "修复阶段", "body": "当前更像回撤后的修复，而不是趋势已经重新加速。"},
        "drivers": {
            "macro": "宏观不逆风，但没有额外加速项。",
            "flow": "资金承接一般，等待更强确认。",
            "relative": "相对强弱尚未重回领先。",
            "technical": "技术结构仍偏修复，先看关键位。",
        },
        "contradiction": "逻辑没坏，但价格和资金没有重新形成强共振。",
        "positives": ["方向仍在。", "支撑没有明显失守。"],
        "cautions": ["追高盈亏比一般。", "短线催化不足。"],
        "watch_points": ["观察支撑是否继续有效。"],
        "scenarios": {"base": "先震荡修复。", "bull": "放量突破后升级。", "bear": "支撑失守后转弱。"},
        "playbook": {"trend": "等右侧确认。", "allocation": "先小仓。", "defensive": "不急着抢。"},
        "summary_lines": ["方向没坏，但更适合先观察，再等新的量价共振。"],
        "risk_points": {
            "fundamental": "主题波动仍大。",
            "valuation": "高估值仍要消化。",
            "crowding": "板块情绪可能反复。",
            "external": "外部科技风险偏好变化会先影响它。",
        },
    }
    analysis["visuals"] = {
        "dashboard": "/tmp/dashboard.png",
        "windows": "/tmp/windows.png",
        "indicators": "/tmp/indicators.png",
    }
    rendered = ClientReportRenderer().render_scan_detailed(analysis)
    assert "# 科创芯片ETF (588200) | 详细分析 | 2026-03-11" in rendered.splitlines()[0]
    assert "## 首页判断" in rendered
    assert "### 板块 / 主题认知" in rendered
    assert "## 执行摘要" in rendered
    assert "| 当前建议 | 小仓试仓；持有优于追高 |" in rendered
    assert "## 图表速览" in rendered
    assert "![分析看板](/tmp/dashboard.png)" in rendered
    assert "## 为什么这么判断" in rendered
    assert "## 硬检查" in rendered
    assert "## 当前更合适的动作" in rendered
    assert "## 证据时点与来源" in rendered
    assert "## 值得继续看的地方" in rendered
    assert "## 现在不适合激进的地方" in rendered


def test_fund_profile_sections_use_asset_allocation_field() -> None:
    analysis = _sample_analysis("520840", "港股通恒生科技ETF", "cn_etf", rank=2)
    analysis["fund_profile"] = {
        "overview": {
            "基金类型": "股票型 / 被动指数型",
            "基金管理人": "华夏基金",
            "基金经理人": "王超",
            "业绩比较基准": "恒生港股通科技主题指数收益率(经汇率调整)",
        },
        "style": {},
        "asset_allocation": [
            {"资产类型": "股票", "仓位占比": 99.21},
            {"资产类型": "现金", "仓位占比": 1.10},
        ],
    }

    rendered = "\n".join(_fund_profile_sections(analysis))
    assert "### 资产配置" in rendered
    assert "| 股票 | 99.21% |" in rendered
    assert "| 现金 | 1.10% |" in rendered


def test_fund_profile_sections_render_fund_sales_ratio_snapshot() -> None:
    analysis = _sample_analysis("022365", "永赢科技智选混合发起C", "cn_fund", rank=2)
    analysis["fund_profile"] = {
        "overview": {
            "基金类型": "混合型-偏股",
            "基金管理人": "永赢基金",
            "基金经理人": "任桀",
            "业绩比较基准": "中国战略新兴产业成份指数收益率*70%+恒生科技指数收益率*10%",
        },
        "sales_ratio_snapshot": {
            "latest_year": "2025",
            "lead_channel": "商业银行",
            "summary": "2025年渠道保有结构：商业银行占比最高，约 41.20% 。",
            "channel_mix": [
                {"channel": "商业银行", "ratio": 41.2},
                {"channel": "独立基金销售机构", "ratio": 28.5},
            ],
        },
        "style": {},
    }

    rendered = "\n".join(_fund_profile_sections(analysis))
    assert "### 公募渠道环境" in rendered
    assert "| 统计年度 | 2025 |" in rendered
    assert "| 商业银行 | 41.20% |" in rendered
    assert "商业银行占比最高" in rendered


def test_fund_profile_sections_render_etf_specific_rows() -> None:
    analysis = _sample_analysis("563360", "A500ETF华泰柏瑞", "cn_etf", rank=2)
    analysis["fund_profile"] = {
        "overview": {
            "基金类型": "ETF / 境内",
            "基金管理人": "华泰柏瑞基金",
            "基金经理人": "柳军",
            "业绩比较基准": "中证A500指数",
            "ETF类型": "境内",
            "交易所": "SH",
            "ETF基准指数中文全称": "中证A500指数",
            "ETF基准指数代码": "000510.SH",
            "ETF基准指数发布机构": "中证指数有限公司",
            "ETF基准指数调样周期": "半年",
            "ETF总份额": "3091998.74万份",
            "ETF总规模": "3818927.64万元",
            "ETF份额规模日期": "2026-03-31",
            "ETF最近份额变化": "净创设 +2.58亿份 (+0.84%)",
            "ETF最近规模变化": "规模收缩 -1.44亿元 (-0.38%)",
        },
        "fund_factor_snapshot": {
            "trend_label": "趋势偏强",
            "momentum_label": "动能改善",
            "latest_date": "2026-04-01",
        },
        "style": {},
    }

    rendered = "\n".join(_fund_profile_sections(analysis))
    assert "### ETF专用信息" in rendered
    assert "| 跟踪指数 | 中证A500指数 |" in rendered
    assert "| 指数代码 | 000510.SH |" in rendered
    assert "| 场内基金技术状态 | 趋势偏强 / 动能改善（2026-04-01） |" in rendered
    assert "| 最新总份额 | 3091998.74万份 |" in rendered
    assert "| 最近份额变化 | 净创设 +2.58亿份 (+0.84%) |" in rendered


def test_fund_profile_sections_fill_etf_scale_estimate_and_single_day_snapshot_note() -> None:
    analysis = _sample_analysis("563360", "A500ETF华泰柏瑞", "cn_etf", rank=2)
    analysis["history"] = pd.DataFrame([{"收盘": 1.24}])
    analysis["fund_profile"] = {
        "overview": {
            "基金类型": "ETF / 境内",
            "基金管理人": "华泰柏瑞基金",
            "基金经理人": "柳军",
            "业绩比较基准": "中证A500指数",
        },
        "etf_snapshot": {
            "index_name": "中证A500指数",
            "index_code": "000510.SH",
            "share_as_of": "2026-04-01",
            "total_share": 3087198.74,
            "total_share_yi": 308.719874,
        },
        "style": {},
    }

    rendered = "\n".join(_fund_profile_sections(analysis))

    assert "按最近收盘估算" in rendered
    assert "仅有单日快照，不能据此写成净创设/净赎回" in rendered


def test_fund_profile_sections_fall_back_to_dimension_rows_for_etf_without_profile() -> None:
    analysis = _sample_analysis("512400", "有色金属ETF", "cn_etf", rank=2)
    analysis["fund_profile"] = {}
    analysis["benchmark_name"] = "中证细分有色金属产业主题指数"
    analysis["benchmark_symbol"] = "930632.CSI"
    analysis["metadata"] = {
        "tracked_index_name": "中证细分有色金属产业主题指数",
        "tracked_index_symbol": "930632.CSI",
    }
    analysis["dimension_rows"] = [
        ["跟踪指数技术状态", "趋势偏弱 / 动能偏弱（2026-04-16）", "先按指数主链理解当前节奏。"],
        ["场内基金技术状态（ETF/基金专属）", "场内基金技术因子 趋势偏弱 / 动能偏弱（2026-04-16）", "产品层趋势和动能仍偏弱。"],
    ]

    rendered = "\n".join(_fund_profile_sections(analysis))

    assert "### ETF专用信息" in rendered
    assert "| 跟踪指数 | 中证细分有色金属产业主题指数 |" in rendered
    assert "| 指数代码 | 930632.CSI |" in rendered
    assert "| 场内基金技术状态 | 趋势偏弱 / 动能偏弱（2026-04-16） |" in rendered


def test_render_scan_detailed_sanitizes_intraday_wording_inside_news_titles() -> None:
    analysis = _sample_analysis("159698", "粮食ETF", "cn_etf", rank=2)
    analysis["narrative"]["judgment"].update(
        {
            "direction": "中性偏多",
            "cycle": "中期(1-3月)",
            "odds": "中",
        }
    )
    analysis["narrative"]["phase"] = {"label": "整理", "body": "仍在等新的确认。"}
    analysis["narrative"]["drivers"] = {
        "macro": "宏观不逆风。",
        "flow": "资金仍在观察。",
        "relative": "相对强弱一般。",
        "technical": "技术结构仍待确认。",
    }
    analysis["narrative"]["contradiction"] = "方向未坏，但节奏一般。"
    analysis["narrative"]["risk_points"] = {
        "fundamental": "主题景气仍要跟踪。",
        "valuation": "估值和位置都还要消化。",
        "crowding": "情绪一致时波动会放大。",
        "external": "外部宏观变化会先改写节奏。",
    }
    analysis["narrative"]["watch_points"] = ["继续观察量价和催化是否共振。"]
    analysis["narrative"]["validation_points"] = [
        {"watch": "量价确认", "judge": "重新放量转强", "bull": "说明趋势改善。", "bear": "说明仍需等待。"}
    ]
    analysis["narrative"]["scenarios"] = {"base": "先震荡整理。", "bull": "放量后转强。", "bear": "跌破支撑后转弱。"}
    analysis["narrative"]["playbook"] = {"trend": "等确认后再跟。", "allocation": "先观察。", "defensive": "别急着追。"}
    analysis["narrative"]["summary_lines"] = ["先按观察期理解。"]
    analysis["risks"] = []
    analysis["dimensions"]["catalyst"]["factors"] = [
        {
            "name": "产品/跟踪方向催化",
            "signal": "粮食ETF涨超1%，盘中净申购5650万份 - 新浪财经",
            "detail": "优先看跟踪基准、行业暴露和核心成分的共振。",
            "display_score": "12/12",
        }
    ]

    rendered = ClientReportRenderer().render_scan_detailed(analysis)
    assert "盘中净申购" not in rendered
    assert "交易时段净申购" in rendered


def test_render_scan_detailed_trims_execution_template_for_observe_only() -> None:
    analysis = _sample_analysis("300274", "阳光电源", "cn_stock", rank=1)
    analysis["rating"]["rank"] = 0
    analysis["dimensions"]["technical"]["score"] = 20
    analysis["dimensions"]["fundamental"]["score"] = 35
    analysis["dimensions"]["catalyst"]["score"] = 0
    analysis["dimensions"]["relative_strength"]["score"] = 10
    analysis["dimensions"]["risk"]["score"] = 25
    analysis["narrative"]["judgment"].update(
        {
            "direction": "中性",
            "cycle": "短期(1-4周)",
            "odds": "低",
            "state": "观察为主",
        }
    )
    analysis["narrative"]["phase"] = {"label": "震荡整理", "body": "说明逻辑没有完全失效，但价格和催化暂时没有形成新的入场共振。"}
    analysis["narrative"]["drivers"] = {
        "macro": "宏观仍偏逆风。",
        "flow": "资金承接一般。",
        "relative": "相对强弱仍待改善。",
        "technical": "技术结构仍偏修复。",
    }
    analysis["narrative"]["contradiction"] = "逻辑未破，但还缺价格、资金和催化共振。"
    analysis["narrative"]["positives"] = ["方向仍在。", "支撑没有明显失守。"]
    analysis["narrative"]["cautions"] = ["追高盈亏比一般。", "短线催化不足。"]
    analysis["narrative"]["watch_points"] = ["继续观察量价与催化是否同步改善。"]
    analysis["narrative"]["scenarios"] = {"base": "先震荡修复。", "bull": "放量突破后升级。", "bear": "支撑失守后转弱。"}
    analysis["narrative"]["playbook"] = {"trend": "等右侧确认。", "allocation": "先观察。", "defensive": "不急着抢。"}
    analysis["narrative"]["summary_lines"] = ["今天更适合观察，不适合直接翻译成交易动作。"]
    analysis["narrative"]["risk_points"] = {
        "fundamental": "主题波动仍大。",
        "valuation": "估值仍要消化。",
        "crowding": "板块情绪可能反复。",
        "external": "外部科技风险偏好变化会先影响它。",
    }
    analysis["risks"] = []
    analysis["action"].update(
        {
            "direction": "回避",
            "entry": "等 MA20/MA60 修复后再看",
            "position": "暂不出手",
            "scaling_plan": "确认后再考虑第二笔",
            "stop_ref": 1.58,
            "target": "1.84",
            "target_ref": 1.84,
            "trim_range": "1.73-1.84",
        }
    )
    analysis["visuals"] = {"dashboard": "/tmp/dashboard.png"}
    rendered = ClientReportRenderer().render_scan_detailed(analysis)
    assert "## 图表速览" in rendered
    assert "![分析看板](/tmp/dashboard.png)" in rendered
    assert "## 公司研究层判断" in rendered
    assert rendered.index("## 公司研究层判断") < rendered.index("## 执行摘要")
    assert "| 研究定位 | 这是观察 / 风控稿，重点先回答“现在能不能动”。 |" in rendered
    assert "这里这档分数（`35/100`）更像“性价比一般”" in rendered
    assert "### 基本面（当前位置/性价比） 35/100" in rendered
    assert "| 多大仓位 |" in rendered
    assert "没触发前 `0%`" in rendered
    assert "`2% - 3%` 试错" in rendered
    assert "最多加到 `5%` 观察仓" in rendered
    assert "| 哪里止损 |" in rendered
    assert "失效位先看 `1.580`" in rendered
    assert "别把观察稿硬扛成持仓" in rendered
    assert "关键位先看：" in rendered
    assert "反弹再看 **1.73-1.84** 一带的承压" in rendered
    assert "| 建议买入区间 |" not in rendered
    assert "| 组合落单前 |" not in rendered
    assert "| 预演命令 |" not in rendered
    assert "| 空仓怎么做 |" not in rendered
    assert "| 持仓怎么做 |" not in rendered
    assert "| 适合谁 |" not in rendered
    assert "## 组合落单前" not in rendered
    assert "## 当前更合适的动作" not in rendered


def test_render_scan_detailed_keeps_action_section_for_observe_only_etf() -> None:
    analysis = _sample_analysis("512400", "有色ETF", "cn_etf", rank=1)
    analysis["rating"]["rank"] = 0
    analysis["dimensions"]["technical"]["score"] = 20
    analysis["dimensions"]["fundamental"]["score"] = 35
    analysis["dimensions"]["catalyst"]["score"] = 0
    analysis["dimensions"]["relative_strength"]["score"] = 10
    analysis["dimensions"]["risk"]["score"] = 25
    analysis["narrative"]["judgment"].update(
        {
            "direction": "中性",
            "cycle": "短期(1-4周)",
            "odds": "低",
            "state": "观察为主",
        }
    )
    analysis["narrative"]["phase"] = {"label": "震荡整理", "body": "先观察节奏和确认，不急着升级动作。"}
    analysis["narrative"]["drivers"] = {
        "macro": "宏观仍偏中性。",
        "flow": "资金承接一般。",
        "relative": "相对强弱仍待改善。",
        "technical": "技术结构仍偏修复。",
    }
    analysis["narrative"]["contradiction"] = "方向没完全走坏，但确认和催化还不够。"
    analysis["narrative"]["positives"] = ["方向仍在。", "支撑没有明显失守。"]
    analysis["narrative"]["cautions"] = ["追高盈亏比一般。", "短线催化不足。"]
    analysis["narrative"]["watch_points"] = ["继续观察量价与催化是否同步改善。"]
    analysis["narrative"]["scenarios"] = {"base": "先震荡修复。", "bull": "放量突破后升级。", "bear": "支撑失守后转弱。"}
    analysis["narrative"]["playbook"] = {"trend": "等右侧确认。", "allocation": "先观察。", "defensive": "不急着抢。"}
    analysis["narrative"]["summary_lines"] = ["今天更适合观察，不适合直接翻译成交易动作。"]
    analysis["narrative"]["risk_points"] = {
        "fundamental": "主题波动仍大。",
        "valuation": "估值仍要消化。",
        "crowding": "板块情绪可能反复。",
        "external": "外部风险偏好变化会先影响它。",
    }
    analysis["action"].update(
        {
            "direction": "回避",
            "entry": "等 MA20/MA60 修复后再看",
            "position": "≤2% 轮动跟踪仓",
            "scaling_plan": "确认后再考虑第二笔",
            "buy_range": "6.20-6.35",
            "trim_range": "6.90-7.20",
        }
    )
    analysis["risks"] = []
    rendered = ClientReportRenderer().render_scan_detailed(analysis)
    assert "## 公司研究层判断" not in rendered
    assert "## 当前更合适的动作" in rendered
    assert "| 当前动作 | 观察为主（偏回避） |" in rendered
    assert "关键观察位：回踩先看 `6.20-6.35` 一带的承接" in rendered
    assert "若触发也只按 ≤2% 轮动跟踪仓" in rendered
    assert "6.90" in rendered
    assert "7.20" in rendered


def test_render_scan_detailed_surfaces_catalyst_diagnosis_for_search_gap() -> None:
    analysis = _sample_analysis("512480", "半导体ETF", "cn_etf", rank=1)
    analysis["risks"] = []
    analysis["narrative"] = {
        "headline": "当前更适合把它当成待复核观察稿。",
        "judgment": {
            "direction": "中性",
            "cycle": "短期(1-4周)",
            "odds": "低",
            "state": "观察为主",
        },
        "phase": {"label": "高位震荡", "body": "说明产业逻辑仍在，但价格和催化没有形成新的确认。"},
        "drivers": {
            "macro": "宏观不构成直接顺风。",
            "flow": "资金承接一般。",
            "relative": "相对强弱仍偏弱。",
            "technical": "技术结构仍在修复。",
        },
        "contradiction": "主题关注度仍高，但当前新闻抓取未给出足够直连证据。",
        "positives": ["产业逻辑仍在。"],
        "cautions": ["短线催化需要复核。"],
        "watch_points": ["先复核催化链，再看价格是否站稳关键位。"],
        "scenarios": {"base": "高位震荡。", "bull": "催化确认后修复。", "bear": "失守支撑后转弱。"},
        "playbook": {"trend": "先观察。", "allocation": "不急着追。", "defensive": "优先等复核结果。"},
        "summary_lines": ["当前先按待复核观察稿理解。"],
        "risk_points": {
            "fundamental": "高景气预期已被部分计价。",
            "valuation": "估值仍需消化。",
            "crowding": "情绪一致时波动放大。",
            "external": "全球科技风险偏好会先影响它。",
        },
    }
    analysis["dimensions"]["catalyst"]["score"] = None
    analysis["dimensions"]["catalyst"]["summary"] = "当前实时新闻关键词检索未命中高置信标题；对这类高关注主题更像搜索覆盖不足，本次催化维度暂按待 AI 联网复核处理，不直接记成零催化。"
    analysis["dimensions"]["catalyst"]["coverage"] = {
        "news_mode": "live",
        "diagnosis": "suspected_search_gap",
        "ai_web_search_recommended": True,
    }
    analysis["dimensions"]["catalyst"]["evidence"] = []
    rendered = ClientReportRenderer().render_scan_detailed(analysis)
    assert "催化诊断" in rendered
    assert "待 AI 联网复核" in rendered


def test_render_scan_detailed_surfaces_completed_catalyst_web_review() -> None:
    analysis = _sample_analysis("512480", "半导体ETF", "cn_etf", rank=1)
    analysis["risks"] = []
    analysis["narrative"] = {
        "headline": "当前更适合把它当成已完成联网复核的观察稿。",
        "judgment": {
            "direction": "中性",
            "cycle": "短期(1-4周)",
            "odds": "低",
            "state": "观察为主",
        },
        "phase": {"label": "高位震荡", "body": "说明产业逻辑仍在，但价格和催化没有形成新的确认。"},
        "drivers": {
            "macro": "宏观不构成直接顺风。",
            "flow": "资金承接一般。",
            "relative": "相对强弱仍偏弱。",
            "technical": "技术结构仍在修复。",
        },
        "contradiction": "已经补完催化联网复核，但复核结果更多是在修正边界，不等于直接升级成买点。",
        "positives": ["产业逻辑仍在。"],
        "cautions": ["短线仍需等价格确认。"],
        "watch_points": ["先看价格是否站稳关键位。"],
        "scenarios": {"base": "高位震荡。", "bull": "催化确认后修复。", "bear": "失守支撑后转弱。"},
        "playbook": {"trend": "先观察。", "allocation": "不急着追。", "defensive": "先尊重复核后的边界。"},
        "summary_lines": ["当前先按已完成联网复核的观察稿理解。"],
        "risk_points": {
            "fundamental": "高景气预期已被部分计价。",
            "valuation": "估值仍需消化。",
            "crowding": "情绪一致时波动放大。",
            "external": "全球科技风险偏好会先影响它。",
        },
    }
    analysis["dimensions"]["catalyst"]["coverage"] = {
        "news_mode": "live",
        "diagnosis": "web_review_completed",
        "ai_web_search_recommended": False,
    }
    analysis["dimensions"]["catalyst"]["summary"] = "联网复核：只有主题级催化。"
    analysis["catalyst_web_review"] = {
        "decision": "只有主题级催化",
        "impact": ["不足以把观察稿升级为推荐稿，但不能再写成“零催化”。"],
        "completed": True,
    }
    rendered = ClientReportRenderer().render_scan_detailed(analysis)
    assert "联网复核结论" in rendered
    assert "只有主题级催化" in rendered
    assert "联网复核影响" in rendered


def test_render_scan_detailed_surfaces_new_tushare_market_event_rows_in_key_evidence(monkeypatch) -> None:
    _install_fake_thesis_repo(monkeypatch)
    analysis = _sample_analysis("300308", "中际旭创", "cn_stock", rank=2)
    analysis["risks"] = []
    analysis["narrative"] = {
        "headline": "当前更适合把它当成主题确认中的观察稿。",
        "judgment": {
            "direction": "中性偏多",
            "cycle": "短期(1-4周)",
            "odds": "中性",
            "state": "观察为主",
        },
        "phase": {"label": "修复观察", "body": "主线归因比之前更清晰，但价格和风险提示还没完全收敛。"},
        "drivers": {
            "macro": "宏观不构成新增顺风。",
            "flow": "主题扩散开始变清晰，但资金承接还要继续确认。",
            "relative": "相对强弱不算差，但还没到正式升级阶段。",
            "technical": "技术结构还在修复，先看关键位和量能。",
        },
        "contradiction": "主题成员关系和交易所风险提示同时出现，说明机会和节奏约束并存。",
        "positives": ["主题链路更清晰了。"],
        "cautions": ["交易所风险提示还在。"],
        "watch_points": ["继续盯主题扩散、成交承接和交易所风险提示。"],
        "scenarios": {"base": "先按修复观察理解。", "bull": "主题扩散继续强化。", "bear": "风险提示升级后转弱。"},
        "playbook": {"trend": "先观察确认。", "allocation": "不急着放大仓位。", "defensive": "优先尊重风险提示边界。"},
        "summary_lines": ["当前更适合把它当成主题确认中的观察稿。"],
        "risk_points": {
            "fundamental": "盈利兑现还要继续跟踪。",
            "valuation": "高波动下估值容错率有限。",
            "crowding": "主线升温时波动会放大。",
            "external": "风格切换会先影响这类高弹性方向。",
        },
    }
    analysis["news_report"] = {"items": []}
    analysis["market_event_rows"] = [
        [
            "2026-04-01",
            "A股概念成员：中际旭创 属于 共封装光学(CPO)（+3.65%）",
            "同花顺主题成分",
            "高",
            "共封装光学(CPO)",
            "",
            "主线归因",
            "偏利多，`中际旭创` 属于 `共封装光学(CPO)` 链路。",
        ],
        [
            "2026-04-01",
            "交易所重点提示：中际旭创 当前仍在重点提示证券名单",
            "交易所风险专题",
            "中",
            "中际旭创",
            "",
            "风险提示",
            "偏谨慎，先按高波动样本管理节奏。",
        ],
        [
            "2026-04-01",
            "筹码确认：中际旭创 胜率约 70.2%，现价已回到平均成本上方",
            "筹码分布专题",
            "中",
            "中际旭创",
            "",
            "筹码确认",
            "偏利多，真实筹码分布开始配合价格修复。",
        ],
    ]
    analysis["dimensions"]["catalyst"]["evidence"] = []

    rendered = ClientReportRenderer().render_scan_detailed(analysis)

    assert "## 事件消化" in rendered
    assert "A股概念成员：中际旭创 属于 共封装光学(CPO)（+3.65%）" in rendered
    assert "同花顺主题成分" in rendered
    assert "交易所重点提示：中际旭创 当前仍在重点提示证券名单" in rendered
    assert "筹码确认：中际旭创 胜率约 70.2%" in rendered
    assert "信号类型：`主线归因`" in rendered
    assert "信号类型：`风险提示`" in rendered
    assert "信号类型：`筹码确认`" in rendered


def test_render_scan_detailed_surfaces_p1_stock_signal_rows_in_key_evidence(monkeypatch) -> None:
    _install_fake_thesis_repo(monkeypatch)
    analysis = _sample_analysis("300308", "中际旭创", "cn_stock", rank=2)
    analysis["risks"] = []
    analysis["narrative"] = {
        "headline": "当前更像有资金承接、但节奏约束仍在的观察稿。",
        "judgment": {
            "direction": "中性偏多",
            "cycle": "短期(1-4周)",
            "odds": "中性",
            "state": "观察为主",
        },
        "phase": {"label": "修复观察", "body": "资金承接比前期更实，但两融和打板情绪开始升温。"},
        "drivers": {
            "macro": "宏观不构成新增顺风。",
            "flow": "个股级资金流开始承接，但短线节奏仍要盯拥挤度。",
            "relative": "相对强弱不算差，但还没到正式升级阶段。",
            "technical": "技术结构还在修复，先看关键位和量能。",
        },
        "contradiction": "方向和微观结构并不是完全没亮点，但两融/打板情绪已经开始升温。",
        "positives": ["个股资金流开始给出直接承接。"],
        "cautions": ["两融拥挤和打板情绪升温。"],
        "watch_points": ["继续盯资金承接、融资盘和次日承接。"],
        "scenarios": {"base": "先按修复观察理解。", "bull": "资金承接继续强化。", "bear": "拥挤交易先反噬。"},
        "playbook": {"trend": "先观察确认。", "allocation": "不急着放大仓位。", "defensive": "优先尊重拥挤交易边界。"},
        "summary_lines": ["当前更像有资金承接、但节奏约束仍在的观察稿。"],
        "risk_points": {
            "fundamental": "盈利兑现还要继续跟踪。",
            "valuation": "高波动下估值容错率有限。",
            "crowding": "两融和打板情绪同时升温，波动会先被情绪盘放大。",
            "external": "风格切换会先影响这类高弹性方向。",
        },
    }
    analysis["news_report"] = {"items": []}
    analysis["market_event_rows"] = [
        [
            "2026-04-01",
            "个股资金流确认：中际旭创 当日主力净流入 1.60亿",
            "个股资金流向专题",
            "高",
            "中际旭创",
            "",
            "资金承接",
            "偏利多，个股主力资金开始给出直接承接。",
        ],
        [
            "2026-04-01",
            "两融拥挤提示：中际旭创 当前融资盘升温明显",
            "两融专题",
            "高",
            "中际旭创",
            "",
            "两融拥挤",
            "偏谨慎，融资盘一致性交易会放大短线波动。",
        ],
        [
            "2026-04-01",
            "打板信号确认：中际旭创 龙虎榜净买入/竞价高开",
            "龙虎榜/打板专题",
            "中",
            "中际旭创",
            "",
            "龙虎榜确认",
            "偏利多，微观交易结构开始配合。",
        ],
    ]
    analysis["dimensions"]["catalyst"]["evidence"] = []

    rendered = ClientReportRenderer().render_scan_detailed(analysis)

    assert "## 事件消化" in rendered
    assert "个股资金流确认：中际旭创 当日主力净流入 1.60亿" in rendered
    assert "两融拥挤提示：中际旭创 当前融资盘升温明显" in rendered
    assert "打板信号确认：中际旭创 龙虎榜净买入/竞价高开" in rendered
    assert "信号类型：`资金承接`" in rendered
    assert "信号类型：`两融拥挤`" in rendered
    assert "信号类型：`龙虎榜确认`" in rendered


def test_render_scan_and_stock_analysis_surface_standard_industry_framework_rows() -> None:
    analysis = _sample_analysis("300308", "中际旭创", "cn_stock", rank=2)
    analysis["market_event_rows"] = [
        [
            "2026-04-01",
            "标准行业框架：中际旭创 属于 申万二级行业·通信设备（+3.60%）",
            "申万行业框架",
            "高",
            "通信设备",
            "",
            "标准行业归因",
            "偏利多，先按标准行业框架理解。",
        ]
    ]

    scan_rendered = ClientReportRenderer().render_scan(analysis)
    stock_analysis_rendered = ClientReportRenderer().render_stock_analysis(analysis)

    assert "标准行业框架：中际旭创 属于 申万二级行业·通信设备（+3.60%）" in scan_rendered
    assert "申万行业框架" in scan_rendered
    assert "标准行业框架：中际旭创 属于 申万二级行业·通信设备（+3.60%）" in stock_analysis_rendered
    assert "申万行业框架" in stock_analysis_rendered


def test_render_scan_and_stock_analysis_surface_tdx_structure_rows() -> None:
    analysis = _sample_analysis("300308", "中际旭创", "cn_stock", rank=2)
    analysis["market_event_rows"] = [
        [
            "2026-04-01",
            "TDX 结构框架：中际旭创 通信设备 / 进攻 / CN（+3.60%）",
            "TDX结构专题",
            "高",
            "通信设备",
            "",
            "标准结构归因",
            "偏利多，`中际旭创` 的标准板块/风格/地区框架已可直接用来解释当前强弱。",
        ]
    ]

    scan_rendered = ClientReportRenderer().render_scan(analysis)
    stock_analysis_rendered = ClientReportRenderer().render_stock_analysis(analysis)

    assert "TDX 结构框架：中际旭创 通信设备 / 进攻 / CN（+3.60%）" in scan_rendered
    assert "TDX结构专题" in scan_rendered
    assert "标准结构归因" in scan_rendered
    assert "TDX 结构框架：中际旭创 通信设备 / 进攻 / CN（+3.60%）" in stock_analysis_rendered
    assert "TDX结构专题" in stock_analysis_rendered


def test_render_scan_surfaces_second_stage_stock_signals_early() -> None:
    analysis = _attach_second_stage_stock_signals(_sample_analysis("300502", "新易盛", "cn_stock", rank=3))

    rendered = ClientReportRenderer().render_scan(analysis)

    assert "技术层确认：趋势偏强 / 动能改善（2026-04-03）。" in rendered
    assert "跨市场层确认：A/H 比价溢价 +18.20%（2026-04-03）。" in rendered
    assert "转债层补充：新易转债 / 趋势偏强 / 动能改善 / 转股溢价 +9.80%。" in rendered
    assert "调研层补充：机构调研：多家机构追问新品放量与订单兑现节奏；偏利多，机构关注点集中在订单兑现和景气延续。" in rendered
    assert rendered.index("技术层确认：趋势偏强 / 动能改善（2026-04-03）。") < rendered.index("| 维度 | 分数 | 为什么是这个分 |")


def test_render_stock_analysis_uses_stock_analysis_title() -> None:
    analysis = _sample_analysis("META", "Meta", "us", rank=3)
    rendered = ClientReportRenderer().render_stock_analysis(analysis)
    assert "# Meta (META) | 个股详细分析 | 2026-03-11" in rendered.splitlines()[0]
    assert "## 为什么这么判断" in rendered
    assert "## 历史相似样本验证" in rendered


def test_render_stock_analysis_detailed_uses_detailed_title() -> None:
    analysis = _sample_analysis("META", "Meta", "us", rank=3)
    analysis["risks"] = []
    analysis["narrative"] = {
        "headline": "这是一个基本面和趋势都更完整的标的。",
        "judgment": {
            "direction": "偏多",
            "cycle": "中线",
            "odds": "中高",
            "state": "持有优于追高",
        },
        "phase": {"label": "趋势延续", "body": "更像一段完整趋势中的中继，而不是纯反弹。"},
        "drivers": {
            "macro": "宏观和风格对成长不逆风。",
            "flow": "资金仍在承接。",
            "relative": "相对强弱仍优于基准。",
            "technical": "技术结构仍偏多。",
        },
        "contradiction": "趋势在，但短线也不适合无脑追高。",
        "positives": ["基本面站得住。", "相对强弱仍在。"],
        "cautions": ["追高要看节奏。", "事件前后波动会加大。"],
        "watch_points": ["看趋势是否继续延续。"],
        "scenarios": {"base": "延续震荡上行。", "bull": "催化兑现后加速。", "bear": "跌破支撑后转弱。"},
        "playbook": {"trend": "更适合分批。", "allocation": "先按中线理解。", "defensive": "失守支撑就降级。"},
        "summary_lines": ["核心趋势仍在，但更适合按中线节奏处理，而不是追当天波动。"],
        "risk_points": {
            "fundamental": "盈利兑现若低于预期会承压。",
            "valuation": "估值仍需财报匹配。",
            "crowding": "强势股拥挤时波动会放大。",
            "external": "利率和海外科技风险偏好仍有扰动。",
        },
    }
    analysis["visuals"] = {"dashboard": "/tmp/dashboard.png"}
    rendered = ClientReportRenderer().render_stock_analysis_detailed(analysis)
    assert "# Meta (META) | 个股详细分析 | 2026-03-11" in rendered.splitlines()[0]
    assert "## 先看执行" in rendered
    assert rendered.index("## 先看执行") < rendered.index("## 首页判断")
    assert "| 看不看 |" in rendered
    assert "| 怎么触发 |" in rendered
    assert "| 多大仓位 |" in rendered
    assert "| 哪里止损 |" in rendered
    assert '<div class="report-page-break"></div>' in rendered
    assert rendered.index('<div class="report-page-break"></div>') < rendered.index("## 一句话结论")
    assert "## 图表速览" in rendered
    assert "## 当前更合适的动作" in rendered


def test_render_stock_analysis_detailed_keeps_action_section_for_observe_only() -> None:
    analysis = _sample_analysis("600313", "农发种业", "cn_stock", rank=1)
    analysis["narrative"] = {
        "headline": "底层逻辑还在，但当前更适合继续观察。",
        "judgment": {
            "direction": "中性偏多",
            "cycle": "中线",
            "odds": "一般",
            "state": "观察为主",
        },
        "phase": {"label": "修复早期", "body": "还没到可以直接升级动作的阶段。"},
        "drivers": {
            "macro": "宏观不逆风。",
            "flow": "资金还在观察。",
            "relative": "相对强弱尚未恢复。",
            "technical": "技术确认还不够。",
        },
        "contradiction": "逻辑还在，但价格和动量还没翻译成买点。",
        "positives": ["基本面没坏。"],
        "cautions": ["技术确认不足。"],
        "watch_points": ["先看是否站回关键均线。"],
        "scenarios": {"base": "继续震荡观察。", "bull": "确认后再升级。", "bear": "失守后继续降级。"},
        "playbook": {"trend": "先观察。", "allocation": "先按观察仓。", "defensive": "失守再降级。"},
        "summary_lines": ["今天没有有效动作信号。"],
        "risk_points": {
            "fundamental": "暂无新的基本面恶化。",
            "valuation": "估值还要等景气确认。",
            "crowding": "暂未出现极端拥挤。",
            "external": "外部风险偏好变化仍会干扰。",
        },
    }
    analysis["action"] = {
        "direction": "观察为主",
        "timeframe": "中线",
        "entry": "等站回 MA20 后再决定是否给买入区间。",
        "stop": "失守前低就继续当观察样本。",
        "position": "首次仓位先按观察仓理解。",
    }
    analysis["visuals"] = {"dashboard": "/tmp/dashboard.png"}
    analysis["risks"] = []
    rendered = ClientReportRenderer().render_stock_analysis_detailed(analysis)
    assert "## 先看执行" in rendered
    assert "| 哪里建仓 |" in rendered
    assert "| 多大仓位 |" in rendered
    assert "没触发前 `0%`" in rendered
    assert "`2% - 3%` 试错" in rendered
    assert "最多加到 `5%` 观察仓" in rendered
    assert "| 哪里止损 | 失守前低就继续当观察样本。 |" in rendered
    assert "## 图表速览" in rendered
    assert "![分析看板](/tmp/dashboard.png)" in rendered
    assert "## 当前更合适的动作" in rendered
    assert "观察为主" in rendered


def test_render_stock_analysis_detailed_keeps_intraday_snapshot_visible() -> None:
    analysis = _sample_analysis("600584", "长电科技", "cn_stock", rank=1)
    analysis["narrative"] = {
        "headline": "短线先看确认。",
        "judgment": {"state": "观察为主", "direction": "中性偏多", "cycle": "观察期", "odds": "一般"},
        "phase": {"label": "修复观察", "body": "还没完成右侧确认。"},
        "drivers": {"macro": "中性", "flow": "待确认", "relative": "改善", "technical": "待确认"},
        "contradiction": "盘中不弱，但正式动作仍要等触发。",
        "positives": ["盘中承接不弱。"],
        "cautions": ["证据硬度仍低。"],
        "watch_points": ["看近端压力。"],
        "scenarios": {"base": "观察", "bull": "突破", "bear": "失守"},
        "playbook": {"trend": "右侧", "allocation": "小仓", "defensive": "失守重评"},
        "summary_lines": ["先看确认。"],
        "risk_points": {"fundamental": "待确认", "valuation": "不低", "crowding": "未知", "external": "扰动"},
    }
    analysis["intraday"] = {
        "enabled": True,
        "current": 43.73,
        "open": 43.60,
        "high": 44.02,
        "low": 43.20,
        "prev_close": 43.59,
        "vwap": 43.644,
        "range_position": 0.65,
        "change_vs_prev_close": 0.0032,
        "change_vs_open": 0.0030,
        "trend": "偏强",
        "snapshot_time": pd.Timestamp("2026-04-16 14:59:00"),
        "updated_at": pd.Timestamp("2026-04-16 14:59:00"),
        "auction_gap": 0.001,
        "auction_amount": 114122853.0,
        "auction_volume_ratio": 1.35,
        "commentary": "盘中价格站上 VWAP 且处于日内高位区域，更接近强势承接。",
        "auction_commentary": "集合竞价没有出现特别强的方向性信号，更适合等开盘后确认。",
    }
    analysis["risks"] = []

    rendered = ClientReportRenderer().render_stock_analysis_detailed(analysis)

    assert "## 今日盘中视角" in rendered
    assert rendered.index("## 今日盘中视角") < rendered.index('<div class="report-page-break"></div>')
    assert "| 当前价 | 43.730 |" in rendered
    assert "| VWAP | 43.644 |" in rendered
    assert "| 竞价成交 | 114122853 |" in rendered
    assert "| 竞价量能 | 1.35x |" in rendered
    assert "集合竞价没有出现特别强的方向性信号" in rendered
    assert "| 分钟级快照 as_of | 2026-04-16 14:59 |" in rendered
    assert "分钟级盘口未启用" not in rendered.split("## 先看执行", 1)[1].split("## 首页判断", 1)[0]


def test_render_stock_analysis_detailed_observe_prefers_near_pressure_over_far_target() -> None:
    analysis = _sample_analysis("600584", "长电科技", "cn_stock", rank=1)
    analysis["metrics"] = {"last_close": 43.73}
    analysis["narrative"] = {
        "headline": "长电科技中期逻辑没死，但短线还没完成确认。",
        "judgment": {
            "direction": "中性偏多",
            "cycle": "波段",
            "odds": "一般",
            "state": "观察为主",
        },
        "phase": {"label": "修复观察", "body": "价格和催化还没重新站到一边。"},
        "drivers": {
            "macro": "宏观不是一阶变量。",
            "flow": "资金承接仍待确认。",
            "relative": "相对强弱仍在。",
            "technical": "技术还没走成右侧确认。",
        },
        "contradiction": "赛道没坏，但载体和执行确认还没补齐。",
        "positives": ["半导体方向没坏。"],
        "cautions": ["短线确认还不够。"],
        "watch_points": ["先看近端压力能否被消化。"],
        "scenarios": {"base": "继续震荡确认。", "bull": "突破后升级。", "bear": "失守后转弱。"},
        "playbook": {"trend": "先等右侧。", "allocation": "先按观察仓。", "defensive": "失守就先处理。"},
        "summary_lines": ["先看确认，不直接翻译成交易动作。"],
        "risk_points": {
            "fundamental": "估值和盈利质量还要继续消化。",
            "valuation": "当前位置不算便宜。",
            "crowding": "板块资金会反复。",
            "external": "外部风险偏好变化仍会先影响它。",
        },
    }
    analysis["action"].update(
        {
            "direction": "等右侧确认（偏回避）",
            "entry": "技术结构偏弱，等 MA20 / MA60 方向向上拐头后再考虑介入时机",
            "buy_range": "",
            "position": "≤2% 观察仓",
            "scaling_plan": "确认后再考虑第二笔",
            "stop": "跌破 40.767 或主线/催化失效时重新评估",
            "stop_ref": 40.767,
            "target": "先看前高/近60日高点",
            "target_ref": 54.630,
            "trim_range": "52.991 - 56.269",
        }
    )
    analysis["dimensions"]["catalyst"]["score"] = 0
    analysis["dimensions"]["catalyst"]["coverage"] = {
        "degraded": True,
        "diagnosis": "theme_only_live",
        "news_mode": "proxy",
    }
    analysis["signal_confidence"].update(
        {
            "sample_count": 19,
            "non_overlapping_count": 19,
            "win_rate_20d": 0.47,
            "confidence_label": "低",
            "confidence_score": 40,
        }
    )
    analysis["dimensions"]["relative_strength"]["summary"] = "相对强弱有改善，但行业宽度/龙头确认仍缺失，按低置信代理理解。"
    analysis["dimensions"]["chips"]["summary"] = "个股级资金承接仍是行业代理，筹码确认缺失。"
    analysis["risks"] = []
    analysis["dimensions"]["technical"]["factors"].extend(
        [
            {
                "name": "股票技术面状态",
                "signal": "stk_factor_pro 缺失",
                "detail": "Tushare stk_factor_pro 当前未返回可用股票技术因子；不把空结果写成今天趋势已确认。",
                "display_score": "信息项",
                "factor_id": "j1_stk_factor_pro",
            },
            {
                "name": "压力位",
                "signal": "上方存在近端压力：近20日高点 45.170（上方 3.3%）",
                "detail": "上方近端压力会直接影响反弹空间和加速概率；先确认承压位能不能被有效消化。",
                "display_score": "-5/10",
                "factor_id": "j1_resistance_zone",
            },
            {
                "name": "均线",
                "signal": "MA5 43.506 / MA20 40.972 / MA60 45.008",
                "detail": "多头排列代表中期趋势向上",
                "display_score": "0/10",
            },
        ]
    )

    rendered = ClientReportRenderer().render_stock_analysis_detailed(analysis)
    first_screen = rendered.split("## 先看执行", 1)[1].split("## 首页判断", 1)[0]

    assert "| 怎么触发 |" in first_screen
    assert "`45.170`" in first_screen
    assert "不是第一笔必要条件" in first_screen
    assert "MA20 / MA60 向上拐头" not in first_screen
    assert "| 哪里建仓 | 先看 `MA20 40.972` 一带回踩承接；若回踩更深，再看 `40.767` 上方能否止跌。 |" in first_screen
    assert "| 哪里止损 | 跌破 40.767 或主线/催化失效时重新评估 |" in first_screen
    assert "| 证据硬度 |" in first_screen
    assert "样本置信度 `低`（40/100）、非重叠样本 19 个、20日胜率约 47%" in first_screen
    assert "不是这次结论的总把握" in first_screen
    assert "行业/板块代理" in first_screen
    assert "| 还差什么 |" in first_screen
    assert "分钟级盘口未启用" in first_screen
    assert "实时情报仍是 `proxy`" in first_screen
    assert "stk_factor_pro 未命中 fresh" in first_screen
    assert "行业宽度 / 龙头确认未补齐" in first_screen
    assert "个股级资金/筹码确认仍不足" in first_screen
    assert "`54.630`" not in first_screen
    assert "等 MA20 / MA60 方向向上拐头" not in rendered
    assert "## 执行拆解" in rendered
    assert "| 怎么用这段 | 上面的 `先看执行` 已经给了触发、建仓、仓位和止损；这里不重复参数，只补三种打法。 |" in rendered
    assert "| 短线打法 | 只做右侧：先看 `45.170` 近端压力能否放量站上并回踩不破；MA20 / MA60 只做波段或中线确认，不是第一笔硬门槛；失败或失守 `40.767` 就继续观察。 |" in rendered
    assert "| 波段打法 | 更适合等 `MA20 40.972` 一带企稳再分批；有效跌破 `40.767` 先认错。 |" in rendered
    assert "| 中线打法 | 现在先不急着按中线仓做；至少要等 `45.170` / `MA60 45.008` 这一带真正站稳，并补到新增公司级催化，再考虑升级。 |" in rendered
    assert rendered.count("| 证据硬度 |") == 1
    assert rendered.count("| 还差什么 |") == 1
    assert rendered.count("| 哪里建仓 |") == 1
    assert "| 仓位节奏 |" not in rendered
    assert "这几个词怎么读" in rendered
    assert "`样本置信度` 看的是历史上类似图形靠不靠谱" in rendered


def test_render_stock_analysis_detailed_tightens_observe_homepage_after_prepend() -> None:
    analysis = _sample_analysis("600313", "农发种业", "cn_stock", rank=1)
    analysis["narrative"] = {
        "headline": "底层逻辑还在，但当前更适合继续观察。",
        "judgment": {
            "direction": "中性偏多",
            "cycle": "中线",
            "odds": "一般",
            "state": "观察为主",
        },
        "phase": {"label": "修复早期", "body": "还没到可以直接升级动作的阶段。"},
        "drivers": {
            "macro": "宏观不逆风。",
            "flow": "资金还在观察。",
            "relative": "相对强弱尚未恢复。",
            "technical": "技术确认还不够。",
        },
        "contradiction": "逻辑还在，但价格和动量还没翻译成买点。",
        "positives": ["基本面没坏。"],
        "cautions": ["技术确认不足。"],
        "watch_points": ["先看是否站回关键均线。"],
        "scenarios": {"base": "继续震荡观察。", "bull": "确认后再升级。", "bear": "失守后继续降级。"},
        "playbook": {"trend": "先观察。", "allocation": "先按观察仓。", "defensive": "失守再降级。"},
        "summary_lines": ["今天没有有效动作信号。"],
        "risk_points": {
            "fundamental": "暂无新的基本面恶化。",
            "valuation": "估值还要等景气确认。",
            "crowding": "暂未出现极端拥挤。",
            "external": "外部风险偏好变化仍会干扰。",
        },
    }
    analysis["action"] = {
        "direction": "观察为主",
        "timeframe": "中线",
        "entry": "等站回 MA20 后再决定是否给买入区间。",
        "stop": "失守前低就继续当观察样本。",
        "position": "首次仓位先按观察仓理解。",
    }
    analysis["risks"] = []
    rendered = ClientReportRenderer().render_stock_analysis_detailed(analysis)
    homepage = rendered.split('<div class="report-page-break"></div>', 1)[0]
    hedge_total = sum(homepage.count(token) for token in ("不把", "不等于", "当前更像", "先按"))

    assert hedge_total <= 1


def test_stock_observe_helpers_accept_t1_direct_layers_without_gap(monkeypatch) -> None:
    analysis = _sample_analysis("600584", "长电科技", "cn_stock", rank=1)
    analysis["intraday"] = {"enabled": True, "fallback_mode": False}
    monkeypatch.setattr(
        client_report_module,
        "build_analysis_provenance",
        lambda analysis: {"intraday_as_of": "2026-04-16 15:00", "news_mode": "live"},  # noqa: ARG005
    )
    analysis["dimensions"]["relative_strength"] = {
        "score": 75,
        "max_score": 100,
        "summary": "轮动已经轮到它，具备主线扩散条件。",
        "factors": [
            {"name": "板块扩散", "signal": "板块涨跌幅 +1.87%", "detail": "行业级代理", "display_score": "25/25"},
            {"name": "行业宽度", "signal": "行业上涨家数比例 100%", "detail": "集成电路封测 上涨家数 13/13，扩散比例 100%；领涨股 汇成股份 +7.04%", "display_score": "15/15"},
            {"name": "龙头确认", "signal": "龙头方向与板块一致，扩散结构健康", "detail": "行业龙头结构已命中。", "display_score": "10/10"},
        ],
    }
    analysis["dimensions"]["chips"] = {
        "score": 48,
        "max_score": 100,
        "summary": "真实筹码分布偏谨慎：平均成本或上方套牢盘压力还没完全消化，当前更像先磨筹码。",
        "factors": [
            {
                "name": "机构资金承接",
                "signal": "上一交易日个股主力净流入 5049.45万 / 近 5 日累计 1.55亿（T+1 直连）",
                "detail": "个股主力资金最新停在 2026-04-15（上一交易日，T+1 直连）；当日先看概念代理。",
                "display_score": "12/12",
            },
            {
                "name": "平均成本位置",
                "signal": "现价相对加权平均成本 +1.6%（均价约 43.06 元）（上一交易日 T+1 直连）",
                "detail": "当前筹码快照来自上一交易日直连数据，能回答成本区和套牢盘。",
                "display_score": "6/12",
            },
            {
                "name": "套牢盘压力",
                "signal": "现价上方筹码约 47.8%（上一交易日 T+1 直连）",
                "detail": "当前筹码快照来自上一交易日直连数据，能回答成本区和套牢盘。",
                "display_score": "-4/12",
            },
        ],
    }

    assert _stock_observe_data_gap_text(analysis) == ""
    hardness = _stock_observe_evidence_hardness_text(analysis)
    assert "关键直连证据已经补到" in hardness
    assert "行业宽度/龙头确认" in hardness
    assert "上一交易日直连数据" in hardness


def test_render_stock_analysis_detailed_uses_stock_specific_what_changed_contract() -> None:
    analysis = _sample_analysis("600313", "农发种业", "cn_stock", rank=1)
    analysis["narrative"] = {
        "headline": "底层逻辑还在，但当前更适合继续观察。",
        "judgment": {
            "direction": "中性偏多",
            "cycle": "中线",
            "odds": "一般",
            "state": "观察为主",
        },
        "phase": {"label": "修复早期", "body": "还没到可以直接升级动作的阶段。"},
        "drivers": {
            "macro": "宏观不逆风。",
            "flow": "资金还在观察。",
            "relative": "相对强弱尚未恢复。",
            "technical": "技术确认还不够。",
        },
        "contradiction": "逻辑还在，但价格和动量还没翻译成买点。",
        "positives": ["基本面没坏。"],
        "cautions": ["技术确认不足。"],
        "watch_points": ["先看是否站回关键均线。"],
        "scenarios": {"base": "继续震荡观察。", "bull": "确认后再升级。", "bear": "失守后继续降级。"},
        "playbook": {"trend": "先观察。", "allocation": "先按观察仓。", "defensive": "失守再降级。"},
        "summary_lines": ["今天没有有效动作信号。"],
        "risk_points": {
            "fundamental": "暂无新的基本面恶化。",
            "valuation": "估值还要等景气确认。",
            "crowding": "暂未出现极端拥挤。",
            "external": "外部风险偏好变化仍会干扰。",
        },
    }
    analysis["action"]["direction"] = "观察为主"
    analysis["risks"] = []
    original_builder = client_report_module.build_stock_analysis_editor_packet

    def _patched_builder(payload: dict) -> dict:
        packet = original_builder(payload)
        packet["what_changed"] = {
            "previous_view": "上次按旧事件基线理解。",
            "change_summary": "这次主要是延续确认，不是改写 thesis。",
            "current_event_understanding": "公告类型：一般公告；更直接影响 `盈利 / 估值`；当前更像 `历史基线`",
            "conclusion_label": "维持",
            "current_view": "观察为主 / 回避 / 半导体",
            "state_trigger": "事件延续确认",
            "state_summary": "当前 thesis 仍处在已消化后的延续确认阶段，先按维持处理。",
        }
        return packet

    client_report_module.build_stock_analysis_editor_packet = _patched_builder
    try:
        rendered = ClientReportRenderer().render_stock_analysis_detailed(analysis)
    finally:
        client_report_module.build_stock_analysis_editor_packet = original_builder

    assert "## What Changed" in rendered
    assert "触发：事件延续确认" in rendered
    assert "状态解释：当前 thesis 仍处在已消化后的延续确认阶段，先按维持处理。" in rendered


def test_render_stock_analysis_detailed_adds_company_research_layer_for_observe_stock() -> None:
    analysis = _sample_analysis("600584", "长电科技", "cn_stock", rank=1)
    analysis["metadata"] = {"sector": "半导体"}
    analysis["narrative"] = {
        "headline": "逻辑还在，但当前更适合继续观察。",
        "judgment": {
            "direction": "中性",
            "cycle": "中线",
            "odds": "一般",
            "state": "观察为主",
        },
        "phase": {"label": "等待确认", "body": "逻辑未破，但价格和催化还没形成新共振。"},
        "drivers": {
            "macro": "宏观不逆风。",
            "flow": "资金承接仍偏弱。",
            "relative": "相对强弱有改善，但还不够典型。",
            "technical": "技术确认还不够。",
        },
        "contradiction": "逻辑没坏，但确认还没补齐。",
        "positives": ["赛道方向仍能继续跟踪。"],
        "cautions": ["估值和性价比不算舒服。"],
        "watch_points": ["先看放量过压力，再看回踩承接。"],
        "scenarios": {"base": "继续观察。", "bull": "确认后再升级。", "bear": "跌破关键位后降级。"},
        "playbook": {"trend": "等确认。", "allocation": "观察名单。", "defensive": "失效就重评。"},
        "summary_lines": ["今天先不升级动作。"],
        "risk_points": {
            "fundamental": "盈利和现金流还要继续验证。",
            "valuation": "估值不便宜。",
            "crowding": "主线分化时波动会放大。",
            "external": "外部风险偏好变化仍会干扰。",
        },
    }
    analysis["action"] = {
        "direction": "观察为主",
        "timeframe": "中线",
        "entry": "等放量站上压力位后再决定是否给买入区间。",
        "stop": "跌破关键支撑就重评。",
        "position": "暂不出手",
    }
    analysis["dimensions"]["fundamental"] = {
        "score": 0,
        "max_score": 100,
        "core_signal": "估值不便宜",
        "summary": "当前估值不便宜，且财务质量安全边际不够厚。",
        "factors": [
            {"name": "个股估值", "signal": "长电科技 PE 49.8x", "detail": "按绝对估值看并不便宜", "display_score": "0/25"},
            {"name": "现金流质量", "signal": "每股经营现金流 -2.11", "detail": "盈利现金含量偏弱", "display_score": "-6/10"},
        ],
    }
    analysis["dimensions"]["technical"]["summary"] = "技术结构仍偏弱，暂不支持激进介入。"
    analysis["dimensions"]["catalyst"]["summary"] = "结构化事件已出现，但公司级验证还不够完整。"
    analysis["dimensions"]["relative_strength"]["summary"] = "相对强弱有改善，但还不是最典型的扩散点。"
    analysis["dimensions"]["risk"]["summary"] = "风险和估值都不算轻松，仍要留足安全边际。"
    analysis["risks"] = []

    rendered = ClientReportRenderer().render_stock_analysis_detailed(analysis)

    assert "## 公司研究层判断" in rendered
    assert rendered.index("## 公司研究层判断") < rendered.index("## 执行摘要")
    assert "| 研究定位 | 这是“研究可以继续跟、交易还要等确认”的过渡稿。 |" in rendered
    assert "不代表公司价值归零" in rendered


def test_tighten_observe_client_markdown_reduces_template_hedge_density() -> None:
    text = """
- 先按观察稿处理，不把旧闻直接当成新增催化。
- 当前更像等待确认，不等于逻辑失效。
- 先按观察仓理解，不把它升级成动作。
- 当前更像在修复段，先按辅助线索看。
""".strip()

    tightened = _tighten_observe_client_markdown(text)
    hedge_total = sum(tightened.count(token) for token in ("不把", "不等于", "当前更像", "先按"))

    assert hedge_total <= 1
    assert "别把旧闻直接当成新增催化" in tightened
    assert "不代表逻辑失效" in tightened


def test_render_scan_summary_rows_compact_observe_and_avoid_separate_hard_bias_row() -> None:
    analysis = _sample_analysis("600313", "农发种业", "cn_stock", rank=1)
    analysis["trade_state"] = "观察为主"
    analysis["action"]["direction"] = "回避"

    rendered = ClientReportRenderer().render_scan(analysis)

    assert "| 当前建议 | 观察为主（偏回避） |" in rendered
    assert "| 方向偏向 | 回避 |" not in rendered


def test_render_stock_analysis_detailed_surfaces_sector_theme_boundary_in_body() -> None:
    analysis = _sample_analysis("600313", "农发种业", "cn_stock", rank=1)
    analysis["metadata"] = {"sector": "农业"}
    analysis["notes"] = ["粮食安全、价格链条和库存周期一起在交易。"]
    analysis["risks"] = []
    analysis["narrative"] = {
        "headline": "底层逻辑还在，但当前更适合继续观察。",
        "judgment": {
            "direction": "中性偏多",
            "cycle": "中线",
            "odds": "一般",
            "state": "观察为主",
        },
        "phase": {"label": "修复早期", "body": "还没到可以直接升级动作的阶段。"},
        "drivers": {
            "macro": "宏观不逆风。",
            "flow": "资金还在观察。",
            "relative": "相对强弱尚未恢复。",
            "technical": "技术确认还不够。",
        },
        "contradiction": "逻辑还在，但价格和动量还没翻译成买点。",
        "positives": ["基本面没坏。"],
        "cautions": ["技术确认不足。"],
        "watch_points": ["先看是否站回关键均线。"],
        "scenarios": {"base": "继续震荡观察。", "bull": "确认后再升级。", "bear": "失守后继续降级。"},
        "playbook": {"trend": "先观察。", "allocation": "先按观察仓。", "defensive": "失守再降级。"},
        "summary_lines": ["今天没有有效动作信号。"],
        "risk_points": {
            "fundamental": "暂无新的基本面恶化。",
            "valuation": "估值还要等景气确认。",
            "crowding": "暂未出现极端拥挤。",
            "external": "外部风险偏好变化仍会干扰。",
        },
    }
    rendered = ClientReportRenderer().render_stock_analysis_detailed(analysis)
    assert "| 主题边界 |" in rendered
    assert "先把行业逻辑、景气和风格顺逆风看清" in rendered


def test_render_stock_analysis_detailed_includes_what_changed_section(monkeypatch) -> None:
    _install_fake_thesis_repo(monkeypatch, {"600313": {
        "core_assumption": "农业政策催化会往种业兑现",
        "validation_metric": "公告和景气验证同步改善",
        "holding_period": "1-3个月",
        "event_digest_snapshot": {
            "status": "待补充",
            "lead_layer": "政策",
            "lead_detail": "政策影响层：方向表态",
            "lead_title": "农业政策预期升温",
            "impact_summary": "资金偏好 / 景气",
            "thesis_scope": "待确认",
        },
    }})
    analysis = _sample_analysis("600313", "农发种业", "cn_stock", rank=1)
    analysis["metadata"] = {"sector": "农业"}
    analysis["notes"] = ["粮食安全、价格链条和库存周期一起在交易。"]
    analysis["risks"] = []
    analysis["narrative"] = {
        "headline": "底层逻辑还在，但当前更适合继续观察。",
        "judgment": {
            "direction": "中性偏多",
            "cycle": "中线",
            "odds": "一般",
            "state": "观察为主",
        },
        "phase": {"label": "修复早期", "body": "还没到可以直接升级动作的阶段。"},
        "drivers": {
            "macro": "宏观不逆风。",
            "flow": "资金还在观察。",
            "relative": "相对强弱尚未恢复。",
            "technical": "技术确认还不够。",
        },
        "contradiction": "逻辑还在，但价格和动量还没翻译成买点。",
        "positives": ["基本面没坏。"],
        "cautions": ["技术确认不足。"],
        "watch_points": ["先看是否站回关键均线。"],
        "scenarios": {"base": "继续震荡观察。", "bull": "确认后再升级。", "bear": "失守后继续降级。"},
        "playbook": {"trend": "先观察。", "allocation": "先按观察仓。", "defensive": "失守再降级。"},
        "summary_lines": ["今天没有有效动作信号。"],
        "risk_points": {
            "fundamental": "暂无新的基本面恶化。",
            "valuation": "估值还要等景气确认。",
            "crowding": "暂未出现极端拥挤。",
            "external": "外部风险偏好变化仍会干扰。",
        },
    }

    rendered = ClientReportRenderer().render_stock_analysis_detailed(analysis)

    assert "## What Changed" in rendered
    assert "上次怎么看：核心假设是 `农业政策催化会往种业兑现`" in rendered
    assert "政策影响层：方向表态" in rendered
    assert "当前事件理解：" in rendered
    assert "结论变化：`升级`" in rendered


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
        "selection_context": {
            "discovery_mode_label": "全市场初筛",
            "scan_pool": 12,
            "passed_pool": 5,
            "theme_filter_label": "黄金",
            "style_filter_label": "商品/黄金",
            "manager_filter_label": "未指定",
            "coverage_note": "本轮新闻/事件覆盖基本正常。",
            "coverage_lines": ["结构化事件覆盖 60%（3/5）", "高置信直接新闻覆盖 40%（2/5）"],
            "coverage_total": 5,
            "baseline_snapshot_at": "2026-03-11 09:00:00",
            "comparison_basis_at": "2026-03-11 09:00:00",
            "comparison_basis_label": "当日基准版",
            "delivery_tier_label": "标准推荐稿",
            "delivery_observe_only": False,
            "delivery_notes": ["当前流程不是把全市场每只标的都做完整八维深扫，而是先初筛 `12` 只，再对其中 `5` 只做完整分析。"],
            "blind_spots": ["场外基金画像有 1 只拉取失败，已跳过。"],
        },
        "winner": {
            "name": "前海开源黄金ETF联接C",
            "symbol": "021740",
            "asset_type": "cn_fund",
            "portfolio_overlap_summary": {
                "summary_line": "这条建议和现有组合的主线有一定重合，更像同主题延伸。",
                "style_summary_line": "当前组合风格偏防守，最重风格是防守 44.0%。",
                "style_priority_hint": "如果只是同风格加码，优先级低于补新方向。",
            },
            "visuals": {
                "dashboard": "/tmp/fund_dashboard.png",
                "indicators": "/tmp/fund_indicators.png",
            },
            "trade_state": "持有优于追高",
            "positives": ["方向对。", "防守属性更顺风。"],
            "dimension_rows": [["技术面", "44/100", "有支撑、没加速"]],
            "action": {
                "direction": "继续持有",
                "timeframe": "中线配置(1-3月)",
                "horizon": {
                    "code": "position_trade",
                    "label": "中线配置（1-3月）",
                    "style": "更像 1-3 个月的分批配置或波段跟踪，不按隔日涨跌去做快进快出。",
                    "fit_reason": "基本面、风险收益和趋势至少有两项站得住，更适合按一段完整主线去拿，而不是只博短催化。",
                    "misfit_reason": "现在不适合当成纯隔夜交易，也还没强到可以长期不复核地持有一年以上。",
                    "allocation_view": "如果按配置视角理解，更适合作为中期主题暴露分批持有，不适合一次打满。",
                    "trading_view": "如果按交易视角理解，更适合等回踩承接后的再确认，不必天天换仓。",
                },
                "entry": "等回撤再看",
                "buy_range": "1.960 - 2.010",
                "position": "计划仓位的 1/3 - 1/2",
                "scaling_plan": "确认后再加",
                "trim_range": "2.180 - 2.240",
                "stop": "跌破支撑离场",
            },
            "narrative": {"playbook": {"allocation": "先按中线配置理解。", "trend": "等回踩确认后再动。"}},
            "positioning_lines": ["先小仓。"],
            "dimensions": {
                "fundamental": {
                    "factors": [
                        {
                            "name": "风格漂移评估",
                            "signal": "当前风格标签稳定，未见明显漂移",
                            "detail": "主动基金风格漂移是核心风险：持仓和基准偏离越大，暴露越难预测。",
                            "display_score": "10/10",
                            "awarded": 10,
                            "max": 10,
                            "factor_id": "j5_style_drift",
                        }
                    ]
                }
            },
            "taxonomy_rows": [
                ["产品形态", "场外基金"],
                ["载体角色", "ETF联接"],
                ["管理方式", "被动跟踪"],
                ["暴露类型", "商品"],
                ["主方向", "黄金"],
                ["份额类别", "ETF联接C类"],
            ],
            "taxonomy_summary": "这只标的按统一分类更接近 `场外基金 / ETF联接 / 被动跟踪`，主暴露属于 `商品`。",
            "score_changes": [{"label": "催化面", "previous": 58, "current": 72, "reason": "地缘催化增强"}],
            "fund_profile": {
                "overview": {
                    "基金类型": "商品型 / 黄金现货合约",
                    "基金管理人": "前海开源基金",
                    "基金经理人": "梁溥森、孔芳",
                    "成立日期": "2024-06-19",
                    "首发规模": "1.0300亿份",
                    "净资产规模": "9.38亿元（截止至：2025年12月31日）",
                    "业绩比较基准": "上海黄金交易所Au99.99现货实盘合约收益率*90%+人民币活期存款利率(税后)*10%",
                },
                "manager": {"begin_date": "2024-06-19", "ann_date": "2026-01-10", "education": "硕士", "nationality": "中国", "aum_billion": 27.93},
                "company": {"short_name": "前海开源", "province": "广东", "city": "深圳", "general_manager": "秦某", "website": "https://example.com/fund"},
                "dividends": {"rows": [{"ann_date": "2025-12-20", "ex_date": "2025-12-25", "pay_date": "2025-12-26", "div_cash": 0.12, "progress": "实施"}]},
                "style": {},
            },
        },
        "alternatives": [{"name": "永赢科技智选混合发起C", "symbol": "022365", "cautions": ["节奏不对。"]}],
    }
    rendered = ClientReportRenderer().render_fund_pick(payload)
    assert "## 首页判断" in rendered
    assert "### 板块 / 主题认知" in rendered
    assert "今日场外基金推荐" in rendered
    assert "## 今日结论" not in rendered
    assert "## 执行摘要" in rendered
    assert "| 当前建议 | 继续持有；持有优于追高 |" in rendered
    assert "| 交付等级 | 标准推荐稿 |" in rendered
    assert "| 空仓怎么做 |" in rendered
    assert "| 持仓怎么做 |" in rendered
    assert "| 主要利好 |" in rendered
    assert "| 主要利空 |" in rendered
    assert "发现方式: 全市场初筛 | 初筛池: 12 | 完整分析: 5" in rendered
    assert "主题过滤: 黄金 | 风格过滤: 商品/黄金 | 管理人过滤: 未指定" in rendered
    assert "## 数据完整度" in rendered
    assert "## 交付等级" in rendered
    assert "标准推荐稿" in rendered
    assert "覆盖率的分母是今天进入完整分析的 `5` 只基金" in rendered
    assert "中线配置（1-3月）" in rendered
    assert "## 图表速览" in rendered
    assert "![分析看板](/tmp/fund_dashboard.png)" in rendered
    assert "| 持有周期 | 中线配置（1-3月） |" in rendered
    assert "| 为什么按这个周期看 | 基本面、风险收益和趋势至少有两项站得住，更适合按一段完整主线去拿，而不是只博短催化。 |" in rendered
    assert "| 配置视角 | 如果按配置视角理解，更适合作为中期主题暴露分批持有，不适合一次打满。 |" in rendered
    assert "| 交易视角 | 如果按交易视角理解，更适合等回踩承接后的再确认，不必天天换仓。 |" in rendered
    assert "| 建议买入区间 | 1.960 - 2.010 |" in rendered
    assert "| 第一减仓位 | `2.180` 附近先兑现第一段反弹，不把第一波空间一次性坐回去。 |" in rendered
    assert "| 第二减仓位 | 若放量站上 `2.180`，再看 `2.240` 一带是否做第二次减仓。 |" in rendered
    assert "| 上修条件 | 只有放量站上 `2.240` 且催化、相对强弱和量能继续增强，才考虑继续上修。 |" in rendered
    assert "| 预演命令 | `portfolio whatif buy 021740 最新净值 计划金额` |" in rendered
    assert "## 组合落单前" in rendered
    assert "## 与现有持仓的关系" in rendered
    assert "同主题延伸" in rendered
    assert "组合优先级：" in rendered
    assert "## 标准化分类" in rendered
    assert "## 证据时点与来源" in rendered
    assert "ETF联接" in rendered
    assert "### 基金经理补充" not in rendered
    assert "经理画像" in rendered
    assert "梁溥森、孔芳" in rendered
    assert "在管规模 27.93 亿" in rendered
    assert "### 基金公司补充" in rendered
    assert "### 分红记录" in rendered
    assert "## 跟今天首个快照版相比" in rendered
    assert "## 数据限制与说明" in rendered
    assert "为什么不是另外几只" in rendered
    assert "## 关键强因子拆解" in rendered
    assert "风格漂移评估（ETF/基金专属）" in rendered


def test_render_fund_pick_surfaces_second_stage_tushare_signals_in_front_reason() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "selection_context": {
            "delivery_tier_label": "标准推荐稿",
            "delivery_observe_only": False,
        },
        "winner": {
            "name": "前海开源黄金ETF联接C",
            "symbol": "021740",
            "asset_type": "cn_fund",
            "trade_state": "持有优于追高",
            "positives": ["方向对。"],
            "dimension_rows": [["产品质量/基本面代理", "61/100", "当前更多是产品结构和主题代理判断"]],
            "dimensions": {
                "fundamental": {
                    "factors": [
                        {
                            "name": "现货锚定",
                            "signal": "Au99.99 现货锚定已接入",
                            "detail": "这里只用来确认产品贴近黄金现货链条。",
                            "display_score": "10/10",
                            "awarded": 10,
                            "max": 10,
                            "factor_id": "j5_gold_spot_anchor",
                        }
                    ]
                }
            },
            "action": {
                "direction": "继续持有",
                "horizon": {"label": "中线配置（1-3月）"},
            },
            "narrative": {"playbook": {}},
            "fund_profile": {
                "sales_ratio_snapshot": {
                    "latest_year": "2025",
                    "lead_channel": "商业银行",
                    "channel_mix": [{"channel": "商业银行", "ratio": 41.2}],
                    "summary": "渠道保有以银行代销为主，更像稳健配置资金在主导。",
                }
            },
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_fund_pick(payload)

    assert "## 为什么推荐它" in rendered
    assert "渠道层确认：2025 / 商业银行 / 41.20%；渠道保有以银行代销为主，更像稳健配置资金在主导。" in rendered
    assert "现货链确认：Au99.99 现货锚定已接入。" in rendered


def test_render_etf_pick_why_not_section_lists_folded_formal_alternatives() -> None:
    winner = _sample_analysis("159870", "化工ETF", "cn_etf", rank=3)
    winner["trade_state"] = "持有优于追高"
    winner["action"]["direction"] = "观望偏多"
    winner["positioning_lines"] = ["先小仓。"]
    winner["fund_sections"] = []
    winner["taxonomy_rows"] = [["产品形态", "ETF"]]
    winner["taxonomy_summary"] = "ETF / 场内ETF / 被动跟踪"
    winner["visuals"] = {}

    payload = {
        "generated_at": "2026-04-08 13:05:47",
        "selection_context": {
            "delivery_tier_label": "标准推荐稿",
            "delivery_observe_only": False,
            "delivery_summary_only": False,
        },
        "recommendation_tracks": {
            "short_term": {"name": "化工ETF", "symbol": "159870", "horizon_label": "波段跟踪（2-6周）", "reason": "趋势和产品承接都在。"},
            "medium_term": {"name": "港股创新药ETF", "symbol": "159570", "horizon_label": "波段跟踪（2-6周）", "reason": "相对强弱和主题延续还在。"},
            "third_term": {"name": "红利低波", "symbol": "512890", "horizon_label": "中线配置（1-3月）", "reason": "防守收益比仍有价值。"},
        },
        "winner": winner,
        "alternatives": [
            {"name": "港股创新药ETF", "symbol": "159570", "cautions": ["不是被排除对象。"]},
            {"name": "红利低波", "symbol": "512890", "cautions": ["不是被排除对象。"]},
        ],
        "regime": {"current_regime": "recovery"},
        "day_theme": {"label": "背景宏观主导"},
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "## 为什么不是另外几只" in rendered
    assert "其余高分候选已并入上面的正式推荐层" in rendered
    assert "港股创新药ETF (159570)" in rendered
    assert "红利低波 (512890)" in rendered


def test_render_stock_picks_exec_summary_surfaces_theme_boundary() -> None:
    analysis = _sample_analysis("560001", "政策主线ETF", "cn_etf", rank=1)
    analysis["action"]["direction"] = "观察为主"
    analysis["action"]["position"] = "暂不出手"
    analysis["narrative"]["judgment"] = {"state": "观察为主"}
    analysis["metadata"] = {"sector": "工业"}
    analysis["notes"] = ["一带一路、新质生产力和中特估一起走强。"]

    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "政策主线"},
        "regime": {"current_regime": "recovery"},
        "top": [analysis],
        "watch_positive": [],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "| 主题边界 |" in rendered
    assert "还没拉开" in rendered


def test_render_stock_picks_body_section_surfaces_theme_boundary_explainer() -> None:
    analysis = _sample_analysis("600001", "政策主线龙头", "cn_stock", rank=2)
    analysis["metadata"] = {"sector": "工业"}
    analysis["notes"] = ["一带一路、新质生产力和中特估一起走强。"]

    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "政策主线"},
        "regime": {"current_regime": "recovery"},
        "top": [analysis],
        "watch_positive": [],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "主题边界：" in rendered


def test_render_stock_picks_body_section_avoids_repeating_trading_role_boilerplate(monkeypatch) -> None:
    analyses = [
        _sample_analysis("600001", "观察票A", "cn_stock", rank=2),
        _sample_analysis("600002", "观察票B", "cn_stock", rank=2),
        _sample_analysis("600003", "观察票C", "cn_stock", rank=2),
    ]

    def _fake_packet(*_args, **_kwargs):
        return {
            "theme_playbook": {
                "playbook_level": "sector",
                "label": "工业",
                "trading_role_label": "轮动",
                "trading_position_label": "轮动仓",
                "trading_role_summary": "这条线更像轮动方向，执行上更适合低吸、分批和冲高处理，不宜直接包装成长时间主攻仓。",
            }
        }

    monkeypatch.setattr(client_report_module, "build_scan_editor_packet", _fake_packet)

    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "工业轮动"},
        "regime": {"current_regime": "recovery"},
        "top": analyses,
        "watch_positive": [],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "主题边界：" in rendered
    assert "交易分层：`轮动` / `轮动仓`" not in rendered


def test_render_stock_picks_body_section_surfaces_second_stage_stock_signals() -> None:
    analysis = _attach_second_stage_stock_signals(_sample_analysis("300502", "新易盛", "cn_stock", rank=3))
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "AI算力"},
        "regime": {"current_regime": "recovery"},
        "top": [analysis],
        "watch_positive": [],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "技术层确认：趋势偏强 / 动能改善（2026-04-03）。" in rendered
    assert "跨市场层确认：A/H 比价溢价 +18.20%（2026-04-03）。" in rendered
    assert "转债层补充：新易转债 / 趋势偏强 / 动能改善 / 转股溢价 +9.80%。" in rendered
    assert "调研层补充：机构调研：多家机构追问新品放量与订单兑现节奏；偏利多，机构关注点集中在订单兑现和景气延续。" in rendered


def test_render_stock_picks_does_not_pull_etf_proxy_from_unselected_coverage_pool() -> None:
    lead = _sample_analysis("600030", "中信证券", "cn_stock", rank=3)
    ai_coverage = _sample_analysis("300999", "人工智能龙头", "cn_stock", rank=3)
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "高股息 / 红利"},
        "regime": {"current_regime": "deflation"},
        "top": [lead],
        "coverage_analyses": [lead, ai_coverage],
        "watch_positive": [],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "人工智能ETF" not in rendered


def test_render_stock_picks_include_what_changed_section(monkeypatch) -> None:
    _install_fake_thesis_repo(monkeypatch, {"300502": {
        "core_assumption": "AI算力景气继续扩散",
        "validation_metric": "公告和订单同步兑现",
        "holding_period": "1-3个月",
        "event_digest_snapshot": {
            "status": "待补充",
            "lead_layer": "行业主题事件",
            "lead_detail": "主题事件：主题热度/映射",
            "lead_title": "AI算力链热度扩散",
            "impact_summary": "资金偏好 / 景气",
            "thesis_scope": "待确认",
        },
    }})
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "AI算力"},
        "regime": {"current_regime": "recovery"},
        "top": [_sample_analysis("300502", "新易盛", "cn_stock", rank=3)],
        "watch_positive": [],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "## What Changed" in rendered
    assert "上次怎么看：核心假设是 `AI算力景气继续扩散`" in rendered
    assert "当前事件理解：" in rendered
    assert "这次什么变了：事件状态从 `待补充` 升到 `已消化`" in rendered


def test_render_fund_pick_summary_only_skips_full_action_template() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "selection_context": {
            "discovery_mode_label": "全市场初筛",
            "scan_pool": 12,
            "passed_pool": 2,
            "theme_filter_label": "黄金",
            "style_filter_label": "商品/黄金",
            "manager_filter_label": "未指定",
            "coverage_note": "本轮新闻/事件覆盖存在降级。",
            "coverage_lines": ["结构化事件覆盖 0%（0/1）", "高置信直接新闻覆盖 0%（0/1）"],
            "coverage_total": 1,
            "delivery_tier_label": "降级观察稿",
            "delivery_observe_only": True,
            "delivery_summary_only": True,
            "delivery_notes": ["当前覆盖率过低，本次只输出摘要观察稿，不展开完整动作模板。"],
            "blind_spots": ["基金画像覆盖不足，当前只保留摘要判断。"],
        },
        "winner": {
            "name": "前海开源黄金ETF联接C",
            "symbol": "021740",
            "asset_type": "cn_fund",
            "trade_state": "观察为主",
            "positives": ["方向还在。", "但覆盖率太低。"],
            "dimension_rows": [["产品质量/基本面代理", "61/100", "当前更多是产品结构和主题代理判断"]],
            "action": {
                "direction": "观察为主",
                "horizon": {
                    "label": "观察期",
                    "allocation_view": "先按配置观察，不急着升级动作。",
                    "trading_view": "等覆盖率和确认信号一起改善。",
                },
                "entry": "等覆盖率恢复后再看",
                "stop_ref": 1.88,
                "target_ref": 2.06,
            },
            "narrative": {"playbook": {"allocation": "先观察。", "trend": "等确认。"}},
        },
        "alternatives": [],
    }
    rendered = ClientReportRenderer().render_fund_pick(payload)
    assert "## 首页判断" in rendered
    assert "### 板块 / 主题认知" in rendered
    assert "## 今日结论" not in rendered
    assert "观察期" in rendered
    assert "## 正式动作阈值" in rendered
    assert "只要当前结论仍是 `观察为主`，就不把它写成正式申赎动作" in rendered
    assert "## 升级条件" in rendered
    assert "- 升级条件：" in rendered
    assert "## 当前只看什么" in rendered
    assert "| 触发条件 | 等覆盖率恢复后再看；触发前先别急着给精确买入价。 |" in rendered
    assert "| 先看什么 | 下沿先看 `1.880` 上方能不能稳住；上沿先看 `2.060` 附近能不能放量突破。 |" in rendered
    assert "## 标准化分类" in rendered
    assert "## 关键证据" in rendered
    assert "## 为什么不是另外几只" in rendered
    assert "基金画像覆盖不足" in rendered
    assert "建议买入区间" not in rendered


def test_render_fund_pick_observe_only_full_template_prepends_homepage() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "selection_context": {
            "delivery_tier_label": "观察优先稿",
            "delivery_observe_only": True,
            "delivery_summary_only": False,
        },
        "winner": {
            "name": "招商中证A500ETF联接C",
            "symbol": "022456",
            "asset_type": "cn_fund",
            "trade_state": "观察为主",
            "positives": ["方向还在。"],
            "dimension_rows": [["技术面", "24/100", "技术确认还不够"]],
            "action": {
                "direction": "观察为主",
                "horizon": {"label": "观察期"},
                "entry": "等技术确认和相对强弱一起改善后再决定是否给买入区间。",
            },
            "narrative": {
                "headline": "方向未坏，但执行仍要等确认。",
                "judgment": {"state": "观察为主"},
                "phase": {"label": "修复早期"},
                "playbook": {},
            },
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_fund_pick(payload)

    assert "## 先看执行" in rendered
    assert rendered.index("## 先看执行") < rendered.index("## 首页判断")
    assert "| 看不看 | 观察为主 |" in rendered
    assert "| 怎么触发 |" in rendered
    assert "等技术确认和相对强弱一起改善后再决定是否给买入区间" in rendered
    assert "| 多大仓位 | 先按观察仓 / 暂不出手 |" in rendered
    assert "| 哪里止损 | 关键支撑失效就重评，不先给机械止损位。 |" in rendered
    assert "## 首页判断" in rendered
    assert "### 动作建议与结论" in rendered
    assert "## 执行摘要" in rendered


def test_render_fund_pick_skips_index_horizon_for_active_fund() -> None:
    payload = {
        "generated_at": "2026-04-05 10:00:00",
        "selection_context": {
            "delivery_tier_label": "观察优先稿",
            "delivery_observe_only": True,
            "delivery_summary_only": False,
        },
        "winner": {
            "name": "永赢科技智选混合发起C",
            "symbol": "022365",
            "asset_type": "cn_fund",
            "trade_state": "观察为主",
            "positives": ["经理和风格暴露仍可跟踪。"],
            "metadata": {"fund_management_style": "主动管理"},
            "fund_profile": {
                "overview": {
                    "基金类型": "混合型-偏股",
                    "业绩比较基准": "中国战略新兴产业成份指数收益率",
                },
                "style": {
                    "tags": ["科技主题"],
                    "taxonomy": {"management_style": "主动管理"},
                },
            },
            "dimension_rows": [["技术面", "24/100", "技术确认还不够"]],
            "action": {
                "direction": "观察为主",
                "horizon": {"label": "观察期"},
                "entry": "等技术确认和持仓线索一起改善。",
            },
            "narrative": {
                "headline": "先看经理和风格暴露，不按指数主线理解。",
                "judgment": {"state": "观察为主"},
                "phase": {"label": "修复早期"},
                "playbook": {},
            },
            "index_topic_bundle": {
                "history_snapshots": {
                    "weekly": {
                        "status": "matched",
                        "summary": "近 156周 +12.00%，最近一周 +1.10%，修复中，动能改善",
                        "trend_label": "修复中",
                        "momentum_label": "动能改善",
                        "latest_date": "2026-04-03",
                    },
                    "monthly": {
                        "status": "matched",
                        "summary": "近 36月 +18.00%，最近一月 +4.20%，趋势偏强，动能偏强",
                        "trend_label": "趋势偏强",
                        "momentum_label": "动能偏强",
                        "latest_date": "2026-04-03",
                    },
                }
            },
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_fund_pick(payload)

    assert "## 周月节奏" not in rendered
    assert "指数层确认：" not in rendered
    assert "周线：" not in rendered
    assert "## 执行摘要" in rendered


def test_render_fund_pick_exec_summary_surfaces_strategy_background_confidence() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "selection_context": {
            "delivery_tier_label": "观察优先稿",
            "delivery_observe_only": True,
            "delivery_summary_only": False,
        },
        "winner": {
            "name": "前海开源黄金ETF联接C",
            "symbol": "021740",
            "asset_type": "cn_fund",
            "trade_state": "观察为主",
            "strategy_background_confidence": {
                "status": "watch",
                "label": "观察",
                "reason": "最近只有 `2` 个可验证样本，暂时不够把它当成稳定策略，只能做辅助说明。",
            },
            "positives": ["方向还在。"],
            "dimension_rows": [["产品质量/基本面代理", "61/100", "当前更多是产品结构和主题代理判断"]],
            "action": {
                "direction": "观察为主",
                "horizon": {"label": "观察期"},
                "entry": "等覆盖率恢复后再看",
            },
            "narrative": {
                "headline": "方向未坏，但执行仍要等确认。",
                "judgment": {"state": "观察为主"},
                "phase": {"label": "修复早期"},
                "playbook": {},
            },
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_fund_pick(payload)

    assert "| 后台置信度 | 观察：" in rendered
    assert "这次信号先只作辅助说明" in rendered


def test_render_fund_pick_does_not_repeat_too_many_preface_headers() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "selection_context": {
            "delivery_tier_label": "观察优先稿",
            "delivery_observe_only": True,
            "discovery_mode_label": "全市场初筛",
            "scan_pool": 12,
            "passed_pool": 3,
            "coverage_lines": ["结构化事件覆盖 0%（0/3）", "高置信直接新闻覆盖 0%（0/3）"],
            "delivery_notes": ["当前覆盖率过低，本次只输出摘要观察稿，不展开完整动作模板。"],
        },
        "winner": {
            "name": "招商中证A500ETF联接C",
            "symbol": "022456",
            "asset_type": "cn_fund",
            "trade_state": "观察为主",
            "positives": ["方向还在。"],
            "dimension_rows": [["技术面", "26/100", "技术确认还不够"]],
            "action": {
                "direction": "观察为主",
                "horizon": {"label": "观察期"},
                "entry": "等技术确认和相对强弱一起改善后再决定是否给买入区间。",
            },
            "narrative": {
                "headline": "方向未坏，但执行仍要等确认。",
                "judgment": {"state": "观察为主"},
                "phase": {"label": "修复早期"},
                "playbook": {},
            },
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_fund_pick(payload)

    assert rendered.count("先看结论") <= 2


def test_render_fund_pick_observe_compacts_disclosure_and_tightens_wording() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "selection_context": {
            "delivery_tier_label": "降级观察稿",
            "delivery_observe_only": True,
            "delivery_summary_only": True,
            "coverage_note": "本轮覆盖存在降级，当前更适合按观察稿理解。",
            "coverage_lines": ["结构化事件覆盖 67%（2/3）", "高置信直接新闻覆盖 0%（0/3）"],
            "coverage_total": 3,
            "blind_spots": ["基金画像覆盖不足，部分画像指标暂缺。", "外部直连情报偏少。"],
        },
        "winner": {
            "name": "前海开源黄金ETF联接C",
            "symbol": "021740",
            "asset_type": "cn_fund",
            "trade_state": "观察为主",
            "positives": ["方向还在。"],
            "dimension_rows": [["产品质量/基本面代理", "61/100", "当前更多是产品结构和主题代理判断"]],
            "action": {"direction": "观察为主", "horizon": {"label": "观察期"}},
            "narrative": {"playbook": {}},
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_fund_pick(payload)

    def _section_bullets(markdown: str, heading: str) -> list[str]:
        items: list[str] = []
        inside = False
        for raw in markdown.splitlines():
            if raw.strip() == heading:
                inside = True
                continue
            if inside and raw.startswith("## "):
                break
            if inside and raw.strip().startswith("- "):
                items.append(raw.strip())
        return items

    assert len(_section_bullets(rendered, "## 数据完整度")) <= 2
    assert len(_section_bullets(rendered, "## 交付等级")) <= 2
    assert len(_section_bullets(rendered, "## 数据限制与说明")) <= 1
    assert "当前更像" not in rendered
    assert rendered.count("## 升级条件") == 1


def test_render_briefing_does_not_repeat_too_many_preface_headers() -> None:
    payload = {
        "generated_at": "2026-03-12 08:30:00",
        "headline_lines": ["今天更像结构性行情。", "风险偏好没有全面回暖。", "更适合先看主线确认。"],
        "action_lines": ["先小仓。", "等确认。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": ["制造业 PMI 50.1，较前值回升；新订单 50.8、生产 51.7。"],
        "regime": {"current_regime": "recovery", "reasoning": ["PMI 回到 50 上方。"]},
        "day_theme": "中国政策 / 内需确定性",
        "data_coverage": "中国宏观 | Watchlist 行情 | RSS新闻",
        "missing_sources": "跨市场代理",
        "quality_lines": ["本次新闻覆盖源: Reuters / 财联社。"],
        "proxy_contract": {
            "market_flow": {
                "interpretation": "黄金相对成长更抗跌，市场风格偏防守。",
                "confidence_label": "中",
                "coverage_summary": "科技/黄金/国内/海外代理样本",
                "limitation": "这是相对强弱代理，不是原始资金流。",
                "downgrade_impact": "更适合辅助判断风格切换，不适合单独下交易结论。",
            }
        },
        "a_share_watch_meta": {"pool_size": 60, "complete_analysis_size": 8},
        "a_share_watch_candidates": [],
        "a_share_watch_rows": [],
        "a_share_watch_lines": [],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert rendered.count("先看结论") <= 2


def test_render_fund_pick_explains_structured_coverage_before_zero_direct_news() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "selection_context": {
            "discovery_mode_label": "全市场初筛",
            "scan_pool": 8,
            "passed_pool": 3,
            "theme_filter_label": "黄金",
            "style_filter_label": "商品/黄金",
            "manager_filter_label": "未指定",
            "coverage_note": "本轮新闻/事件覆盖基本正常。",
            "coverage_lines": ["结构化事件覆盖 67%（2/3）", "高置信直接新闻覆盖 0%（0/3）"],
            "coverage_total": 3,
            "delivery_tier_label": "降级观察稿",
            "delivery_observe_only": True,
        },
        "winner": {
            "name": "前海开源黄金ETF联接C",
            "symbol": "021740",
            "asset_type": "cn_fund",
            "trade_state": "观察为主",
            "positives": ["方向还在。"],
            "dimension_rows": [["产品质量/基本面代理", "61/100", "当前更多是产品结构和主题代理判断"]],
            "action": {"direction": "观察为主", "horizon": {"label": "观察期"}},
            "narrative": {"playbook": {}},
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_fund_pick(payload)

    assert "当前证据更偏结构化事件、基金画像和持仓/基准映射，不是直连情报催化型驱动。" in rendered


def test_render_fund_pick_includes_what_changed_section(monkeypatch) -> None:
    _install_fake_thesis_repo(monkeypatch, {"021740": {
        "core_assumption": "避险主线会继续向黄金资产传导",
        "validation_metric": "金价和资金流同步改善",
        "holding_period": "1-3个月",
        "event_digest_snapshot": {
            "status": "待补充",
            "lead_layer": "新闻",
            "lead_title": "地缘风险升温",
        },
    }})
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "selection_context": {
            "delivery_tier_label": "观察优先稿",
            "delivery_observe_only": True,
            "coverage_note": "本轮新闻/事件覆盖基本正常。",
            "coverage_lines": ["结构化事件覆盖 67%（2/3）"],
        },
        "winner": {
            "name": "前海开源黄金ETF联接C",
            "symbol": "021740",
            "asset_type": "cn_fund",
            "trade_state": "观察为主",
            "positives": ["方向还在。"],
            "dimension_rows": [["产品质量/基本面代理", "61/100", "当前更多是产品结构和主题代理判断"]],
            "dimensions": _sample_analysis("021740", "前海开源黄金ETF联接C", "cn_fund", rank=1)["dimensions"],
            "action": {"direction": "观察为主", "horizon": {"label": "观察期"}},
            "narrative": {"playbook": {}},
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_fund_pick(payload)

    assert "## What Changed" in rendered
    assert "上次怎么看：核心假设是 `避险主线会继续向黄金资产传导`" in rendered
    assert "结论变化：`升级`" in rendered


def test_render_fund_pick_key_evidence_merges_event_digest_theme_intelligence() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "selection_context": {
            "delivery_tier_label": "观察优先稿",
            "delivery_observe_only": True,
            "delivery_summary_only": False,
        },
        "winner": {
            "name": "前海开源黄金ETF联接C",
            "symbol": "021740",
            "asset_type": "cn_fund",
            "trade_state": "观察为主",
            "positives": ["方向还在。"],
            "dimension_rows": [["产品质量/基本面代理", "61/100", "当前更多是产品结构和主题代理判断"]],
            "dimensions": {
                **_sample_analysis("021740", "前海开源黄金ETF联接C", "cn_fund", rank=1)["dimensions"],
                "catalyst": {
                    **_sample_analysis("021740", "前海开源黄金ETF联接C", "cn_fund", rank=1)["dimensions"]["catalyst"],
                    "evidence": [],
                    "theme_news": [
                        {
                            "layer": "行业主题事件",
                            "title": "黄金避险情绪继续升温",
                            "source": "财联社",
                            "date": "2026-03-10",
                            "freshness_bucket": "fresh",
                            "age_days": 1,
                        }
                    ],
                },
            },
            "action": {"direction": "观察为主", "horizon": {"label": "观察期"}},
            "narrative": {"playbook": {}},
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_fund_pick(payload)

    evidence_section = rendered.split("## 关键证据", 1)[1]
    assert "黄金避险情绪继续升温" in evidence_section
    assert "情报属性：`新鲜情报 / 媒体直连 / 主题级情报`" in evidence_section
    assert "来源层级：`媒体直连`" in evidence_section


def test_render_etf_pick_key_evidence_merges_event_digest_theme_intelligence() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "selection_context": {
            "delivery_tier_label": "观察优先稿",
            "delivery_observe_only": True,
            "delivery_summary_only": False,
        },
        "winner": {
            "name": "黄金ETF",
            "symbol": "518880",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "positives": ["方向还在。"],
            "dimension_rows": [["技术面", "52/100", "方向没坏但不适合追高"]],
            "dimensions": {
                **_sample_analysis("518880", "黄金ETF", "cn_etf", rank=1)["dimensions"],
                "catalyst": {
                    **_sample_analysis("518880", "黄金ETF", "cn_etf", rank=1)["dimensions"]["catalyst"],
                    "evidence": [],
                    "theme_news": [
                        {
                            "layer": "行业主题事件",
                            "title": "金价突破带动黄金主题热度升温",
                            "source": "财联社",
                            "date": "2026-03-10",
                            "freshness_bucket": "fresh",
                            "age_days": 1,
                        }
                    ],
                },
            },
            "action": {"direction": "观察为主", "horizon": {"label": "观察期"}},
            "narrative": {"playbook": {}},
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    evidence_section = rendered.split("## 关键证据", 1)[1]
    assert "金价突破带动黄金主题热度升温" in evidence_section
    assert "情报属性：`新鲜情报 / 媒体直连 / 主题级情报`" in evidence_section
    assert "来源层级：`媒体直连`" in evidence_section


def test_render_etf_pick_surfaces_structure_auxiliary_layer() -> None:
    payload = {
        "generated_at": "2026-04-03 10:00:00",
        "selection_context": {
            "delivery_tier_label": "观察优先稿",
            "delivery_observe_only": True,
            "delivery_summary_only": False,
        },
        "winner": {
            "name": "中际旭创ETF",
            "symbol": "159999",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "positives": ["结构框架还有解释力。"],
            "dimension_rows": [["技术面", "52/100", "方向没坏但不适合追高"]],
            "dimensions": _sample_analysis("518880", "黄金ETF", "cn_etf", rank=1)["dimensions"],
            "action": {"direction": "观察为主", "horizon": {"label": "观察期"}},
            "narrative": {"playbook": {}},
            "market_event_rows": [
                [
                    "2026-04-01",
                    "TDX 结构框架：中际旭创 通信设备 / 进攻 / CN（+3.60%）",
                    "TDX结构专题",
                    "高",
                    "通信设备",
                    "",
                    "标准结构归因",
                    "偏利多，`中际旭创` 的标准板块/风格/地区框架已可直接用来解释当前强弱。",
                ],
                [
                    "2026-04-01",
                    "港股辅助层：小米集团-W 7.20",
                    "港股/短线辅助",
                    "中",
                    "小米集团-W",
                    "",
                    "港股通/CCASS辅助",
                    "偏利多，港股通 / CCASS 命中后，先把它当作港股与短线辅助层，不把它写成正式主线。",
                ],
            ],
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "## 结构辅助层" in rendered
    assert "TDX 结构框架：中际旭创 通信设备 / 进攻 / CN（+3.60%）" in rendered
    assert "港股辅助层：小米集团-W 7.20" in rendered
    assert "信号类型：`港股通/CCASS辅助`" in rendered


def test_render_etf_pick_surfaces_dc_report_rc_and_ccass_hold_structure_layer() -> None:
    payload = {
        "generated_at": "2026-04-03 10:00:00",
        "selection_context": {
            "delivery_tier_label": "观察优先稿",
            "delivery_observe_only": True,
            "delivery_summary_only": False,
        },
        "winner": {
            "name": "中际旭创ETF",
            "symbol": "159999",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "positives": ["结构框架还有解释力。"],
            "dimension_rows": [["技术面", "52/100", "方向没坏但不适合追高"]],
            "dimensions": _sample_analysis("518880", "黄金ETF", "cn_etf", rank=1)["dimensions"],
            "action": {"direction": "观察为主", "horizon": {"label": "观察期"}},
            "narrative": {"playbook": {}},
            "market_event_rows": [
                [
                    "2026-04-01",
                    "DC 结构框架：中际旭创 光通信 / 进攻 / CN（+2.80%）",
                    "DC结构专题",
                    "中",
                    "光通信",
                    "",
                    "标准结构归因",
                    "偏利多，`中际旭创` 的标准板块/风格/地区框架已可直接用来解释当前强弱。",
                ],
                [
                    "2026-04-01",
                    "研报辅助：中际旭创 买入",
                    "研报辅助层",
                    "高",
                    "中际旭创",
                    "",
                    "研报评级/研究报告",
                    "偏利多，研报评级/一致性已命中，只作为辅助证据，不替代正式公告或主线确认。",
                ],
                [
                    "2026-04-01",
                    "港股辅助层：小米集团-W 7.20",
                    "港股/短线辅助",
                    "中",
                    "小米集团-W",
                    "",
                    "CCASS持股统计",
                    "偏利多，CCASS 命中后只作为港股/短线辅助层，不把它写成确定性主线。",
                ],
            ],
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "## 结构辅助层" in rendered
    assert "DC 结构框架：中际旭创 光通信 / 进攻 / CN（+2.80%）" in rendered
    assert "研报辅助：中际旭创 买入" in rendered
    assert "港股辅助层：小米集团-W 7.20" in rendered
    assert "研报评级/研究报告" in rendered


def test_render_etf_pick_has_fund_profile_and_alternatives() -> None:
    analysis = _sample_analysis("159981", "能源化工ETF", "cn_etf", rank=3)
    analysis["portfolio_overlap_summary"] = {
        "summary_line": "这条建议和现有组合最重的行业同线，更像同一主线延伸。",
        "style_summary_line": "风格上至少有 2 只都偏进攻，但还存在一定分化。",
        "style_priority_hint": "这组更像主线相近、风格部分重合的选择题；除非组合本来缺这类暴露，否则不宜同时重仓。",
    }
    analysis["dimensions"]["fundamental"]["factors"].append(
        {
            "name": "跟踪误差",
            "signal": "年化跟踪误差 0.28%（优秀，偏离极小）",
            "detail": "年化跟踪误差 0.28%（直接数据，daily_close，无 lag）；越低越好。",
            "display_score": "10/10",
            "awarded": 10,
            "max": 10,
            "factor_id": "j5_tracking_error",
        }
    )
    analysis["fund_profile"] = {
        "overview": {
            "基金类型": "商品型 / 能源化工期货型",
            "基金管理人": "建信基金",
            "基金经理人": "朱金钰、亢豆",
            "成立日期": "2019-12-13",
            "业绩比较基准": "易盛郑商所能源化工指数A收益率",
        },
        "manager": {"begin_date": "2019-12-13", "ann_date": "2026-01-05", "education": "硕士", "nationality": "中国"},
        "company": {"short_name": "建信", "province": "北京", "city": "北京", "general_manager": "张某", "website": "https://example.com/jx"},
        "dividends": {"rows": [{"ann_date": "2025-11-18", "ex_date": "2025-11-22", "pay_date": "2025-11-24", "div_cash": 0.08, "progress": "实施"}]},
        "style": {},
    }
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "selection_context": {
            "discovery_mode_label": "Tushare 全市场快照",
            "scan_pool": 9,
            "passed_pool": 5,
            "theme_filter_label": "能化",
            "coverage_note": "本轮新闻/事件覆盖基本正常。",
            "coverage_lines": ["结构化事件覆盖 60%（3/5）", "高置信直接新闻覆盖 40%（2/5）"],
            "coverage_total": 5,
            "baseline_snapshot_at": "2026-03-11 09:00:00",
            "comparison_basis_at": "2026-03-11 09:00:00",
            "comparison_basis_label": "当日基准版",
            "delivery_tier_label": "降级观察稿",
            "delivery_observe_only": True,
            "delivery_summary_only": False,
            "delivery_notes": ["当前流程不是把全市场每只标的都做完整八维深扫，而是先初筛 `9` 只，再对其中 `5` 只做完整分析。", "新闻/事件覆盖存在降级，本次更适合作为观察优先对象，不宜当成强执行型推荐。"],
            "proxy_contract": {
                "market_flow": {
                    "interpretation": "黄金相对成长更抗跌，资金风格偏防守。",
                    "confidence_label": "中",
                    "coverage_summary": "科技 / 黄金 / 国内 / 海外",
                    "limitation": "这是相对强弱代理，不是原始流向数据。",
                    "downgrade_impact": "可辅助判断主线切换，但不应单独决定交易动作。",
                },
                "social_sentiment": {
                    "covered": 5,
                    "total": 5,
                    "confidence_labels": {"中": 3, "高": 2},
                    "coverage_summary": "5/5 只候选已生成情绪代理",
                    "limitation": "这是价格和量能行为推导出的情绪代理，不是真实社媒抓取。",
                    "downgrade_impact": "更适合提示拥挤线索，不适合单独作为买卖信号。",
                },
            },
        },
        "recommendation_tracks": {
            "short_term": {
                "name": "能源化工ETF",
                "symbol": "159981",
                "horizon_label": "短线交易（3-10日）",
                "reason": "当前更强的是催化、相对强弱和执行节奏，优势主要集中在接下来几个交易日到一两周内。",
            },
            "medium_term": {
                "name": "红利ETF",
                "symbol": "510880",
                "horizon_label": "中线配置（1-3月）",
                "reason": "更适合围绕一段完整主线分批拿，不只是看一两天波动。",
            },
        },
        "winner": {
            "name": "能源化工ETF",
            "symbol": "159981",
            "asset_type": "cn_etf",
            "portfolio_overlap_summary": dict(analysis["portfolio_overlap_summary"]),
            "visuals": {
                "dashboard": "/tmp/etf_dashboard.png",
                "windows": "/tmp/etf_windows.png",
            },
            "trade_state": "观望偏多",
            "positives": ["方向没坏。", "相对强弱还在。", "催化不算弱。"],
            "dimension_rows": [["技术面", "52/100", "方向没坏但不适合追高"]],
            "action": {
                "direction": "观望偏多",
                "timeframe": "短线交易(1-2周)",
                "horizon": {
                    "code": "short_term",
                    "label": "短线交易（3-10日）",
                    "style": "更看催化、趋势和执行节奏，适合盯右侧确认和止损，不适合当成长线底仓。",
                    "fit_reason": "当前更强的是催化、相对强弱和执行节奏，优势主要集中在接下来几个交易日到一两周内。",
                    "misfit_reason": "现在不适合直接当成长线底仓，一旦催化和强势股状态失效要更快处理。",
                    "allocation_view": "如果按配置视角理解，这条方向已经有一定配置价值，但更适合分批跟踪，不适合一次打满。",
                    "trading_view": "如果按交易视角理解，更看催化兑现、右侧确认和止损纪律，优先等回踩或放量确认，不追当天情绪。",
                },
                "entry": "等回踩再看",
                "buy_range": "0.820 - 0.845",
                "position": "首次建仓 ≤3%",
                "scaling_plan": "分 2-3 批建仓",
                "trim_range": "0.920 - 0.960",
                "stop": "跌破支撑离场",
                "target": "先看前高",
            },
            "narrative": {"playbook": {"allocation": "先按配置仓看。", "trend": "回踩确认后再跟。"}},
            "positioning_lines": ["先小仓。"],
            "evidence": list(analysis["dimensions"]["catalyst"]["evidence"]),
            "dimensions": analysis["dimensions"],
            "fund_sections": _fund_profile_sections(analysis),
            "taxonomy_rows": [
                ["产品形态", "ETF"],
                ["载体角色", "场内ETF"],
                ["管理方式", "被动跟踪"],
                ["暴露类型", "商品"],
                ["主方向", "能源"],
                ["份额类别", "未分级"],
            ],
            "taxonomy_summary": "这只标的按统一分类更接近 `ETF / 场内ETF / 被动跟踪`，主暴露属于 `商品`。",
            "score_changes": [{"label": "催化面", "previous": 45, "current": 62, "reason": "商品链催化增强"}],
            "proxy_signals": {
                "social_sentiment": {
                    "aggregate": {
                        "interpretation": "情绪指数 61.0，讨论热度偏高，需防拥挤交易。",
                        "confidence_label": "高",
                        "limitations": ["这是价格和量能行为推导出的情绪代理，不是真实社媒抓取。"],
                        "downgrade_impact": "更适合提示拥挤线索，不适合单独作为买卖信号。",
                    }
                }
            },
        },
        "alternatives": [{"name": "红利ETF", "symbol": "510880", "cautions": ["今天弹性不如能源主线。"]}],
        "notes": ["当前数据源连接不稳定，已按可用数据降级处理。"],
    }
    rendered = ClientReportRenderer().render_etf_pick(payload)
    assert "## 先看执行" in rendered
    assert rendered.index("## 先看执行") < rendered.index("## 首页判断")
    assert "| 看不看 | 观望偏多 |" in rendered
    assert "| 怎么触发 |" in rendered
    assert "等回踩再看" in rendered
    assert "| 多大仓位 | 若触发也只按 首次建仓 ≤3%；触发前先按观察仓 / 暂不出手。 |" in rendered
    assert "| 哪里止损 | 跌破支撑离场 |" in rendered
    assert "## 首页判断" in rendered
    assert "## 今日结论" not in rendered
    assert "今日ETF观察" in rendered
    assert "等待确认后的参与窗口" in rendered
    assert "## 正式动作阈值" not in rendered
    assert "## 升级条件" in rendered
    assert "## 执行摘要" in rendered
    assert "| 优先观察 | 能源化工ETF (159981)；当前更强的是催化、相对强弱和执行节奏，优势主要集中在接下来几个交易日到一两周内。 |" in rendered
    assert "| 次级观察 | 红利ETF (510880)；更适合围绕一段完整主线分批拿，不只是看一两天波动。 |" in rendered
    assert "| 当前建议 | 观望偏多 |" in rendered
    assert "| 交付等级 | 降级观察稿 |" in rendered
    assert "| 观察重点 |" in rendered
    assert "| 空仓怎么做 |" not in rendered
    assert "| 持仓怎么做 |" not in rendered
    assert "## 数据完整度" in rendered
    assert "## 交付等级" in rendered
    assert "## 代理信号与限制" in rendered
    assert "真实社媒抓取" in rendered
    assert "降级观察稿" in rendered
    assert "## 当前分层建议" not in rendered
    assert "| 优先观察 | 能源化工ETF (159981)；当前更强的是催化、相对强弱和执行节奏，优势主要集中在接下来几个交易日到一两周内。 |" in rendered
    assert "## 为什么先看它" in rendered
    assert "## 为什么推荐它" not in rendered
    assert "## 升级条件" in rendered
    assert "- 为什么还不升级：" in rendered
    assert "- 升级条件：" in rendered
    assert "## 与现有持仓的关系" in rendered
    assert "同一主线延伸" in rendered
    assert "组合优先级：" in rendered
    assert "## 图表速览" in rendered
    assert "![分析看板](/tmp/etf_dashboard.png)" in rendered
    assert "分母是进入完整分析的 `5` 只 ETF" in rendered
    assert "短线交易（3-10日）" in rendered
    assert "| 触发条件 |" in rendered
    assert "| 先看什么 |" in rendered
    assert "| 建议买入区间 | 0.820 - 0.845 |" not in rendered
    assert "| 建议减仓区间 | 0.920 - 0.960 |" not in rendered
    assert "| 预演命令 | `portfolio whatif buy 159981 最新价 计划金额` |" not in rendered
    assert "## 组合落单前" not in rendered
    assert "## 这只ETF为什么是这个分" in rendered
    assert "## 标准化分类" in rendered


def test_render_etf_pick_observe_first_screen_surfaces_numeric_watch_level_and_stop() -> None:
    payload = {
        "generated_at": "2026-04-12 01:22:00",
        "selection_context": {
            "delivery_tier_label": "降级观察稿",
            "delivery_observe_only": True,
            "delivery_summary_only": True,
        },
        "winner": {
            "name": "建信能源化工期货ETF",
            "symbol": "159981",
            "asset_type": "cn_etf",
            "trade_state": "持有优于追高",
            "dimension_rows": [["技术面", "41/100", "空头主导缓解但仍需确认。"]],
            "dimensions": _sample_analysis("159981", "建信能源化工期货ETF", "cn_etf", rank=1)["dimensions"],
            "action": {
                "direction": "观望偏多",
                "entry": "板块轮动信号明确，若回踩 MA20 附近出现承接可分批介入，但需严控仓位",
                "buy_range": "暂不设，当前候选买点离止损位太近（至少留 `1.0%` 缓冲）, 先等回踩更深或右侧确认后再给区间。",
                "position": "首次建仓 ≤3%，等结构进一步确认后再加仓",
                "stop": "跌破 1.592 或主线/催化失效时重新评估",
                "stop_ref": 1.592,
            },
            "narrative": {"playbook": {}},
        },
        "alternatives": [],
        "notes": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "## 先看执行" in rendered
    assert "关键观察位：" in rendered
    assert "`1.592`" in rendered
    assert "| 多大仓位 | 若触发也只按 首次建仓 ≤3%，等结构进一步确认后再加仓；触发前按观察仓 / 暂不出手。 |" in rendered
    assert "| 哪里止损 | 跌破 1.592 或主线/催化失效时重新评估 |" in rendered


def test_render_scan_detailed_semiconductor_frontscreen_prioritizes_catalyst_then_flow() -> None:
    analysis = _sample_analysis("588200", "科创芯片ETF", "cn_etf", rank=2)
    analysis["risks"] = []
    analysis["narrative"] = {
        "headline": "当前更像等待确认的半导体观察稿。",
        "judgment": {
            "direction": "中性偏多",
            "cycle": "波段",
            "odds": "中性",
            "state": "观察为主",
        },
        "phase": {"label": "修复观察", "body": "产业逻辑还在，但价格还没走成确认。"},
        "drivers": {
            "macro": "宏观不构成直接顺风。",
            "flow": "资金承接还要继续看。",
            "relative": "相对强弱还没完全回到前排。",
            "technical": "技术结构还在修复。",
        },
        "contradiction": "产业催化和价格确认还没重新站到一边。",
        "positives": ["产业逻辑还在。"],
        "cautions": ["还不能直接追。"],
        "watch_points": ["先看产业催化和资金承接。"],
        "scenarios": {"base": "继续观察。", "bull": "确认后升级。", "bear": "失守后继续回撤。"},
        "playbook": {"trend": "先看右侧确认。", "allocation": "先小仓观察。", "defensive": "不抢第一根。"},
        "summary_lines": ["当前先按观察稿理解。"],
        "risk_points": {
            "fundamental": "景气预期已被部分计价。",
            "valuation": "估值还需消化。",
            "crowding": "一致预期时波动放大。",
            "external": "风险偏好变化仍会干扰。",
        },
    }
    rendered = ClientReportRenderer().render_scan_detailed(analysis)
    assert "当前先按 `半导体链` 理解，先看产业催化有没有继续兑现，再看风险偏好和资金承接能不能把价格留住。" in rendered


def test_render_etf_pick_zero_direct_news_downgrades_to_observe_packaging() -> None:
    payload = {
        "generated_at": "2026-04-11 00:13:33",
        "selection_context": {
            "coverage_lines": ["结构化事件覆盖 0%（0/15）", "高置信直接新闻覆盖 0%（0/15）"],
            "delivery_tier_label": "标准推荐稿",
            "delivery_observe_only": False,
            "delivery_summary_only": False,
            "delivery_notes": ["新闻/事件覆盖存在局部降级，但当前优先标的自身仍有可执行证据，本次继续按正式推荐框架处理。"],
        },
        "winner": {
            "name": "国泰中证半导体材料设备主题ETF",
            "symbol": "159516",
            "asset_type": "cn_etf",
            "trade_state": "回调更优",
            "positives": ["方向还在。"],
            "dimension_rows": [["技术面", "45/100", "趋势在修复，但还没强到直接追高。"]],
            "action": {
                "direction": "回调更优",
                "entry": "等回踩承接再看",
                "position": "首次建仓 ≤3%",
                "trim_range": "0.966 - 1.026",
                "stop": "跌破支撑重评",
            },
            "narrative": {"playbook": {}},
            "evidence": [
                {
                    "layer": "政策催化",
                    "title": "半导体设备ETF易方达（159558）盘中获1800万份净申购",
                    "source": "财联社",
                    "link": "https://example.com/peer-etf",
                    "date": "2026-04-10T11:35:37",
                }
            ],
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "# 今日ETF观察 | 2026-04-11" in rendered
    assert "## 为什么先看它" in rendered
    assert "| 交付等级 | 降级观察稿 |" in rendered
    assert "| 交付等级 | 标准推荐稿 |" not in rendered
    assert "继续按正式推荐框架处理" not in rendered
    assert "定位：`赛道热度佐证 / 同赛道产品`" in rendered
    assert "| 观察重点 |" in rendered
    assert "| 首次仓位 |" not in rendered
    assert "| 止损参考 |" not in rendered


def test_render_etf_pick_degraded_observe_compacts_disclosure_and_wording(monkeypatch) -> None:
    _install_fake_thesis_repo(monkeypatch, {"159980": {
        "core_assumption": "有色价格与供给扰动还会反复影响板块节奏",
        "validation_metric": "工业金属价格和份额流向同步改善",
        "holding_period": "观察期",
        "event_digest_snapshot": {
            "status": "待补充",
            "lead_layer": "行业主题事件",
            "lead_title": "标准指数框架",
        },
    }})

    payload = {
        "generated_at": "2026-04-06 10:00:00",
            "selection_context": {
                "delivery_tier_label": "降级观察稿",
                "delivery_observe_only": True,
                "delivery_summary_only": True,
                "delivery_notes": [
                    "当前流程不是把全市场每只标的都做完整八维深扫，而是先初筛 `6` 只，再对其中 `2` 只做完整分析。",
                    "新闻/事件覆盖存在降级，本次更适合作为观察优先对象，不宜当成强执行型推荐。",
                ],
            "coverage_note": "本轮实时新闻/事件覆盖存在降级，名单更容易偏保守。",
            "coverage_lines": ["结构化事件覆盖 0%（0/2）", "高置信直接新闻覆盖 0%（0/2）"],
            "coverage_total": 2,
        },
        "winner": {
            "name": "大成有色金属期货ETF",
            "symbol": "159980",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "positives": ["跟踪框架清楚。"],
            "dimension_rows": [["技术面", "37/100", "技术结构仍偏弱，暂不支持激进介入。"]],
            "dimensions": _sample_analysis("159980", "大成有色金属期货ETF", "cn_etf", rank=1)["dimensions"],
            "action": {
                "direction": "观察为主",
                "horizon": {"label": "观察期"},
                "entry": "等价格确认和右侧共振。",
            },
            "narrative": {"playbook": {}},
        },
        "alternatives": [],
        "notes": [
            "部分候选在完整分析阶段取数失败，已按可用数据降级处理。",
            "全市场 ETF 扫描未形成正向入围，本次改按观察级候选继续排序。",
        ],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    def _section_bullets(markdown: str, heading: str) -> list[str]:
        items: list[str] = []
        inside = False
        for raw in markdown.splitlines():
            if raw.strip() == heading:
                inside = True
                continue
            if inside and raw.startswith("## "):
                break
            if inside and raw.strip().startswith("- "):
                items.append(raw.strip())
        return items

    assert len(_section_bullets(rendered, "## 数据完整度")) <= 2
    assert len(_section_bullets(rendered, "## 交付等级")) <= 2
    assert len(_section_bullets(rendered, "## 数据限制与说明")) <= 1
    assert "## 正式动作阈值" not in rendered
    assert "当前更像" not in rendered
    assert "先按辅助线索看，不单独升级动作" not in rendered
    assert rendered.count("## 升级条件") == 1
    assert "## 事件消化" in rendered
    assert "## What Changed" in rendered
    assert "上次怎么看：" in rendered or "首次跟踪" in rendered


def test_render_stock_picks_observe_tightens_wording_and_uses_compact_sections() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "top": [],
        "coverage_analyses": [_sample_analysis("300502", "新易盛", "cn_stock", rank=0)],
        "watch_positive": [],
    }

    rendered = ClientReportRenderer().render_stock_picks(payload)

    assert "## 事件消化" in rendered
    assert "## What Changed" in rendered
    assert "当前更像" not in rendered
    assert "先按辅助线索看，不单独升级动作" not in rendered


def test_render_etf_pick_surfaces_factor_and_share_change_lines_in_front_reason() -> None:
    payload = {
        "generated_at": "2026-04-03 10:00:00",
        "selection_context": {"delivery_observe_only": True},
        "winner": {
            "name": "A500ETF华泰柏瑞",
            "symbol": "563360",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "positives": ["跟踪误差控制还可以。"],
            "dimension_rows": [
                ["跟踪指数技术状态", "趋势偏弱 / 动能偏弱（2026-04-02）", "先按指数主链理解当前节奏。"],
                ["场内基金技术状态（ETF/基金专属）", "场内基金技术因子 趋势偏弱 / 动能偏弱（2026-04-02）", "产品层趋势和动能仍偏弱。"],
            ],
            "action": {
                "direction": "观察为主",
                "timeframe": "中线配置(1-3月)",
                "horizon": {"label": "中线配置（1-3月）"},
                "entry": "等回踩确认",
                "position": "首次建仓 ≤3%",
            },
            "narrative": {"playbook": {}},
            "fund_sections": [
                "### ETF专用信息",
                "",
                "| 字段 | 信息 |",
                "| --- | --- |",
                "| 场内基金技术状态 | 趋势偏弱 / 动能偏弱（2026-04-02） |",
                "| 最近份额变化 | 净赎回 -0.02亿份 (-0.02%)，较 2026-04-01 |",
                "| 最近规模变化 | 截至 2026-04-02 仅有单日规模口径，不能据此写成规模扩张/收缩 |",
            ],
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "## 为什么先看它" in rendered
    assert "产品层确认：场内基金技术状态当前是 `趋势偏弱 / 动能偏弱（2026-04-02）`" in rendered
    assert "申赎线索：净赎回 -0.02亿份 (-0.02%)，较 2026-04-01。" in rendered
    assert "指数层确认：趋势偏弱 / 动能偏弱（2026-04-02）；先按指数主链理解当前节奏。" in rendered


def test_render_etf_pick_explains_structured_coverage_before_zero_direct_news() -> None:
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "selection_context": {
            "discovery_mode_label": "Tushare 全市场快照",
            "scan_pool": 9,
            "passed_pool": 3,
            "theme_filter_label": "黄金",
            "coverage_note": "本轮新闻/事件覆盖基本正常。",
            "coverage_lines": ["结构化事件覆盖 67%（2/3）", "高置信直接新闻覆盖 0%（0/3）"],
            "coverage_total": 3,
            "delivery_tier_label": "降级观察稿",
            "delivery_observe_only": True,
        },
        "winner": {
            "name": "黄金ETF",
            "symbol": "518880",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "positives": ["方向还在。"],
            "dimension_rows": [["技术面", "52/100", "方向没坏但不适合追高"]],
            "action": {"direction": "观察为主", "horizon": {"label": "观察期"}},
            "narrative": {"playbook": {}},
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "当前证据更偏结构化事件、产品画像和持仓/基准映射，不是直连情报催化型驱动" in rendered


def test_render_etf_pick_includes_what_changed_section(monkeypatch) -> None:
    _install_fake_thesis_repo(monkeypatch, {"159981": {
        "core_assumption": "商品链催化会继续强化能化弹性",
        "validation_metric": "期货链景气和资金承接同步改善",
        "holding_period": "3-10日",
        "event_digest_snapshot": {
            "status": "待补充",
            "lead_layer": "新闻",
            "lead_title": "商品价格异动",
        },
    }})
    analysis = _sample_analysis("159981", "能源化工ETF", "cn_etf", rank=1)
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "selection_context": {
            "delivery_tier_label": "观察优先稿",
            "delivery_observe_only": True,
            "coverage_note": "本轮新闻/事件覆盖基本正常。",
            "coverage_lines": ["结构化事件覆盖 67%（2/3）"],
        },
        "winner": {
            "name": "能源化工ETF",
            "symbol": "159981",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "positives": ["方向还在。"],
            "dimension_rows": [["技术面", "28/100", "方向没坏但不适合追高"]],
            "dimensions": analysis["dimensions"],
            "action": {"direction": "观察为主", "horizon": {"label": "观察期"}},
            "narrative": {"playbook": {}},
        },
        "alternatives": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "## What Changed" in rendered
    assert "上次怎么看：核心假设是 `商品链催化会继续强化能化弹性`" in rendered
    assert "结论变化：`升级`" in rendered


def test_render_etf_pick_marks_stale_policy_evidence() -> None:
    analysis = _sample_analysis("513090", "香港证券ETF", "cn_etf", rank=3)
    payload = {
        "generated_at": "2026-03-21 10:00:00",
        "day_theme": {"label": "中国政策 / 内需确定性"},
        "regime": {
            "current_regime": "stagflation",
            "reasoning": ["PMI 低于 50。", "价格压力仍高。"],
        },
        "selection_context": {
            "discovery_mode_label": "Tushare 全市场快照",
            "scan_pool": 8,
            "passed_pool": 3,
            "theme_filter_label": "券商",
            "coverage_note": "本轮新闻/事件覆盖基本正常。",
            "coverage_lines": ["结构化事件覆盖 67%（2/3）"],
            "coverage_total": 3,
            "delivery_tier_label": "标准推荐稿",
            "delivery_observe_only": False,
            "proxy_contract": {
                "market_flow": {
                    "interpretation": "成长相对黄金更强，资金风格偏 risk-on。",
                    "confidence_label": "中",
                    "coverage_summary": "科技 / 黄金 / 国内 / 海外",
                    "limitation": "这是相对强弱代理，不是原始流向数据。",
                    "downgrade_impact": "可辅助判断主线切换，但不应单独决定交易动作。",
                },
            },
        },
        "winner": {
            "name": "香港证券ETF",
            "symbol": "513090",
            "asset_type": "cn_etf",
            "trade_state": "观望偏多",
            "positives": ["方向没坏。", "宽松预期仍在。", "板块弹性保留。"],
            "dimension_rows": [["技术面", "52/100", "方向没坏但不适合追高"]],
            "action": {
                "direction": "观望偏多",
                "timeframe": "波段跟踪(2-6周)",
                "horizon": {
                    "code": "position_trade",
                    "label": "波段跟踪（2-6周）",
                    "style": "更看催化、趋势和执行节奏。",
                    "fit_reason": "当前更像 2-6 周的节奏博弈。",
                    "misfit_reason": "现在不适合当成长线底仓。",
                },
                "entry": "等回踩再看",
                "buy_range": "1.760 - 1.790",
                "position": "首次建仓 ≤3%",
                "scaling_plan": "分 2 批跟踪",
                "trim_range": "1.940 - 1.980",
                "stop": "跌破 1.720 或主线/催化失效时重新评估",
                "target": "先看前高/近 60 日高点 1.980 附近的承压与突破情况",
            },
            "narrative": {"playbook": {"allocation": "先按配置仓看。", "trend": "等确认后再跟。"}},
            "positioning_lines": ["先小仓。"],
            "evidence": [
                {
                    "layer": "政策催化",
                    "title": "PBOC official says monetary easing remains supportive",
                    "source": "Reuters",
                    "link": "https://example.com/policy",
                    "date": "2026-03-09",
                }
            ],
            "dimensions": analysis["dimensions"],
            "taxonomy_rows": [
                ["产品形态", "ETF"],
                ["载体角色", "场内ETF"],
                ["管理方式", "被动跟踪"],
                ["暴露类型", "跨境"],
                ["主方向", "金融"],
                ["份额类别", "未分级"],
            ],
            "taxonomy_summary": "这只标的按统一分类更接近 `ETF / 场内ETF / 被动跟踪`，主暴露属于 `跨境 / 金融`。",
        },
        "alternatives": [],
        "notes": [],
    }
    rendered = ClientReportRenderer().render_etf_pick(payload)
    assert "已过去 12 天，时效在衰减" in rendered
    assert "短线风险偏好修复" in rendered


def test_render_etf_pick_normalizes_nan_holding_names() -> None:
    analysis = _sample_analysis("513090", "香港证券ETF易方达", "cn_etf", rank=3)
    analysis["fund_profile"] = {
        "overview": {
            "基金类型": "股票型 / 被动指数型",
            "基金管理人": "易方达基金",
            "基金经理人": "宋钊贤",
            "成立日期": "2020-03-13",
            "首发规模": "10.1999亿份",
            "净资产规模": "303.25亿元（截止至：2025年12月31日）",
            "业绩比较基准": "中证香港证券投资主题指数收益率(使用估值汇率折算)",
        },
        "style": {
            "taxonomy": {
                "product_form": "ETF",
                "vehicle_role": "场内ETF",
                "management_style": "被动跟踪",
                "exposure_scope": "行业主题",
                "sector": "金融",
                "share_class": "未分级",
                "summary": "这只标的按统一分类更接近 `ETF / 场内ETF / 被动跟踪`，主暴露属于 `行业主题`。",
            }
        },
        "top_holdings": [
            {"股票代码": "0388", "股票名称": float("nan"), "占净值比例": 15.27, "季度": "2025-12-31"},
        ],
        "industry_allocation": [{"行业类别": "金融", "占净值比例": 98.6, "截止时间": "2025-12-31"}],
    }
    payload = {
        "generated_at": "2026-03-21 15:00:00",
        "selection_context": {
            "discovery_mode_label": "Tushare 全市场快照",
            "scan_pool": 30,
            "passed_pool": 11,
            "theme_filter_label": "未指定",
            "coverage_note": "本轮新闻/事件覆盖基本正常。",
            "coverage_lines": ["结构化事件覆盖 0%（0/11）", "高置信直接新闻覆盖 91%（10/11）"],
            "coverage_total": 11,
            "delivery_tier_label": "标准推荐稿",
            "delivery_notes": ["当前流程不是把全市场每只标的都做完整八维深扫，而是先初筛 `30` 只，再对其中 `11` 只做完整分析。"],
        },
        "recommendation_tracks": {
            "short_term": {
                "name": "香港证券ETF易方达",
                "symbol": "513090",
                "horizon_label": "波段跟踪（2-6周）",
                "reason": "当前更适合作为右侧确认后的波段跟踪对象。",
            }
        },
        "winner": {
            "name": "香港证券ETF易方达",
            "symbol": "513090",
            "asset_type": "cn_etf",
            "trade_state": "等右侧确认",
            "positives": ["方向没坏。"],
            "dimension_rows": [["技术面", "38/100", "技术结构仍偏弱，暂不支持激进介入。"]],
            "action": analysis["action"],
            "narrative": {"playbook": {"allocation": "先小仓。", "trend": "等右侧确认。"}},
            "positioning_lines": ["先小仓。"],
            "evidence": list(analysis["dimensions"]["catalyst"]["evidence"]),
            "dimensions": analysis["dimensions"],
            "fund_sections": _fund_profile_sections(analysis),
            "taxonomy_rows": [
                ["产品形态", "ETF"],
                ["载体角色", "场内ETF"],
                ["管理方式", "被动跟踪"],
                ["暴露类型", "行业主题"],
                ["主方向", "金融"],
                ["份额类别", "未分级"],
            ],
            "taxonomy_summary": "这只标的按统一分类更接近 `ETF / 场内ETF / 被动跟踪`，主暴露属于 `行业主题`。",
            "proxy_signals": {},
        },
        "alternatives": [],
        "notes": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "| 0388 | 0388 | 15.27% | 2025-12-31 |" in rendered


def test_render_etf_pick_action_rows_use_enriched_theme_playbook_role() -> None:
    winner = _sample_analysis("159516", "国泰中证半导体材料设备主题ETF", "cn_etf", rank=2)
    winner["metadata"] = {"sector": "半导体设备", "chain_nodes": ["AI算力", "半导体", "成长股估值修复"]}
    winner["trade_state"] = "回调更优"
    winner["action"] = {
        **dict(winner.get("action") or {}),
        "direction": "观望偏多",
        "entry": "先等顶背离/假突破消化后再看",
        "position": "首次建仓 ≤3%",
        "horizon": {
            "code": "swing",
            "label": "波段跟踪（2-6周）",
            "style": "当前更像主线轮动和催化共振驱动的波段跟踪，核心在未来几周能否继续得到资金确认。",
            "fit_reason": "催化和相对强弱都在线，优势主要集中在未来几周的轮动延续，而不是长周期兑现。",
        },
    }
    winner["dimensions"]["technical"]["score"] = 34
    winner["dimensions"]["fundamental"]["score"] = 53
    winner["dimensions"]["catalyst"]["score"] = 64
    winner["dimensions"]["relative_strength"]["score"] = 75
    winner["dimensions"]["relative_strength"]["factors"] = [
        {"name": "超额拐点", "detail": "相对基准从负转正更接近轮动切换窗口。"}
    ]
    winner["dimensions"]["risk"]["score"] = 35
    winner["dimensions"]["macro"]["score"] = 7
    winner["narrative"]["judgment"] = {"state": "回调更优"}
    winner["narrative"]["playbook"] = {"trend": "更适合等短线动能重新修复后再跟随。"}
    winner["dimension_rows"] = [["技术面", "34/100", "技术结构仍偏弱"]]
    winner["fund_sections"] = []
    winner["taxonomy_rows"] = [["产品形态", "ETF"], ["主方向", "半导体"]]
    winner["taxonomy_summary"] = "ETF / 场内ETF / 被动跟踪 / 半导体主题"
    winner["positioning_lines"] = []
    payload = {
        "generated_at": "2026-04-11 11:00:00",
        "day_theme": {"label": "硬科技 / AI硬件链"},
        "regime": {"current_regime": "recovery"},
        "selection_context": {
            "delivery_tier_label": "观察优先稿",
            "delivery_observe_only": True,
            "delivery_summary_only": False,
        },
        "winner": winner,
        "alternatives": [],
        "notes": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "| 交易分层 | `主线扩散` / `卫星仓`" in rendered
    assert "| 仓位身份 | `主线扩散` / `卫星仓`" in rendered
    assert "`轮动` / `轮动仓`" not in rendered


def test_render_etf_pick_splits_direction_bias_from_observe_only_action() -> None:
    payload = {
        "generated_at": "2026-03-13 15:00:00",
        "selection_context": {
            "discovery_mode_label": "Tushare 全市场快照",
            "scan_pool": 30,
            "passed_pool": 5,
            "theme_filter_label": "未指定",
            "coverage_note": "本轮新闻/事件覆盖基本正常。",
            "coverage_lines": ["结构化事件覆盖 60%（3/5）", "高置信直接新闻覆盖 40%（2/5）"],
            "coverage_total": 5,
            "delivery_tier_label": "观察优先稿",
            "delivery_observe_only": True,
            "delivery_notes": ["当前排第一的标的仍是观察/持有优先口径。"],
        },
        "winner": {
            "name": "香港证券ETF易方达",
            "symbol": "513090",
            "trade_state": "观察为主",
            "positives": ["方向没坏。"],
            "dimension_rows": [["技术面", "38/100", "技术结构仍偏弱"]],
            "action": {"direction": "做多", "entry": "等回踩再看", "position": "暂不出手", "horizon": {"label": "观察期"}},
            "positioning_lines": [],
            "evidence": [{"title": "方向仍在。", "source": "测试源"}],
            "taxonomy_rows": [["产品形态", "ETF"]],
            "taxonomy_summary": "产品标签正常。",
            "fund_sections": {"overview": {"基金类型": "ETF", "基金管理人": "易方达", "基金经理人": "测试", "成立日期": "2025-01-01", "业绩比较基准": "香港证券指数"}},
            "score_changes": [],
        },
        "alternatives": [],
        "notes": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "| 当前建议 | 观察为主 |" in rendered
    assert "| 方向偏向 | 做多 |" in rendered
    assert "做多；观察为主" not in rendered


def test_render_etf_pick_keeps_wait_for_confirmation_out_of_hard_action_rows() -> None:
    payload = {
        "generated_at": "2026-04-23 15:00:00",
        "selection_context": {
            "discovery_mode_label": "Tushare 全市场快照",
            "scan_pool": 18,
            "passed_pool": 18,
            "theme_filter_label": "未指定",
            "coverage_note": "本轮新闻/事件覆盖存在降级。",
            "coverage_lines": ["结构化事件覆盖 50%（9/18）", "高置信直接新闻覆盖 0%（0/18）"],
            "coverage_total": 18,
            "delivery_tier_label": "降级观察稿",
            "delivery_observe_only": True,
            "delivery_notes": ["今天先给一个观察优先对象，不按正式买入稿理解。"],
        },
        "winner": {
            "name": "国泰中证全指通信设备ETF",
            "symbol": "515880",
            "asset_type": "cn_etf",
            "trade_state": "等右侧确认",
            "positives": ["通信主线没坏。"],
            "dimension_rows": [["技术面", "42/100", "技术仍待确认"]],
            "action": {"direction": "做多", "entry": "等回踩再看", "position": "暂不出手", "horizon": {"label": "观察期"}},
            "positioning_lines": [],
            "evidence": [{"title": "CPO概念大涨", "source": "证券时报", "link": "https://example.com/cpo"}],
            "taxonomy_rows": [["产品形态", "ETF"], ["主方向", "通信"]],
            "taxonomy_summary": "主暴露属于通信。",
            "fund_sections": {"overview": {"基金类型": "ETF", "基金管理人": "国泰基金", "基金经理人": "测试", "成立日期": "2019-01-01", "业绩比较基准": "通信设备指数"}},
            "score_changes": [],
        },
        "alternatives": [],
        "notes": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "| 当前建议 | 等右侧确认 |" in rendered
    assert "| 当前动作 | 等右侧确认 |" in rendered
    assert "| 当前动作 | 做多 |" not in rendered


def test_render_etf_pick_explains_missing_alternatives() -> None:
    analysis = _sample_analysis("513120", "港股创新药ETF", "cn_etf", rank=1)
    payload = {
        "generated_at": "2026-03-13 15:00:00",
        "selection_context": {
            "discovery_mode_label": "watchlist 回退",
            "scan_pool": 7,
            "passed_pool": 2,
            "theme_filter_label": "未指定",
            "coverage_note": "本轮新闻/事件覆盖存在降级。",
            "coverage_lines": ["结构化事件覆盖 0%（0/1）", "高置信直接新闻覆盖 0%（0/1）"],
            "coverage_total": 1,
            "delivery_tier_label": "代理观察稿",
            "delivery_observe_only": True,
            "delivery_summary_only": True,
            "delivery_notes": ["当前流程不是把全市场每只标的都做完整八维深扫，而是先初筛 `7` 只，再对其中 `2` 只做完整分析。"],
        },
        "recommendation_tracks": {
            "short_term": {
                "name": "港股创新药ETF",
                "symbol": "513120",
                "horizon_label": "观察期",
                "reason": "当前信号还没共振到足以支撑正式动作，先观察更稳妥。",
            }
        },
        "winner": {
            "name": "港股创新药ETF",
            "symbol": "513120",
            "trade_state": "观察为主",
            "positives": ["方向没坏。", "但还没右侧确认。", "先看不急着做。"],
            "dimension_rows": [["技术面", "34/100", "技术结构仍偏弱"]],
            "action": {"direction": "回避", "entry": "等 MA20 向上", "position": "暂不出手", "scaling_plan": "仅观察仓", "stop": "跌破支撑重评", "target": "先看前高"},
            "positioning_lines": ["首次仓位按 `暂不出手` 执行。"],
            "evidence": [{"title": "当前没有抓到高置信直连证据。", "source": "内部覆盖率摘要"}],
            "fund_sections": _fund_profile_sections(analysis),
            "taxonomy_rows": [
                ["产品形态", "ETF"],
                ["载体角色", "场内ETF"],
                ["管理方式", "被动跟踪"],
                ["暴露类型", "跨境"],
                ["主方向", "医药"],
                ["份额类别", "未分级"],
            ],
            "taxonomy_summary": "这只标的按统一分类更接近 `ETF / 场内ETF / 被动跟踪`，主暴露属于 `跨境`。",
            "score_changes": [],
        },
        "alternatives": [],
        "notes": ["全市场 ETF 快照没有形成可交付候选，已回退到 ETF watchlist。"],
    }
    rendered = ClientReportRenderer().render_etf_pick(payload)
    assert "## 首页判断" in rendered
    assert "### 板块 / 主题认知" in rendered
    assert "## 今日结论" not in rendered
    assert "## 周月节奏" not in rendered
    assert "## 升级条件" in rendered
    assert "## 当前只看什么" in rendered
    assert "| 优先观察 | 港股创新药ETF (513120)；当前信号还没共振到足以支撑正式动作，先观察更稳妥。 |" in rendered
    assert "| 触发条件 | 先等 MA20 向上拐头；触发前别急着给精确买入价。 |" in rendered
    assert "关键盯盘价位" not in rendered
    assert "## 标准化分类" in rendered
    assert "## 图表与详细分析说明" in rendered
    assert "没有生成 K 线/阶段走势图表" in rendered
    assert "## 关键证据" in rendered
    assert "## 证据时点与来源" in rendered
    assert "## 为什么不是另外几只" in rendered
    assert "建议买入区间" not in rendered
    assert "摘要版交付" in rendered
    assert "当前可前置的一手情报有限" not in rendered


def test_render_etf_pick_keeps_formal_alternatives_out_of_why_not_section() -> None:
    winner = _sample_analysis("159981", "能源化工ETF", "cn_etf", rank=3)
    second = _sample_analysis("510880", "红利ETF", "cn_etf", rank=3)
    third = _sample_analysis("513120", "港股创新药ETF", "cn_etf", rank=3)
    payload = {
        "generated_at": "2026-03-27 15:00:00",
        "selection_context": {
            "delivery_observe_only": False,
            "delivery_tier_label": "标准推荐稿",
        },
        "recommendation_tracks": {
            "short_term": {
                "name": "能源化工ETF",
                "symbol": "159981",
                "horizon_label": "短线交易（3-10日）",
                "reason": "催化和右侧确认更完整。",
            },
            "medium_term": {
                "name": "红利ETF",
                "symbol": "510880",
                "horizon_label": "中线配置（1-3月）",
                "reason": "中期配置承接更稳。",
            },
            "third_term": {
                "name": "港股创新药ETF",
                "symbol": "513120",
                "horizon_label": "波段跟踪（2-6周）",
                "reason": "主线延续和催化兑现都还在。",
            },
        },
        "winner": winner,
        "alternatives": [second, third],
        "notes": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "| 第三推荐 | 港股创新药ETF (513120) | 波段跟踪（2-6周） | 主线延续和催化兑现都还在。 |" in rendered
    assert "## 其余正式推荐" in rendered
    assert "### 2. 红利ETF (510880)" in rendered
    assert "### 3. 港股创新药ETF (513120)" in rendered
    assert "其余高分候选已并入上面的正式推荐层" in rendered


def test_render_etf_pick_observe_tracks_use_observe_alternatives_heading() -> None:
    winner = _sample_analysis("159981", "能源化工ETF", "cn_etf", rank=3)
    second = _sample_analysis("510880", "红利ETF", "cn_etf", rank=3)
    third = _sample_analysis("513120", "港股创新药ETF", "cn_etf", rank=3)
    payload = {
        "generated_at": "2026-03-27 15:00:00",
        "selection_context": {
            "delivery_observe_only": True,
            "delivery_tier_label": "降级观察稿",
        },
        "recommendation_tracks": {
            "short_term": {
                "name": "能源化工ETF",
                "symbol": "159981",
                "horizon_label": "短线交易（3-10日）",
                "reason": "催化和右侧确认更完整。",
            },
            "medium_term": {
                "name": "红利ETF",
                "symbol": "510880",
                "horizon_label": "中线配置（1-3月）",
                "reason": "中期配置承接更稳。",
            },
            "third_term": {
                "name": "港股创新药ETF",
                "symbol": "513120",
                "horizon_label": "波段跟踪（2-6周）",
                "reason": "主线延续和催化兑现都还在。",
            },
        },
        "winner": winner,
        "alternatives": [second, third],
        "notes": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "| 次级观察 | 红利ETF (510880)；中期配置承接更稳。 |" in rendered
    assert "## 其余观察对象" in rendered
    assert "## 其余正式推荐" not in rendered
    assert "其余高分候选已并入上面的观察顺序层" in rendered
    assert "正式推荐层" not in rendered


def test_render_etf_pick_summary_only_sanitizes_scan_failure_and_keeps_action_prices() -> None:
    payload = {
        "generated_at": "2026-03-27 15:00:00",
        "selection_context": {
            "delivery_observe_only": True,
            "delivery_summary_only": True,
            "delivery_tier_label": "降级观察稿",
            "coverage_note": "本轮实时新闻/事件覆盖存在降级。",
            "coverage_lines": ["结构化事件覆盖 0%（0/17）", "高置信直接新闻覆盖 0%（0/17）"],
            "coverage_total": 17,
        },
        "winner": {
            "name": "港股通创新药ETF",
            "symbol": "159570",
            "trade_state": "观察为主",
            "positives": ["方向没坏。"],
            "dimension_rows": [["催化面", "0/100", "直接催化偏弱。"]],
            "action": {
                "direction": "回避",
                "entry": "先等 MA20 / MA60 向上拐头，再看回踩关键支撑不破",
                "position": "首次建仓 ≤3%",
                "stop": "跌破 1.420 重新评估",
                "stop_ref": 1.420,
                "target_ref": 1.784,
                "horizon": {
                    "label": "观察期",
                    "fit_reason": "价格结构不算最差，但催化和确认都偏弱，继续观察比仓促下手更稳。",
                },
            },
            "positioning_lines": [],
            "evidence": [],
            "taxonomy_rows": [["产品形态", "ETF"]],
            "taxonomy_summary": "这只标的按统一分类更接近 `ETF / 场内ETF / 被动跟踪`。",
            "fund_profile": {
                "overview": {
                    "基金类型": "ETF",
                    "基金管理人": "测试基金",
                    "业绩比较基准": "港股创新药指数收益率",
                },
                "etf_snapshot": {
                    "etf_type": "股票型ETF",
                    "exchange": "SZSE",
                    "index_name": "港股创新药指数",
                    "index_code": "884141.TI",
                    "share_as_of": "2026-03-27",
                    "total_share_yi": 18.6,
                    "total_size_yi": 22.4,
                },
            },
            "score_changes": [],
            "narrative": {"playbook": {"allocation": "先按观察仓理解。", "trend": "等确认后再看。"}},
        },
        "alternatives": [],
        "notes": ["162719 扫描失败: 当前数据源连接不稳定，已按可用数据降级处理。"],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "162719 扫描失败" not in rendered
    assert "部分候选在完整分析阶段取数失败，已按可用数据降级处理" in rendered
    assert "## 这几个词怎么读" in rendered
    assert "`T+1 直连`" not in rendered
    assert "`高置信直连情报 / 结构化披露`" in rendered
    assert "| 怎么用这段 | 上面的 `先看执行` 已经给了观察位、仓位和失效位；这里不重复挂单细节，只补升级条件。 |" in rendered
    assert "| 先看什么 | 下沿先看 `1.420` 上方能不能稳住；上沿先看 `1.784` 附近能不能放量突破。 |" in rendered
    assert "## 基金画像" in rendered
    assert "### ETF专用信息" in rendered
    assert "港股创新药指数" in rendered


def test_render_etf_pick_single_standard_candidate_does_not_self_downgrade() -> None:
    analysis = _sample_analysis("159981", "能源化工ETF", "cn_etf", rank=3)
    payload = {
        "generated_at": "2026-03-13 15:00:00",
        "selection_context": {
            "discovery_mode_label": "Tushare 全市场快照",
            "scan_pool": 30,
            "passed_pool": 3,
            "theme_filter_label": "未指定",
            "preferred_sectors": ["能源", "商品"],
            "preferred_sector_label": "能源 / 商品",
            "coverage_note": "本轮新闻/事件覆盖基本正常。",
            "coverage_lines": ["结构化事件覆盖 33%（1/3）", "高置信直接新闻覆盖 33%（1/3）"],
            "coverage_total": 3,
            "delivery_tier_label": "标准推荐稿",
            "delivery_observe_only": False,
            "delivery_notes": ["当前流程不是把全市场每只标的都做完整八维深扫，而是先初筛 `30` 只，再对其中 `3` 只做完整分析。"],
        },
        "winner": {
            "name": "能源化工ETF",
            "symbol": "159981",
            "trade_state": "观望偏多",
            "strategy_background_confidence": {
                "status": "stable",
                "label": "稳定",
                "reason": "最近验证仍稳定，先作辅助加分。",
            },
            "positives": ["方向没坏。", "相对强弱仍在。", "更适合分批。"],
            "dimension_rows": [["技术面", "52/100", "方向没坏但不适合追高"]],
            "action": {"direction": "观望偏多", "entry": "等回踩再看", "position": "首次建仓 ≤3%", "scaling_plan": "分 2-3 批建仓", "stop": "跌破支撑离场", "target": "先看前高"},
            "positioning_lines": ["先小仓。"],
            "evidence": [{"title": "商品链催化存在。", "source": "测试源"}],
            "fund_sections": _fund_profile_sections(analysis),
            "taxonomy_rows": [
                ["产品形态", "ETF"],
                ["载体角色", "场内ETF"],
                ["管理方式", "被动跟踪"],
                ["暴露类型", "商品"],
                ["主方向", "能源"],
                ["份额类别", "未分级"],
            ],
            "taxonomy_summary": "这只标的按统一分类更接近 `ETF / 场内ETF / 被动跟踪`，主暴露属于 `商品`。",
            "score_changes": [],
        },
        "alternatives": [],
        "notes": [],
    }
    rendered = ClientReportRenderer().render_etf_pick(payload)
    assert "今天可进入完整评分且未被硬排除的 ETF 候选只有 1 只" in rendered
    assert "单候选不等于自动降级" in rendered
    assert "只能按观察优先或降级稿处理" not in rendered
    assert "| 后台置信度 | 稳定：" in rendered
    assert "主题过滤: 未指定 | 偏好主题: 能源 / 商品" in rendered


def test_render_etf_pick_includes_regime_basis_when_available() -> None:
    analysis = _sample_analysis("513090", "香港证券ETF易方达", "cn_etf", rank=3)
    payload = {
        "generated_at": "2026-03-13 15:00:00",
        "regime": {
            "current_regime": "recovery",
            "reasoning": ["PMI 回到 50 上方，增长端修复。", "信用脉冲边际改善，景气修复不再只靠情绪。"],
        },
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "selection_context": {
            "discovery_mode_label": "Tushare 全市场快照",
            "scan_pool": 30,
            "passed_pool": 11,
            "theme_filter_label": "未指定",
            "coverage_note": "本轮新闻/事件覆盖基本正常。",
            "coverage_lines": ["结构化事件覆盖 0%（0/11）", "高置信直接新闻覆盖 100%（11/11）"],
            "coverage_total": 11,
            "delivery_tier_label": "标准推荐稿",
            "delivery_observe_only": False,
            "delivery_notes": ["当前流程不是把全市场每只标的都做完整八维深扫，而是先初筛 `30` 只，再对其中 `11` 只做完整分析。"],
        },
        "winner": {
            "name": "香港证券ETF易方达",
            "symbol": "513090",
            "trade_state": "观望偏多",
            "positives": ["方向没坏。", "相对强弱仍占优。"],
            "dimension_rows": [["技术面", "52/100", "方向没坏但不适合追高"]],
            "action": {"direction": "观望偏多", "entry": "等回踩再看", "position": "首次建仓 ≤3%", "scaling_plan": "分 2-3 批建仓", "stop": "跌破 1.785 或主线/催化失效时重新评估", "target": "先看前高"},
            "positioning_lines": ["先小仓。"],
            "evidence": [{"title": "券商方向相对强弱仍在。", "source": "测试源"}],
            "fund_sections": _fund_profile_sections(analysis),
            "taxonomy_rows": [
                ["产品形态", "ETF"],
                ["载体角色", "场内ETF"],
                ["管理方式", "被动跟踪"],
                ["暴露类型", "行业主题"],
                ["主方向", "金融"],
                ["份额类别", "未分级"],
            ],
            "taxonomy_summary": "这只标的按统一分类更接近 `ETF / 场内ETF / 被动跟踪`，主暴露属于 `行业主题`。",
            "score_changes": [],
        },
        "alternatives": [],
        "notes": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "## 宏观判断依据" in rendered
    assert "PMI 回到 50 上方" in rendered
    assert "当天主线写成 `能源冲击 + 地缘风险`" in rendered


def test_render_etf_pick_hides_buy_range_when_too_close_to_stop() -> None:
    analysis = _sample_analysis("513090", "香港证券ETF易方达", "cn_etf", rank=3)
    payload = {
        "generated_at": "2026-03-13 15:00:00",
        "selection_context": {
            "discovery_mode_label": "Tushare 全市场快照",
            "scan_pool": 30,
            "passed_pool": 11,
            "theme_filter_label": "未指定",
            "coverage_note": "本轮新闻/事件覆盖基本正常。",
            "coverage_lines": ["结构化事件覆盖 0%（0/11）", "高置信直接新闻覆盖 100%（11/11）"],
            "coverage_total": 11,
            "delivery_tier_label": "标准推荐稿",
            "delivery_observe_only": False,
            "delivery_notes": ["当前流程不是把全市场每只标的都做完整八维深扫，而是先初筛 `30` 只，再对其中 `11` 只做完整分析。"],
        },
        "winner": {
            "name": "香港证券ETF易方达",
            "symbol": "513090",
            "trade_state": "观望偏多",
            "positives": ["方向没坏。", "相对强弱仍占优。"],
            "dimension_rows": [["技术面", "52/100", "方向没坏但不适合追高"]],
            "action": {
                "direction": "观望偏多",
                "entry": "等回踩再看",
                "buy_range": "1.803 - 1.814",
                "position": "首次建仓 ≤3%",
                "scaling_plan": "分 2-3 批建仓",
                "trim_range": "2.175 - 2.309",
                "stop": "跌破 1.785 或主线/催化失效时重新评估",
                "target": "先看前高",
            },
            "positioning_lines": ["先小仓。"],
            "evidence": [{"title": "券商方向相对强弱仍在。", "source": "测试源"}],
            "fund_sections": _fund_profile_sections(analysis),
            "taxonomy_rows": [
                ["产品形态", "ETF"],
                ["载体角色", "场内ETF"],
                ["管理方式", "被动跟踪"],
                ["暴露类型", "行业主题"],
                ["主方向", "金融"],
                ["份额类别", "未分级"],
            ],
            "taxonomy_summary": "这只标的按统一分类更接近 `ETF / 场内ETF / 被动跟踪`，主暴露属于 `行业主题`。",
            "score_changes": [],
        },
        "alternatives": [],
        "notes": [],
    }

    rendered = ClientReportRenderer().render_etf_pick(payload)

    assert "| 建议买入区间 |" not in rendered


def test_render_briefing_includes_macro_leading_section() -> None:
    analysis = _sample_analysis("300750", "宁德时代", "cn_stock", rank=3)
    payload = {
        "generated_at": "2026-03-12 08:30:00",
        "news_report": {
            "items": [
                {
                    "category": "china_market_domestic",
                    "title": "国家电网发布新一轮特高压招标",
                    "source": "CNINFO",
                    "published_at": "2026-03-12 07:20:00",
                    "link": "https://example.com/grid",
                    "freshness_bucket": "fresh",
                },
                {
                    "category": "china_market_domestic",
                    "title": "财政部强调扩大设备更新支持范围",
                    "source": "Reuters",
                    "published_at": "2026-03-12 06:50:00",
                    "link": "https://example.com/policy",
                    "freshness_bucket": "fresh",
                },
            ]
        },
        "headline_lines": ["今天更像结构性行情。", "风险偏好没有全面回暖。", "更适合先看主线确认。"],
        "action_lines": ["先小仓。", "等确认。"],
        "theme_tracking_rows": [["新能源", "产业链订单改善", "景气方向还在。", "中线配置", "需要等量价确认。", "当前更多是背景储备和信息环境支持，不等于直接催化已兑现。"]],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [
            "制造业 PMI 50.1，较前值回升；新订单 50.8、生产 51.7。",
            "PPI 同比 -0.9%，较前值回升；CPI 同比 1.3%，价格环境抬升。",
            "M1-M2 剪刀差 -4.1 个百分点，较前值修复；社融近 3 个月均值约 2.56 万亿元。",
        ],
        "regime": {"current_regime": "recovery", "reasoning": ["PMI 回到 50 上方。", "PPI 跌幅收窄。", "信用脉冲边际修复。"]},
        "day_theme": "中国政策 / 内需确定性",
        "data_coverage": "中国宏观 | Watchlist 行情 | RSS新闻",
        "missing_sources": "跨市场代理",
        "evidence_rows": [
            ["分析生成时间", "2026-03-12 08:30:00"],
            ["A股观察池来源", "Tushare 优先全市场初筛；初筛 `60` 只，完整分析 `8` 只，候选上限 `16` 只。"],
            ["时点边界", "默认只使用生成时点前可见的宏观、新闻、观察池和缓存快照。"],
        ],
        "quality_lines": [
            "本次新闻覆盖源: Reuters / 财联社。",
            "HSTECH 当前使用 `3033.HK` 作为行情代理。",
        ],
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
                "total": 3,
                "confidence_labels": {"中": 1, "高": 1},
                "limitation": "这是价格和量能推导出的情绪代理，不是真实社媒抓取。",
                "downgrade_impact": "更适合提示拥挤线索，不适合单独作为买卖信号。",
            },
        },
        "a_share_watch_meta": {"pool_size": 60, "complete_analysis_size": 8},
        "a_share_watch_candidates": [analysis],
        "a_share_watch_rows": [["1", "宁德时代 (300750)", "新能源", "较强机会", "持有优于追高", "首次建仓 ≤3%"]],
        "a_share_watch_lines": [
            "A 股观察池来自 `Tushare 优先` 的全市场快照；初筛池 `60` 只，完整分析 `8` 只。",
            "这不是对全 A 股逐只深扫，而是全市场初筛后，对通过硬排除的少数样本做完整分析。",
        ],
        "alerts": [],
    }
    rendered = ClientReportRenderer().render_briefing(payload)
    assert "## 先看执行" in rendered
    assert rendered.index("## 先看执行") < rendered.index("## 首页判断")
    assert "| 看不看 | 今天更像结构性行情。 |" in rendered
    assert "| 怎么触发 |" in rendered
    assert "| 多大仓位 | 先小仓。 |" in rendered
    assert "| 哪里止损 | 如果 `新能源` 的验证继续走弱" in rendered
    assert "## 首页判断" in rendered
    assert "### 板块 / 主题认知" in rendered
    assert "今天市场更像结构性轮动日" in rendered
    assert "不把晨报理解成单一板块推荐" in rendered
    assert "## 市场结构摘要" in rendered
    assert "## 今日最重要的判断" not in rendered
    assert "## 今日情报看板" in rendered
    assert "## 宏观判断依据" in rendered
    assert "## 宏观领先指标" in rendered
    assert "## 数据完整度" in rendered
    assert "## 证据时点与来源" in rendered
    assert "## 市场代理信号" in rendered
    assert "## 执行补充" in rendered
    assert "## 今日A股观察池" in rendered
    assert "## A股观察池升级条件" in rendered


def test_render_briefing_surfaces_weekly_and_monthly_rhythm_lines() -> None:
    payload = {
        "generated_at": "2026-03-12 08:30:00",
        "headline_lines": ["主线偏防守。"],
        "action_lines": ["今天先按防守优先处理。"],
        "quality_lines": ["本次新闻覆盖源: Reuters / 财联社。"],
        "domestic_market_lines": [
            "沪深300：周线 近 156周 +12.00%，最近一周 +1.10%，修复中，动能改善。",
            "沪深300：月线 近 36月 +18.00%，最近一月 +4.20%，趋势偏强，动能偏强。",
        ],
        "macro_items": [],
        "a_share_watch_rows": [],
        "a_share_watch_lines": [],
        "portfolio_lines": [],
        "portfolio_table_rows": [],
        "verification_rows": [],
        "theme_tracking_rows": [],
        "notes": [],
        "regime": {"current_regime": "recovery"},
        "day_theme": "宽基修复",
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "## 周月节奏" in rendered
    assert "沪深300：周线" in rendered
    assert "沪深300：月线" in rendered
    assert "周月节奏同向偏强" in rendered


def test_render_briefing_frontloads_market_structure_snapshots() -> None:
    payload = {
        "generated_at": "2026-04-04 08:30:00",
        "headline_lines": ["今天更像结构性轮动，不是全面同涨。"],
        "action_lines": ["先盯市场结构和轮动能否延续。"],
        "quality_lines": ["本次已接入日度市场结构、机构调研和外汇结构信号。"],
        "index_signal_rows": [["上证指数", "3400.00", "+0.20%", "偏强修复", "周线金叉", "月线修复", "常态量能", "等待确认"]],
        "market_signal_rows": [["市场宽度", "上涨 2800 / 下跌 2100", "分歧中性", "涨跌比 1.33"]],
        "rotation_rows": [["行业", "银行(+1.40%)", "半导体(-0.80%)", "防守占优，高低切明显"]],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [],
        "regime": {"current_regime": "recovery"},
        "day_theme": "宽基修复",
        "data_coverage": "中国宏观 | 全市场结构快照",
        "missing_sources": "无",
        "a_share_watch_meta": {"pool_size": 60, "complete_analysis_size": 8},
        "a_share_watch_candidates": [],
        "a_share_watch_rows": [],
        "a_share_watch_lines": [],
        "evidence_rows": [],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "## 市场结构摘要" in rendered
    assert "周月节奏" in rendered
    assert "市场结构" in rendered
    assert "轮动" in rendered
    assert "## 今日情报看板" in rendered
    assert "市场宽度" in rendered
    assert "上证指数" in rendered
    assert "## 证据时点与来源" in rendered
    assert "市场结构快照" in rendered


def test_render_briefing_prioritizes_interactive_ir_and_broker_rows_over_framework_rows() -> None:
    payload = {
        "generated_at": "2026-04-04 08:30:00",
        "headline_lines": ["今天更像结构性轮动，不是全面同涨。"],
        "action_lines": ["先盯市场结构和轮动能否延续。"],
        "quality_lines": ["本次已接入日度市场结构、机构调研和外汇结构信号。"],
        "market_event_rows": [
            [
                "2026-04-04",
                "标准行业框架：药明康德 属于 申万二级行业·化学制药（-1.20%）",
                "申万行业框架",
                "低",
                "化学制药",
                "",
                "行业框架承压",
                "行业指数仍在回落。",
            ],
            [
                "2026-04-04",
                "卖方共识升温：中际旭创 本月获 4 家券商金股推荐",
                "卖方共识专题",
                "中",
                "中际旭创",
                "",
                "卖方共识升温",
                "偏利多，卖方月度金股覆盖开始抬升，但这里只当共识热度参考。",
            ],
            [
                "2026-04-04",
                "互动易确认：公司回复海外订单进展",
                "互动易/投资者关系",
                "中",
                "药明康德",
                "",
                "管理层口径确认",
                "先按补充证据处理，不替代正式公告。",
            ],
        ],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [],
        "regime": {"current_regime": "recovery"},
        "day_theme": "宽基修复",
        "data_coverage": "中国宏观 | 全市场结构快照",
        "missing_sources": "无",
        "a_share_watch_meta": {"pool_size": 60, "complete_analysis_size": 8},
        "a_share_watch_candidates": [],
        "a_share_watch_rows": [],
        "a_share_watch_lines": [],
        "evidence_rows": [],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)
    news_section = rendered.split("## 今日情报看板", 1)[1].split("## 数据完整度", 1)[0]

    assert "互动易确认：公司回复海外订单进展" in news_section
    assert "卖方共识升温：中际旭创 本月获 4 家券商金股推荐" in news_section
    assert "标准行业框架：药明康德 属于 申万二级行业·化学制药（-1.20%）" in news_section
    assert news_section.index("互动易确认：公司回复海外订单进展") < news_section.index("标准行业框架：药明康德 属于 申万二级行业·化学制药（-1.20%）")
    assert news_section.index("卖方共识升温：中际旭创 本月获 4 家券商金股推荐") < news_section.index("标准行业框架：药明康德 属于 申万二级行业·化学制药（-1.20%）")


def test_render_briefing_normalizes_evidence_labels_and_backfills_reason_bullets() -> None:
    payload = {
        "generated_at": "2026-04-02 14:10:00",
        "headline_lines": [
            "今天更像强修复，不是趋势反转确认。",
            "资金在从偏防御、偏资源切回成长弹性方向。",
        ],
        "action_lines": [
            "先盯量能能否继续温和放大。",
            "优先跟踪创新药和 AI 算力能否继续强者恒强。",
        ],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [
            "PMI 回到扩张区间。",
            "新订单继续修复。",
            "信用脉冲边际改善。",
        ],
        "regime": {"current_regime": "recovery", "reasoning": ["PMI 回到 50 上方。", "价格链条边际修复。"]},
        "day_theme": "成长修复",
        "data_coverage": "外部情报 | 主题跟踪 | 观察池方向",
        "missing_sources": "A股观察池完整深扫",
        "evidence_rows": [
            ["生成时间", "2026-04-02 14:10:00"],
            ["A股观察池", "Tushare 优先全市场初筛；当前只保留方向级结论。"],
        ],
        "news_report": {
            "items": [
                {
                    "title": "【焦点复盘】A股放量普涨迎4月“开门红”，AI硬件、创新药概念强势领涨",
                    "source": "财联社",
                    "published_at": "2026-04-01T09:39:04",
                    "link": "https://example.com/cailian",
                    "freshness_bucket": "fresh",
                    "category": "china_market_domestic",
                }
            ]
        },
        "quality_lines": ["外部情报和主题方向均已覆盖。", "观察池本轮按方向级口径输出。"],
        "proxy_contract": {},
        "a_share_watch_meta": {"pool_size": 60, "complete_analysis_size": 8},
        "a_share_watch_candidates": [],
        "a_share_watch_rows": [],
        "a_share_watch_lines": [
            "A 股观察池来自 Tushare 优先的全市场快照。",
            "这不是对全 A 股逐只深扫，而是全市场初筛后再做完整分析。",
        ],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    why_section = rendered.split("## 为什么今天这么判断", 1)[1].split("##", 1)[0]
    assert why_section.count("\n- ") >= 3
    assert "| 分析生成时间 | 2026-04-02 14:10:00 |" in rendered
    assert "| A股观察池来源 | Tushare 优先全市场初筛；当前只保留方向级结论。 |" in rendered
    assert "| 时点边界 | 默认只使用生成时点前可见的宏观、外部新闻和观察池快照。 |" in rendered
    assert "## 重点观察" in rendered


def test_render_briefing_intelligence_board_falls_back_to_market_event_rows() -> None:
    payload = {
        "generated_at": "2026-03-31 08:30:00",
        "headline_lines": ["今天更像黄金避险。"],
        "action_lines": ["先按防守优先处理。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [],
        "regime": {"current_regime": "deflation"},
        "day_theme": "黄金避险",
        "data_coverage": "中国宏观 | Watchlist 行情",
        "missing_sources": "实时RSS新闻",
        "quality_lines": [],
        "market_event_rows": [
            ["待定", "黄金盘前走强，避险需求回升", "—", "高", "黄金/防守"],
            ["待定", "有色板块跟随升温", "—", "中", "有色/资源"],
        ],
        "a_share_watch_meta": {"pool_size": 16, "complete_analysis_size": 9},
        "a_share_watch_candidates": [],
        "a_share_watch_rows": [],
        "a_share_watch_lines": [],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "## 今日情报看板" in rendered
    assert "黄金盘前走强，避险需求回升" in rendered
    assert "信号类型：`主题/市场情报`" in rendered


def test_render_briefing_intelligence_board_uses_market_event_signal_type_and_prioritizes_it() -> None:
    payload = {
        "generated_at": "2026-04-01 08:30:00",
        "headline_lines": ["今天更像A股主线轮动。"],
        "action_lines": ["先看创新药和热股扩散。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [],
        "regime": {"current_regime": "recovery"},
        "day_theme": "A股主线轮动",
        "data_coverage": "中国宏观 | Watchlist 行情",
        "missing_sources": "实时RSS新闻",
        "quality_lines": [],
        "market_event_rows": [
            ["2026-04-01", "A股热股前排：新易盛（+12.50%）", "A股热度/个股", "高", "AI算力/光模块", "", "海外科技映射", "偏利多，先看 `AI算力/光模块` 能否继续拿到价格与成交确认。"],
        ],
        "news_report": {
            "items": [
                {
                    "title": "Global bond investors reassess conflict risks",
                    "source": "Bloomberg",
                    "published_at": "2026-04-01T07:00:00",
                    "link": "https://example.com/bloomberg",
                }
            ]
        },
        "a_share_watch_meta": {"pool_size": 16, "complete_analysis_size": 9},
        "a_share_watch_candidates": [],
        "a_share_watch_rows": [],
        "a_share_watch_lines": [],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "A股热股前排：新易盛" in rendered
    assert "信号类型：`海外科技映射`" in rendered
    assert "结论：偏利多，先看 `AI算力/光模块` 能否继续拿到价格与成交确认。" in rendered
    assert "Global bond investors reassess conflict risks" in rendered
    assert "A股热股前排：新易盛" in rendered


def test_render_briefing_intelligence_board_merges_market_events_and_theme_lines() -> None:
    payload = {
        "generated_at": "2026-03-31 08:30:00",
        "headline_lines": ["今天更像黄金避险。"],
        "action_lines": ["先按防守优先处理。"],
        "theme_tracking_rows": [
            ["高股息/红利", "防守配套", "说明", "防守底仓", "若风险偏好快速修复，红利方向会先跑输弹性资产。", True],
        ],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [],
        "regime": {"current_regime": "deflation"},
        "day_theme": "黄金避险",
        "data_coverage": "中国宏观 | Watchlist 行情",
        "missing_sources": "实时RSS新闻",
        "quality_lines": [],
        "market_event_rows": [
            ["2026-03-31", "黄金盘前走强，避险需求回升", "CNBC", "高", "黄金/防守", "https://example.com/gold"],
        ],
        "a_share_watch_meta": {"pool_size": 16, "complete_analysis_size": 9},
        "a_share_watch_candidates": [],
        "a_share_watch_rows": [],
        "a_share_watch_lines": [],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "[黄金盘前走强，避险需求回升](https://example.com/gold)" in rendered
    assert "高股息/红利" in rendered


def test_render_briefing_intelligence_board_surfaces_news_summary_lines() -> None:
    payload = {
        "generated_at": "2026-04-05 20:00:00",
        "headline_lines": ["今天更像有色链结构修复。"],
        "action_lines": ["先按结构修复和资金承接来理解。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [],
        "regime": {"current_regime": "recovery"},
        "day_theme": "有色链结构修复",
        "data_coverage": "中国宏观 | 市场结构",
        "missing_sources": "",
        "quality_lines": [],
        "market_event_rows": [],
        "news_report": {
            "summary_lines": [
                "主题聚类：价格/供需 2 条，产业/公司 1 条",
                "来源分层：主流媒体 1 条，行业/协会 2 条",
            ],
            "items": [
                {
                    "title": "铜价上行带动有色链活跃",
                    "source": "财联社",
                    "published_at": "2026-04-05T10:00:00",
                    "link": "https://example.com/nonferrous",
                }
            ],
        },
        "a_share_watch_meta": {"pool_size": 12, "complete_analysis_size": 5},
        "a_share_watch_candidates": [],
        "a_share_watch_rows": [],
        "a_share_watch_lines": [],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "情报摘要" in rendered
    assert "这批外部情报主要围绕 价格/供需 2 条，产业/公司 1 条，先看其中哪一条和当前标的或主题最直接。" in rendered


def test_render_briefing_surfaces_client_final_runtime_quality_notes() -> None:
    analysis = _sample_analysis("300750", "宁德时代", "cn_stock", rank=3)
    payload = {
        "generated_at": "2026-03-30 08:30:00",
        "headline_lines": ["今天更像结构性观察日。"],
        "action_lines": ["先看主线确认，再决定是否升级执行优先级。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": ["制造业 PMI 49.8，仍在荣枯线附近。"],
        "regime": {"current_regime": "stagflation", "reasoning": ["PMI 仍未明显走强。"]},
        "day_theme": "高股息 / 宽基",
        "data_coverage": "中国宏观 | Watchlist 行情",
        "missing_sources": "跨市场代理 / 实时RSS新闻",
        "quality_lines": [
            "client-final 默认自动跳过跨市场代理和 market monitor 慢链。",
            "client-final 已把快照超时阈值收紧到 8 秒。",
            "client-final 已切换到轻量新闻源配置。",
        ],
        "a_share_watch_meta": {"pool_size": 40, "complete_analysis_size": 6},
        "a_share_watch_candidates": [analysis],
        "a_share_watch_rows": [["1", "宁德时代 (300750)", "新能源", "观察为主", "等待确认", "首次建仓 ≤3%"]],
        "a_share_watch_lines": ["A 股观察池来自 `Tushare 优先` 的全市场快照；初筛池 `40` 只，完整分析 `6` 只。"],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "## 数据完整度" in rendered
    assert "client-final 默认自动跳过跨市场代理和 market monitor 慢链。" in rendered
    assert "client-final 已把快照超时阈值收紧到 8 秒。" in rendered


def test_render_briefing_includes_what_changed_section(monkeypatch) -> None:
    _install_fake_thesis_repo(monkeypatch)
    analysis = _sample_analysis("300750", "宁德时代", "cn_stock", rank=3)
    payload = {
        "generated_at": "2026-03-12 08:30:00",
        "headline_lines": ["今天更像新能源链回到观察池前列。"],
        "action_lines": ["先看龙头确认，再决定是否升级执行优先级。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": ["制造业 PMI 50.1，较前值回升。"],
        "regime": {"current_regime": "recovery", "reasoning": ["PMI 回到 50 上方。"]},
        "day_theme": "新能源",
        "data_coverage": "中国宏观 | Watchlist 行情 | RSS新闻",
        "missing_sources": "跨市场代理",
        "quality_lines": [],
        "a_share_watch_meta": {"pool_size": 60, "complete_analysis_size": 8},
        "a_share_watch_candidates": [analysis],
        "a_share_watch_rows": [["1", "宁德时代 (300750)", "新能源", "较强机会", "持有优于追高", "首次建仓 ≤3%"]],
        "a_share_watch_lines": ["A 股观察池来自 `Tushare 优先` 的全市场快照；初筛池 `60` 只，完整分析 `8` 只。"],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "## What Changed" in rendered
    assert "上次怎么看：上次还没有可复用的 thesis / 事件记忆" in rendered
    assert "这次什么变了：thesis 里还没有上次事件快照" in rendered
    assert "结论变化：`首次跟踪`" in rendered


def test_render_briefing_sanitizes_intraday_execution_language() -> None:
    payload = {
        "generated_at": "2026-03-13 08:30:00",
        "headline_lines": ["今天更像事件驱动。", "日内优先跟随主线。", "背景框架先放后面。"],
        "action_lines": ["执行节奏: 先观察开盘 30 分钟风格延续性，确认后再执行。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [],
        "data_coverage": "中国宏观",
        "missing_sources": "无",
        "quality_lines": [],
        "a_share_watch_rows": [],
        "a_share_watch_lines": ["A 股观察池来自 `Tushare 优先` 的全市场快照；初筛池 `60` 只，完整分析 `8` 只。"],
        "alerts": ["当前没有触发强提醒，但仍需关注强弱方向是否在盘中发生切换。"],
    }
    rendered = ClientReportRenderer().render_briefing(payload)
    assert "开盘 30 分钟" not in rendered
    assert "盘中" not in rendered
    assert "日内优先" not in rendered
    assert "先观察早段风格延续性" in rendered
    assert "交易时段发生切换" in rendered
    assert "当天优先跟随主线" in rendered


def test_render_briefing_exec_summary_surfaces_theme_boundary() -> None:
    analysis = _sample_analysis("560001", "政策主线ETF", "cn_etf", rank=1)
    analysis["trade_state"] = "观察为主"
    analysis["metadata"] = {"sector": "工业"}
    analysis["notes"] = ["一带一路、新质生产力和中特估一起走强。"]
    payload = {
        "generated_at": "2026-03-13 08:30:00",
        "headline_lines": ["今天更像政策主题内部轮动。"],
        "action_lines": ["先看主线内部先往哪条线收敛。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [],
        "regime": {"current_regime": "recovery"},
        "day_theme": "政策主线",
        "quality_lines": [],
        "a_share_watch_meta": {"pool_size": 60, "complete_analysis_size": 8},
        "a_share_watch_candidates": [analysis],
        "a_share_watch_rows": [],
        "a_share_watch_lines": [],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "| 主题边界 |" in rendered
    assert "还没拉开" in rendered


def test_render_briefing_usage_and_term_translation_sections_frontload_reader_guidance() -> None:
    payload = {
        "generated_at": "2026-03-13 08:30:00",
        "headline_lines": ["今天更像结构性行情，先看主线验证。"],
        "action_lines": ["先看主线验证是否成立，再决定是否提高风险暴露。"],
        "theme_tracking_rows": [["新能源", "量能回流", "主线继续扩散", "中", "冲高回落"]],
        "verification_rows": [["A股", "主线验证", "量能继续放大", "提高风险暴露", "回到防守"]],
        "macro_asset_rows": [],
        "macro_items": [],
        "regime": {"current_regime": "recovery"},
        "day_theme": "硬科技",
        "quality_lines": [],
        "a_share_watch_meta": {"pool_size": 60, "complete_analysis_size": 8},
        "a_share_watch_rows": [],
        "a_share_watch_lines": [],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "## 怎么用这份晨报" in rendered
    assert "## 这几个词怎么读" in rendered
    assert "## 执行补充" not in rendered
    assert "不是直接买入清单" in rendered
    assert "真到执行层，买点、仓位和失效位仍以对应复核卡为准" in rendered


def test_render_briefing_execution_supplement_mirrors_source_action_lines_when_rich_guidance_enabled() -> None:
    payload = {
        "generated_at": "2026-04-22 16:10:00",
        "headline_lines": ["收盘后先复盘主线强弱。", "明天先看算力和创新药谁先继续扩散。", "市场还没到全面 risk-on。"],
        "action_lines": ["明天先看高景气主线能否继续拿到量价确认。", "如果扩散不够，就继续按结构性行情和观察仓处理。"],
        "theme_tracking_rows": [["AI硬件链", "CPO / 光模块承接", "主线弹性仍在", "短线观察", "高位分歧加大", "当前仍有主线背景和信息环境支撑。"]],
        "verification_rows": [["A股", "主线验证", "前排继续放量走强", "提高风险暴露", "回到观察"]],
        "macro_items": ["PMI 维持扩张。"],
        "quality_lines": ["本次新闻覆盖源: 财联社 / 路透。"],
        "data_coverage": "中国宏观 | 主题跟踪 | 观察池",
        "missing_sources": "无",
        "evidence_rows": [["分析生成时间", "2026-04-22 16:10:00"]],
        "a_share_watch_meta": {"pool_size": 24, "complete_analysis_size": 6},
        "a_share_watch_candidates": [],
        "a_share_watch_rows": [],
        "a_share_watch_lines": ["A 股观察池来自 Tushare 优先全市场初筛。", "当前先按观察池管理。"],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)
    execution_section = rendered.split("## 执行补充", 1)[1].split("## 这几个词怎么读", 1)[0]

    assert "明天先看高景气主线能否继续拿到量价确认。" in execution_section
    assert "如果扩散不够，就继续按结构性行情和观察仓处理。" in execution_section
    assert "晨报先负责定节奏" not in execution_section


def test_render_briefing_focus_section_surfaces_theme_boundary_explainer() -> None:
    analysis = _sample_analysis("560001", "政策主线ETF", "cn_etf", rank=1)
    analysis["trade_state"] = "观察为主"
    analysis["metadata"] = {"sector": "工业"}
    analysis["notes"] = ["一带一路、新质生产力和中特估一起走强。"]
    payload = {
        "generated_at": "2026-03-13 08:30:00",
        "headline_lines": ["今天更像政策主题内部轮动。"],
        "action_lines": ["先看主线内部先往哪条线收敛。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [],
        "regime": {"current_regime": "recovery"},
        "day_theme": "政策主线",
        "quality_lines": [],
        "a_share_watch_meta": {"pool_size": 60, "complete_analysis_size": 8},
        "a_share_watch_candidates": [analysis],
        "a_share_watch_rows": [],
        "a_share_watch_lines": [],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "## 重点观察" in rendered
    assert "主题边界：" in rendered


def test_render_briefing_watch_upgrade_lines_surface_strategy_background_confidence() -> None:
    analysis = _sample_analysis("601899", "紫金矿业", "cn_stock", rank=1)
    analysis["trade_state"] = "观察为主"
    analysis["narrative"]["judgment"] = {"state": "观察为主"}
    analysis["strategy_background_confidence"] = {
        "status": "watch",
        "label": "观察",
        "reason": "最近只有 `2` 个可验证样本，暂时不够把它当成稳定策略，只能做辅助说明。",
    }
    payload = {
        "generated_at": "2026-03-29 08:30:00",
        "headline_lines": ["今天更像结构性轮动。"],
        "action_lines": ["先看观察池里谁先补齐确认。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [],
        "regime": {"current_regime": "recovery"},
        "day_theme": "资源轮动",
        "quality_lines": [],
        "a_share_watch_meta": {"pool_size": 60, "complete_analysis_size": 8},
        "a_share_watch_candidates": [analysis],
        "a_share_watch_rows": [],
        "a_share_watch_lines": [],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "## A股观察池升级条件" in rendered
    assert "| 后台置信度 | 后台验证当前只到观察" in rendered
    assert "策略后台置信度只作辅助约束，不替代今天的宏观与主题判断。" in rendered
    assert "后台验证当前只到观察，这次先只作辅助说明，不单靠它升级动作" in rendered


def test_render_briefing_still_surfaces_upgrade_section_when_watch_pool_is_empty() -> None:
    payload = {
        "generated_at": "2026-04-03 13:48:07",
        "headline_lines": ["今天更像先看主线，再决定是否升级风险暴露。"],
        "action_lines": ["先等强势方向和成交确认。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [],
        "regime": {"current_regime": "recovery"},
        "day_theme": "背景宏观",
        "quality_lines": [],
        "data_coverage": "中国宏观 | Watchlist 行情",
        "missing_sources": "A股观察池完整深扫",
        "a_share_watch_meta": {"pool_size": 0, "complete_analysis_size": 0},
        "a_share_watch_candidates": [],
        "a_share_watch_rows": [],
        "a_share_watch_lines": ["A 股全市场观察池拉取超时，本轮先保留已确认的宏观、盘面和外部情报。"],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "## A股观察池升级条件" in rendered
    assert "今天还没有能直接升级成正式动作票的样本" in rendered
    assert "升级要等价格、成交或主线扩散里至少一项先给出更清晰的确认" in rendered


def test_render_briefing_ignores_reuse_only_candidates_in_upgrade_copy() -> None:
    payload = {
        "generated_at": "2026-04-03 13:48:07",
        "headline_lines": ["今天更像先看主线，再决定是否升级风险暴露。"],
        "action_lines": ["先等强势方向和成交确认。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [],
        "regime": {"current_regime": "recovery"},
        "day_theme": "背景宏观",
        "quality_lines": [],
        "data_coverage": "中国宏观 | Watchlist 行情",
        "missing_sources": "A股观察池完整深扫",
        "a_share_watch_meta": {"pool_size": 0, "complete_analysis_size": 0},
        "a_share_watch_candidates": [
            {
                "name": "",
                "symbol": "",
                "briefing_reuse_only": True,
                "market_event_rows": [
                    ["2026-04-03", "标准行业框架：宁德时代 属于 申万二级行业·电池", "申万行业框架", "高", "电池", "", "标准行业归因", "偏利多。"]
                ],
            }
        ],
        "a_share_watch_rows": [],
        "a_share_watch_lines": ["A 股全市场观察池拉取超时，本轮已回退复用今日 stock_pick 已确认的 A 股证据。"],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "今天还没有能直接升级成正式动作票的样本" in rendered
    assert "主要卡在：" not in rendered
    assert "` (`" not in rendered
    assert "** ()**" not in rendered


def test_render_briefing_surfaces_portfolio_overlap_and_holdings_section() -> None:
    payload = {
        "generated_at": "2026-03-30 08:30:00",
        "headline_lines": ["今天更像结构性轮动。"],
        "action_lines": ["先看观察池里谁先补齐确认。"],
        "theme_tracking_rows": [],
        "verification_rows": [],
        "macro_asset_rows": [],
        "macro_items": [],
        "regime": {"current_regime": "recovery"},
        "day_theme": "资源轮动",
        "quality_lines": [],
        "a_share_watch_meta": {"pool_size": 60, "complete_analysis_size": 8},
        "a_share_watch_candidates": [],
        "a_share_watch_rows": [],
        "a_share_watch_lines": [],
        "portfolio_lines": [
            "组合市值约 200000.00 CNY。",
            "组合联动: 行业集中度偏高；暂未看到明显主线冲突",
            "风格与方向: 当前组合风格偏 `防守偏重`，最重风格是 `防守` `55.0%`。",
            "组合优先级: 如果只是同风格加码，优先级低于补新方向。",
        ],
        "portfolio_table_rows": [["561380", "多", "2.100", "2.230", "+6.19%", "电网投资提升", "持有观察"]],
        "alerts": [],
    }

    rendered = ClientReportRenderer().render_briefing(payload)

    assert "## 组合与持仓" in rendered
    assert "组合联动:" in rendered
    assert "风格与方向:" in rendered
    assert "| 标的 | 方向 | 成本 | 现价 | 浮盈亏 | 核心论点 | 当前状态 |" in rendered


def test_pick_client_safe_line_softens_internal_catalyst_and_runtime_tokens() -> None:
    assert "主题检索可能有漏抓" in _pick_client_safe_line("当前新增直接情报偏少，且主题检索疑似漏抓；先不把它写成零催化")
    assert "跨市场代理快线暂未启用" in _pick_client_safe_line("为保证个股详细稿 `client-final` 可交付，本轮自动跳过跨市场代理与 market monitor 慢链。")
    assert "观察提示" in _pick_client_safe_line("当前因子为 observation_only")
