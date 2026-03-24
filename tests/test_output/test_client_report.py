from src.output.client_report import ClientReportRenderer, _fund_profile_sections


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
    assert "**先看结论：** 先看短线/中线和低门槛入口" in rendered
    assert "## 今日动作摘要" in rendered
    assert "| 报告定位 | 推荐稿 |" in rendered
    assert "| 空仓怎么做 |" in rendered
    assert "| 持仓怎么做 |" in rendered
    assert "| 首次仓位 | 单票 `2% - 5%` 试仓 |" in rendered
    assert "## A股" in rendered
    assert "| 标的 | 技术 | 基本面 | 催化 | 相对强弱 | 风险 | 结论 |" in rendered
    assert "### 第一批：核心主线" in rendered
    assert "为什么能进正式推荐" in rendered
    assert "持有周期：中线配置（1-3月）" in rendered
    assert "portfolio whatif buy 300502" in rendered
    assert "为什么按这个周期理解" in rendered
    assert "现在不适合的打法" in rendered
    assert "建议买入区间：9.850 - 10.000" in rendered
    assert "建议减仓区间：11.200 - 11.500" in rendered
    assert "## 仓位管理" in rendered


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
    assert "## 今日结论" in rendered
    assert "## 今日动作摘要" in rendered
    assert "| 当前框架 | `stagflation` / `能源冲击 + 地缘风险` |" in rendered
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
    assert "高置信直连催化" in rendered
    assert "下一步怎么盯：" in rendered
    assert "## 历史相似样本验证" in rendered
    assert "非重叠样本" in rendered
    assert "样本质量" in rendered
    assert "关键盯盘价位" in rendered


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
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "market_label": "A股",
        "top": [watch_one, watch_two],
        "watch_positive": [watch_one, watch_two],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "### 观察触发器" in rendered
    assert "## 正式动作阈值" in rendered
    assert "先看能不能从 `观察为主` 升到 `看好但暂不推荐`" in rendered
    assert "只要当前动作仍是 `暂不出手 / 观察为主 / 先按观察仓`，就还不算正式动作票" in rendered
    assert "| 层次 | 标的 | 为什么继续看 | 主要卡点 | 升级条件 | 关键盯盘价位 |" in rendered
    assert "关键盯盘价位" in rendered
    assert "### 看好但暂不推荐" not in rendered
    assert "## 观察名单代表样本详细拆解" not in rendered
    assert rendered.count("**先看结论：**") <= 6
    assert "## 仓位纪律" in rendered
    assert "相对强弱：更像主题方向没坏，但个股右侧扩散和价格确认还没完成" in rendered


def test_render_stock_picks_detailed_downgrades_observe_only_title_and_history_heading() -> None:
    analysis = _sample_analysis("002241", "歌尔股份", "cn_stock", rank=1)
    analysis["action"]["direction"] = "回避"
    analysis["action"]["position"] = "暂不出手"
    analysis["action"]["entry"] = "等回踩确认前先别急着动手。"
    analysis["metrics"] = {"last_close": 23.4}
    analysis["metadata"] = {"sector": "人工智能", "chain_nodes": ["AI算力"]}
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
    assert "当前主线更像在支撑方向不删，还不够支撑价格与动量确认都已完成" in rendered
    assert "### 第一批：优先观察" in rendered
    assert "### 观察触发器" in rendered
    assert "## 历史相似样本附注" in rendered
    assert "样本给的是边界，不是免确认通行证" in rendered
    assert "## 历史相似样本验证" not in rendered
    assert "#### 低门槛可执行" not in rendered
    assert "## 观察名单代表样本详细拆解" not in rendered


def test_render_stock_picks_detailed_leads_with_structured_event_coverage_explanation() -> None:
    analysis = _sample_analysis("000975", "山金国际", "cn_stock", rank=1)
    payload = {
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "recovery"},
        "market_label": "A股",
        "stock_pick_coverage": {
            "note": "本轮新闻/事件覆盖基本正常。",
            "lines": ["A股 结构化事件覆盖 100%（3/3） / 高置信公司新闻覆盖 0%（0/3）"],
        },
        "top": [analysis],
        "coverage_analyses": [analysis],
        "watch_positive": [analysis],
    }

    rendered = ClientReportRenderer().render_stock_picks_detailed(payload)

    assert "当前证据更偏结构化事件与公告日历，不是新闻催化型驱动。" in rendered
    assert "0%` 只代表没命中高置信个股直连新闻" in rendered


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

    assert "当前这份稿更像 `黄金` 主题内的相对排序，不是跨主题分散候选池。" in rendered
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
    assert "### 第二批：观察替代" in rendered
    assert "#### 关联ETF观察" in rendered
    assert "短线先看" not in rendered
    assert "#### 低门槛可执行" not in rendered


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
    assert "| 建议减仓区间 | 11.200 - 11.500 |" in rendered
    assert "## 仓位管理" in rendered
    assert "## 组合落单前" in rendered
    assert "portfolio whatif buy 561380" in rendered
    assert "## 分维度详解" in rendered
    assert "**历史相似样本附注：**" in rendered
    assert "只作边界附注" in rendered
    assert "| 相对强弱基准 | 沪深300ETF (510300) |" in rendered


def test_render_scan_observe_trigger_uses_trading_language_template() -> None:
    analysis = _sample_analysis("601857", "中国石油", "cn_stock", rank=1)
    analysis["action"]["entry"] = "当前缺少完整日线历史，先按观察仓处理；更适合等补齐日线后再确认趋势。"
    analysis["action"]["buy_range"] = ""
    analysis["action"]["stop_ref"] = 7.86
    analysis["action"]["target_ref"] = 8.52
    analysis["dimensions"]["relative_strength"]["score"] = 10
    rendered = ClientReportRenderer().render_scan(analysis)
    assert "| 触发买点条件 | 先等补齐日线并确认 MA20 / MA60 拐头，再看相对强弱转正；触发前先别急着给精确买入价。 |" in rendered
    assert "| 关键盯盘价位 | 下沿先看 `7.860` 上方能不能稳住；上沿先看 `8.520` 附近能不能放量突破。 |" in rendered
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
    assert "## 图表速览" in rendered
    assert "## 当前更合适的动作" in rendered


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
    assert "今日场外基金推荐" in rendered
    assert "## 执行摘要" in rendered
    assert "| 当前建议 | 继续持有；持有优于追高 |" in rendered
    assert "| 交付等级 | 标准推荐稿 |" in rendered
    assert "| 空仓怎么做 |" in rendered
    assert "| 持仓怎么做 |" in rendered
    assert "| 主要利好 |" in rendered
    assert "| 主要利空 |" in rendered
    assert "如果按今天的申赎决策只看一只场外基金，我给" in rendered
    assert "发现方式: 全市场初筛 | 初筛池: 12 | 完整分析: 5" in rendered
    assert "主题过滤: 黄金 | 风格过滤: 商品/黄金 | 管理人过滤: 未指定" in rendered
    assert "## 数据完整度" in rendered
    assert "## 交付等级" in rendered
    assert "标准推荐稿" in rendered
    assert "覆盖率的分母是今天进入完整分析的 `5` 只基金" in rendered
    assert "当前更合适的持有周期：**`中线配置（1-3月）`**" in rendered
    assert "## 图表速览" in rendered
    assert "![分析看板](/tmp/fund_dashboard.png)" in rendered
    assert "| 持有周期 | 中线配置（1-3月） |" in rendered
    assert "| 为什么按这个周期看 | 基本面、风险收益和趋势至少有两项站得住，更适合按一段完整主线去拿，而不是只博短催化。 |" in rendered
    assert "| 配置视角 | 如果按配置视角理解，更适合作为中期主题暴露分批持有，不适合一次打满。 |" in rendered
    assert "| 交易视角 | 如果按交易视角理解，更适合等回踩承接后的再确认，不必天天换仓。 |" in rendered
    assert "| 建议买入区间 | 1.960 - 2.010 |" in rendered
    assert "| 建议减仓区间 | 2.180 - 2.240 |" in rendered
    assert "| 预演命令 | `portfolio whatif buy 021740 最新净值 计划金额` |" in rendered
    assert "## 组合落单前" in rendered
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
    assert "当前方向还在，但更像保留申赎观察资格" in rendered
    assert "## 正式动作阈值" in rendered
    assert "只要当前结论仍是 `观察为主`，就不把它写成正式申赎动作" in rendered
    assert "## 升级条件" in rendered
    assert "- 升级条件：" in rendered
    assert "## 当前只看什么" in rendered
    assert "| 触发买点条件 | 等覆盖率恢复后再看；触发前先别急着给精确买入价。 |" in rendered
    assert "| 关键盯盘价位 | 下沿先看 `1.880` 上方能不能稳住；上沿先看 `2.060` 附近能不能放量突破。 |" in rendered
    assert "## 标准化分类" in rendered
    assert "## 关键证据" in rendered
    assert "## 为什么不是另外几只" in rendered
    assert "基金画像覆盖不足" in rendered
    assert "建议买入区间" not in rendered


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

    assert "当前证据更偏结构化事件、基金画像和持仓/基准映射，不是直接新闻催化型驱动。" in rendered


def test_render_etf_pick_has_fund_profile_and_alternatives() -> None:
    analysis = _sample_analysis("159981", "能源化工ETF", "cn_etf", rank=3)
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
    assert "今日ETF观察" in rendered
    assert "当前方向还在，但更像保留观察资格" in rendered
    assert "## 正式动作阈值" in rendered
    assert "只要当前结论仍是 `观望偏多`，就不把它写成正式交易动作" in rendered
    assert "## 执行摘要" in rendered
    assert "| 短线优先 | 能源化工ETF (159981)；当前更强的是催化、相对强弱和执行节奏，优势主要集中在接下来几个交易日到一两周内。 |" in rendered
    assert "| 中线优先 | 红利ETF (510880)；更适合围绕一段完整主线分批拿，不只是看一两天波动。 |" in rendered
    assert "| 当前建议 | 观望偏多 |" in rendered
    assert "| 交付等级 | 降级观察稿 |" in rendered
    assert "| 空仓怎么做 |" in rendered
    assert "| 持仓怎么做 |" in rendered
    assert "如果按今天剩余交易时段的计划一定要给执行入口，我会分成两档：短线先看：`能源化工ETF`；中线先看：`红利ETF`" in rendered
    assert "这份建议的适用时段：" in rendered
    assert "## 数据完整度" in rendered
    assert "## 交付等级" in rendered
    assert "## 代理信号与限制" in rendered
    assert "真实社媒抓取" in rendered
    assert "降级观察稿" in rendered
    assert "## 当前分层建议" in rendered
    assert "| 短线优先 | 能源化工ETF (159981) | 短线交易（3-10日） |" in rendered
    assert "## 为什么先看它" in rendered
    assert "## 为什么推荐它" not in rendered
    assert "## 升级条件" in rendered
    assert "- 为什么还不升级：" in rendered
    assert "- 升级条件：" in rendered
    assert "## 图表速览" in rendered
    assert "![分析看板](/tmp/etf_dashboard.png)" in rendered
    assert "覆盖率的分母是今天进入完整分析的 `5` 只 ETF" in rendered
    assert "当前更合适的持有周期：**`短线交易（3-10日）`**" in rendered
    assert "| 持有周期 | 短线交易（3-10日） |" in rendered
    assert "| 触发买点条件 | 先等回踩关键支撑不破；如果触发，再优先看 `0.820 - 0.845` 一带的承接。 |" in rendered
    assert "| 关键盯盘价位 | 回踩先看 `0.820 - 0.845` 一带的承接；如果反弹延续，再看 `0.920 - 0.960` 一带的承压。 |" in rendered
    assert "现在不适合：现在不适合直接当成长线底仓" in rendered
    assert "| 配置视角 | 如果按配置视角理解，这条方向已经有一定配置价值，但更适合分批跟踪，不适合一次打满。 |" in rendered
    assert "| 交易视角 | 如果按交易视角理解，更看催化兑现、右侧确认和止损纪律，优先等回踩或放量确认，不追当天情绪。 |" in rendered
    assert "| 建议买入区间 | 0.820 - 0.845 |" not in rendered
    assert "| 建议减仓区间 | 0.920 - 0.960 |" not in rendered
    assert "| 预演命令 | `portfolio whatif buy 159981 最新价 计划金额` |" not in rendered
    assert "## 组合落单前" not in rendered
    assert "## 这只ETF为什么是这个分" in rendered
    assert "## 标准化分类" in rendered
    assert "场内ETF" in rendered
    assert "## 跟今天首个快照版相比" in rendered
    assert "## 关键证据" in rendered
    assert "## 证据时点与来源" in rendered
    assert "## 基金画像" in rendered
    assert "### 基金经理补充" not in rendered
    assert "经理画像" in rendered
    assert "朱金钰、亢豆" in rendered
    assert "### 基金公司补充" in rendered


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

    assert "当前证据更偏结构化事件、产品画像和持仓/基准映射，不是直接新闻催化型驱动。" in rendered


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
    assert "如果按下一个交易日的计划只能先给一档，我先看：短线先看：`港股创新药ETF`" in rendered
    assert "## 升级条件" in rendered
    assert "## 当前只看什么" in rendered
    assert "| 短线优先 | 港股创新药ETF (513120)；当前信号还没共振到足以支撑正式动作，先观察更稳妥。 |" in rendered
    assert "| 触发买点条件 | 先等 MA20 向上拐头；触发前先别急着给精确买入价。 |" in rendered
    assert "关键盯盘价位" not in rendered
    assert "## 标准化分类" in rendered
    assert "## 关键证据" in rendered
    assert "## 为什么不是另外几只" in rendered
    assert "建议买入区间" not in rendered
    assert "摘要版交付" in rendered


def test_render_etf_pick_single_standard_candidate_does_not_self_downgrade() -> None:
    analysis = _sample_analysis("159981", "能源化工ETF", "cn_etf", rank=3)
    payload = {
        "generated_at": "2026-03-13 15:00:00",
        "selection_context": {
            "discovery_mode_label": "Tushare 全市场快照",
            "scan_pool": 30,
            "passed_pool": 3,
            "theme_filter_label": "未指定",
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
    assert "1.803 - 1.814" not in rendered


def test_render_briefing_includes_macro_leading_section() -> None:
    analysis = _sample_analysis("300750", "宁德时代", "cn_stock", rank=3)
    payload = {
        "generated_at": "2026-03-12 08:30:00",
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
    assert "## 执行摘要" in rendered
    assert "## 宏观判断依据" in rendered
    assert "## 宏观领先指标" in rendered
    assert "## 数据完整度" in rendered
    assert "## 证据时点与来源" in rendered
    assert "## 市场代理信号" in rendered
    assert "## 今日A股观察池" in rendered
    assert "## A股观察池升级条件" in rendered
    assert "## 重点观察" in rendered
    assert "PPI 同比 -0.9%" in rendered
    assert "M1-M2 剪刀差" in rendered
    assert "本次覆盖：中国宏观 | Watchlist 行情 | RSS新闻。" in rendered
    assert "当前缺失：跨市场代理。" in rendered
    assert "直接催化" in rendered
    assert "信息环境" in rendered
    assert "宁德时代 (300750)" in rendered


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
