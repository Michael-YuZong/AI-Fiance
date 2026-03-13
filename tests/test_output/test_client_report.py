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
    assert "## A股" in rendered
    assert "| 标的 | 技术 | 基本面 | 催化 | 相对强弱 | 风险 | 结论 |" in rendered
    assert "为什么能进正式推荐" in rendered
    assert "持有周期：中线配置（1-3月）" in rendered
    assert "为什么按这个周期理解" in rendered
    assert "现在不适合的打法" in rendered
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
    assert "非重叠样本" in rendered
    assert "95%区间" in rendered
    assert "样本质量" in rendered
    assert "持有周期：中线配置（1-3月）" in rendered
    assert "为什么按这个周期理解" in rendered
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
    assert "## 历史相似样本验证" in rendered


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
                },
                "entry": "等回撤再看",
                "position": "计划仓位的 1/3 - 1/2",
                "scaling_plan": "确认后再加",
                "stop": "跌破支撑离场",
            },
            "positioning_lines": ["先小仓。"],
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
        },
        "alternatives": [{"name": "永赢科技智选混合发起C", "symbol": "022365", "cautions": ["节奏不对。"]}],
    }
    rendered = ClientReportRenderer().render_fund_pick(payload)
    assert "今日场外基金推荐" in rendered
    assert "发现方式: 全市场初筛 | 初筛池: 12 | 完整分析: 5" in rendered
    assert "主题过滤: 黄金 | 风格过滤: 商品/黄金 | 管理人过滤: 未指定" in rendered
    assert "## 数据完整度" in rendered
    assert "## 交付等级" in rendered
    assert "标准推荐稿" in rendered
    assert "覆盖率的分母是今天进入完整分析的 `5` 只基金" in rendered
    assert "当前更合适的持有周期：**`中线配置（1-3月）`**" in rendered
    assert "| 持有周期 | 中线配置（1-3月） |" in rendered
    assert "| 为什么按这个周期看 | 基本面、风险收益和趋势至少有两项站得住，更适合按一段完整主线去拿，而不是只博短催化。 |" in rendered
    assert "## 标准化分类" in rendered
    assert "ETF联接" in rendered
    assert "## 跟今天首个快照版相比" in rendered
    assert "## 数据限制与说明" in rendered
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
            "delivery_notes": ["当前流程不是把全市场每只标的都做完整八维深扫，而是先初筛 `9` 只，再对其中 `5` 只做完整分析。", "新闻/事件覆盖存在降级，本次更适合作为观察优先对象，不宜当成强执行型推荐。"],
        },
        "winner": {
            "name": "能源化工ETF",
            "symbol": "159981",
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
                },
                "entry": "等回踩再看",
                "position": "首次建仓 ≤3%",
                "scaling_plan": "分 2-3 批建仓",
                "stop": "跌破支撑离场",
                "target": "先看前高",
            },
            "positioning_lines": ["先小仓。"],
            "evidence": list(analysis["dimensions"]["catalyst"]["evidence"]),
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
        },
        "alternatives": [{"name": "红利ETF", "symbol": "510880", "cautions": ["今天弹性不如能源主线。"]}],
        "notes": ["当前数据源连接不稳定，已按可用数据降级处理。"],
    }
    rendered = ClientReportRenderer().render_etf_pick(payload)
    assert "今日ETF观察" in rendered
    assert "今天先给一个观察优先的 ETF 对象" in rendered
    assert "## 数据完整度" in rendered
    assert "## 交付等级" in rendered
    assert "降级观察稿" in rendered
    assert "## 为什么先看它" in rendered
    assert "## 为什么推荐它" not in rendered
    assert "覆盖率的分母是今天进入完整分析的 `5` 只 ETF" in rendered
    assert "当前更合适的持有周期：**`短线交易（3-10日）`**" in rendered
    assert "| 持有周期 | 短线交易（3-10日） |" in rendered
    assert "现在不适合：现在不适合直接当成长线底仓" in rendered
    assert "## 这只ETF为什么是这个分" in rendered
    assert "## 标准化分类" in rendered
    assert "场内ETF" in rendered
    assert "## 跟今天首个快照版相比" in rendered
    assert "## 关键证据" in rendered
    assert "## 基金画像" in rendered
    assert "## 为什么不是另外几只" in rendered


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
            "delivery_notes": ["当前流程不是把全市场每只标的都做完整八维深扫，而是先初筛 `7` 只，再对其中 `2` 只做完整分析。"],
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
    assert "## 为什么不是另外几只" in rendered
    assert "今天可进入完整评分且未被硬排除的 ETF 候选只有 1 只" in rendered
    assert "只能按观察优先或降级稿处理" in rendered


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
        "data_coverage": "中国宏观 | Watchlist 行情 | RSS新闻",
        "missing_sources": "跨市场代理",
        "quality_lines": [
            "本次新闻覆盖源: Reuters / 财联社。",
            "HSTECH 当前使用 `3033.HK` 作为行情代理。",
        ],
        "a_share_watch_rows": [["1", "宁德时代 (300750)", "新能源", "较强机会", "持有优于追高", "首次建仓 ≤3%"]],
        "a_share_watch_lines": [
            "A 股观察池来自 `Tushare 优先` 的全市场快照；初筛池 `60` 只，完整分析 `8` 只。",
            "这不是对全 A 股逐只深扫，而是全市场初筛后，对通过硬排除的少数样本做完整分析。",
        ],
        "alerts": [],
    }
    rendered = ClientReportRenderer().render_briefing(payload)
    assert "## 宏观领先指标" in rendered
    assert "## 数据完整度" in rendered
    assert "## 今日A股观察池" in rendered
    assert "## 重点观察" in rendered
    assert "PPI 同比 -0.9%" in rendered
    assert "M1-M2 剪刀差" in rendered
    assert "本次覆盖：中国宏观 | Watchlist 行情 | RSS新闻。" in rendered
    assert "当前缺失：跨市场代理。" in rendered
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
