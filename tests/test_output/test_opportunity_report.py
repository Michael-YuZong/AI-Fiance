"""Tests for opportunity report rendering."""

from __future__ import annotations

import pandas as pd

from src.output.opportunity_report import OpportunityReportRenderer


def _sample_analysis(symbol: str, name: str) -> dict:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-10-01", periods=80, freq="B"),
            "open": [1.0 + i * 0.005 for i in range(80)],
            "high": [1.02 + i * 0.005 for i in range(80)],
            "low": [0.98 + i * 0.005 for i in range(80)],
            "close": [1.0 + i * 0.005 for i in range(80)],
            "volume": [10_000_000 + i * 1_000 for i in range(80)],
            "amount": [20_000_000 + i * 2_000 for i in range(80)],
        }
    )
    benchmark_history = pd.DataFrame(
        {
            "date": pd.date_range("2025-10-01", periods=80, freq="B"),
            "open": [0.98 + i * 0.003 for i in range(80)],
            "high": [1.0 + i * 0.003 for i in range(80)],
            "low": [0.96 + i * 0.003 for i in range(80)],
            "close": [0.98 + i * 0.003 for i in range(80)],
            "volume": [9_000_000 + i * 800 for i in range(80)],
            "amount": [18_000_000 + i * 1_500 for i in range(80)],
        }
    )
    return {
        "symbol": symbol,
        "name": name,
        "asset_type": "cn_etf",
        "generated_at": "2026-03-09 08:00:00",
        "regime": {"current_regime": "stagflation"},
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "metadata": {"sector": "电网", "history_source": "akshare", "history_source_label": "AKShare 日线回退"},
        "history": history,
        "benchmark_symbol": "000300",
        "benchmark_name": "沪深300ETF",
        "benchmark_history": benchmark_history,
        "metrics": {"return_5d": 0.032, "return_20d": 0.081},
        "rating": {"stars": "⭐⭐⭐", "label": "较强机会", "meaning": "逻辑成立，但还需要一个维度继续确认。", "rank": 3, "warnings": ["⚠️ 已进入超买区，追高性价比下降"]},
        "conclusion": "技术到位 + 催化渐强，但估值代理偏中性。",
        "hard_checks": [
            {"name": "流动性", "status": "✅", "detail": "日均成交 1.20 亿"},
            {"name": "估值极端", "status": "⚠️", "detail": "价格位置代理分位 92%"},
        ],
        "dimensions": {
            "technical": {"score": 82, "max_score": 100, "summary": "技术信号偏强。", "core_signal": "MACD 零轴上方金叉 · ADX 36", "factors": [{"name": "MACD 金叉", "display_score": "20/20", "signal": "MACD 零轴上方金叉", "detail": "DIF 0.1 / DEA 0.08"}, {"name": "假突破识别", "display_score": "0/8", "signal": "未识别到明确假突破形态", "detail": "假突破是多空双方试探失败的信号", "factor_id": "j1_false_break"}, {"name": "压缩启动", "display_score": "0/10", "signal": "量价压缩状态中性", "detail": "压缩后放量启动是最干净的介入 setup", "factor_id": "j1_compression_breakout"}]},
            "fundamental": {"score": 61, "max_score": 100, "summary": "估值代理中性偏正面。", "core_signal": "价格位置代理 28%", "factors": [{"name": "估值代理分位", "display_score": "25/25", "signal": "价格位置代理 28%", "detail": "当前用价格位置代理"}]},
            "catalyst": {"score": 70, "max_score": 100, "summary": "催化偏强。", "core_signal": "政策催化 · 海外映射", "factors": [{"name": "政策催化", "display_score": "30/30", "signal": "电网投资政策", "detail": "近 7 日政策落地"}]},
            "relative_strength": {"score": 65, "max_score": 100, "summary": "轮动有改善。", "core_signal": "超额拐点", "factors": [{"name": "超额拐点", "display_score": "30/30", "signal": "相对基准 5日 +3.2%", "detail": "从负转正"}]},
            "chips": {"score": None, "max_score": 100, "summary": "ℹ️ 筹码结构数据缺失，本次评级未纳入该维度", "core_signal": "当前没有明确亮点", "factors": [{"name": "北向/南向", "display_score": "缺失", "signal": "缺失", "detail": "数据缺失"}]},
            "risk": {"score": 58, "max_score": 100, "summary": "风险可控。", "core_signal": "回撤分位", "factors": [{"name": "回撤分位", "display_score": "30/30", "signal": "当前回撤 18%", "detail": "历史 75% 分位"}]},
            "seasonality": {"score": 42, "max_score": 100, "summary": "时间窗口一般。", "core_signal": "月度胜率", "factors": [{"name": "月度胜率", "display_score": "10/30", "signal": "同月胜率 55%", "detail": "中性"}]},
            "macro": {"score": 30, "max_score": 40, "summary": "宏观大体顺风。", "core_signal": "rate 顺风 · usd 顺风", "factors": [{"name": "敏感度向量", "display_score": "30/40", "signal": "rate 顺风 / usd 顺风", "detail": "四因子匹配"}]},
        },
        "action": {
            "direction": "做多",
            "entry": "等回踩 MA20/MA60 后企稳",
            "position": "首次建仓 ≤5%",
            "stop": "跌破 2.10 重评",
            "target": "先看前高 2.35",
            "timeframe": "中线配置(1-3月)",
            "max_portfolio_exposure": "单标的 ≤10%",
            "scaling_plan": "分 2-3 批建仓，每次确认后加仓",
            "stop_loss_pct": "-8%",
            "correlated_warning": "",
        },
        "narrative": {
            "headline": "这是一个**中期偏多，但短线略有拥挤**的标的。当前核心不是没逻辑，而是**逻辑仍在但当前位置赔率一般**。",
            "judgment": {"direction": "中性偏多", "cycle": "中期(1-3月)", "odds": "中", "state": "持有优于追高"},
            "drivers": {
                "macro": "宏观和主线背景整体偏顺风。",
                "flow": "资金面没有特别强，但也没有显著背离。",
                "relative": "相对强弱仍占优。",
                "technical": "技术结构仍完整，但需要继续确认。",
            },
            "contradiction": "中期逻辑偏正面，但短线位置已经不算低，因此更适合等节奏而不是直接追价。",
            "positives": ["相对强弱仍占优。", "宏观环境对该方向没有明显逆风。", "中期趋势结构没有破坏。"],
            "cautions": ["价格位置不低。", "催化需要继续验证。", "短线有一定拥挤风险。"],
            "phase": {"label": "强势整理", "body": "说明大方向未坏，但短线已经不在最舒服的位置。"},
            "risk_points": {
                "fundamental": "行业景气如果低于预期，会先打掉估值支撑。",
                "valuation": "当前价格已经反映一部分预期。",
                "crowding": "一旦共识撤退，回撤会变快。",
                "external": "利率和风险偏好变化会直接影响定价。",
            },
            "watch_points": ["观察动能是否重新修复。", "观察价格是否守住关键支撑。", "观察资金是否重新共振。", "观察主线变量是否继续强化。"],
            "validation_points": [
                {
                    "watch": "动能重启",
                    "judge": "MACD 金叉且收盘站回 MA20",
                    "bull": "说明趋势确认增强。",
                    "bear": "说明仍需继续观察。",
                }
            ],
            "scenarios": {
                "base": "更可能维持强势整理。",
                "bull": "若催化与资金同步强化，有望转成趋势加速。",
                "bear": "若支撑失守，先处理回撤风险。",
            },
            "playbook": {
                "trend": "等动能重新转强后再跟随。",
                "allocation": "可小仓分批，不宜追价。",
                "defensive": "先观察，等更舒服的位置。",
            },
            "summary_lines": ["核心逻辑仍在。", "短期制约在于位置与节奏。", "更合理的动作是持有优于追高。"],
        },
        "risks": ["⚠️ 已进入超买区，追高性价比下降"],
        "notes": ["该标的已在 watchlist 中，本次更偏复核。"],
        "technical_raw": {
            "rsi": {"RSI": 63.2},
            "dmi": {"ADX": 28.4},
            "ma_system": {"mas": {"MA20": 1.31, "MA60": 1.19}},
            "fibonacci": {"levels": {"0.500": 1.24, "0.618": 1.28}},
        },
    }


def test_opportunity_renderer_scan_sections():
    rendered = OpportunityReportRenderer().render_scan(_sample_analysis("561380", "电网ETF"))
    assert "# 电网ETF (561380) 全景分析 | 2026-03-09" in rendered
    assert "## 一句话结论" in rendered
    assert "## 当前判断" in rendered
    assert "## 硬性检查" in rendered
    assert "## 核心矛盾" in rendered
    assert "## 八维评分" in rendered
    assert "## 核心驱动" in rendered
    assert "## 值得继续看的理由" in rendered
    assert "## 现在不适合激进的理由" in rendered
    assert "## 分维度详解" in rendered
    assert "## 情景分析" in rendered
    assert "## 后续验证点" in rendered
    assert "## 操作建议" in rendered
    assert "## 分析元数据" in rendered
    assert "### 技术面 82/100" in rendered
    assert "## 风险提示" in rendered
    assert "| 维度 | 判断 | 说明 |" in rendered
    assert "| 维度 | 得分 | 一句话判断 | 详情 |" in rendered
    assert "| 因子 | 当前值/信号 | 说明 | 得分 |" in rendered
    assert "为什么还不升级" in rendered
    assert "升级条件" in rendered
    assert "行情 as_of" in rendered
    assert "行情来源" in rendered
    assert "AKShare 日线回退" in rendered
    assert "Yahoo" not in rendered
    assert "催化证据 as_of" in rendered
    assert "时点边界" in rendered
    assert "当前图形标签：" in rendered
    assert "技术上先看" in rendered
    assert "未命中显式日期" not in rendered
    assert "日期未单独披露" in rendered


def test_opportunity_renderer_scan_surfaces_strategy_background_confidence_in_followup_section() -> None:
    analysis = _sample_analysis("561380", "电网ETF")
    analysis["strategy_background_confidence"] = {
        "status": "stable",
        "label": "稳定",
        "reason": "最近验证仍稳定，先作辅助加分。",
    }

    rendered = OpportunityReportRenderer().render_scan(analysis)

    assert "## 后续观察重点" in rendered
    assert "- 后台置信度：稳定：" in rendered
    assert "只作辅助加分，不单独替代当前事实层" in rendered


def test_opportunity_renderer_scan_hides_missing_perf_values_and_maps_check_status():
    analysis = _sample_analysis("159698", "粮食ETF")
    analysis["fund_profile"] = {
        "overview": {
            "基金简称": "粮食ETF",
            "基金类型": "股票型 / 被动指数型",
            "基金管理人": "鹏华基金",
            "基金经理人": "陈龙",
            "业绩比较基准": "国证粮食产业指数收益率",
        },
        "style": {
            "summary": "这只基金更像在买`农业`方向的被动暴露，当前标签是 `农业主题 / 被动跟踪`。",
            "tags": ["农业主题", "被动跟踪"],
            "positioning": "这类基金更看跟踪标的暴露、跟踪误差和申赎效率，不以基金经理主观择时择股为核心。",
            "selection": "核心不是基金经理主动选股，而是跟踪 `农业` 暴露及其对应基准。",
            "consistency": "这类产品更重要的是跟踪误差、费率和标的暴露是否清晰，基金经理风格漂移不是核心变量。",
            "benchmark_note": "国证粮食产业指数收益率",
        },
        "achievement": {
            "近1月": {"return_pct": 2.7, "max_drawdown_pct": None, "peer_rank": "428/3956"},
        },
    }

    rendered = OpportunityReportRenderer().render_scan(analysis)
    assert "nan%" not in rendered
    assert "| 流动性 | 通过 | 日均成交 1.20 亿 |" in rendered
    assert "| 估值极端 | 警示 | 价格位置代理分位 92% |" in rendered


def test_opportunity_renderer_scan_rewrites_watch_headline_and_splits_catalyst_layers() -> None:
    analysis = _sample_analysis("159698", "粮食ETF")
    analysis["rating"] = {"rank": 0, "label": "无信号", "stars": "—", "warnings": []}
    analysis["dimensions"]["technical"]["score"] = 28
    analysis["dimensions"]["technical"]["summary"] = "技术结构仍偏弱，暂不支持激进介入。"
    analysis["dimensions"]["catalyst"] = {
        "score": 23,
        "max_score": 100,
        "summary": "催化不足，当前更像静态博弈。",
        "core_signal": "相关头条 7 条 · 覆盖源 4 个",
        "factors": [
            {"name": "政策催化", "display_score": "0/30", "signal": "近 7 日未命中直接政策催化", "detail": "政策原文和一级媒体优先"},
            {"name": "龙头公告/业绩", "display_score": "0/25", "signal": "未命中直接龙头公告", "detail": "优先看订单、扩产、回购、并购或超预期业绩"},
            {"name": "产品/跟踪方向催化", "display_score": "12/12", "signal": "粮食ETF涨超1%", "detail": "当前命中跟踪基准 / 行业暴露"},
            {"name": "研报/新闻密度", "display_score": "10/10", "signal": "相关头条 7 条", "detail": "个股直接提及的一级媒体头条密度"},
            {"name": "新闻热度", "display_score": "10/10", "signal": "覆盖源 4 个", "detail": "从少量提及到多源同步，是热度拐点的代理"},
        ],
    }
    analysis["dimensions"]["seasonality"] = {
        "score": 19,
        "max_score": 100,
        "summary": "时间窗口不占优，更多靠主线和技术本身。",
        "core_signal": "同月胜率 0%（3 年样本）",
        "factors": [
            {"name": "月度胜率", "display_score": "-15/25", "signal": "同月胜率 0%（3 年样本）", "detail": "样本 3 年（2024–2026）"},
        ],
    }

    rendered = OpportunityReportRenderer().render_scan(analysis)

    assert "**— 中期逻辑未坏，短线暂无信号**" in rendered
    assert "技术面 `28/100` 且催化面 `23/100`" in rendered
    assert "| 直接催化 | 偏弱 |" in rendered
    assert "| 舆情/信息环境 | 偏强 |" in rendered
    assert "直接催化偏弱，舆情关注度尚可，因此当前更像静态博弈。" in rendered
    assert "当前样本偏薄，只作辅助参考，不作为主结论依据。" in rendered


def test_opportunity_renderer_scan_observe_only_hides_execution_template() -> None:
    analysis = _sample_analysis("563360", "A500ETF华泰柏瑞")
    analysis["delivery_observe_only"] = True
    analysis["action"]["direction"] = "回避"
    analysis["narrative"]["judgment"]["state"] = "观察为主"

    rendered = OpportunityReportRenderer().render_scan(analysis)

    assert "# A500ETF华泰柏瑞 (563360) 观察型详细分析 | 2026-03-09" in rendered
    assert "这份稿当前按观察型详细分析理解，先不展开正式仓位、止损和目标模板。" in rendered
    assert "## 观察方式" in rendered
    assert "- 观察重点：" in rendered
    assert "| 仓位 |" not in rendered
    assert "| 止损 |" not in rendered
    assert "| 目标 |" not in rendered
    assert "| 适合谁 |" not in rendered
    assert "## 操作建议" not in rendered


def test_opportunity_renderer_scan_auto_observe_only_from_action_state() -> None:
    analysis = _sample_analysis("512480", "半导体ETF")
    analysis.pop("delivery_observe_only", None)
    analysis["action"]["direction"] = "回避"
    analysis["action"]["position"] = "暂不出手"
    analysis["narrative"]["judgment"]["state"] = "观察为主"

    rendered = OpportunityReportRenderer().render_scan(analysis)

    assert "# 半导体ETF (512480) 观察型详细分析 | 2026-03-09" in rendered
    assert "## 观察方式" in rendered
    assert "## 操作建议" not in rendered
    assert "这份稿当前按观察型详细分析理解，先不展开正式仓位、止损和目标模板。" in rendered
    assert "| 仓位 |" not in rendered
    assert "| 止损 |" not in rendered


def test_opportunity_renderer_backfills_cautions_when_narrative_is_too_short():
    analysis = _sample_analysis("520840", "港股通恒生科技ETF")
    analysis["narrative"]["cautions"] = ["短线动能还需要确认。"]
    rendered = OpportunityReportRenderer().render_scan(analysis)
    section = rendered.split("## 现在不适合激进的理由", 1)[1].split("## 所处阶段", 1)[0]
    assert section.count("- ") >= 2


def test_opportunity_renderer_scan_visual_section():
    rendered = OpportunityReportRenderer().render_scan(
        _sample_analysis("561380", "电网ETF"),
        visuals={
            "dashboard": "/tmp/demo_dashboard.png",
            "windows": "/tmp/demo_windows.png",
            "indicators": "/tmp/demo_indicators.png",
        },
    )
    assert "## 图表速览" in rendered
    assert "### 总览看板" in rendered
    assert "### 阶段走势" in rendered
    assert "### 技术指标总览" in rendered
    assert "![分析看板](/tmp/demo_dashboard.png)" in rendered
    assert "![阶段走势](/tmp/demo_windows.png)" in rendered
    assert "![技术指标](/tmp/demo_indicators.png)" in rendered


def test_opportunity_renderer_scan_visual_section_skips_snapshot_fallback():
    rendered = OpportunityReportRenderer().render_scan(
        _sample_analysis("561380", "电网ETF"),
        visuals={
            "dashboard": "/tmp/demo_snapshot.png",
            "mode": "snapshot_fallback",
            "note": "完整日线历史当前不可用，阶段走势和技术指标图已关闭；这里只展示实时快照降级卡。",
        },
    )
    assert "## 图表速览" not in rendered
    assert "### 降级快照卡" not in rendered
    assert "### 阶段走势" not in rendered
    assert "### 技术指标总览" not in rendered


def test_opportunity_renderer_scan_estimated_notes_strip_trailing_punctuation() -> None:
    analysis = _sample_analysis("561380", "电网ETF")
    analysis["notes"] = [
        "全球代理数据已按运行配置关闭，本次先按国内宏观与本地行情上下文生成。",
        "为保证单标的扫描稿 `client-final` 可交付，本轮自动跳过跨市场代理与 market monitor 慢链。",
    ]

    rendered = OpportunityReportRenderer().render_scan(analysis)

    assert "。；" not in rendered
    assert "全球代理数据已按运行配置关闭，本次先按国内宏观与本地行情上下文生成；为保证单标的扫描稿 `client-final` 可交付，本轮自动跳过跨市场代理与 market monitor 慢链" in rendered


def test_opportunity_renderer_scan_uses_dimension_display_names() -> None:
    analysis = _sample_analysis("561380", "电网ETF")
    analysis["dimensions"]["fundamental"]["display_name"] = "产品质量/基本面代理"
    analysis["dimensions"]["chips"]["display_name"] = "筹码结构（辅助项）"

    rendered = OpportunityReportRenderer().render_scan(analysis)

    assert "| 产品质量/基本面代理 | 61/100 |" in rendered
    assert "| 筹码结构（辅助项） | 辅助项 |" in rendered


def test_opportunity_renderer_discovery_and_compare():
    analysis_a = _sample_analysis("561380", "电网ETF")
    analysis_b = _sample_analysis("159611", "电力ETF")
    analysis_b["rating"] = {"stars": "⭐⭐", "label": "储备机会", "meaning": "单维度亮灯但未共振。", "rank": 2, "warnings": []}
    analysis_a["discovery"] = {
        "bucket": "next_step",
        "driver_type": "主线驱动",
        "horizon_label": "中线",
        "today_reason_lines": ["今天把它捞出来，首先是因为电网方向仍在主线里。", "价格和催化至少有两项在发光。"],
        "next_step_reason": "主线、催化和价格结构至少有两项共振，已经够资格进入下一步候选。",
        "blockers": ["discover 当前只是 pre-screen 入口，还没经过 ETF pick 的同池排序、回看和发布门禁。", "短线位置不算低。"],
        "next_steps": [
            {"command": "python -m src.commands.scan 561380", "reason": "先展开单标的八维分析。"},
            {"command": "python -m src.commands.etf_pick 电网", "reason": "放回同主题 ETF 池里正式排序。"},
            {"command": "python -m src.commands.fund_pick --theme 电网", "reason": "如果要把场外基金一起筛。"},
        ],
        "data_notes": ["催化面存在降级，当前更多依赖结构化事件。"],
    }
    analysis_b["discovery"] = {
        "bucket": "observe",
        "driver_type": "趋势驱动",
        "horizon_label": "观察期",
        "today_reason_lines": ["今天把它捞出来，主要因为走势没有完全坏掉。"],
        "next_step_reason": "它现在更像观察发现：有一条线索在亮，但还不足以直接进入正式 pick。",
        "blockers": ["催化不足。"],
        "next_steps": [{"command": "继续观察", "reason": "先盯验证点。"}],
        "data_notes": [],
    }
    payload = {
        "generated_at": "2026-03-09 08:00:00",
        "scan_pool": 20,
        "passed_pool": 8,
        "regime": {"current_regime": "stagflation"},
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "theme_filter": "电网",
        "discovery_mode": "mixed_pool",
        "pool_summary": {
            "boundary_note": "当前 discover 只是 pre-screen 入口。",
            "scan_scope_note": "当前只扫描 ETF 候选池。",
            "mode_label": "全市场 + watchlist 混合池",
            "summary_lines": ["本轮最终进入分析 `20` 只 ETF。"],
            "source_rows": [["Tushare 全市场 ETF 快照", "18"], ["watchlist 回退池", "2"]],
            "sector_rows": [["电网", "6"], ["黄金", "4"]],
            "filter_rules": ["池构建阶段会先排掉债券/货币/REIT/低成交额产品。", "本轮额外应用主题过滤 `电网`。"],
        },
        "data_coverage": {
            "summary": "结构化事件覆盖 5/8，高置信直接新闻覆盖 2/8。",
            "news_mode": "proxy",
            "note": "当前催化/事件覆盖存在降级，discovery 更适合作为发现线索。",
        },
        "top": [analysis_a],
        "ready_candidates": [analysis_a],
        "observation_candidates": [analysis_b],
        "blind_spots": ["全市场 ETF 扫描池拉取失败，已回退到 watchlist"],
    }
    discovery = OpportunityReportRenderer().render_discovery(payload)
    assert "# 每日发现入口 | 2026-03-09" in discovery
    assert "## 这轮 discover 在做什么" in discovery
    assert "## 已足够进入下一步 pick / deep scan 的候选" in discovery
    assert "## 只是值得继续观察的发现" in discovery
    assert "发现类型: `主线驱动`" in discovery
    assert "组合落单前怎么预演" in discovery
    assert "portfolio whatif buy 561380" in discovery
    assert "python -m src.commands.etf_pick 电网" in discovery
    assert "python -m src.commands.fund_pick --theme 电网" in discovery
    assert "## discover 之后怎么接" in discovery
    assert "## 数据盲区与降级说明" in discovery

    compare = OpportunityReportRenderer().render_compare(
        {"generated_at": "2026-03-09 08:00:00", "analyses": [analysis_a, analysis_b], "best_symbol": "561380"}
    )
    assert "# 561380 vs 159611 对比分析 | 2026-03-09" in compare
    assert "## 八维对比" in compare
    assert "## 核心差异" in compare
    assert "## 场景化建议" in compare


def test_opportunity_renderer_stock_picks_includes_explainability_sections():
    analysis = _sample_analysis("00700.HK", "腾讯控股")
    analysis["asset_type"] = "hk"
    analysis["previous_snapshot_at"] = "2026-03-09 20:00:00"
    analysis["score_changes"] = [
        {
            "dimension": "catalyst",
            "label": "催化面",
            "previous": 55,
            "current": 75,
            "delta": 20,
            "reason": "海外映射 `0/20` -> `20/20`；负面事件 从 `-15` 变为 `信息项`",
        }
    ]
    payload = {
        "generated_at": "2026-03-10 08:00:00",
        "scan_pool": 20,
        "passed_pool": 8,
        "market_label": "港股",
        "regime": {"current_regime": "stagflation"},
        "day_theme": {"label": "AI / 半导体催化"},
        "top": [analysis],
        "model_version": "stock-pick-2026-03-10-daily-baseline-v1",
        "baseline_snapshot_at": "2026-03-10 08:00:00",
        "is_daily_baseline": False,
        "comparison_basis_label": "当日基准版",
        "comparison_basis_at": "2026-03-10 08:00:00",
        "model_changelog": ["A 股估值口径统一为 `PE_TTM`。"],
        "watch_positive": [
            {
                **analysis,
                "symbol": "300750",
                "name": "宁德时代",
                "asset_type": "cn_stock",
                "rating": {"rank": 0, "label": "无信号", "stars": "—"},
                "dimensions": {
                    **analysis["dimensions"],
                    "technical": {**analysis["dimensions"]["technical"], "score": 38},
                    "fundamental": {**analysis["dimensions"]["fundamental"], "score": 75},
                    "catalyst": {**analysis["dimensions"]["catalyst"], "score": 35},
                    "relative_strength": {**analysis["dimensions"]["relative_strength"], "score": 55},
                    "risk": {**analysis["dimensions"]["risk"], "score": 65},
                },
            }
        ],
    }
    rendered = OpportunityReportRenderer().render_stock_picks(payload)
    assert "# 个股精选 TOP 1 | 2026-03-10" in rendered
    assert "模型版本" in rendered
    assert "当日基准版" in rendered
    assert "当前输出角色: 当日修正版" in rendered
    assert "## 本版口径变更" in rendered
    assert "分数变动对比基准" in rendered
    assert "**分数变化：**" in rendered
    assert "**催化拆解：**" in rendered
    assert "| 催化子项 | 层级 | 当前信号 | 得分 |" in rendered
    assert "## 看好但暂不推荐" in rendered
    assert "宁德时代 (300750)" in rendered
    assert "| 维度 | 分数变化 | 主要原因 |" in rendered
    assert "**硬排除检查：**" in rendered
    assert "| 检查项 | 状态 | 说明 |" in rendered
    assert "**风险拆解：** 当前风险分 `58/100`" in rendered
    assert "| 风险子项 | 当前信号 | 说明 | 得分 |" in rendered
    # Step 2: 分维度详解折叠区包含新因子
    assert "<details>" in rendered
    assert "分维度详解" in rendered
    assert "#### 技术面 82/100" in rendered
    assert "假突破识别" in rendered
    assert "压缩启动" in rendered
    assert "#### 基本面 61/100" in rendered
    assert "#### 季节/日历 42/100" in rendered
    assert "月度胜率" in rendered
    assert "</details>" in rendered


def test_opportunity_renderer_stock_picks_keeps_degraded_history_contract_rows():
    analysis = _sample_analysis("600989", "宝丰能源")
    analysis["asset_type"] = "cn_stock"
    analysis["signal_confidence"] = {
        "available": False,
        "reason": "当前用了历史降级快照，不能在低置信历史上继续推导相似样本统计。",
    }
    payload = {
        "generated_at": "2026-03-10 08:00:00",
        "scan_pool": 12,
        "passed_pool": 4,
        "market_label": "A股",
        "regime": {"current_regime": "stagflation"},
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "top": [analysis],
        "watch_positive": [analysis],
    }
    rendered = OpportunityReportRenderer().render_stock_picks(payload)
    assert "非重叠样本" in rendered
    assert "20日胜率区间" in rendered
    assert "样本质量" in rendered


def test_opportunity_renderer_stock_picks_downgrades_observe_only_packaging() -> None:
    analysis = _sample_analysis("300308", "中际旭创")
    analysis["asset_type"] = "cn_stock"
    analysis["rating"]["rank"] = 1
    analysis["rating"]["label"] = "无信号"
    analysis["rating"]["stars"] = "—"
    analysis["action"].update(
        {
            "direction": "观察为主",
            "position": "暂不出手",
            "entry": "等量价重新确认后再看",
            "stop": "跌破关键支撑重评",
            "target": "先看前高",
        }
    )
    payload = {
        "generated_at": "2026-03-10 08:00:00",
        "scan_pool": 12,
        "passed_pool": 2,
        "market_label": "A股",
        "regime": {"current_regime": "stagflation"},
        "day_theme": {"label": "AI算力"},
        "top": [analysis],
        "watch_positive": [analysis],
    }

    rendered = OpportunityReportRenderer().render_stock_picks(payload)

    assert "# 个股观察池 TOP 1 | 2026-03-10" in rendered
    assert "**观察重点：**" in rendered
    assert "- 当前是观察稿，先不前置精确仓位、止损和目标模板。" in rendered
    assert "## 继续观察名单" in rendered
    assert "- 建议止损：" not in rendered
    assert "- 目标参考：" not in rendered


def test_opportunity_renderer_compare_uses_total_score_when_rank_ties():
    analysis_a = _sample_analysis("561380", "电网ETF")
    analysis_b = _sample_analysis("512400", "有色ETF")
    analysis_b["dimensions"]["technical"]["score"] = 70
    analysis_b["dimensions"]["relative_strength"]["score"] = 40
    compare = OpportunityReportRenderer().render_compare(
        {"generated_at": "2026-03-09 08:00:00", "analyses": [analysis_a, analysis_b], "best_symbol": "561380"}
    )
    assert "评级相同，但综合八维总分" in compare


def test_opportunity_renderer_compare_supports_multiple_analyses():
    analysis_a = _sample_analysis("561380", "电网ETF")
    analysis_b = _sample_analysis("512400", "有色ETF")
    analysis_c = _sample_analysis("QQQM", "纳指ETF")
    analysis_b["rating"] = {"stars": "⭐⭐", "label": "储备机会", "meaning": "催化偏弱。", "rank": 2, "warnings": []}
    analysis_b["dimensions"]["catalyst"]["score"] = 38
    analysis_c["dimensions"]["technical"]["score"] = 88
    analysis_c["dimensions"]["risk"]["score"] = 45
    analysis_c["dimensions"]["macro"]["score"] = 34

    compare = OpportunityReportRenderer().render_compare(
        {
            "generated_at": "2026-03-09 08:00:00",
            "analyses": [analysis_a, analysis_b, analysis_c],
            "best_symbol": "QQQM",
        }
    )

    assert "# 561380 vs 512400 vs QQQM 对比分析 | 2026-03-09" in compare
    assert "## 综合排序" in compare
    assert "| 维度 | 561380 | 512400 | QQQM | 优势方 |" in compare
    assert "纳指ETF (QQQM)" in compare
    assert "如果你想优先押催化弹性" in compare


def test_opportunity_renderer_compare_includes_etf_product_layer_table():
    analysis_a = _sample_analysis("563360", "A500ETF华泰柏瑞")
    analysis_b = _sample_analysis("512400", "有色ETF")
    analysis_a["fund_profile"] = {
        "overview": {"ETF基准指数中文全称": "中证A500指数"},
        "etf_snapshot": {
            "share_change_text": "净创设 +2.58亿份 (+0.84%)，较 2026-04-01",
            "share_as_of": "2026-04-02",
            "total_share": 3_087_198.74,
        },
        "fund_factor_snapshot": {
            "trend_label": "趋势偏强",
            "momentum_label": "动能改善",
            "latest_date": "2026-04-02",
        },
    }
    analysis_a["news_report"] = {
        "items": [{"title": "A500权重调整后资金回流", "source": "财联社", "link": "https://example.com/a500"}]
    }
    analysis_b["fund_profile"] = {
        "overview": {"ETF基准指数中文全称": "中证申万有色金属指数"},
        "etf_snapshot": {
            "share_as_of": "2026-04-02",
            "total_share": 1_200_000.00,
        },
        "fund_factor_snapshot": {
            "trend_label": "震荡",
            "momentum_label": "动能偏弱",
            "latest_date": "2026-04-02",
        },
    }
    analysis_b["dimensions"]["catalyst"]["coverage"] = {"degraded": True}

    compare = OpportunityReportRenderer().render_compare(
        {
            "generated_at": "2026-04-02 08:00:00",
            "analyses": [analysis_a, analysis_b],
            "best_symbol": "563360",
        }
    )

    assert "## ETF产品层对比" in compare
    assert "| 标的 | 跟踪指数 | 最近份额变化 | 场内基金技术状态 | 外部情报 |" in compare
    assert "中证A500指数" in compare
    assert "已接上 1 条可点击外部情报" in compare
    assert "截至 2026-04-02 仅有单日快照" in compare
    assert "当前外部情报仍偏薄" in compare


def test_opportunity_renderer_compare_surfaces_strategy_confidence_and_linkage_summary():
    analysis_a = _sample_analysis("561380", "电网ETF")
    analysis_b = _sample_analysis("159611", "电力ETF")
    analysis_a["strategy_background_confidence"] = {
        "status": "stable",
        "label": "稳定",
        "reason": "最近验证仍稳定，先作辅助加分。",
    }
    analysis_b["strategy_background_confidence"] = {
        "status": "watch",
        "label": "观察",
        "reason": "最近样本偏少，只能作辅助说明。",
    }
    compare = OpportunityReportRenderer().render_compare(
        {
            "generated_at": "2026-03-09 08:00:00",
            "analyses": [analysis_a, analysis_b],
            "best_symbol": "561380",
            "compare_linkage_summary": {
                "overlap_label": "同一行业主线对比",
                "summary_line": "这组候选本质上都在比较 `电网` 这条线里的内部优先级。",
                "region_line": "地区上都在 `CN`，地域分散度有限。",
                "style_summary_line": "风格上都偏 `进攻`，更适合比较执行节奏。",
                "style_priority_hint": "如果只选一个，优先看谁的确认条件更完整。",
                "detail_lines": [
                    "主线分布: `561380` 电网 / `159611` 电网",
                    "地区分布: `561380` CN / `159611` CN",
                    "风格分布: `561380` 进攻 / `159611` 进攻",
                ],
            },
        }
    )

    assert "## 后台置信度" in compare
    assert "| 标的 | 状态 | 说明 |" in compare
    assert "稳定" in compare
    assert "观察" in compare
    assert "## 主线与风格联动" in compare
    assert "重复度: `同一行业主线对比`" in compare
    assert "- 如果只选一个，优先看谁的确认条件更完整。" in compare
    assert "主线分布:" in compare


def test_opportunity_renderer_includes_fund_profile_sections():
    analysis = _sample_analysis("022365", "永赢科技智选混合发起C")
    analysis["asset_type"] = "cn_fund"
    analysis["fund_profile"] = {
        "overview": {
            "基金类型": "混合型-偏股",
            "基金管理人": "永赢基金",
            "基金经理人": "任桀",
            "净资产规模": "107.79亿元（截止至：2025年12月31日）",
            "成立日期/规模": "2024年10月30日 / 0.103亿份",
            "业绩比较基准": "中国战略新兴产业成份指数收益率*70%+恒生科技指数收益率*10%",
        },
        "achievement": {
            "近1月": {"return_pct": -1.06, "max_drawdown_pct": 6.8, "peer_rank": "968/5198"},
            "今年以来": {"return_pct": -1.36, "max_drawdown_pct": 6.8, "peer_rank": "3780/5246"},
        },
        "asset_allocation": [{"资产类型": "股票", "仓位占比": 80.34}, {"资产类型": "现金", "仓位占比": 18.91}],
        "top_holdings": [{"股票代码": "300738", "股票名称": "奥飞数据", "占净值比例": 9.30, "持仓市值": 2338.66, "季度": "2025年1季度股票投资明细"}],
        "industry_allocation": [{"行业类别": "科技", "占净值比例": 45.2, "截止时间": "2025-12-31"}],
        "manager": {"name": "任桀", "tenure_days": 495, "aum_billion": 161.72, "best_return_pct": 281.99, "current_fund_count": 2, "begin_date": "2024-10-30", "ann_date": "2026-01-10", "education": "硕士", "nationality": "中国"},
        "company": {"short_name": "永赢", "province": "上海", "city": "上海", "general_manager": "王某", "website": "https://example.com"},
        "dividends": {"rows": [{"ann_date": "2025-12-20", "ex_date": "2025-12-25", "pay_date": "2025-12-26", "div_cash": 0.12, "progress": "实施"}]},
        "rating": {"five_star_count": 1, "morningstar": 3, "shanghai": 4, "zhaoshang": 5, "jiaan": 4},
        "style": {
            "tags": ["科技主题", "高仓位主动", "高集中选股"],
            "summary": "这只基金更像在买科技方向的主动选股框架。",
            "positioning": "股票仓位高。",
            "selection": "前五大重仓合计较高。",
            "consistency": "风格一致性较强。",
            "benchmark_note": "基准偏科技成长。",
        },
        "notes": ["基金评级缺失"],
    }
    rendered = OpportunityReportRenderer().render_scan(analysis)
    assert "## 基金画像" in rendered
    assert "## 基金成分分析" in rendered
    assert "## 基金经理风格分析" in rendered
    assert "### 经理任职补充" not in rendered
    assert "任桀 · 从业 495 天 · 在管规模 161.72 亿 · 最佳回报 281.99% · 任职起点 2024-10-30 · 硕士 / 中国" in rendered
    assert "### 基金公司补充" in rendered
    assert "### 分红记录" in rendered
    assert "### 前十大持仓" in rendered


def test_opportunity_renderer_scan_surfaces_weekly_and_monthly_horizon_lines():
    analysis = _sample_analysis("510300", "华泰柏瑞沪深300ETF")
    analysis["index_topic_bundle"] = {
        "index_snapshot": {"index_name": "沪深300"},
        "history_snapshots": {
            "weekly": {
                "status": "matched",
                "summary": "近 154周 +7.70%，最近一周 -1.37%，修复中，动能承压",
                "trend_label": "修复中",
                "momentum_label": "动能承压",
                "latest_date": "2026-04-03",
            },
            "monthly": {
                "status": "matched",
                "summary": "近 96月 +18.45%，最近一月 -5.53%，修复中，动能偏弱",
                "trend_label": "修复中",
                "momentum_label": "动能偏弱",
                "latest_date": "2026-04-03",
            },
        },
    }

    rendered = OpportunityReportRenderer().render_scan(analysis)

    assert "## 周月节奏" in rendered
    assert "周线：沪深300" in rendered
    assert "月线：沪深300" in rendered
    assert "周月节奏同向偏强" in rendered


def test_opportunity_renderer_includes_etf_profile_and_intraday_sections():
    analysis = _sample_analysis("159819", "人工智能ETF易方达")
    analysis["fund_profile"] = {
        "overview": {
            "基金类型": "指数型-股票",
            "基金管理人": "易方达基金",
            "基金经理人": "张湛",
            "净资产规模": "217.96亿元（截止至：2025年12月31日）",
            "成立日期/规模": "2020年07月27日 / 61.552亿份",
            "业绩比较基准": "中证人工智能主题指数收益率",
            "ETF类型": "境内",
            "交易所": "SZ",
            "ETF基准指数中文全称": "中证人工智能主题指数",
            "ETF基准指数代码": "930713.CSI",
            "ETF总份额": "615520.00万份",
            "ETF总规模": "2179600.00万元",
            "ETF份额规模日期": "2026-03-31",
        },
        "asset_allocation": [{"资产类型": "股票", "仓位占比": 99.83}, {"资产类型": "现金", "仓位占比": 0.40}],
        "top_holdings": [{"股票代码": "300308", "股票名称": "中际旭创", "占净值比例": 10.57, "持仓市值": 230284.94, "季度": "2025年4季度股票投资明细"}],
        "industry_allocation": [{"行业类别": "信息技术", "占净值比例": 88.0, "截止时间": "2025-12-31"}],
        "manager": {"name": "张湛", "tenure_days": 2160, "aum_billion": 1313.31, "best_return_pct": 57.16, "current_fund_count": 26, "begin_date": "2020-07-27", "ann_date": "2026-01-05", "education": "硕士", "nationality": "中国"},
        "company": {"short_name": "易方达", "province": "广东", "city": "广州", "general_manager": "刘某", "website": "https://example.com"},
        "dividends": {"rows": [{"ann_date": "2025-10-18", "ex_date": "2025-10-22", "pay_date": "2025-10-24", "div_cash": 0.05, "progress": "实施"}]},
        "rating": {},
        "fund_factor_snapshot": {
            "trend_label": "趋势偏强",
            "momentum_label": "动能改善",
            "latest_date": "2026-04-01",
        },
        "style": {
            "tags": ["科技主题", "被动跟踪"],
            "summary": "这只 ETF 更像在买人工智能方向的被动暴露。",
            "positioning": "股票仓位接近满仓。",
            "selection": "核心看跟踪指数和前十大权重，不看基金经理主观择时。",
            "consistency": "更重要的是跟踪误差和标的暴露是否清晰。",
            "benchmark_note": "中证人工智能主题指数收益率",
        },
        "notes": [],
    }
    analysis["intraday"] = {
        "enabled": True,
        "fallback_mode": False,
        "current": 1.554,
        "open": 1.572,
        "high": 1.587,
        "low": 1.553,
        "vwap": 1.566,
        "range_position": 0.03,
        "change_vs_prev_close": -0.0108,
        "change_vs_open": -0.0115,
        "trend": "偏弱",
        "commentary": "盘中价格弱于 VWAP 且靠近日内低位，更像承接不足。",
    }
    rendered = OpportunityReportRenderer().render_scan(analysis)
    assert "## 今日盘中视角" in rendered
    assert "盘中价格弱于 VWAP" in rendered
    assert "## 基金画像" in rendered
    assert "### ETF专用信息" in rendered
    assert "930713.CSI" in rendered
    assert "趋势偏强 / 动能改善（2026-04-01）" in rendered
    assert "### 经理任职补充" not in rendered
    assert "张湛 · 从业 2160 天 · 在管规模 1313.31 亿 · 最佳回报 57.16% · 任职起点 2020-07-27 · 硕士 / 中国" in rendered
    assert "### 基金公司补充" in rendered
    assert "### 分红记录" in rendered
    assert "中证人工智能主题指数收益率" in rendered


def test_opportunity_renderer_labels_single_day_share_snapshot_as_observation_not_flow():
    analysis = _sample_analysis("563360", "A500ETF华泰柏瑞")
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
            "share_as_of": "2026-04-01",
            "total_share": 3087198.74,
            "total_share_yi": 308.719874,
        },
        "style": {},
    }

    rendered = OpportunityReportRenderer().render_scan(analysis)

    assert "不能据此写成净创设/净赎回" in rendered
    assert "不能据此写成规模扩张/收缩" in rendered
