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
        "metadata": {"sector": "电网"},
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
            "technical": {"score": 82, "max_score": 100, "summary": "技术信号偏强。", "core_signal": "MACD 零轴上方金叉 · ADX 36", "factors": [{"name": "MACD 金叉", "display_score": "20/20", "signal": "MACD 零轴上方金叉", "detail": "DIF 0.1 / DEA 0.08"}]},
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


def test_opportunity_renderer_discovery_and_compare():
    analysis_a = _sample_analysis("561380", "电网ETF")
    analysis_b = _sample_analysis("159611", "电力ETF")
    analysis_b["rating"] = {"stars": "⭐⭐", "label": "储备机会", "meaning": "单维度亮灯但未共振。", "rank": 2, "warnings": []}
    payload = {
        "generated_at": "2026-03-09 08:00:00",
        "scan_pool": 20,
        "passed_pool": 8,
        "regime": {"current_regime": "stagflation"},
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "top": [analysis_a],
        "blind_spots": ["全市场 ETF 扫描池拉取失败，已回退到 watchlist"],
    }
    discovery = OpportunityReportRenderer().render_discovery(payload)
    assert "# 每日机会发现 | 2026-03-09" in discovery
    assert "## TOP 1 机会" in discovery
    assert "**八维雷达：**" in discovery
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


def test_opportunity_renderer_compare_uses_total_score_when_rank_ties():
    analysis_a = _sample_analysis("561380", "电网ETF")
    analysis_b = _sample_analysis("512400", "有色ETF")
    analysis_b["dimensions"]["technical"]["score"] = 70
    analysis_b["dimensions"]["relative_strength"]["score"] = 40
    compare = OpportunityReportRenderer().render_compare(
        {"generated_at": "2026-03-09 08:00:00", "analyses": [analysis_a, analysis_b], "best_symbol": "561380"}
    )
    assert "评级相同，但综合八维总分" in compare


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
        "manager": {"name": "任桀", "tenure_days": 495, "aum_billion": 161.72, "best_return_pct": 281.99, "current_fund_count": 2},
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
    assert "### 前十大持仓" in rendered
