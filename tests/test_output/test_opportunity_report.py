"""Tests for opportunity report rendering."""

from __future__ import annotations

from src.output.opportunity_report import OpportunityReportRenderer


def _sample_analysis(symbol: str, name: str) -> dict:
    return {
        "symbol": symbol,
        "name": name,
        "generated_at": "2026-03-09 08:00:00",
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
        },
        "risks": ["⚠️ 已进入超买区，追高性价比下降"],
        "notes": ["该标的已在 watchlist 中，本次更偏复核。"],
    }


def test_opportunity_renderer_scan_sections():
    rendered = OpportunityReportRenderer().render_scan(_sample_analysis("561380", "电网ETF"))
    assert "# 电网ETF (561380) 全景分析 | 2026-03-09" in rendered
    assert "## 一句话结论" in rendered
    assert "## 硬性检查" in rendered
    assert "## 八维详细分析" in rendered
    assert "### 技术面 82/100" in rendered
    assert "## 综合评级" in rendered
    assert "## 操作建议" in rendered
    assert "## 风险提示" in rendered


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


def test_opportunity_renderer_compare_uses_total_score_when_rank_ties():
    analysis_a = _sample_analysis("561380", "电网ETF")
    analysis_b = _sample_analysis("512400", "有色ETF")
    analysis_b["dimensions"]["technical"]["score"] = 70
    analysis_b["dimensions"]["relative_strength"]["score"] = 40
    compare = OpportunityReportRenderer().render_compare(
        {"generated_at": "2026-03-09 08:00:00", "analyses": [analysis_a, analysis_b], "best_symbol": "561380"}
    )
    assert "评级相同，但综合八维总分" in compare
