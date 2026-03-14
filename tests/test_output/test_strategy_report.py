from __future__ import annotations

from src.output.strategy_report import StrategyReportRenderer


def test_strategy_report_renderer_renders_prediction_sections() -> None:
    payload = {
        "prediction_id": "stratv1_600519_test",
        "status": "predicted",
        "symbol": "600519",
        "name": "贵州茅台",
        "universe": "a_share_liquid_stock_v1",
        "prediction_target": "20d_excess_return_vs_csi800_rank",
        "horizon": {"label": "20个交易日"},
        "as_of": "2026-03-13",
        "effective_from": "2026-03-16",
        "visibility_class": "post_close_t_plus_1_v1",
        "prediction_value": {"summary": "更像 20 日超额收益的上层候选。"},
        "confidence_label": "中",
        "seed_score": 66.2,
        "confidence_type": "rank_confidence_v1",
        "benchmark": {"name": "中证800", "symbol": "000906.SH"},
        "cohort_contract": {"cohort_frequency_days": 5, "holding_period_days": 20},
        "key_factors": [{"label": "技术/趋势", "factor": "technical", "score": 76, "direction": "supportive", "summary": "趋势保持偏强。"}],
        "factor_snapshot": {
            "price_momentum": {"return_5d": 0.03, "return_20d": 0.11, "return_60d": 0.22},
            "technical": {"ma_signal": "bullish", "macd_signal": "bullish", "rsi": 63.0},
            "liquidity": {"avg_turnover_20d": 1.6e8, "median_turnover_60d": 1.5e8},
            "risk": {"volatility_20d": 0.24, "max_drawdown_1y": -0.18},
        },
        "evidence_sources": {
            "market_data_as_of": "2026-03-13",
            "market_data_source": "Tushare 优先日线",
            "benchmark_as_of": "2026-03-13",
            "benchmark_source": "000906.SH 日线历史",
            "catalyst_evidence_as_of": "2026-03-13",
            "catalyst_sources": ["rss", "events"],
            "point_in_time_note": "默认只使用生成时点前可见信息。",
            "notes": [],
        },
        "downgrade_flags": ["intraday_not_used"],
    }

    rendered = StrategyReportRenderer().render_prediction(payload, persisted=True)

    assert "# Strategy Prediction Ledger" in rendered
    assert "## 一句话结论" in rendered
    assert "## 预测合同" in rendered
    assert "## 关键因子" in rendered
    assert "## 因子快照" in rendered
    assert "## 证据时点与来源" in rendered
    assert "## 降级与边界" in rendered
    assert "更像 20 日超额收益的上层候选" in rendered


def test_strategy_report_renderer_renders_no_prediction_reasons() -> None:
    rendered = StrategyReportRenderer().render_prediction(
        {
            "prediction_id": "stratv1_300750_test",
            "status": "no_prediction",
            "symbol": "300750",
            "name": "宁德时代",
            "universe": "a_share_liquid_stock_v1",
            "prediction_target": "20d_excess_return_vs_csi800_rank",
            "horizon": {"label": "20个交易日"},
            "as_of": "2026-03-13",
            "effective_from": "",
            "visibility_class": "degraded_snapshot_only_v1",
            "benchmark": {"name": "中证800", "symbol": "000906.SH"},
            "cohort_contract": {"cohort_frequency_days": 5, "holding_period_days": 20},
            "key_factors": [],
            "factor_snapshot": {"price_momentum": {}, "technical": {}, "liquidity": {}, "risk": {}},
            "evidence_sources": {},
            "downgrade_flags": ["history_fallback_mode"],
            "no_prediction_reasons": ["当前只能拿到降级历史/快照，不满足 strategy v1 的完整日线合同。"],
        },
        persisted=False,
    )

    assert "## 拒绝预测原因" in rendered
    assert "完整日线合同" in rendered


def test_strategy_report_renderer_renders_prediction_list() -> None:
    rendered = StrategyReportRenderer().render_prediction_list(
        [
            {
                "as_of": "2026-03-13",
                "symbol": "600519",
                "status": "predicted",
                "prediction_value": {"expected_rank_bucket": "upper_quintile_candidate"},
                "confidence_label": "中",
                "seed_score": 66.2,
            }
        ]
    )

    assert "| as_of | symbol | status | rank bucket | confidence | score |" in rendered
    assert "`600519`" in rendered
