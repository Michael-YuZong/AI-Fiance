from __future__ import annotations

from src.output.retrospect_report import DecisionRetrospectReportRenderer


def test_retrospect_report_renders_calibration_attribution_and_snapshot_sections() -> None:
    payload = {
        "month": "2026-03",
        "generated_at": "2026-03-13 10:00:00",
        "lookahead": 20,
        "stop_pct": 0.08,
        "target_pct": 0.15,
        "summary_lines": ["本次共回看 `1` 笔决策。"],
        "basis_rows": [["rule", "1", "+6.00%", "+3.00%", "100.0%", "0.0%", "100.0%"]],
        "setup_rows": [["高把握", "1", "+6.00%", "+3.00%", "100.0%"]],
        "horizon_rows": [["中线配置（1-3月）", "1", "+6.00%", "+3.00%", "100.0%"]],
        "attribution_rows": [["alpha兑现", "1", "+6.00%", "+3.00%"]],
        "items": [
            {
                "name": "电网ETF",
                "symbol": "561380",
                "action": "buy",
                "basis": "rule",
                "timestamp": "2026-03-10T10:00:00",
                "entry_date": "2026-03-10",
                "entry_price": 2.1,
                "note": "测试",
                "thesis": {"core_assumption": "电网主线", "validation_metric": "订单", "stop_condition": "主线走弱", "holding_period": "1-3月"},
                "reason_lines": ["顺势买入。"],
                "forward_returns": {"1d": "+1.00%", "3d": "+2.00%", "5d": "+4.00%", "20d": "+6.00%"},
                "benchmark_return": 0.03,
                "excess_return": 0.03,
                "mfe": 0.09,
                "mae": -0.02,
                "stop_level": 1.932,
                "target_level": 2.415,
                "first_event": "先触发目标（第 5 个交易日）",
                "coverage_days": 20,
                "signal_alignment": "顺势买入",
                "signal_snapshot": {
                    "ma_signal": "bullish",
                    "macd_signal": "bullish",
                    "rsi": 58.0,
                    "adx": 25.0,
                    "volume_structure": "放量上攻",
                    "return_20d": 0.08,
                    "price_percentile_1y": 0.72,
                },
                "setup_profile": {"bucket": "高把握", "score": 65},
                "horizon": {
                    "label": "中线配置（1-3月）",
                    "fit_reason": "历史 thesis 里写的预期周期是 `1-3月`，这笔交易更应按 `中线配置（1-3月）` 的框架复盘。",
                    "misfit_reason": "现在不适合当成纯隔夜交易，也还没强到可以长期不复核地持有一年以上。",
                    "source": "review_reconstructed_from_thesis",
                },
                "decision_snapshot": {
                    "recorded_at": "2026-03-10T10:00:00",
                    "market_data_as_of": "2026-03-10",
                    "market_data_source": "561380",
                    "history_window": "3y",
                    "thesis_snapshot_at": "2026-03-09 20:00:00",
                    "factor_contract": {
                        "families": {"J-1": 2, "J-3": 1},
                        "states": {"strategy_challenger": 2, "scoring_supportive": 1},
                        "strategy_candidate_factor_ids": ["j1_volume_structure"],
                        "point_in_time_blockers": [],
                    },
                    "notes": ["只使用当时可见日线。"],
                },
                "execution_snapshot": {
                    "execution_mode": "场内成交",
                    "tradability_label": "可成交",
                    "estimated_total_cost": 12.5,
                    "liquidity_note": "参与率可控。",
                    "execution_note": "未含极端行情冲击。",
                },
                "verdict": {"outcome": "结果兑现", "summary": "顺势决策且后验结果匹配。", "detail": "窗口内兑现。"},
                "attribution": {"label": "alpha兑现", "detail": "明显跑赢基准。"},
                "thesis_is_historical": True,
            }
        ],
    }

    markdown = DecisionRetrospectReportRenderer().render(payload)

    assert "## Setup 校准" in markdown
    assert "## 周期校准" in markdown
    assert "## 结果归因" in markdown
    assert "### 周期判断" in markdown
    assert "### 时点与执行快照" in markdown
    assert "### 因子合同快照" in markdown
    assert "同区基准20日" in markdown
    assert "20日超额" in markdown
    assert "alpha兑现" in markdown
