from __future__ import annotations

import pandas as pd

import src.processors.strategy as strategy_module
from src.processors.factor_meta import factor_meta_payload
from src.processors.strategy import (
    attribute_strategy_rows,
    build_strategy_prediction_from_analysis,
    generate_strategy_experiment,
    generate_strategy_multi_symbol_replay_predictions,
    generate_strategy_replay_predictions,
    validate_strategy_rows,
)


def _history(days: int = 260, *, amount: float = 1.6e8, start: str = "2025-01-01") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.bdate_range(start, periods=days),
            "open": [10.0 + index * 0.01 for index in range(days)],
            "high": [10.2 + index * 0.01 for index in range(days)],
            "low": [9.8 + index * 0.01 for index in range(days)],
            "close": [10.1 + index * 0.01 for index in range(days)],
            "volume": [20_000_000.0 for _ in range(days)],
            "amount": [amount for _ in range(days)],
        }
    )


def _analysis(asset_type: str = "cn_stock", *, amount: float = 1.6e8, history_fallback: bool = False) -> dict:
    return {
        "symbol": "600519",
        "name": "贵州茅台",
        "asset_type": asset_type,
        "history": _history(amount=amount),
        "history_fallback_mode": history_fallback,
        "metrics": {
            "return_5d": 0.03,
            "return_20d": 0.11,
            "return_60d": 0.22,
            "price_percentile_1y": 0.82,
            "avg_turnover_20d": amount,
            "volatility_20d": 0.24,
            "max_drawdown_1y": -0.18,
        },
        "technical_raw": {
            "ma_system": {"signal": "bullish"},
            "macd": {"signal": "bullish"},
            "rsi": {"RSI": 63.0},
            "volume": {"vol_ratio": 1.3},
        },
        "dimensions": {
            "technical": {
                "score": 76,
                "summary": "趋势保持偏强。",
                "factors": [
                    {
                        "name": "量价结构",
                        "signal": "放量突破",
                        "display_score": "15/15",
                        "factor_id": "j1_volume_structure",
                        "factor_meta": factor_meta_payload("j1_volume_structure"),
                    }
                ],
            },
            "relative_strength": {
                "score": 73,
                "summary": "相对强弱领先。",
                "factors": [
                    {
                        "name": "超额拐点",
                        "signal": "20日超额为正",
                        "display_score": "20/30",
                        "factor_id": "j3_benchmark_relative",
                        "factor_meta": factor_meta_payload("j3_benchmark_relative"),
                    }
                ],
            },
            "catalyst": {"score": 61, "summary": "事件支持仍在。"},
            "fundamental": {
                "score": 69,
                "summary": "基本面质量较稳。",
                "factors": [
                    {
                        "name": "盈利动量",
                        "signal": "观察提示",
                        "display_score": "观察提示",
                        "factor_id": "j4_earnings_momentum",
                        "factor_meta": factor_meta_payload(
                            "j4_earnings_momentum",
                            overrides={"degraded": True, "degraded_reason": "缺少 EPS point-in-time 源"},
                        ),
                    }
                ],
            },
            "risk": {"score": 66, "summary": "风险收益比尚可。"},
            "macro": {"score": 55, "summary": "宏观不逆风。"},
            "seasonality": {
                "score": 51,
                "summary": "季节性中性略正。",
                "factors": [
                    {
                        "name": "政策事件窗",
                        "signal": "观察提示：政策窗口",
                        "display_score": "观察提示",
                        "factor_id": "j2_policy_event",
                        "factor_meta": factor_meta_payload(
                            "j2_policy_event",
                            overrides={"degraded": True, "degraded_reason": "lag / visibility fixture incomplete"},
                        ),
                    }
                ],
            },
            "chips": {"score": 48, "summary": "筹码并不算最优。"},
        },
        "regime": {"label": "recovery", "summary": "风险偏好修复。"},
        "day_theme": {"label": "高景气主线"},
        "provenance": {
            "market_data_as_of": "2026-03-13",
            "market_data_source": "Tushare 优先日线",
            "catalyst_evidence_as_of": "2026-03-13",
            "catalyst_sources": ["rss", "events"],
            "point_in_time_note": "默认只使用生成时点前可见信息。",
            "notes": [],
        },
    }


def test_build_strategy_prediction_from_analysis_records_predicted_snapshot() -> None:
    payload = build_strategy_prediction_from_analysis(
        _analysis(),
        benchmark_history=_history(amount=2.0e8),
        note="首笔预测样本",
    )

    assert payload["status"] == "predicted"
    assert payload["prediction_target"] == "20d_excess_return_vs_csi800_rank"
    assert payload["benchmark"]["symbol"] == "000906.SH"
    assert payload["confidence_type"] == "rank_confidence_v1"
    assert payload["prediction_value"]["expected_excess_direction"] == "positive"
    assert payload["cohort_contract"]["holding_period_days"] == 20
    assert payload["notes"] == ["首笔预测样本"]
    assert payload["key_factors"]
    assert payload["benchmark_fixture"]["status"] == "aligned"
    assert payload["benchmark_fixture"]["aligned_as_of"] is True
    assert payload["lag_visibility_fixture"]["status"] == "partial"
    assert payload["lag_visibility_fixture"]["strategy_candidate_ready_count"] == 2
    assert payload["lag_visibility_fixture"]["point_in_time_blocked_count"] == 2
    assert payload["overlap_fixture"]["status"] == "ready"
    assert payload["overlap_fixture"]["required_gap_days"] == 20
    assert "lag_visibility_fixture_partial" in payload["downgrade_flags"]
    assert "j1_volume_structure" in payload["factor_contract"]["strategy_candidate_factor_ids"]
    assert any(item["factor_id"] == "j2_policy_event" for item in payload["factor_contract"]["point_in_time_blockers"])
    assert "factor_contract_pti_blockers" in payload["downgrade_flags"]


def test_build_strategy_prediction_from_analysis_blocks_when_no_strategy_candidate_is_point_in_time_ready() -> None:
    analysis = _analysis()
    analysis["dimensions"]["technical"]["factors"][0]["factor_meta"] = factor_meta_payload(
        "j1_volume_structure",
        overrides={
            "degraded": True,
            "degraded_reason": "lag / visibility fixture incomplete",
            "lag_fixture_ready": False,
            "visibility_fixture_ready": False,
        },
    )
    analysis["dimensions"]["relative_strength"]["factors"][0]["factor_meta"] = factor_meta_payload(
        "j3_benchmark_relative",
        overrides={
            "degraded": True,
            "degraded_reason": "lag / visibility fixture incomplete",
            "lag_fixture_ready": False,
            "visibility_fixture_ready": False,
        },
    )

    payload = build_strategy_prediction_from_analysis(
        analysis,
        benchmark_history=_history(amount=2.0e8),
    )

    assert payload["status"] == "no_prediction"
    assert payload["lag_visibility_fixture"]["status"] == "blocked"
    assert payload["lag_visibility_fixture"]["strategy_candidate_ready_count"] == 0
    assert "lag_visibility_fixture_blocked" in payload["no_prediction_reason_codes"]
    assert "lag_visibility_fixture_blocked" in payload["downgrade_flags"]


def test_build_strategy_prediction_from_analysis_rejects_partial_benchmark_fixture() -> None:
    payload = build_strategy_prediction_from_analysis(
        _analysis(),
        benchmark_history=_history(days=180, amount=2.0e8),
    )

    assert payload["status"] == "no_prediction"
    assert "benchmark_overlap_insufficient" in payload["no_prediction_reason_codes"]
    assert payload["benchmark_fixture"]["status"] == "partial"
    assert "benchmark_fixture_partial" in payload["downgrade_flags"]


def test_build_strategy_prediction_from_analysis_marks_no_prediction_when_liquidity_or_asset_type_fail() -> None:
    payload = build_strategy_prediction_from_analysis(
        _analysis(asset_type="cn_etf", amount=4.0e7, history_fallback=True),
        benchmark_history=pd.DataFrame(),
    )

    assert payload["status"] == "no_prediction"
    assert "unsupported_asset_type" in payload["no_prediction_reason_codes"]
    assert "low_liquidity" in payload["no_prediction_reason_codes"]
    assert "history_fallback_mode" in payload["no_prediction_reason_codes"]
    assert "benchmark_missing" in payload["no_prediction_reason_codes"]


def test_generate_strategy_replay_predictions_respects_asset_gap(monkeypatch) -> None:
    asset_history = _history(days=520, amount=1.8e8, start="2024-01-01")
    benchmark_history = _history(days=520, amount=2.2e8, start="2024-01-01")
    monkeypatch.setattr(strategy_module, "detect_asset_type", lambda symbol, config: "cn_stock")
    monkeypatch.setattr(
        strategy_module,
        "fetch_asset_history",
        lambda symbol, asset_type, config: benchmark_history if symbol == "000906.SH" else asset_history,
    )

    payload = generate_strategy_replay_predictions(
        "600519",
        {},
        start="2025-01-01",
        end="2025-12-31",
        asset_gap_days=20,
        max_samples=3,
    )

    assert len(payload["rows"]) == 3
    assert all(row["prediction_mode"] == "historical_replay_v1" for row in payload["rows"])
    assert payload["benchmark_fixture_summary"]["sample_count"] == 3
    assert payload["benchmark_fixture_summary"]["status_counts"]["aligned"] == 3
    assert payload["lag_visibility_fixture_summary"]["status_counts"]["not_applicable"] == 3
    assert payload["overlap_fixture_summary"]["status_counts"]["ready"] == 3
    assert payload["overlap_fixture_summary"]["violation_count"] == 0
    as_of_values = [row["as_of"] for row in payload["rows"]]
    assert as_of_values == sorted(as_of_values)


def test_generate_strategy_replay_predictions_flags_overlap_when_gap_is_shorter_than_horizon(monkeypatch) -> None:
    asset_history = _history(days=520, amount=1.8e8, start="2024-01-01")
    benchmark_history = _history(days=520, amount=2.2e8, start="2024-01-01")
    monkeypatch.setattr(strategy_module, "detect_asset_type", lambda symbol, config: "cn_stock")
    monkeypatch.setattr(
        strategy_module,
        "fetch_asset_history",
        lambda symbol, asset_type, config: benchmark_history if symbol == "000906.SH" else asset_history,
    )

    payload = generate_strategy_replay_predictions(
        "600519",
        {},
        start="2025-01-01",
        end="2025-12-31",
        asset_gap_days=5,
        max_samples=3,
    )

    assert len(payload["rows"]) == 3
    assert payload["overlap_fixture_summary"]["violation_count"] == 2
    assert payload["overlap_fixture_summary"]["status_counts"]["blocked"] == 2
    assert payload["rows"][1]["overlap_fixture"]["status"] == "blocked"
    assert "overlap_fixture_blocked" in payload["rows"][1]["downgrade_flags"]


def test_generate_strategy_replay_predictions_carries_batch_context_and_asset_gap(monkeypatch) -> None:
    asset_history = _history(days=520, amount=1.8e8, start="2024-01-01")
    benchmark_history = _history(days=520, amount=2.2e8, start="2024-01-01")
    monkeypatch.setattr(strategy_module, "detect_asset_type", lambda symbol, config: "cn_stock")
    monkeypatch.setattr(
        strategy_module,
        "fetch_asset_history",
        lambda symbol, asset_type, config: benchmark_history if symbol == "000906.SH" else asset_history,
    )

    payload = generate_strategy_replay_predictions(
        "600519",
        {},
        start="2025-01-01",
        end="2025-12-31",
        asset_gap_days=40,
        max_samples=2,
        batch_context={"key": "cn_liquid_core", "label": "A股核心流动性样本", "effective_symbol_count": 1},
        cohort_recipe={"key": "monthly_deep", "label": "低频深样本", "asset_gap_days": 40, "max_samples": 2},
    )

    assert payload["asset_gap_days"] == 40
    assert payload["batch_context"]["key"] == "cn_liquid_core"
    assert payload["cohort_recipe"]["key"] == "monthly_deep"
    assert payload["rows"][0]["cohort_contract"]["asset_reentry_gap_days"] == 40
    assert payload["overlap_fixture_summary"]["max_required_gap_days"] == 40


def test_generate_strategy_multi_symbol_replay_predictions_builds_same_day_supply(monkeypatch) -> None:
    asset_history = _history(days=520, amount=1.8e8, start="2024-01-01")
    benchmark_history = _history(days=520, amount=2.2e8, start="2024-01-01")
    monkeypatch.setattr(strategy_module, "detect_asset_type", lambda symbol, config: "cn_stock")
    monkeypatch.setattr(
        strategy_module,
        "fetch_asset_history",
        lambda symbol, asset_type, config: benchmark_history if symbol == "000906.SH" else asset_history,
    )

    payload = generate_strategy_multi_symbol_replay_predictions(
        ["600519", "300750", "000333"],
        {},
        start="2025-01-01",
        end="2025-12-31",
        asset_gap_days=20,
        max_samples=3,
    )

    assert payload["symbol_count"] == 3
    assert len(payload["rows"]) == 9
    assert len(payload["symbol_rows"]) == 3
    assert payload["cross_sectional_supply_summary"]["cohorts_ge_3"] == 3
    assert payload["cross_sectional_supply_summary"]["unique_symbol_count"] == 3
    assert payload["benchmark_fixture_summary"]["sample_count"] == 9


def test_validate_strategy_rows_adds_validation_snapshot(monkeypatch) -> None:
    asset_history = _history(days=320, amount=1.8e8)
    benchmark_history = _history(days=320, amount=2.0e8)
    benchmark_history["close"] = benchmark_history["close"] * 0.995
    monkeypatch.setattr(
        strategy_module,
        "fetch_asset_history",
        lambda symbol, asset_type, config: benchmark_history if symbol == "000906.SH" else asset_history,
    )
    rows = [
        {
            "prediction_id": "pred_1",
            "symbol": "600519",
            "asset_type": "cn_stock",
            "status": "predicted",
            "as_of": str(asset_history["date"].iloc[260].date()),
            "prediction_value": {"expected_excess_direction": "positive"},
            "horizon": {"days": 20},
            "confidence_label": "中",
        }
    ]

    updated_rows, summary = validate_strategy_rows(rows, {})

    assert updated_rows[0]["validation"]["validation_status"] == "validated"
    assert updated_rows[0]["benchmark_fixture"]["status"] == "aligned"
    assert summary["benchmark_fixture_summary"]["sample_count"] == 1
    assert summary["lag_visibility_fixture_summary"]["sample_count"] == 1
    assert summary["overlap_fixture_summary"]["sample_count"] == 1
    assert summary["out_of_sample_validation"]["status"] == "blocked"
    assert summary["chronological_cohort_validation"]["status"] == "blocked"
    assert summary["cross_sectional_validation"]["status"] == "blocked"
    assert summary["rollback_gate"]["status"] == "blocked"
    assert "validated_rows_below_floor" in summary["rollback_gate"]["blockers"]
    assert summary["validated_rows"] == 1
    assert "单标的时间序列口径" in summary["notes"][0]


def test_out_of_sample_validation_marks_watchlist_when_holdout_regresses() -> None:
    rows = []
    for index in range(6):
        rows.append(
            {
                "as_of": f"2024-01-0{index + 1}",
                "validation": {
                    "validation_status": "validated",
                    "hit": index < 4,
                    "excess_return": 0.03 if index < 4 else -0.04,
                    "cost_adjusted_directional_return": 0.025 if index < 4 else -0.045,
                    "max_drawdown": -0.02 if index < 4 else -0.06,
                },
            }
        )

    summary = strategy_module._out_of_sample_validation(rows, overlap_fixture_summary={"violation_count": 0})

    assert summary["status"] == "watchlist"
    assert summary["development_metrics"]["count"] == 4
    assert summary["holdout_metrics"]["count"] == 2
    assert "holdout_avg_excess_regressed" in summary["decision_reasons"]


def test_chronological_cohort_validation_splits_earliest_middle_latest() -> None:
    rows = []
    for index in range(6):
        rows.append(
            {
                "as_of": f"2024-02-0{index + 1}",
                "validation": {
                    "validation_status": "validated",
                    "hit": True,
                    "excess_return": 0.01 * (index + 1),
                    "cost_adjusted_directional_return": 0.008 * (index + 1),
                    "max_drawdown": -0.02,
                },
            }
        )

    summary = strategy_module._chronological_cohort_validation(rows)

    assert summary["status"] == "stable"
    assert [row["label"] for row in summary["cohort_rows"]] == ["earliest", "middle", "latest"]
    assert summary["cohort_rows"][0]["count"] == 2


def test_cross_sectional_validation_marks_stable_for_positive_same_day_cohorts() -> None:
    rows = []
    dates = ["2024-03-01", "2024-03-08", "2024-03-15"]
    symbols = [("600519", 80.0, 0.05), ("300750", 55.0, 0.01), ("000333", 30.0, -0.02)]
    for as_of in dates:
        for symbol, score, excess in symbols:
            rows.append(
                {
                    "as_of": as_of,
                    "symbol": symbol,
                    "seed_score": score,
                    "validation": {
                        "validation_status": "validated",
                        "hit": excess > 0,
                        "excess_return": excess,
                        "cost_adjusted_directional_return": excess - 0.002,
                        "max_drawdown": -0.03,
                    },
                }
            )

    summary = strategy_module._cross_sectional_validation(rows)

    assert summary["status"] == "stable"
    assert summary["cohort_count"] == 3
    assert summary["eligible_symbol_count"] == 3
    assert summary["avg_rank_corr"] > 0
    assert summary["avg_top_bottom_spread"] > 0


def test_promotion_gate_queues_next_stage_for_clear_challenger_win() -> None:
    gate = strategy_module._promotion_gate(
        [
            {
                "variant": "defensive_tilt",
                "validated_sample_count": 8,
                "out_of_sample_status": "stable",
                "holdout_avg_excess_return": 0.03,
                "holdout_avg_cost_adjusted_directional_return": 0.025,
                "primary_score": 6.4,
                "hit_rate": 0.75,
                "avg_excess_return": 0.045,
                "avg_cost_adjusted_directional_return": 0.038,
                "avg_max_drawdown": -0.03,
            },
            {
                "variant": "baseline",
                "validated_sample_count": 8,
                "out_of_sample_status": "stable",
                "holdout_avg_excess_return": 0.012,
                "holdout_avg_cost_adjusted_directional_return": 0.01,
                "primary_score": 4.8,
                "hit_rate": 0.5,
                "avg_excess_return": 0.012,
                "avg_cost_adjusted_directional_return": 0.01,
                "avg_max_drawdown": -0.04,
            },
        ],
        overlap_fixture_summary={"violation_count": 0},
        sample_count=8,
    )

    assert gate["status"] == "queue_for_next_stage"
    assert gate["candidate_variant"] == "defensive_tilt"
    assert gate["production_ready"] is False
    assert gate["primary_score_delta"] > 0


def test_rollback_gate_marks_candidate_when_structural_misses_dominate() -> None:
    rows = [
        {
            "validation": {
                "validation_status": "validated",
                "hit": False,
                "excess_return": -0.05,
                "cost_adjusted_directional_return": -0.045,
            },
            "attribution": {"label": "missing_factor"},
        }
        for _ in range(6)
    ]

    gate = strategy_module._rollback_gate(
        rows,
        overlap_fixture_summary={"violation_count": 0},
        current_label="baseline",
    )

    assert gate["status"] == "rollback_candidate"
    assert gate["structural_miss_count"] == 6
    assert gate["validated_rows"] == 6


def test_attribute_strategy_rows_labels_weight_misallocation_for_mixed_low_confidence_miss() -> None:
    rows = [
        {
            "symbol": "600519",
            "status": "predicted",
            "seed_score": 56.0,
            "confidence_label": "低",
            "prediction_value": {"expected_excess_direction": "positive"},
            "key_factors": [
                {"direction": "supportive"},
                {"direction": "supportive"},
                {"direction": "drag"},
                {"direction": "drag"},
            ],
            "downgrade_flags": [],
            "validation": {
                "validation_status": "validated",
                "hit": False,
                "realized_return": -0.01,
                "excess_return": -0.06,
                "cost_adjusted_directional_return": -0.065,
                "neutral_band": 0.02,
            },
        }
    ]

    updated_rows, summary = attribute_strategy_rows(rows)

    assert updated_rows[0]["attribution"]["label"] == "weight_misallocation"
    assert summary["label_rows"][0]["label"] == "weight_misallocation"
    assert "strategy experiment" in summary["recommendations"][0]


def test_attribute_strategy_rows_labels_universe_bias_when_absolute_direction_right_but_relative_wrong() -> None:
    rows = [
        {
            "symbol": "600519",
            "status": "predicted",
            "seed_score": 68.0,
            "confidence_label": "中",
            "prediction_value": {"expected_excess_direction": "positive"},
            "key_factors": [{"direction": "supportive"}],
            "downgrade_flags": [],
            "validation": {
                "validation_status": "validated",
                "hit": False,
                "realized_return": 0.03,
                "excess_return": -0.02,
                "cost_adjusted_directional_return": -0.025,
                "neutral_band": 0.01,
            },
        }
    ]

    updated_rows, _ = attribute_strategy_rows(rows)

    assert updated_rows[0]["attribution"]["label"] == "universe_bias"


def test_generate_strategy_experiment_compares_variants(monkeypatch) -> None:
    asset_history = _history(days=520, amount=1.8e8, start="2024-01-01")
    benchmark_history = _history(days=520, amount=2.2e8, start="2024-01-01")
    benchmark_history["close"] = benchmark_history["close"] * 0.997
    monkeypatch.setattr(strategy_module, "detect_asset_type", lambda symbol, config: "cn_stock")
    monkeypatch.setattr(
        strategy_module,
        "fetch_asset_history",
        lambda symbol, asset_type, config: benchmark_history if symbol == "000906.SH" else asset_history,
    )

    payload = generate_strategy_experiment(
        "600519",
        {},
        start="2025-01-01",
        end="2025-12-31",
        max_samples=3,
        variants=["baseline", "defensive_tilt"],
    )

    assert payload["sample_count"] == 3
    assert len(payload["variant_rows"]) == 2
    assert payload["benchmark_fixture_summary"]["sample_count"] == 3
    assert payload["lag_visibility_fixture_summary"]["status_counts"]["not_applicable"] == 3
    assert payload["overlap_fixture_summary"]["sample_count"] == 3
    assert payload["promotion_gate"]["status"] == "blocked"
    assert payload["rollback_gate"]["status"] == "blocked"
    assert payload["variant_rows"][0]["out_of_sample_status"] == "blocked"
    assert payload["variant_rows"][0]["variant"] in {"baseline", "defensive_tilt"}


def test_generate_strategy_multi_symbol_experiment_carries_cross_sectional_status(monkeypatch) -> None:
    benchmark_history = _history(days=520, amount=2.2e8, start="2024-01-01")
    symbol_histories = {
        "600519": _history(days=520, amount=1.8e8, start="2024-01-01"),
        "300750": _history(days=520, amount=1.8e8, start="2024-01-01").assign(close=lambda df: df["close"] * 1.05),
        "000333": _history(days=520, amount=1.8e8, start="2024-01-01").assign(close=lambda df: df["close"] * 0.97),
    }
    monkeypatch.setattr(strategy_module, "detect_asset_type", lambda symbol, config: "cn_stock")
    monkeypatch.setattr(
        strategy_module,
        "fetch_asset_history",
        lambda symbol, asset_type, config: benchmark_history if symbol == "000906.SH" else symbol_histories[symbol],
    )

    payload = strategy_module.generate_strategy_multi_symbol_experiment(
        ["600519", "300750", "000333"],
        {},
        start="2025-01-01",
        end="2025-03-31",
        max_samples=3,
        variants=["baseline", "momentum_tilt"],
    )

    assert payload["symbol_count"] == 3
    assert payload["cross_sectional_supply_summary"]["cohorts_ge_3"] == 3
    assert len(payload["variant_rows"]) == 2
    assert payload["variant_rows"][0]["cross_sectional_status"] in {"watchlist", "stable"}
    assert payload["promotion_gate"]["candidate_cross_sectional_status"] in {"watchlist", "stable"}
