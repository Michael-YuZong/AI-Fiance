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
        "benchmark_fixture": {
            "status": "aligned",
            "summary": "基准窗口已对齐。",
            "overlap_rows": 260,
            "aligned_as_of": True,
            "future_window_ready": False,
            "asset_as_of": "2026-03-13",
            "benchmark_as_of": "2026-03-13",
            "as_of_gap_days": 0,
            "asset_window": {"start": "2025-01-01", "end": "2026-03-13", "rows": 260},
            "benchmark_window": {"start": "2025-01-01", "end": "2026-03-13", "rows": 260},
            "blockers": [],
        },
        "lag_visibility_fixture": {
            "status": "partial",
            "summary": "当前已就绪 `2/2` 个 strategy candidate，但仍有 `2` 个因子没完成 lag / visibility fixture。",
            "strategy_candidate_ready_count": 2,
            "strategy_candidate_total": 2,
            "point_in_time_blocked_count": 2,
            "lag_ready_count": 2,
            "visibility_ready_count": 2,
            "degraded_count": 2,
            "max_lag_days": 0,
            "blocker_factor_ids": ["j2_policy_event", "j4_earnings_momentum"],
        },
        "overlap_fixture": {
            "status": "ready",
            "summary": "当前批次里还没有更早的同标的主样本，这条记录先作为 overlap anchor。",
            "window_start": "2026-03-13",
            "window_end": "2026-04-10",
            "required_gap_days": 20,
            "previous_sample_as_of": "—",
            "gap_trading_days": 0,
            "overlap_policy": "no_new_primary_sample_before_previous_20d_window_finishes",
        },
        "cohort_contract": {"cohort_frequency_days": 5, "holding_period_days": 20},
        "key_factors": [{"label": "技术/趋势", "factor": "technical", "score": 76, "direction": "supportive", "summary": "趋势保持偏强。"}],
        "factor_snapshot": {
            "price_momentum": {"return_5d": 0.03, "return_20d": 0.11, "return_60d": 0.22},
            "benchmark_relative": {"relative_return_20d": 0.05, "relative_return_60d": 0.08},
            "technical": {"ma_signal": "bullish", "macd_signal": "bullish", "rsi": 63.0},
            "liquidity": {"avg_turnover_20d": 1.6e8, "median_turnover_60d": 1.5e8},
            "risk": {"volatility_20d": 0.24, "max_drawdown_1y": -0.18},
        },
        "factor_contract": {
            "families": {"J-1": 1, "J-2": 1, "J-3": 1},
            "states": {"observation_only": 1, "strategy_challenger": 2},
            "strategy_candidate_factor_ids": ["j1_volume_structure", "j3_benchmark_relative"],
            "point_in_time_blockers": [{"factor_id": "j2_policy_event", "reason": "lag / visibility fixture incomplete"}],
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
    assert "## 执行摘要" in rendered
    assert "## 预测合同" in rendered
    assert "## Benchmark Fixture" in rendered
    assert "## Lag / Visibility Fixture" in rendered
    assert "## Overlap Fixture" in rendered
    assert "## 关键因子" in rendered
    assert "## 因子快照" in rendered
    assert "## 因子合同" in rendered
    assert "## 证据时点与来源" in rendered
    assert "## 降级与边界" in rendered
    assert "更像 20 日超额收益的上层候选" in rendered
    assert "相对基准" in rendered
    assert "j2_policy_event" in rendered


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


def test_strategy_report_renderer_renders_replay_summary() -> None:
    rendered = StrategyReportRenderer().render_replay_summary(
        {
            "symbol": "600519",
            "start": "2024-01-01",
            "end": "2024-12-31",
            "asset_gap_days": 20,
            "benchmark_fixture_summary": {
                "sample_count": 1,
                "status_counts": {"aligned": 1},
                "min_overlap_rows": 260,
                "max_as_of_gap_days": 0,
                "future_window_ready_count": 1,
                "future_window_pending_count": 0,
                "summary": "当前样本的 benchmark 合同整体对齐，未见 overlap 或 as_of 缺口。",
            },
            "lag_visibility_fixture_summary": {
                "sample_count": 1,
                "status_counts": {"not_applicable": 1},
                "min_strategy_candidate_ready_count": 0,
                "max_point_in_time_blocked_count": 0,
                "max_lag_days": 0,
                "summary": "当前样本全部走 price-only replay，lag / visibility fixture 对共享因子池暂不适用。",
            },
            "overlap_fixture_summary": {
                "sample_count": 1,
                "status_counts": {"ready": 1},
                "compared_rows": 0,
                "violation_count": 0,
                "min_gap_trading_days": 0,
                "max_required_gap_days": 20,
                "summary": "当前样本数不足以形成 overlap 比较，第一条记录只作为 anchor sample。",
            },
            "notes": ["单标的 replay。"],
            "rows": [
                {
                    "as_of": "2024-06-28",
                    "status": "predicted",
                    "prediction_value": {"expected_rank_bucket": "upper_quintile_candidate"},
                    "confidence_label": "中",
                    "seed_score": 64.0,
                }
            ],
        },
        persisted=True,
    )

    assert "# Strategy Replay" in rendered
    assert "## 执行摘要" in rendered
    assert "## Benchmark Fixture" in rendered
    assert "## Lag / Visibility Fixture" in rendered
    assert "## Overlap Fixture" in rendered
    assert "单标的 replay" in rendered
    assert "upper_quintile_candidate" in rendered


def test_strategy_report_renderer_renders_multi_symbol_replay_summary() -> None:
    rendered = StrategyReportRenderer().render_replay_summary(
        {
            "symbols": ["600519", "300750", "000333"],
            "symbol_count": 3,
            "scope": "multi_symbol_historical_replay_supply_v1",
            "start": "2024-01-01",
            "end": "2024-12-31",
            "asset_gap_days": 20,
            "batch_context": {
                "key": "cn_liquid_core",
                "label": "A股核心流动性样本",
                "mode": "explicit_symbols",
                "source_symbol_count": 3,
                "effective_symbol_count": 3,
                "explicit_symbol_count": 3,
                "watchlist_match_count": 0,
                "summary": "batch source `A股核心流动性样本` 解析出 `3` 只 A 股普通股票。",
                "notes": ["默认用于多标的 replay / experiment 的最小同日 cohort 样本。"],
            },
            "cohort_recipe": {
                "key": "weekly_non_overlap",
                "label": "每周非重叠主样本",
                "applied_via": "config_recipe",
                "configured_asset_gap_days": 20,
                "configured_max_samples": 6,
                "asset_gap_days": 20,
                "max_samples": 6,
                "summary": "cohort recipe `每周非重叠主样本` 采用资产重入间隔 `20` 个交易日。",
                "notes": ["默认保持 20 个交易日主持有期内不重复进入同一标的。"],
            },
            "benchmark_fixture_summary": {
                "sample_count": 9,
                "status_counts": {"aligned": 9},
                "min_overlap_rows": 260,
                "max_as_of_gap_days": 0,
                "future_window_ready_count": 9,
                "future_window_pending_count": 0,
                "summary": "当前样本的 benchmark 合同整体对齐，未见 overlap 或 as_of 缺口。",
            },
            "lag_visibility_fixture_summary": {
                "sample_count": 9,
                "status_counts": {"not_applicable": 9},
                "min_strategy_candidate_ready_count": 0,
                "max_point_in_time_blocked_count": 0,
                "max_lag_days": 0,
                "summary": "当前样本全部走 price-only replay，lag / visibility fixture 对共享因子池暂不适用。",
            },
            "overlap_fixture_summary": {
                "sample_count": 9,
                "status_counts": {"ready": 9},
                "compared_rows": 6,
                "violation_count": 0,
                "min_gap_trading_days": 20,
                "max_required_gap_days": 20,
                "summary": "当前样本的 primary windows 没有重叠，已满足单标的 non-overlap 合同。",
            },
            "symbol_rows": [
                {"symbol": "600519", "status": "ready", "sample_count": 3, "predicted_count": 3, "no_prediction_count": 0, "first_as_of": "2024-06-28", "last_as_of": "2024-08-23"},
                {"symbol": "300750", "status": "ready", "sample_count": 3, "predicted_count": 3, "no_prediction_count": 0, "first_as_of": "2024-06-28", "last_as_of": "2024-08-23"},
                {"symbol": "000333", "status": "ready", "sample_count": 3, "predicted_count": 3, "no_prediction_count": 0, "first_as_of": "2024-06-28", "last_as_of": "2024-08-23"},
            ],
            "cross_sectional_supply_summary": {
                "summary": "当前已有 `3` 个日期至少覆盖 3 只标的，可以开始积累 cross-sectional validate 样本。",
                "cohort_count": 3,
                "unique_symbol_count": 3,
                "cohorts_ge_2": 3,
                "cohorts_ge_3": 3,
                "min_symbols_per_as_of": 3,
                "max_symbols_per_as_of": 3,
                "cohort_rows": [
                    {"as_of": "2024-06-28", "symbol_count": 3, "predicted_count": 3, "no_prediction_count": 0},
                    {"as_of": "2024-07-26", "symbol_count": 3, "predicted_count": 3, "no_prediction_count": 0},
                    {"as_of": "2024-08-23", "symbol_count": 3, "predicted_count": 3, "no_prediction_count": 0},
                ],
            },
            "notes": ["当前 replay 已扩到多标的样本供给。"],
            "rows": [
                {"as_of": "2024-06-28", "symbol": "600519", "status": "predicted", "prediction_value": {"expected_rank_bucket": "upper_quintile_candidate"}, "confidence_label": "中", "seed_score": 64.0},
                {"as_of": "2024-06-28", "symbol": "300750", "status": "predicted", "prediction_value": {"expected_rank_bucket": "upper_half_candidate"}, "confidence_label": "低", "seed_score": 58.0},
            ],
        },
        persisted=True,
    )

    assert "## Symbol Coverage" in rendered
    assert "## Same-Day Cohorts" in rendered
    assert "## 执行摘要" in rendered
    assert "## Batch Source" in rendered
    assert "## Cohort Recipe" in rendered
    assert "| as_of | symbol | status | rank bucket | confidence | score |" in rendered
    assert "`300750`" in rendered


def test_strategy_report_renderer_renders_validation_summary() -> None:
    rendered = StrategyReportRenderer().render_validation_summary(
        {
            "total_rows": 2,
            "validated_rows": 1,
            "pending_rows": 1,
            "predicted_rows": 2,
            "no_prediction_rows": 0,
            "skipped_rows": 0,
            "benchmark_fixture_summary": {
                "sample_count": 2,
                "status_counts": {"aligned": 2},
                "min_overlap_rows": 260,
                "max_as_of_gap_days": 0,
                "future_window_ready_count": 1,
                "future_window_pending_count": 1,
                "summary": "当前样本的 benchmark 合同整体对齐，未见 overlap 或 as_of 缺口。",
            },
            "lag_visibility_fixture_summary": {
                "sample_count": 2,
                "status_counts": {"not_applicable": 2},
                "min_strategy_candidate_ready_count": 0,
                "max_point_in_time_blocked_count": 0,
                "max_lag_days": 0,
                "summary": "当前样本全部走 price-only replay，lag / visibility fixture 对共享因子池暂不适用。",
            },
            "overlap_fixture_summary": {
                "sample_count": 2,
                "status_counts": {"ready": 2},
                "compared_rows": 1,
                "violation_count": 0,
                "min_gap_trading_days": 20,
                "max_required_gap_days": 20,
                "summary": "当前样本的 primary windows 没有重叠，已满足单标的 non-overlap 合同。",
            },
            "out_of_sample_validation": {
                "status": "stable",
                "validated_rows": 8,
                "required_validated_rows": 6,
                "development_metrics": {
                    "count": 6,
                    "start_as_of": "2024-01-01",
                    "end_as_of": "2024-06-01",
                    "hit_rate": 0.67,
                    "avg_excess_return": 0.03,
                    "avg_cost_adjusted_directional_return": 0.025,
                },
                "holdout_metrics": {
                    "count": 2,
                    "start_as_of": "2024-07-01",
                    "end_as_of": "2024-08-01",
                    "hit_rate": 0.67,
                    "avg_excess_return": 0.02,
                    "avg_cost_adjusted_directional_return": 0.015,
                },
                "hit_rate_delta": 0.0,
                "avg_excess_return_delta": -0.01,
                "avg_cost_adjusted_directional_return_delta": -0.01,
                "avg_max_drawdown_delta": -0.01,
                "blockers": [],
                "decision_reasons": [],
                "summary": "当前 holdout 没有出现明显退化。",
                "next_action": "继续滚动更新 holdout。",
            },
            "chronological_cohort_validation": {
                "status": "watchlist",
                "cohort_rows": [
                    {"label": "earliest", "start_as_of": "2024-01-01", "end_as_of": "2024-02-01", "count": 2, "hit_rate": 1.0, "avg_excess_return": 0.05, "avg_cost_adjusted_directional_return": 0.04},
                    {"label": "middle", "start_as_of": "2024-03-01", "end_as_of": "2024-04-01", "count": 2, "hit_rate": 0.5, "avg_excess_return": 0.01, "avg_cost_adjusted_directional_return": 0.0},
                    {"label": "latest", "start_as_of": "2024-05-01", "end_as_of": "2024-06-01", "count": 2, "hit_rate": 0.5, "avg_excess_return": -0.01, "avg_cost_adjusted_directional_return": -0.015},
                ],
                "hit_rate_delta_latest_vs_earliest": -0.5,
                "avg_excess_return_delta_latest_vs_earliest": -0.06,
                "avg_cost_adjusted_directional_return_delta_latest_vs_earliest": -0.055,
                "blockers": [],
                "summary": "latest cohort 相比 earliest cohort 已经出现明显退化。",
            },
            "cross_sectional_validation": {
                "status": "blocked",
                "cohort_count": 0,
                "required_cohorts": 3,
                "eligible_symbol_count": 1,
                "required_cohort_symbols": 3,
                "avg_rank_corr": 0.0,
                "avg_top_bottom_spread": 0.0,
                "avg_top_bottom_net_spread": 0.0,
                "positive_rank_corr_count": 0,
                "positive_spread_count": 0,
                "eligible_cohort_rows": [],
                "blockers": ["cross_sectional_cohorts_below_floor", "cross_sectional_symbols_below_floor"],
                "decision_reasons": [],
                "summary": "当前账本里还没有足够的同日多标的 cohort，cross-sectional validate 先阻断。",
                "next_action": "先补多标的样本。",
            },
            "rollback_gate": {
                "status": "watchlist",
                "current_label": "current_batch",
                "validated_rows": 8,
                "required_validated_rows": 6,
                "overlap_violation_count": 0,
                "hit_rate": 0.42,
                "avg_excess_return": -0.01,
                "avg_cost_adjusted_directional_return": -0.015,
                "structural_miss_share": 0.5,
                "structural_miss_count": 4,
                "degraded_miss_count": 1,
                "execution_cost_drag_count": 1,
                "horizon_mismatch_count": 1,
                "confirmed_edge_count": 2,
                "blockers": [],
                "summary": "当前 baseline 已经出现持续压力，但还没到直接 rollback 的强结论。",
                "next_action": "继续扩大样本。",
            },
            "hit_rate": 1.0,
            "avg_excess_return": 0.06,
            "avg_cost_adjusted_directional_return": 0.055,
            "avg_max_drawdown": -0.03,
            "bucket_rows": [{"bucket": "中", "count": 1, "hit_rate": 1.0, "avg_excess_return": 0.06, "avg_net_directional_return": 0.055}],
            "recent_rows": [{"as_of": "2024-06-28", "symbol": "600519", "direction": "positive", "confidence_label": "中", "excess_return": 0.06, "net_directional_return": 0.055, "hit": True, "validation_status": "validated"}],
            "notes": ["当前还是单标的时间序列口径。"],
        },
        persisted=True,
    )

    assert "# Strategy Validation" in rendered
    assert "## 这套策略是什么" in rendered
    assert "## 这次到底看出来什么" in rendered
    assert "## 执行摘要" in rendered
    assert "## 总体结果" in rendered
    assert "## Benchmark Fixture" in rendered
    assert "## Lag / Visibility Fixture" in rendered
    assert "## Overlap Fixture" in rendered
    assert "## Out-Of-Sample Validation" in rendered
    assert "## Chronological Cohorts" in rendered
    assert "## Cross-Sectional Validation" in rendered
    assert "## Rollback Gate" in rendered
    assert "## 置信度分桶" in rendered
    assert "当前还是单标的时间序列口径" in rendered


def test_strategy_report_renderer_renders_client_facing_validation_summary() -> None:
    rendered = StrategyReportRenderer().render_validation_summary(
        {
            "total_rows": 1,
            "validated_rows": 1,
            "pending_rows": 0,
            "predicted_rows": 1,
            "no_prediction_rows": 0,
            "skipped_rows": 0,
            "benchmark_fixture_summary": {
                "sample_count": 1,
                "status_counts": {"aligned": 1},
                "min_overlap_rows": 260,
                "max_as_of_gap_days": 0,
                "future_window_ready_count": 0,
                "future_window_pending_count": 1,
                "summary": "当前样本的 benchmark 合同整体对齐，未见 overlap 或 as_of 缺口。",
            },
            "rollback_gate": {
                "status": "watchlist",
                "summary": "当前 baseline 已进入观察。",
                "validated_rows": 1,
                "required_validated_rows": 1,
            },
            "hit_rate": 0.5,
            "avg_excess_return": -0.01,
            "avg_cost_adjusted_directional_return": -0.015,
            "avg_max_drawdown": -0.05,
            "recent_rows": [
                {
                    "as_of": "2024-06-28",
                    "symbol": "600519",
                    "direction": "negative",
                    "confidence_label": "中",
                    "excess_return": -0.01,
                    "net_directional_return": -0.015,
                    "hit": False,
                    "validation_status": "validated",
                }
            ],
        },
        persisted=True,
        client_facing=True,
    )

    assert "正式成稿" in rendered
    assert "已回写账本" not in rendered
    assert "## 这套策略是什么" in rendered
    assert "## 这次到底看出来什么" in rendered
    assert "预测时窗口快照" in rendered
    assert "| 2024-06-28 | `600519` | 偏弱 |" in rendered


def test_strategy_report_renderer_renders_attribute_summary() -> None:
    rendered = StrategyReportRenderer().render_attribute_summary(
        {
            "total_rows": 3,
            "attributed_rows": 2,
            "pending_rows": 1,
            "not_applicable_rows": 0,
            "label_rows": [{"label": "weight_misallocation", "count": 1, "share": 0.5, "hit_rate": 0.0, "avg_excess_return": -0.04, "avg_net_directional_return": -0.045}],
            "recent_rows": [{"as_of": "2024-06-28", "symbol": "600519", "label": "weight_misallocation", "summary": "更像权重失衡。", "next_action": "先做权重实验。", "excess_return": -0.04, "hit": False, "status": "attributed"}],
            "recommendations": ["先做权重实验。"],
            "notes": ["v1 窄标签集。"],
        },
        persisted=True,
    )

    assert "# Strategy Attribution" in rendered
    assert "## 执行摘要" in rendered
    assert "## 归因分桶" in rendered
    assert "先做权重实验" in rendered


def test_strategy_report_renderer_renders_experiment_summary() -> None:
    rendered = StrategyReportRenderer().render_experiment_summary(
        {
            "symbol": "600519",
            "start": "2024-01-01",
            "end": "2024-12-31",
            "sample_count": 6,
            "baseline_variant": "baseline",
            "champion_variant": "defensive_tilt",
            "challenger_variant": "defensive_tilt",
            "benchmark_fixture_summary": {
                "sample_count": 6,
                "status_counts": {"aligned": 6},
                "min_overlap_rows": 260,
                "max_as_of_gap_days": 0,
                "future_window_ready_count": 6,
                "future_window_pending_count": 0,
                "summary": "当前样本的 benchmark 合同整体对齐，未见 overlap 或 as_of 缺口。",
            },
            "lag_visibility_fixture_summary": {
                "sample_count": 6,
                "status_counts": {"not_applicable": 6},
                "min_strategy_candidate_ready_count": 0,
                "max_point_in_time_blocked_count": 0,
                "max_lag_days": 0,
                "summary": "当前样本全部走 price-only replay，lag / visibility fixture 对共享因子池暂不适用。",
            },
            "overlap_fixture_summary": {
                "sample_count": 6,
                "status_counts": {"ready": 6},
                "compared_rows": 5,
                "violation_count": 0,
                "min_gap_trading_days": 20,
                "max_required_gap_days": 20,
                "summary": "当前样本的 primary windows 没有重叠，已满足单标的 non-overlap 合同。",
            },
            "promotion_gate": {
                "status": "queue_for_next_stage",
                "baseline_variant": "baseline",
                "candidate_variant": "defensive_tilt",
                "sample_count": 8,
                "baseline_validated_rows": 8,
                "candidate_validated_rows": 8,
                "baseline_out_of_sample_status": "stable",
                "candidate_out_of_sample_status": "stable",
                "baseline_cross_sectional_status": "stable",
                "candidate_cross_sectional_status": "stable",
                "required_validated_rows": 6,
                "primary_score_delta": 1.2,
                "hit_rate_delta": 0.1,
                "avg_excess_return_delta": 0.02,
                "avg_cost_adjusted_directional_return_delta": 0.015,
                "avg_max_drawdown_delta": 0.01,
                "holdout_avg_excess_return_delta": 0.01,
                "holdout_avg_cost_adjusted_directional_return_delta": 0.008,
                "blockers": [],
                "decision_reasons": [],
                "production_ready": False,
                "summary": "当前最佳 challenger 已通过窄版 promotion gate，可进入下一阶段验证。",
                "next_action": "进入更严格 validate。",
            },
            "rollback_gate": {
                "status": "hold",
                "current_label": "baseline",
                "validated_rows": 8,
                "required_validated_rows": 6,
                "overlap_violation_count": 0,
                "hit_rate": 0.58,
                "avg_excess_return": 0.015,
                "avg_cost_adjusted_directional_return": 0.01,
                "structural_miss_share": 0.12,
                "structural_miss_count": 1,
                "degraded_miss_count": 0,
                "execution_cost_drag_count": 1,
                "horizon_mismatch_count": 0,
                "confirmed_edge_count": 5,
                "blockers": [],
                "summary": "当前 baseline 还维持在可 hold 区间，暂时没有进入 rollback 讨论。",
                "next_action": "继续累积 validated rows。",
            },
            "variant_rows": [
                {
                    "variant": "baseline",
                    "validated_sample_count": 8,
                    "out_of_sample_status": "stable",
                    "cross_sectional_status": "stable",
                    "cross_sectional_avg_rank_corr": 0.12,
                    "holdout_rows": 2,
                    "holdout_avg_excess_return": -0.005,
                    "holdout_avg_cost_adjusted_directional_return": -0.01,
                    "hit_rate": 0.5,
                    "avg_excess_return": -0.01,
                    "avg_cost_adjusted_directional_return": -0.015,
                    "avg_max_drawdown": -0.05,
                    "dominant_attribution": "weight_misallocation",
                    "hypothesis": "基线。",
                }
            ],
            "notes": ["只用于研究。"],
        }
    )

    assert "# Strategy Experiment" in rendered
    assert "## 这套策略是什么" in rendered
    assert "## 这次到底看出来什么" in rendered
    assert "## 执行摘要" in rendered
    assert "## Benchmark Fixture" in rendered
    assert "## Lag / Visibility Fixture" in rendered
    assert "## Overlap Fixture" in rendered
    assert "## Promotion Gate" in rendered
    assert "## Rollback Gate" in rendered
    assert "## 变体对比" in rendered
    assert "weight_misallocation" in rendered
    assert "holdout" in rendered
    assert "可直接切换" in rendered
    assert "生产链路" not in rendered


def test_strategy_report_renderer_renders_multi_symbol_experiment_summary() -> None:
    rendered = StrategyReportRenderer().render_experiment_summary(
        {
            "symbols": ["600519", "300750", "000333"],
            "symbol_count": 3,
            "scope": "multi_symbol_strategy_experiment_v1",
            "start": "2024-01-01",
            "end": "2024-12-31",
            "sample_count": 9,
            "batch_context": {
                "key": "cn_liquid_core",
                "label": "A股核心流动性样本",
                "mode": "explicit_symbols",
                "source_symbol_count": 3,
                "effective_symbol_count": 3,
                "explicit_symbol_count": 3,
                "watchlist_match_count": 0,
                "summary": "batch source `A股核心流动性样本` 解析出 `3` 只 A 股普通股票。",
            },
            "cohort_recipe": {
                "key": "monthly_deep",
                "label": "低频深样本",
                "applied_via": "config_recipe",
                "configured_asset_gap_days": 40,
                "configured_max_samples": 4,
                "asset_gap_days": 40,
                "max_samples": 4,
                "summary": "cohort recipe `低频深样本` 采用资产重入间隔 `40` 个交易日。",
            },
            "baseline_variant": "baseline",
            "champion_variant": "momentum_tilt",
            "challenger_variant": "momentum_tilt",
            "benchmark_fixture_summary": {
                "sample_count": 18,
                "status_counts": {"aligned": 18},
                "min_overlap_rows": 260,
                "max_as_of_gap_days": 0,
                "future_window_ready_count": 18,
                "future_window_pending_count": 0,
                "summary": "当前样本的 benchmark 合同整体对齐，未见 overlap 或 as_of 缺口。",
            },
            "lag_visibility_fixture_summary": {
                "sample_count": 18,
                "status_counts": {"not_applicable": 18},
                "min_strategy_candidate_ready_count": 0,
                "max_point_in_time_blocked_count": 0,
                "max_lag_days": 0,
                "summary": "当前样本全部走 price-only replay，lag / visibility fixture 对共享因子池暂不适用。",
            },
            "overlap_fixture_summary": {
                "sample_count": 18,
                "status_counts": {"ready": 18},
                "compared_rows": 12,
                "violation_count": 0,
                "min_gap_trading_days": 20,
                "max_required_gap_days": 20,
                "summary": "当前样本的 primary windows 没有重叠，已满足单标的 non-overlap 合同。",
            },
            "cross_sectional_supply_summary": {
                "summary": "当前已有 `3` 个日期至少覆盖 3 只标的，可以开始积累 cross-sectional validate 样本。",
                "cohort_count": 3,
                "unique_symbol_count": 3,
                "cohorts_ge_2": 3,
                "cohorts_ge_3": 3,
                "min_symbols_per_as_of": 3,
                "max_symbols_per_as_of": 3,
                "cohort_rows": [{"as_of": "2024-06-28", "symbol_count": 3, "predicted_count": 3, "no_prediction_count": 0}],
            },
            "promotion_gate": {
                "status": "stay_on_baseline",
                "baseline_variant": "baseline",
                "candidate_variant": "momentum_tilt",
                "sample_count": 18,
                "baseline_validated_rows": 9,
                "candidate_validated_rows": 9,
                "baseline_out_of_sample_status": "stable",
                "candidate_out_of_sample_status": "watchlist",
                "baseline_cross_sectional_status": "stable",
                "candidate_cross_sectional_status": "watchlist",
                "required_validated_rows": 6,
                "primary_score_delta": 0.9,
                "hit_rate_delta": 0.05,
                "avg_excess_return_delta": 0.01,
                "avg_cost_adjusted_directional_return_delta": 0.012,
                "avg_max_drawdown_delta": 0.0,
                "holdout_avg_excess_return_delta": 0.002,
                "holdout_avg_cost_adjusted_directional_return_delta": 0.004,
                "blockers": [],
                "decision_reasons": ["candidate_cross_sectional_not_stable"],
                "production_ready": False,
                "summary": "当前最佳 challenger 还没有稳定跑赢 baseline，promotion gate 先保持 baseline。",
                "next_action": "继续扩大样本。",
            },
            "rollback_gate": {
                "status": "hold",
                "current_label": "baseline",
                "validated_rows": 9,
                "required_validated_rows": 6,
                "overlap_violation_count": 0,
                "hit_rate": 0.56,
                "avg_excess_return": 0.012,
                "avg_cost_adjusted_directional_return": 0.01,
                "structural_miss_share": 0.22,
                "structural_miss_count": 2,
                "degraded_miss_count": 0,
                "execution_cost_drag_count": 1,
                "horizon_mismatch_count": 0,
                "confirmed_edge_count": 6,
                "blockers": [],
                "summary": "当前 baseline 还维持在可 hold 区间。",
                "next_action": "继续累积 validated rows。",
            },
            "variant_rows": [
                {
                    "variant": "baseline",
                    "validated_sample_count": 9,
                    "out_of_sample_status": "stable",
                    "cross_sectional_status": "stable",
                    "cross_sectional_avg_rank_corr": 0.11,
                    "holdout_rows": 3,
                    "holdout_avg_excess_return": 0.01,
                    "holdout_avg_cost_adjusted_directional_return": 0.008,
                    "hit_rate": 0.56,
                    "avg_excess_return": 0.012,
                    "avg_cost_adjusted_directional_return": 0.01,
                    "avg_max_drawdown": -0.04,
                    "dominant_attribution": "confirmed_edge",
                    "hypothesis": "基线。",
                }
            ],
            "notes": ["已扩到多标的 experiment。"],
        }
    )

    assert "## Same-Day Cohorts" in rendered
    assert "## 执行摘要" in rendered
    assert "## Batch Source" in rendered
    assert "## Cohort Recipe" in rendered
    assert "cross-sectional" in rendered
    assert "xsec rank corr" in rendered
