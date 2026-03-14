from __future__ import annotations

import sys

from src.commands import strategy as strategy_module


def test_strategy_predict_main_persists_prediction(monkeypatch, capsys) -> None:
    saved = {}

    class _Repo:
        def upsert_prediction(self, payload):
            saved.update(payload)

        def list_predictions(self, **kwargs):
            return []

    monkeypatch.setattr(strategy_module, "load_config", lambda _path=None: {})
    monkeypatch.setattr(
        strategy_module,
        "generate_strategy_prediction",
        lambda symbol, config, note="": {
            "prediction_id": "stratv1_600519_test",
            "status": "predicted",
            "symbol": symbol,
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
            "key_factors": [],
            "factor_snapshot": {"price_momentum": {}, "technical": {}, "liquidity": {}, "risk": {}},
            "evidence_sources": {
                "market_data_as_of": "2026-03-13",
                "market_data_source": "Tushare 优先日线",
                "benchmark_as_of": "2026-03-13",
                "benchmark_source": "000906.SH 日线历史",
                "catalyst_evidence_as_of": "2026-03-13",
                "catalyst_sources": [],
                "point_in_time_note": "",
                "notes": [],
            },
            "downgrade_flags": [],
            "notes": [note] if note else [],
        },
    )
    monkeypatch.setattr(strategy_module, "StrategyRepository", lambda: _Repo())
    monkeypatch.setattr(
        sys,
        "argv",
        ["strategy", "predict", "600519", "--note", "首笔样本"],
    )

    strategy_module.main()

    captured = capsys.readouterr()
    assert saved["symbol"] == "600519"
    assert "首笔样本" in captured.out
    assert "## 一句话结论" in captured.out


def test_strategy_list_main_renders_rows(monkeypatch, capsys) -> None:
    class _Repo:
        def list_predictions(self, **kwargs):
            return [
                {
                    "as_of": "2026-03-13",
                    "symbol": "600519",
                    "status": "predicted",
                    "prediction_value": {"expected_rank_bucket": "upper_quintile_candidate"},
                    "confidence_label": "中",
                    "seed_score": 66.2,
                }
            ]

    monkeypatch.setattr(strategy_module, "StrategyRepository", lambda: _Repo())
    monkeypatch.setattr(sys, "argv", ["strategy", "list", "--limit", "5"])

    strategy_module.main()

    captured = capsys.readouterr()
    assert "| as_of | symbol | status | rank bucket | confidence | score |" in captured.out
    assert "`600519`" in captured.out


def test_strategy_replay_main_persists_rows(monkeypatch, capsys) -> None:
    saved = []

    class _Repo:
        def upsert_prediction(self, payload):
            saved.append(payload["prediction_id"])

    monkeypatch.setattr(strategy_module, "load_config", lambda _path=None: {})
    monkeypatch.setattr(
        strategy_module,
        "generate_strategy_replay_predictions",
        lambda symbol, config, **kwargs: {
            "symbol": symbol,
            "start": "2024-01-01",
            "end": "2024-12-31",
            "asset_gap_days": 20,
            "notes": ["单标的 replay。"],
            "rows": [
                {
                    "prediction_id": "stratv1_replay_600519_2024-06-28",
                    "as_of": "2024-06-28",
                    "status": "predicted",
                    "prediction_value": {"expected_rank_bucket": "upper_quintile_candidate"},
                    "confidence_label": "中",
                    "seed_score": 64.0,
                }
            ],
        },
    )
    monkeypatch.setattr(strategy_module, "StrategyRepository", lambda: _Repo())
    monkeypatch.setattr(sys, "argv", ["strategy", "replay", "600519", "--max-samples", "1"])

    strategy_module.main()

    captured = capsys.readouterr()
    assert saved == ["stratv1_replay_600519_2024-06-28"]
    assert "# Strategy Replay" in captured.out
    assert "单标的 replay" in captured.out


def test_strategy_validate_main_renders_summary(monkeypatch, capsys) -> None:
    class _Repo:
        def list_predictions(self, **kwargs):
            return [{"prediction_id": "pred_1", "symbol": "600519"}]

        def upsert_prediction(self, payload):
            return payload

    monkeypatch.setattr(strategy_module, "load_config", lambda _path=None: {})
    monkeypatch.setattr(
        strategy_module,
        "validate_strategy_rows",
        lambda rows, config: (
            rows,
            {
                "total_rows": 1,
                "validated_rows": 1,
                "pending_rows": 0,
                "predicted_rows": 1,
                "no_prediction_rows": 0,
                "skipped_rows": 0,
                "hit_rate": 1.0,
                "avg_excess_return": 0.06,
                "avg_cost_adjusted_directional_return": 0.055,
                "avg_max_drawdown": -0.03,
                "bucket_rows": [{"bucket": "中", "count": 1, "hit_rate": 1.0, "avg_excess_return": 0.06, "avg_net_directional_return": 0.055}],
                "recent_rows": [{"as_of": "2024-06-28", "symbol": "600519", "direction": "positive", "confidence_label": "中", "excess_return": 0.06, "net_directional_return": 0.055, "hit": True, "validation_status": "validated"}],
                "notes": ["单标的时间序列口径。"],
            },
        ),
    )
    monkeypatch.setattr(strategy_module, "StrategyRepository", lambda: _Repo())
    monkeypatch.setattr(sys, "argv", ["strategy", "validate", "--symbol", "600519"])

    strategy_module.main()

    captured = capsys.readouterr()
    assert "# Strategy Validation" in captured.out
    assert "单标的时间序列口径" in captured.out


def test_strategy_attribute_main_persists_rows(monkeypatch, capsys) -> None:
    saved = []

    class _Repo:
        def list_predictions(self, **kwargs):
            return [{"prediction_id": "pred_1", "symbol": "600519", "validation": {"validation_status": "validated"}}]

        def upsert_prediction(self, payload):
            saved.append(payload["prediction_id"])

    monkeypatch.setattr(
        strategy_module,
        "attribute_strategy_rows",
        lambda rows: (
            [{"prediction_id": "pred_1", "symbol": "600519", "attribution": {"label": "weight_misallocation"}}],
            {
                "total_rows": 1,
                "attributed_rows": 1,
                "pending_rows": 0,
                "not_applicable_rows": 0,
                "label_rows": [{"label": "weight_misallocation", "count": 1, "share": 1.0, "hit_rate": 0.0, "avg_excess_return": -0.04, "avg_net_directional_return": -0.045}],
                "recent_rows": [{"as_of": "2024-06-28", "symbol": "600519", "label": "weight_misallocation", "excess_return": -0.04, "hit": False, "status": "attributed"}],
                "recommendations": ["先做权重实验。"],
                "notes": ["v1 窄标签集。"],
            },
        ),
    )
    monkeypatch.setattr(strategy_module, "StrategyRepository", lambda: _Repo())
    monkeypatch.setattr(sys, "argv", ["strategy", "attribute", "--symbol", "600519"])

    strategy_module.main()

    captured = capsys.readouterr()
    assert saved == ["pred_1"]
    assert "# Strategy Attribution" in captured.out
    assert "先做权重实验" in captured.out


def test_strategy_experiment_main_renders_summary(monkeypatch, capsys) -> None:
    monkeypatch.setattr(strategy_module, "load_config", lambda _path=None: {})
    monkeypatch.setattr(
        strategy_module,
        "generate_strategy_experiment",
        lambda symbol, config, **kwargs: {
            "symbol": symbol,
            "start": "2024-01-01",
            "end": "2024-12-31",
            "sample_count": 6,
            "baseline_variant": "baseline",
            "champion_variant": "defensive_tilt",
            "challenger_variant": "defensive_tilt",
            "variant_rows": [
                {
                    "variant": "baseline",
                    "hit_rate": 0.5,
                    "avg_excess_return": -0.01,
                    "avg_cost_adjusted_directional_return": -0.015,
                    "avg_max_drawdown": -0.05,
                    "dominant_attribution": "weight_misallocation",
                    "hypothesis": "基线。",
                }
            ],
            "notes": ["只用于研究，不直接 promotion。"],
        },
    )
    monkeypatch.setattr(sys, "argv", ["strategy", "experiment", "600519", "--variants", "baseline"])

    strategy_module.main()

    captured = capsys.readouterr()
    assert "# Strategy Experiment" in captured.out
    assert "只用于研究，不直接 promotion" in captured.out
