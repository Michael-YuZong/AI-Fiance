from __future__ import annotations

import sys
from pathlib import Path

import pytest

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


def test_strategy_replay_main_supports_multiple_symbols(monkeypatch, capsys) -> None:
    saved = []

    class _Repo:
        def upsert_prediction(self, payload):
            saved.append(payload["prediction_id"])

    monkeypatch.setattr(strategy_module, "load_config", lambda _path=None: {})
    monkeypatch.setattr(
        strategy_module,
        "generate_strategy_multi_symbol_replay_predictions",
        lambda symbols, config, **kwargs: {
            "symbols": list(symbols),
            "symbol_count": 2,
            "scope": "multi_symbol_historical_replay_supply_v1",
            "start": "2024-01-01",
            "end": "2024-12-31",
            "asset_gap_days": 20,
            "notes": ["当前 replay 已扩到多标的样本供给。"],
            "symbol_rows": [
                {"symbol": "600519", "status": "ready", "sample_count": 1, "predicted_count": 1, "no_prediction_count": 0, "first_as_of": "2024-06-28", "last_as_of": "2024-06-28"},
                {"symbol": "300750", "status": "ready", "sample_count": 1, "predicted_count": 1, "no_prediction_count": 0, "first_as_of": "2024-06-28", "last_as_of": "2024-06-28"},
            ],
            "cross_sectional_supply_summary": {"summary": "已有 1 个日期至少覆盖 2 只标的。", "cohort_count": 1, "unique_symbol_count": 2, "cohorts_ge_2": 1, "cohorts_ge_3": 0, "min_symbols_per_as_of": 2, "max_symbols_per_as_of": 2, "cohort_rows": [{"as_of": "2024-06-28", "symbol_count": 2, "predicted_count": 2, "no_prediction_count": 0}]},
            "rows": [
                {"prediction_id": "stratv1_replay_600519_2024-06-28", "symbol": "600519", "as_of": "2024-06-28", "status": "predicted", "prediction_value": {"expected_rank_bucket": "upper_quintile_candidate"}, "confidence_label": "中", "seed_score": 64.0},
                {"prediction_id": "stratv1_replay_300750_2024-06-28", "symbol": "300750", "as_of": "2024-06-28", "status": "predicted", "prediction_value": {"expected_rank_bucket": "upper_half_candidate"}, "confidence_label": "低", "seed_score": 58.0},
            ],
        },
    )
    monkeypatch.setattr(strategy_module, "StrategyRepository", lambda: _Repo())
    monkeypatch.setattr(sys, "argv", ["strategy", "replay", "600519", "300750", "--max-samples", "1"])

    strategy_module.main()

    captured = capsys.readouterr()
    assert saved == ["stratv1_replay_600519_2024-06-28", "stratv1_replay_300750_2024-06-28"]
    assert "# Strategy Replay" in captured.out
    assert "## Symbol Coverage" in captured.out


def test_strategy_replay_main_supports_batch_source_and_recipe(monkeypatch, capsys) -> None:
    seen = {}

    monkeypatch.setattr(strategy_module, "load_config", lambda _path=None: {})
    monkeypatch.setattr(
        strategy_module,
        "load_strategy_batches",
        lambda _path: {
            "batch_sources": {
                "cn_liquid_core": {
                    "label": "A股核心流动性样本",
                    "symbols": ["600519", "300750", "000333"],
                    "notes": ["默认用于多标的 replay / experiment 的最小同日 cohort 样本。"],
                }
            },
            "cohort_recipes": {
                "weekly_non_overlap": {
                    "label": "每周非重叠主样本",
                    "asset_gap_days": 20,
                    "max_samples": 6,
                }
            },
        },
    )
    monkeypatch.setattr(strategy_module, "resolve_project_path", lambda value: value)
    monkeypatch.setattr(strategy_module, "detect_asset_type", lambda symbol, config: "cn_stock")
    monkeypatch.setattr(
        strategy_module,
        "generate_strategy_multi_symbol_replay_predictions",
        lambda symbols, config, **kwargs: seen.update({"symbols": list(symbols), "kwargs": kwargs}) or {
            "symbols": list(symbols),
            "symbol_count": 3,
            "scope": "multi_symbol_historical_replay_supply_v1",
            "start": "2024-01-01",
            "end": "2024-12-31",
            "asset_gap_days": kwargs["asset_gap_days"],
            "batch_context": kwargs["batch_context"],
            "cohort_recipe": kwargs["cohort_recipe"],
            "notes": ["当前 replay 已扩到多标的样本供给。"],
            "symbol_rows": [
                {"symbol": "600519", "status": "ready", "sample_count": 1, "predicted_count": 1, "no_prediction_count": 0, "first_as_of": "2024-06-28", "last_as_of": "2024-06-28"},
                {"symbol": "300750", "status": "ready", "sample_count": 1, "predicted_count": 1, "no_prediction_count": 0, "first_as_of": "2024-06-28", "last_as_of": "2024-06-28"},
                {"symbol": "000333", "status": "ready", "sample_count": 1, "predicted_count": 1, "no_prediction_count": 0, "first_as_of": "2024-06-28", "last_as_of": "2024-06-28"},
            ],
            "cross_sectional_supply_summary": {"summary": "已有 1 个日期至少覆盖 3 只标的。", "cohort_count": 1, "unique_symbol_count": 3, "cohorts_ge_2": 1, "cohorts_ge_3": 1, "min_symbols_per_as_of": 3, "max_symbols_per_as_of": 3, "cohort_rows": [{"as_of": "2024-06-28", "symbol_count": 3, "predicted_count": 3, "no_prediction_count": 0}]},
            "rows": [
                {"prediction_id": "stratv1_replay_600519_2024-06-28", "symbol": "600519", "as_of": "2024-06-28", "status": "predicted", "prediction_value": {"expected_rank_bucket": "upper_quintile_candidate"}, "confidence_label": "中", "seed_score": 64.0}
            ],
        },
    )
    monkeypatch.setattr(
        strategy_module,
        "StrategyRepository",
        lambda: type("_Repo", (), {"upsert_prediction": lambda self, payload: None})(),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["strategy", "replay", "--batch-source", "cn_liquid_core", "--cohort-recipe", "weekly_non_overlap", "--preview"],
    )

    strategy_module.main()

    captured = capsys.readouterr()
    assert seen["symbols"] == ["600519", "300750", "000333"]
    assert seen["kwargs"]["asset_gap_days"] == 20
    assert seen["kwargs"]["max_samples"] == 6
    assert seen["kwargs"]["batch_context"]["key"] == "cn_liquid_core"
    assert seen["kwargs"]["cohort_recipe"]["key"] == "weekly_non_overlap"
    assert "## Batch Source" in captured.out
    assert "## Cohort Recipe" in captured.out


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


def test_strategy_validate_main_exports_client_final(monkeypatch, capsys, tmp_path: Path) -> None:
    called = {}

    def _fake_finalize_client_markdown(**kwargs):
        release_checker = kwargs.get("release_checker")
        if release_checker:
            release_checker(kwargs["client_markdown"], kwargs["detail_markdown"])
        called["bundle_kwargs"] = kwargs
        return {
            "markdown": Path(kwargs["markdown_path"]),
            "html": Path(kwargs["markdown_path"]).with_suffix(".html"),
            "pdf": Path(kwargs["markdown_path"]).with_suffix(".pdf"),
        }

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
                "hit_rate": 0.5,
                "avg_excess_return": -0.01,
                "avg_cost_adjusted_directional_return": -0.015,
                "avg_max_drawdown": -0.05,
                "rollback_gate": {
                    "status": "watchlist",
                    "summary": "当前 baseline 已进入观察。",
                    "validated_rows": 1,
                    "required_validated_rows": 1,
                },
                "recent_rows": [
                    {
                        "as_of": "2024-06-28",
                        "symbol": "600519",
                        "direction": "positive",
                        "confidence_label": "中",
                        "excess_return": -0.01,
                        "net_directional_return": -0.015,
                        "hit": False,
                        "validation_status": "validated",
                    }
                ],
            },
        ),
    )
    monkeypatch.setattr(strategy_module, "StrategyRepository", lambda: _Repo())
    monkeypatch.setattr(strategy_module, "ensure_report_task_registered", lambda report_type: called.setdefault("registered", []).append(report_type))
    monkeypatch.setattr(
        strategy_module,
        "check_generic_client_report",
        lambda markdown, report_type, source_text="": called.update({"checked_type": report_type, "checked_markdown": markdown}) or [],
    )
    monkeypatch.setattr(
        strategy_module,
        "finalize_client_markdown",
        _fake_finalize_client_markdown,
    )
    monkeypatch.setattr(strategy_module, "resolve_project_path", lambda value: tmp_path / value)
    monkeypatch.setattr(sys, "argv", ["strategy", "validate", "--symbol", "600519", "--client-final"])

    strategy_module.main()

    captured = capsys.readouterr()
    assert called["registered"] == ["strategy"]
    assert called["checked_type"] == "strategy"
    assert called["bundle_kwargs"]["report_type"] == "strategy"
    assert called["bundle_kwargs"]["markdown_path"].name.startswith("strategy_validate_600519_")
    assert called["bundle_kwargs"]["text_sidecars"]["editor_prompt"][0].name.endswith("editor_prompt.md")
    assert called["bundle_kwargs"]["json_sidecars"]["editor_payload"][0].name.endswith("editor_payload.json")
    assert "正式成稿" in called["checked_markdown"]
    assert "已回写账本" not in called["checked_markdown"]
    assert "## 动作卡片" in called["checked_markdown"]
    assert "## 当前结论" in called["checked_markdown"]
    assert "## 这套策略是什么" in called["checked_markdown"]
    assert "## 这次到底看出来什么" in called["checked_markdown"]
    assert "[client pdf]" in captured.out


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


def test_strategy_experiment_main_exports_client_final(monkeypatch, capsys, tmp_path: Path) -> None:
    called = {}

    def _fake_finalize_client_markdown(**kwargs):
        release_checker = kwargs.get("release_checker")
        if release_checker:
            release_checker(kwargs["client_markdown"], kwargs["detail_markdown"])
        called["bundle_kwargs"] = kwargs
        return {
            "markdown": Path(kwargs["markdown_path"]),
            "html": Path(kwargs["markdown_path"]).with_suffix(".html"),
            "pdf": Path(kwargs["markdown_path"]).with_suffix(".pdf"),
        }

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
            "champion_variant": "baseline",
            "challenger_variant": "momentum_tilt",
            "promotion_gate": {"status": "stay_on_baseline", "summary": "当前 challenger 还没有稳定跑赢。", "candidate_variant": "momentum_tilt", "baseline_variant": "baseline"},
            "rollback_gate": {"status": "hold", "summary": "当前 baseline 仍可 hold。", "validated_rows": 6, "required_validated_rows": 6},
            "variant_rows": [
                {
                    "variant": "baseline",
                    "validated_sample_count": 6,
                    "out_of_sample_status": "watchlist",
                    "cross_sectional_status": "blocked",
                    "hit_rate": 0.5,
                    "avg_excess_return": -0.01,
                    "avg_cost_adjusted_directional_return": -0.015,
                    "avg_max_drawdown": -0.05,
                    "dominant_attribution": "weight_misallocation",
                }
            ],
        },
    )
    monkeypatch.setattr(strategy_module, "ensure_report_task_registered", lambda report_type: called.setdefault("registered", []).append(report_type))
    monkeypatch.setattr(
        strategy_module,
        "check_generic_client_report",
        lambda markdown, report_type, source_text="": called.update({"checked_type": report_type, "checked_markdown": markdown}) or [],
    )
    monkeypatch.setattr(
        strategy_module,
        "finalize_client_markdown",
        _fake_finalize_client_markdown,
    )
    monkeypatch.setattr(strategy_module, "resolve_project_path", lambda value: tmp_path / value)
    monkeypatch.setattr(sys, "argv", ["strategy", "experiment", "600519", "--variants", "baseline,momentum_tilt", "--client-final"])

    strategy_module.main()

    captured = capsys.readouterr()
    assert called["registered"] == ["strategy"]
    assert called["checked_type"] == "strategy"
    assert called["bundle_kwargs"]["report_type"] == "strategy"
    assert called["bundle_kwargs"]["markdown_path"].name.startswith("strategy_experiment_600519_")
    assert called["bundle_kwargs"]["extra_manifest"]["variants"] == ["baseline", "momentum_tilt"]
    assert called["bundle_kwargs"]["text_sidecars"]["editor_prompt"][0].name.endswith("editor_prompt.md")
    assert called["bundle_kwargs"]["json_sidecars"]["editor_payload"][0].name.endswith("editor_payload.json")
    assert "## 动作卡片" in called["checked_markdown"]
    assert "## 当前结论" in called["checked_markdown"]
    assert "## 这套策略是什么" in called["checked_markdown"]
    assert "## 这次到底看出来什么" in called["checked_markdown"]
    assert "[client pdf]" in captured.out


def test_strategy_validate_main_scaffolds_review_when_missing(monkeypatch, tmp_path: Path) -> None:
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
                "hit_rate": 0.5,
                "avg_excess_return": -0.01,
                "avg_cost_adjusted_directional_return": -0.015,
                "avg_max_drawdown": -0.05,
                "rollback_gate": {
                    "status": "watchlist",
                    "summary": "当前 baseline 已进入观察。",
                    "validated_rows": 1,
                    "required_validated_rows": 1,
                },
            },
        ),
    )
    monkeypatch.setattr(strategy_module, "StrategyRepository", lambda: _Repo())
    monkeypatch.setattr(strategy_module, "check_generic_client_report", lambda markdown, report_type, source_text="": [])
    monkeypatch.setattr(strategy_module, "resolve_project_path", lambda value: tmp_path / value)
    review_path = tmp_path / "reports/reviews/strategy/validate/final/strategy_validate_600519_2026-03-23_client_final__external_review.md"

    def _fake_finalize_client_markdown(**kwargs):
        review_path.parent.mkdir(parents=True, exist_ok=True)
        review_path.write_text("pending_structural_reviewer", encoding="utf-8")
        raise SystemExit("已生成外审模板")

    monkeypatch.setattr(
        strategy_module,
        "finalize_client_markdown",
        _fake_finalize_client_markdown,
    )
    monkeypatch.setattr(sys, "argv", ["strategy", "validate", "--symbol", "600519", "--client-final"])

    with pytest.raises(SystemExit, match="已生成外审模板"):
        strategy_module.main()

    assert review_path.exists()
    assert "pending_structural_reviewer" in review_path.read_text(encoding="utf-8")


def test_strategy_experiment_main_supports_multiple_symbols(monkeypatch, capsys) -> None:
    monkeypatch.setattr(strategy_module, "load_config", lambda _path=None: {})
    monkeypatch.setattr(
        strategy_module,
        "generate_strategy_multi_symbol_experiment",
        lambda symbols, config, **kwargs: {
            "symbols": list(symbols),
            "symbol_count": 2,
            "scope": "multi_symbol_strategy_experiment_v1",
            "start": "2024-01-01",
            "end": "2024-12-31",
            "sample_count": 6,
            "baseline_variant": "baseline",
            "champion_variant": "momentum_tilt",
            "challenger_variant": "momentum_tilt",
            "cross_sectional_supply_summary": {"summary": "已有 3 个日期至少覆盖 2 只标的。", "cohort_count": 3, "unique_symbol_count": 2, "cohorts_ge_2": 3, "cohorts_ge_3": 0, "min_symbols_per_as_of": 2, "max_symbols_per_as_of": 2, "cohort_rows": [{"as_of": "2024-06-28", "symbol_count": 2, "predicted_count": 2, "no_prediction_count": 0}]},
            "variant_rows": [
                {
                    "variant": "baseline",
                    "validated_sample_count": 6,
                    "out_of_sample_status": "watchlist",
                    "cross_sectional_status": "watchlist",
                    "cross_sectional_avg_rank_corr": 0.08,
                    "hit_rate": 0.5,
                    "avg_excess_return": -0.01,
                    "avg_cost_adjusted_directional_return": -0.015,
                    "avg_max_drawdown": -0.05,
                    "dominant_attribution": "weight_misallocation",
                    "hypothesis": "基线。",
                }
            ],
            "notes": ["已扩到多标的 experiment。"],
        },
    )
    monkeypatch.setattr(sys, "argv", ["strategy", "experiment", "600519", "300750", "--variants", "baseline"])

    strategy_module.main()

    captured = capsys.readouterr()
    assert "# Strategy Experiment" in captured.out
    assert "## Same-Day Cohorts" in captured.out
    assert "已扩到多标的 experiment" in captured.out


def test_strategy_experiment_main_supports_batch_source_and_recipe(monkeypatch, capsys) -> None:
    seen = {}

    monkeypatch.setattr(strategy_module, "load_config", lambda _path=None: {})
    monkeypatch.setattr(
        strategy_module,
        "load_strategy_batches",
        lambda _path: {
            "batch_sources": {
                "cn_liquid_core": {
                    "label": "A股核心流动性样本",
                    "symbols": ["600519", "300750", "000333"],
                }
            },
            "cohort_recipes": {
                "monthly_deep": {
                    "label": "低频深样本",
                    "asset_gap_days": 40,
                    "max_samples": 4,
                }
            },
        },
    )
    monkeypatch.setattr(strategy_module, "resolve_project_path", lambda value: value)
    monkeypatch.setattr(strategy_module, "detect_asset_type", lambda symbol, config: "cn_stock")
    monkeypatch.setattr(
        strategy_module,
        "generate_strategy_multi_symbol_experiment",
        lambda symbols, config, **kwargs: seen.update({"symbols": list(symbols), "kwargs": kwargs}) or {
            "symbols": list(symbols),
            "symbol_count": 3,
            "scope": "multi_symbol_strategy_experiment_v1",
            "start": "2024-01-01",
            "end": "2024-12-31",
            "sample_count": 4,
            "asset_gap_days": kwargs["asset_gap_days"],
            "batch_context": kwargs["batch_context"],
            "cohort_recipe": kwargs["cohort_recipe"],
            "baseline_variant": "baseline",
            "champion_variant": "baseline",
            "challenger_variant": "momentum_tilt",
            "cross_sectional_supply_summary": {"summary": "已有 3 个日期至少覆盖 3 只标的。", "cohort_count": 3, "unique_symbol_count": 3, "cohorts_ge_2": 3, "cohorts_ge_3": 3, "min_symbols_per_as_of": 3, "max_symbols_per_as_of": 3, "cohort_rows": [{"as_of": "2024-06-28", "symbol_count": 3, "predicted_count": 3, "no_prediction_count": 0}]},
            "variant_rows": [
                {
                    "variant": "baseline",
                    "validated_sample_count": 4,
                    "out_of_sample_status": "blocked",
                    "cross_sectional_status": "watchlist",
                    "cross_sectional_avg_rank_corr": 0.08,
                    "hit_rate": 0.5,
                    "avg_excess_return": -0.01,
                    "avg_cost_adjusted_directional_return": -0.015,
                    "avg_max_drawdown": -0.05,
                    "dominant_attribution": "weight_misallocation",
                }
            ],
            "notes": ["已扩到多标的 experiment。"],
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["strategy", "experiment", "--batch-source", "cn_liquid_core", "--cohort-recipe", "monthly_deep", "--variants", "baseline"],
    )

    strategy_module.main()

    captured = capsys.readouterr()
    assert seen["symbols"] == ["600519", "300750", "000333"]
    assert seen["kwargs"]["asset_gap_days"] == 40
    assert seen["kwargs"]["max_samples"] == 4
    assert seen["kwargs"]["batch_context"]["key"] == "cn_liquid_core"
    assert seen["kwargs"]["cohort_recipe"]["key"] == "monthly_deep"
    assert "## Batch Source" in captured.out
    assert "## Cohort Recipe" in captured.out
