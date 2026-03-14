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
