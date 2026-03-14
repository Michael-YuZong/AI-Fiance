from __future__ import annotations

import pandas as pd

import src.processors.strategy as strategy_module
from src.processors.strategy import (
    attribute_strategy_rows,
    build_strategy_prediction_from_analysis,
    generate_strategy_experiment,
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
            "technical": {"score": 76, "summary": "趋势保持偏强。"},
            "relative_strength": {"score": 73, "summary": "相对强弱领先。"},
            "catalyst": {"score": 61, "summary": "事件支持仍在。"},
            "fundamental": {"score": 69, "summary": "基本面质量较稳。"},
            "risk": {"score": 66, "summary": "风险收益比尚可。"},
            "macro": {"score": 55, "summary": "宏观不逆风。"},
            "seasonality": {"score": 51, "summary": "季节性中性略正。"},
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
    as_of_values = [row["as_of"] for row in payload["rows"]]
    assert as_of_values == sorted(as_of_values)


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
    assert summary["validated_rows"] == 1
    assert "单标的时间序列口径" in summary["notes"][0]


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
    assert payload["variant_rows"][0]["variant"] in {"baseline", "defensive_tilt"}
