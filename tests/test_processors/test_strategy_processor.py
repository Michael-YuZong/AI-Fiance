from __future__ import annotations

import pandas as pd

from src.processors.strategy import build_strategy_prediction_from_analysis


def _history(days: int = 260, *, amount: float = 1.6e8) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.bdate_range("2025-01-01", periods=days),
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
