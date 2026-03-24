"""Tests for shared market-analysis payloads."""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.processors.market_analysis import build_market_analysis


def _trend_frame(
    *,
    rows: int = 780,
    start: float = 10.0,
    end: float = 30.0,
    volume_start: float = 1000.0,
    volume_end: float = 2500.0,
) -> pd.DataFrame:
    dates = pd.date_range("2023-01-02", periods=rows, freq="B")
    close = np.linspace(start, end, rows)
    volume = np.linspace(volume_start, volume_end, rows)
    return pd.DataFrame(
        {
            "date": dates,
            "open": close - 0.2,
            "high": close + 0.6,
            "low": close - 0.7,
            "close": close,
            "volume": volume,
            "amount": close * volume * 100,
        }
    )


def _repair_frame(rows: int = 780) -> pd.DataFrame:
    dates = pd.date_range("2023-01-02", periods=rows, freq="B")
    first_half = np.linspace(40.0, 19.0, rows // 2)
    second_half = np.linspace(19.2, 31.5, rows - len(first_half))
    close = np.concatenate([first_half, second_half])
    volume = np.concatenate([np.linspace(1800, 900, rows // 2), np.linspace(1000, 2600, rows - rows // 2)])
    return pd.DataFrame(
        {
            "date": dates,
            "open": close - 0.2,
            "high": close + 0.7,
            "low": close - 0.8,
            "close": close,
            "volume": volume,
            "amount": close * volume * 100,
        }
    )


def test_build_market_analysis_returns_core_signal_tables(monkeypatch) -> None:
    frames = {
        "sh000001": _trend_frame(start=3000.0, end=3450.0, volume_start=2e8, volume_end=3e8),
        "sh000300": _trend_frame(start=3500.0, end=4020.0, volume_start=1.5e8, volume_end=2.7e8),
        "sz399006": _repair_frame(),
    }

    monkeypatch.setattr(
        "src.processors.market_analysis.fetch_asset_history",
        lambda symbol, asset_type, config: frames[symbol],  # noqa: ARG005
    )

    overview = {
        "domestic_indices": [
            {"name": "上证指数", "latest": 3450.0, "change_pct": 0.008},
            {"name": "沪深300", "latest": 4020.0, "change_pct": 0.011},
            {"name": "创业板指", "latest": 31.5, "change_pct": 0.013},
        ],
        "breadth": {"up_count": 3200, "down_count": 1600, "flat_count": 120, "turnover": 14200.0},
    }
    pulse = {
        "zt_pool": pd.DataFrame([{"所属行业": "银行"}, {"所属行业": "电力"}, {"所属行业": "电力"}]),
        "strong_pool": pd.DataFrame([{"所属行业": "银行"}, {"所属行业": "电力"}]),
        "dt_pool": pd.DataFrame([{"所属行业": "半导体"}]),
        "prev_zt_pool": pd.DataFrame({"涨跌幅": [2.6, 1.8, 0.9]}),
    }
    drivers = {
        "industry_spot": pd.DataFrame(
            [
                {"名称": "银行", "涨跌幅": 2.4},
                {"名称": "电力", "涨跌幅": 2.1},
                {"名称": "煤炭", "涨跌幅": 1.7},
                {"名称": "半导体", "涨跌幅": -1.3},
                {"名称": "传媒", "涨跌幅": -1.1},
            ]
        ),
        "concept_spot": pd.DataFrame(
            [
                {"名称": "中特估", "涨跌幅": 2.0},
                {"名称": "电网设备", "涨跌幅": 1.8},
                {"名称": "AI算力", "涨跌幅": 1.3},
            ]
        ),
    }

    analysis = build_market_analysis({}, overview, pulse, drivers)

    assert len(analysis["index_rows"]) == 3
    assert analysis["index_rows"][1][0] == "中证核心(沪深300)"
    assert analysis["market_signal_rows"][0][0] == "市场宽度"
    assert analysis["market_signal_rows"][1][0] == "成交量能"
    assert analysis["market_signal_rows"][2][0] == "情绪极端"
    assert any("中证" in line for line in analysis["summary_lines"])
    assert analysis["rotation_rows"][0][0] == "行业"
    assert "防守占优" in analysis["rotation_rows"][0][3]
    assert any("情绪极端指标" in line for line in analysis["market_signal_lines"])


def test_build_market_analysis_handles_missing_history_and_sparse_breadth(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.market_analysis.fetch_asset_history",
        lambda symbol, asset_type, config: (_ for _ in ()).throw(RuntimeError("history unavailable")),  # noqa: ARG005
    )

    analysis = build_market_analysis(
        {},
        {"domestic_indices": [], "breadth": {}},
        {"zt_pool": pd.DataFrame(), "strong_pool": pd.DataFrame(), "dt_pool": pd.DataFrame(), "prev_zt_pool": pd.DataFrame()},
        {},
    )

    assert analysis["index_rows"][0][3] == "数据缺失"
    assert analysis["market_signal_rows"][0][1] == "N/A"
    assert analysis["market_signal_rows"][1][2] == "量能缺失"
    assert analysis["rotation_lines"][0].startswith("当前轮动数据有限")
