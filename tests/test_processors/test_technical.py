"""Tests for the technical indicator engine."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame


def _sample_price_frame(rows: int = 120) -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=rows, freq="D")
    base = np.linspace(10, 30, rows)
    return pd.DataFrame(
        {
            "date": dates,
            "open": base - 0.3,
            "high": base + 0.5,
            "low": base - 0.8,
            "close": base,
            "volume": np.linspace(1000, 3000, rows),
        }
    )


def _wilder_average(values: pd.Series, period: int, *, start: int = 0) -> pd.Series:
    series = pd.to_numeric(values, errors="coerce").astype(float).fillna(0.0)
    result = pd.Series(np.nan, index=series.index, dtype=float)
    seed_end = start + period - 1
    if len(series) <= seed_end:
        return result
    result.iloc[seed_end] = float(series.iloc[start : seed_end + 1].mean())
    for i in range(seed_end + 1, len(series)):
        result.iloc[i] = ((float(result.iloc[i - 1]) * (period - 1)) + float(series.iloc[i])) / period
    return result


def _seeded_recursive_average(values: pd.Series, alpha: float, seed: float) -> pd.Series:
    series = pd.to_numeric(values, errors="coerce").astype(float).fillna(seed)
    result = pd.Series(index=series.index, dtype=float)
    prev = float(seed)
    for idx, value in series.items():
        prev = (1 - alpha) * prev + alpha * float(value)
        result.loc[idx] = prev
    return result


def test_normalize_ohlcv_frame_accepts_chinese_columns():
    frame = pd.DataFrame(
        {
            "日期": pd.date_range("2025-01-01", periods=35, freq="D"),
            "开盘": np.linspace(9, 12, 35),
            "最高": np.linspace(10, 13, 35),
            "最低": np.linspace(8, 11, 35),
            "收盘": np.linspace(9.5, 12.5, 35),
            "成交量": np.linspace(100, 500, 35),
        }
    )
    normalized = normalize_ohlcv_frame(frame)
    assert list(normalized.columns) == ["date", "open", "high", "low", "close", "volume", "amount"]
    assert len(normalized) == 35


def test_normalize_ohlcv_frame_sanitizes_invalid_ohlcv_values():
    frame = pd.DataFrame(
        {
            "日期": pd.date_range("2025-01-01", periods=35, freq="D"),
            "开盘": np.linspace(10, 12, 35),
            "最高": np.linspace(9, 11, 35),
            "最低": np.linspace(11, 13, 35),
            "收盘": np.linspace(10.5, 12.5, 35),
            "成交量": np.linspace(-100, 500, 35),
            "成交额": np.linspace(-1000, 2000, 35),
        }
    )
    normalized = normalize_ohlcv_frame(frame)
    assert (normalized["high"] >= normalized[["open", "close", "low"]].max(axis=1)).all()
    assert (normalized["low"] <= normalized[["open", "close", "high"]].min(axis=1)).all()
    assert (normalized["volume"] >= 0).all()
    assert (normalized["amount"] >= 0).all()


def test_technical_analyzer_generates_bullish_ma_signal():
    analyzer = TechnicalAnalyzer(_sample_price_frame())
    scorecard = analyzer.generate_scorecard()
    assert scorecard["ma_system"]["signal"] == "bullish"
    assert scorecard["macd"]["signal"] in {"bullish", "bearish"}
    assert scorecard["kdj"]["signal"] in {"bullish", "bearish", "neutral"}
    assert scorecard["obv"]["signal"] in {"bullish", "bearish", "neutral"}
    assert scorecard["fibonacci"]["nearest_level"] in {"0.236", "0.382", "0.500", "0.618", "0.786"}
    assert 0 <= scorecard["fibonacci"]["position_pct"] <= 1.2
    assert scorecard["volatility"]["signal"] in {"compressed", "neutral", "expanding"}
    assert "candlestick" in scorecard
    assert "divergence" in scorecard


def test_volume_analysis_detects_breakout_structure():
    frame = _sample_price_frame(80).copy()
    frame.loc[40:, "close"] = np.linspace(20.0, 20.6, 40)
    frame.loc[40:, "open"] = frame.loc[40:, "close"] - 0.1
    frame.loc[40:, "high"] = frame.loc[40:, "close"] + 0.2
    frame.loc[40:, "low"] = frame.loc[40:, "close"] - 0.3
    frame.loc[40:78, "volume"] = 1_000
    frame.loc[79, "close"] = 21.4
    frame.loc[79, "open"] = 20.8
    frame.loc[79, "high"] = 21.7
    frame.loc[79, "low"] = 20.7
    frame.loc[79, "volume"] = 4_000

    analyzer = TechnicalAnalyzer(frame)
    volume = analyzer.volume_analysis()

    assert volume["structure"] == "放量突破"
    assert volume["breakout_20d"] is True
    assert volume["vol_ratio_20"] > 1.2


def test_volatility_profile_identifies_compression():
    rows = 120
    dates = pd.date_range("2025-01-01", periods=rows, freq="D")
    close = np.concatenate([np.linspace(20, 23, 90), np.linspace(23.1, 23.4, 30)])
    wide_range = np.concatenate([np.full(90, 1.8), np.full(30, 0.25)])
    frame = pd.DataFrame(
        {
            "date": dates,
            "open": close - 0.05,
            "high": close + wide_range,
            "low": close - wide_range,
            "close": close,
            "volume": np.linspace(1000, 1800, rows),
        }
    )

    analyzer = TechnicalAnalyzer(frame)
    profile = analyzer.volatility_profile()

    assert profile["signal"] == "compressed"
    assert profile["atr_ratio_20"] < 1
    assert profile["boll_width_percentile"] <= 0.35


def test_rsi_uses_wilder_seed_average():
    frame = _sample_price_frame(80).copy()
    frame.loc[::6, "close"] -= 0.9
    frame.loc[::5, "close"] += 1.1
    analyzer = TechnicalAnalyzer(frame)
    result = analyzer.rsi(period=14)

    close = frame["close"]
    delta = close.diff()
    gain = delta.clip(lower=0).fillna(0.0)
    loss = (-delta.clip(upper=0)).fillna(0.0)
    avg_gain = _wilder_average(gain, 14, start=1)
    avg_loss = _wilder_average(loss, 14, start=1)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    expected = float((100 - (100 / (1 + rs))).clip(lower=0, upper=100).fillna(50.0).iloc[-1])

    assert result["RSI"] == pytest.approx(expected, rel=1e-6)


def test_dmi_uses_wilder_smoothing_for_adx():
    frame = _sample_price_frame(80).copy()
    frame["high"] = frame["close"] + np.linspace(0.6, 1.2, 80)
    frame["low"] = frame["close"] - np.linspace(0.5, 1.1, 80)
    frame.loc[::7, "high"] += 0.8
    frame.loc[::5, "low"] -= 0.6

    analyzer = TechnicalAnalyzer(frame)
    dmi = analyzer.dmi(period=14)

    high = frame["high"]
    low = frame["low"]
    close = frame["close"]
    plus_move = high.diff()
    minus_move = -low.diff()
    plus_dm = plus_move.where((plus_move > minus_move) & (plus_move > 0), 0.0).fillna(0.0)
    minus_dm = minus_move.where((minus_move > plus_move) & (minus_move > 0), 0.0).fillna(0.0)
    tr = pd.concat(
        [high - low, (high - close.shift()).abs(), (low - close.shift()).abs()],
        axis=1,
    ).max(axis=1).fillna(0.0)
    atr = _wilder_average(tr, 14)
    plus_di = 100 * (_wilder_average(plus_dm, 14) / atr.replace(0, np.nan))
    minus_di = 100 * (_wilder_average(minus_dm, 14) / atr.replace(0, np.nan))
    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)).clip(lower=0, upper=100)
    adx = pd.Series(np.nan, index=dx.index, dtype=float)
    first_dx = 13
    adx_seed_end = first_dx + 13
    adx.iloc[adx_seed_end] = float(dx.iloc[first_dx : adx_seed_end + 1].dropna().mean())
    for i in range(adx_seed_end + 1, len(dx)):
        adx.iloc[i] = ((float(adx.iloc[i - 1]) * 13) + float(dx.iloc[i])) / 14
    expected_adx = float(adx.fillna(0).iloc[-1])

    assert dmi["ADX"] == pytest.approx(expected_adx, rel=1e-6)


def test_kdj_uses_seeded_recursive_average():
    frame = _sample_price_frame(60).copy()
    frame.loc[::4, "high"] += 0.6
    frame.loc[::3, "low"] -= 0.4
    analyzer = TechnicalAnalyzer(frame)
    result = analyzer.kdj(period=9, smooth_k=3, smooth_d=3)

    high_n = frame["high"].rolling(9).max()
    low_n = frame["low"].rolling(9).min()
    rsv = ((frame["close"] - low_n) / (high_n - low_n).replace(0, np.nan) * 100).clip(lower=0, upper=100).fillna(50)
    k = _seeded_recursive_average(rsv, alpha=1 / 3, seed=50.0).clip(lower=0, upper=100)
    d = _seeded_recursive_average(k, alpha=1 / 3, seed=50.0).clip(lower=0, upper=100)
    j = 3 * k - 2 * d

    assert result["K"] == pytest.approx(float(k.iloc[-1]), rel=1e-6)
    assert result["D"] == pytest.approx(float(d.iloc[-1]), rel=1e-6)
    assert result["J"] == pytest.approx(float(j.iloc[-1]), rel=1e-6)


def test_indicator_series_matches_latest_scorecard_values():
    analyzer = TechnicalAnalyzer(_sample_price_frame(90))
    series = analyzer.indicator_series()
    scorecard = analyzer.generate_scorecard()

    assert float(series["macd_dif"].iloc[-1]) == pytest.approx(scorecard["macd"]["DIF"], rel=1e-6)
    assert float(series["macd_dea"].iloc[-1]) == pytest.approx(scorecard["macd"]["DEA"], rel=1e-6)
    assert float(series["rsi"].iloc[-1]) == pytest.approx(scorecard["rsi"]["RSI"], rel=1e-6)
    assert float(series["adx"].iloc[-1]) == pytest.approx(scorecard["dmi"]["ADX"], rel=1e-6)
    assert float(series["plus_di"].iloc[-1]) == pytest.approx(scorecard["dmi"]["DI+"], rel=1e-6)
    assert float(series["minus_di"].iloc[-1]) == pytest.approx(scorecard["dmi"]["DI-"], rel=1e-6)
    assert float(series["kdj_k"].iloc[-1]) == pytest.approx(scorecard["kdj"]["K"], rel=1e-6)
    assert float(series["kdj_d"].iloc[-1]) == pytest.approx(scorecard["kdj"]["D"], rel=1e-6)
    assert float(series["kdj_j"].iloc[-1]) == pytest.approx(scorecard["kdj"]["J"], rel=1e-6)
    assert float(series["obv"].iloc[-1]) == pytest.approx(scorecard["obv"]["OBV"], rel=1e-6)


def test_divergence_analysis_detects_bullish_macd_divergence():
    close = np.concatenate(
        [
            np.linspace(100, 80, 12),
            np.linspace(80, 96, 10)[1:],
            np.linspace(96, 78.5, 22)[1:],
            np.linspace(78.5, 88, 10)[1:],
        ]
    )
    volume = np.concatenate(
        [
            np.full(12, 2200.0),
            np.full(9, 1500.0),
            np.full(21, 900.0),
            np.full(9, 1200.0),
        ]
    )
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=len(close), freq="D"),
            "open": close - 0.3,
            "high": close + 0.6,
            "low": close - 0.7,
            "close": close,
            "volume": volume,
        }
    )

    divergence = TechnicalAnalyzer(frame).divergence_analysis()

    assert divergence["signal"] == "bullish"
    assert divergence["kind"] == "底背离"
    assert "MACD" in divergence["indicators"]


def test_divergence_analysis_detects_bearish_macd_divergence():
    close = np.concatenate(
        [
            np.linspace(50, 72, 14),
            np.linspace(72, 64, 8)[1:],
            np.linspace(64, 73, 20)[1:],
            np.linspace(73, 68, 10)[1:],
        ]
    )
    volume = np.concatenate(
        [
            np.full(14, 1800.0),
            np.full(7, 1200.0),
            np.full(19, 700.0),
            np.full(9, 900.0),
        ]
    )
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=len(close), freq="D"),
            "open": close - 0.3,
            "high": close + 0.6,
            "low": close - 0.7,
            "close": close,
            "volume": volume,
        }
    )

    divergence = TechnicalAnalyzer(frame).divergence_analysis()

    assert divergence["signal"] == "bearish"
    assert divergence["kind"] == "顶背离"
    assert "MACD" in divergence["indicators"]


def test_candlestick_patterns_detect_bullish_engulfing():
    frame = _sample_price_frame(35).copy()
    close = [18.0, 17.5, 17.0, 16.6, 16.2, 15.8, 15.2, 14.8, 14.3, 13.8, 13.0, 14.5]
    open_ = [18.2, 17.7, 17.2, 16.9, 16.5, 16.1, 15.5, 15.0, 14.5, 14.3, 13.7, 12.8]
    high = [max(o, c) + 0.3 for o, c in zip(open_, close)]
    low = [min(o, c) - 0.3 for o, c in zip(open_, close)]
    frame.loc[23:, "close"] = close
    frame.loc[23:, "open"] = open_
    frame.loc[23:, "high"] = high
    frame.loc[23:, "low"] = low
    frame.loc[23:, "volume"] = np.linspace(1000, 1500, len(close))

    patterns = TechnicalAnalyzer(frame).candlestick_patterns()

    assert "bullish_engulfing" in patterns


def test_candlestick_patterns_detect_evening_star():
    frame = _sample_price_frame(35).copy()
    close = [10.0, 10.4, 10.8, 11.2, 11.7, 12.1, 12.5, 12.9, 13.3, 14.4, 14.6, 13.5]
    open_ = [9.8, 10.1, 10.5, 10.9, 11.4, 11.8, 12.2, 12.6, 13.0, 13.2, 14.55, 14.4]
    high = [max(o, c) + 0.25 for o, c in zip(open_, close)]
    low = [min(o, c) - 0.25 for o, c in zip(open_, close)]
    frame.loc[23:, "close"] = close
    frame.loc[23:, "open"] = open_
    frame.loc[23:, "high"] = high
    frame.loc[23:, "low"] = low
    frame.loc[23:, "volume"] = np.linspace(1000, 1500, len(close))

    patterns = TechnicalAnalyzer(frame).candlestick_patterns()

    assert "evening_star" in patterns


def test_candlestick_patterns_detect_bullish_harami():
    base = list(np.linspace(21.0, 18.8, 38))
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=40, freq="B"),
            "open": [price + 0.1 for price in base] + [18.7, 18.25],
            "high": [price + 0.3 for price in base] + [18.9, 18.55],
            "low": [price - 0.3 for price in base] + [17.85, 18.05],
            "close": base + [18.0, 18.45],
            "volume": [1_000_000] * 40,
        }
    )

    patterns = TechnicalAnalyzer(frame).candlestick_patterns()

    assert "bullish_harami" in patterns


def test_candlestick_patterns_detect_hanging_man():
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=40, freq="B"),
            "open": list(np.linspace(10.0, 14.0, 39)) + [14.45],
            "high": list(np.linspace(10.3, 14.3, 39)) + [14.55],
            "low": list(np.linspace(9.8, 13.8, 39)) + [13.9],
            "close": list(np.linspace(10.1, 14.4, 39)) + [14.48],
            "volume": [1_000_000] * 40,
        }
    )

    patterns = TechnicalAnalyzer(frame).candlestick_patterns()

    assert "hanging_man" in patterns


def test_ma_system_omits_unavailable_long_averages():
    analyzer = TechnicalAnalyzer(_sample_price_frame(35))
    ma = analyzer.ma_system([5, 10, 20, 60])
    assert "MA5" in ma["mas"]
    assert "MA60" not in ma["mas"]


def test_setup_analysis_detects_bullish_false_break():
    # 构造：日内触及近期高点但收盘回落（看涨假突破）
    rows = 80
    dates = pd.date_range("2025-01-01", periods=rows, freq="D")
    close = np.linspace(10.0, 14.0, rows)
    high = close + 0.4
    low = close - 0.4
    open_ = close - 0.1
    # 近期高点约为 13.6（第 70 根附近）
    # 最后一根：日内突破近期高点，但收盘回落
    high[-1] = float(high[-2]) + 0.5   # 日内突破
    close[-1] = float(close[-2]) - 0.1  # 收盘回落
    open_[-1] = float(close[-2]) + 0.1
    low[-1] = float(close[-1]) - 0.2
    frame = pd.DataFrame({"date": dates, "open": open_, "high": high, "low": low, "close": close, "volume": np.full(rows, 1000.0)})
    result = TechnicalAnalyzer(frame).setup_analysis()
    assert result["false_break"]["kind"] == "bullish_false_break"
    assert result["signal"] in {"bearish", "neutral"}


def test_setup_analysis_detects_compression_breakout():
    # 构造：前期波动压缩，最后一根放量上涨
    rows = 120
    dates = pd.date_range("2025-01-01", periods=rows, freq="D")
    # 前 90 根正常波动，后 30 根压缩（包括最后一根）
    close = np.concatenate([np.linspace(20.0, 23.0, 90), np.linspace(23.1, 23.4, 30)])
    wide_range = np.concatenate([np.full(90, 1.5), np.full(30, 0.2)])
    volume = np.concatenate([np.full(90, 1000.0), np.full(29, 600.0), [3000.0]])
    # 最后一根：close 上涨但 high/low range 仍小（ATR 不会立刻扩张）
    close[-1] = 23.75  # 涨幅约 1.5%，满足 >= 0.015
    frame = pd.DataFrame({
        "date": dates,
        "open": close - 0.05,
        "high": close + wide_range,
        "low": close - wide_range,
        "close": close,
        "volume": volume,
    })
    result = TechnicalAnalyzer(frame).setup_analysis()
    assert result["compression_setup"]["kind"] == "compression_breakout"
    assert result["compression_setup"]["was_compressed"] is True
    assert result["compression_setup"]["vol_ratio_20"] >= 1.5


def test_setup_analysis_detects_support_breakdown():
    # 构造：前一日跌破近期低点，最后一根反弹但未收复
    rows = 80
    dates = pd.date_range("2025-01-01", periods=rows, freq="D")
    close = np.linspace(20.0, 15.0, rows)  # 下跌趋势
    high = close + 0.3
    low = close - 0.3
    open_ = close + 0.1
    # 近期低点约为 15.0，前一日跌破，最后一根小幅反弹但未收复
    close[-2] = 14.5   # 跌破支撑
    close[-1] = 14.7   # 反弹但仍在支撑下方
    high[-1] = 14.9
    low[-1] = 14.4
    open_[-1] = 14.5
    frame = pd.DataFrame({"date": dates, "open": open_, "high": high, "low": low, "close": close, "volume": np.full(rows, 1000.0)})
    result = TechnicalAnalyzer(frame).setup_analysis()
    assert result["support_setup"]["kind"] in {"failed_recovery", "breakdown_continuation", "breakdown_watching"}


def test_setup_analysis_included_in_scorecard():
    analyzer = TechnicalAnalyzer(_sample_price_frame(80))
    scorecard = analyzer.generate_scorecard()
    assert "setup" in scorecard
    setup = scorecard["setup"]
    assert "signal" in setup
    assert setup["signal"] in {"bullish", "bearish", "neutral"}
    assert "false_break" in setup
    assert "support_setup" in setup
    assert "compression_setup" in setup
