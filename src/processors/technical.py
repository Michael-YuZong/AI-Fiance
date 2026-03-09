"""Technical indicator engine."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

import numpy as np
import pandas as pd


COLUMN_ALIASES = {
    "date": ("date", "Date", "日期", "时间", "datetime", "Datetime"),
    "open": ("open", "Open", "开盘", "开盘价"),
    "high": ("high", "High", "最高", "最高价"),
    "low": ("low", "Low", "最低", "最低价"),
    "close": ("close", "Close", "收盘", "收盘价", "最新价"),
    "volume": ("volume", "Volume", "成交量"),
    "amount": ("amount", "Amount", "成交额"),
}


def normalize_ohlcv_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize mixed-source market data to a common OHLCV schema."""
    if df is None or df.empty:
        raise ValueError("Price dataframe is empty")

    frame = df.copy()
    if isinstance(frame.index, pd.DatetimeIndex) and "date" not in frame.columns:
        index_name = frame.index.name or "date"
        frame = frame.reset_index().rename(columns={index_name: "date"})
    elif not isinstance(frame.index, pd.RangeIndex) and "date" not in frame.columns:
        frame = frame.reset_index()

    rename_map = {}
    for target, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in frame.columns:
                rename_map[alias] = target
                break
    frame = frame.rename(columns=rename_map)

    required_columns = {"date", "open", "high", "low", "close"}
    missing = required_columns - set(frame.columns)
    if missing:
        raise ValueError(f"Price dataframe missing columns: {sorted(missing)}")

    if "volume" not in frame.columns:
        frame["volume"] = 0.0
    if "amount" not in frame.columns:
        frame["amount"] = np.nan

    frame["date"] = pd.to_datetime(frame["date"])
    for column in ("open", "high", "low", "close", "volume", "amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.dropna(subset=["date", "open", "high", "low", "close"])
    frame = frame.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
    return frame[["date", "open", "high", "low", "close", "volume", "amount"]]


class TechnicalAnalyzer:
    """技术指标计算。"""

    def __init__(self, df: pd.DataFrame):
        self.df = normalize_ohlcv_frame(df)
        if len(self.df) < 30:
            raise ValueError("At least 30 rows are required for technical analysis")

    def macd(self, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, float]:
        close = self.df["close"]
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        dif = ema_fast - ema_slow
        dea = dif.ewm(span=signal, adjust=False).mean()
        hist = 2 * (dif - dea)
        return {
            "DIF": float(dif.iloc[-1]),
            "DEA": float(dea.iloc[-1]),
            "HIST": float(hist.iloc[-1]),
            "signal": "bullish" if dif.iloc[-1] > dea.iloc[-1] else "bearish",
        }

    def rsi(self, period: int = 14, overbought: int = 70, oversold: int = 30) -> Dict[str, float]:
        delta = self.df["close"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        value = float(rsi.fillna(50).iloc[-1])
        if value > overbought:
            signal = "overbought"
        elif value < oversold:
            signal = "oversold"
        else:
            signal = "neutral"
        return {"RSI": value, "signal": signal}

    def bollinger(self, period: int = 20, std: int = 2) -> Dict[str, float]:
        mid = self.df["close"].rolling(period).mean()
        std_dev = self.df["close"].rolling(period).std(ddof=0)
        upper = mid + std * std_dev
        lower = mid - std * std_dev
        price = self.df["close"].iloc[-1]
        band_width = upper.iloc[-1] - lower.iloc[-1]
        pct_b = 0.5 if band_width == 0 or pd.isna(band_width) else (price - lower.iloc[-1]) / band_width
        signal = "near_upper" if pct_b > 0.8 else "near_lower" if pct_b < 0.2 else "neutral"
        return {
            "MID": float(mid.iloc[-1]),
            "UPPER": float(upper.iloc[-1]),
            "LOWER": float(lower.iloc[-1]),
            "%B": float(pct_b),
            "signal": signal,
        }

    def kdj(
        self,
        period: int = 9,
        smooth_k: int = 3,
        smooth_d: int = 3,
        overbought: int = 80,
        oversold: int = 20,
    ) -> Dict[str, float]:
        high_n = self.df["high"].rolling(period).max()
        low_n = self.df["low"].rolling(period).min()
        denominator = (high_n - low_n).replace(0, np.nan)
        rsv = ((self.df["close"] - low_n) / denominator * 100).fillna(50)
        k = rsv.ewm(alpha=1 / max(smooth_k, 1), adjust=False).mean()
        d = k.ewm(alpha=1 / max(smooth_d, 1), adjust=False).mean()
        j = 3 * k - 2 * d

        k_latest = float(k.iloc[-1])
        d_latest = float(d.iloc[-1])
        j_latest = float(j.iloc[-1])
        cross = "golden_cross" if k_latest > d_latest else "death_cross" if k_latest < d_latest else "neutral"
        zone = "overbought" if max(k_latest, d_latest, j_latest) >= overbought else "oversold" if min(k_latest, d_latest, j_latest) <= oversold else "neutral"
        signal = "bullish" if cross == "golden_cross" else "bearish" if cross == "death_cross" else "neutral"

        return {
            "K": k_latest,
            "D": d_latest,
            "J": j_latest,
            "cross": cross,
            "zone": zone,
            "signal": signal,
        }

    def dmi(self, period: int = 14, adx_strong: int = 25) -> Dict[str, float]:
        high = self.df["high"]
        low = self.df["low"]
        close = self.df["close"]

        plus_move = high.diff()
        minus_move = -low.diff()
        plus_dm = plus_move.where((plus_move > minus_move) & (plus_move > 0), 0.0)
        minus_dm = minus_move.where((minus_move > plus_move) & (minus_move > 0), 0.0)

        tr_components = pd.concat(
            [high - low, (high - close.shift()).abs(), (low - close.shift()).abs()],
            axis=1,
        )
        tr = tr_components.max(axis=1)
        atr = tr.rolling(period).mean()
        plus_di = 100 * (plus_dm.rolling(period).mean() / atr.replace(0, np.nan))
        minus_di = 100 * (minus_dm.rolling(period).mean() / atr.replace(0, np.nan))
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
        adx = dx.rolling(period).mean().fillna(0)

        if adx.iloc[-1] > adx_strong and plus_di.iloc[-1] > minus_di.iloc[-1]:
            signal = "bullish_trend"
        elif adx.iloc[-1] > adx_strong and plus_di.iloc[-1] < minus_di.iloc[-1]:
            signal = "bearish_trend"
        else:
            signal = "weak_trend"

        return {
            "DI+": float(plus_di.fillna(0).iloc[-1]),
            "DI-": float(minus_di.fillna(0).iloc[-1]),
            "ADX": float(adx.iloc[-1]),
            "signal": signal,
        }

    def obv(self, period: int = 20) -> Dict[str, float]:
        close = self.df["close"]
        volume = self.df["volume"].fillna(0)
        direction = np.sign(close.diff().fillna(0))
        obv = (direction * volume).cumsum()
        obv_ma = obv.rolling(period).mean()
        slope_5d = obv.diff(5).fillna(0)

        latest_obv = float(obv.iloc[-1])
        latest_ma = float(obv_ma.fillna(obv).iloc[-1])
        latest_slope = float(slope_5d.iloc[-1])

        if latest_obv > latest_ma and latest_slope >= 0:
            signal = "bullish"
        elif latest_obv < latest_ma and latest_slope <= 0:
            signal = "bearish"
        else:
            signal = "neutral"

        return {
            "OBV": latest_obv,
            "MA": latest_ma,
            "slope_5d": latest_slope,
            "signal": signal,
        }

    def fibonacci(self, lookback: int = 60) -> Dict[str, Any]:
        window = self.df.tail(max(lookback, 20)).copy()
        high = float(window["high"].max())
        low = float(window["low"].min())
        price = float(window["close"].iloc[-1])
        range_value = high - low
        if range_value == 0:
            levels = {key: price for key in ("0.236", "0.382", "0.500", "0.618", "0.786")}
            return {
                "swing_high": high,
                "swing_low": low,
                "levels": levels,
                "position_pct": 0.5,
                "nearest_level": "0.500",
                "signal": "neutral",
            }

        levels = {
            "0.236": low + range_value * 0.236,
            "0.382": low + range_value * 0.382,
            "0.500": low + range_value * 0.500,
            "0.618": low + range_value * 0.618,
            "0.786": low + range_value * 0.786,
        }
        position_pct = (price - low) / range_value
        nearest_level = min(levels, key=lambda key: abs(levels[key] - price))

        if position_pct >= 0.786:
            signal = "upper_zone"
        elif position_pct >= 0.618:
            signal = "strong_zone"
        elif position_pct <= 0.236:
            signal = "lower_zone"
        else:
            signal = "mid_zone"

        return {
            "swing_high": high,
            "swing_low": low,
            "levels": {key: float(value) for key, value in levels.items()},
            "position_pct": float(position_pct),
            "nearest_level": nearest_level,
            "signal": signal,
        }

    def volume_analysis(self) -> Dict[str, float]:
        vol = self.df["volume"].fillna(0)
        ma5 = vol.rolling(5).mean()
        ma20 = vol.rolling(20).mean()
        denominator = ma5.iloc[-1] if ma5.iloc[-1] not in (0, np.nan) else np.nan
        ratio = 1.0 if pd.isna(denominator) or denominator == 0 else float(vol.iloc[-1] / denominator)
        signal = "heavy_volume" if ratio > 1.5 else "light_volume" if ratio < 0.6 else "normal"
        return {
            "volume": float(vol.iloc[-1]),
            "MA5": float(ma5.fillna(0).iloc[-1]),
            "MA20": float(ma20.fillna(0).iloc[-1]),
            "vol_ratio": ratio,
            "signal": signal,
        }

    def candlestick_patterns(self) -> list:
        """识别最近一根 K 线的形态特征。"""
        row = self.df.iloc[-1]
        open_price, high, low, close = row["open"], row["high"], row["low"], row["close"]
        body = abs(close - open_price)
        upper_shadow = high - max(open_price, close)
        lower_shadow = min(open_price, close) - low
        total_range = high - low
        patterns = []
        if total_range > 0:
            if lower_shadow > 2 * body and upper_shadow < max(body * 0.3, total_range * 0.05):
                patterns.append("hammer" if close >= open_price else "inverted_hammer")
            if upper_shadow > 2 * body and lower_shadow < max(body * 0.3, total_range * 0.05):
                patterns.append("shooting_star")
            if body <= total_range * 0.1:
                patterns.append("doji")
            if body >= total_range * 0.7:
                patterns.append("marubozu")
        return patterns

    def ma_system(self, periods: Optional[Iterable[int]] = None) -> Dict[str, Any]:
        ma_periods = list(periods or [5, 10, 20, 30, 60])
        mas = {}
        for period in ma_periods:
            mas[f"MA{period}"] = float(self.df["close"].rolling(period).mean().iloc[-1])
        price = float(self.df["close"].iloc[-1])
        above_count = sum(1 for value in mas.values() if price > value)
        if above_count >= max(len(mas) - 1, 1):
            signal = "bullish"
        elif above_count <= 1:
            signal = "bearish"
        else:
            signal = "neutral"
        alignment = {"bullish": "bullish", "bearish": "bearish"}.get(signal, "mixed")
        return {"mas": mas, "alignment": alignment, "signal": signal}

    def generate_scorecard(self, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        technical_config = config or {}
        return {
            "macd": self.macd(**technical_config.get("macd", {})),
            "rsi": self.rsi(**technical_config.get("rsi", {})),
            "bollinger": self.bollinger(**technical_config.get("bollinger", {})),
            "kdj": self.kdj(**technical_config.get("kdj", {})),
            "dmi": self.dmi(**technical_config.get("dmi", {})),
            "obv": self.obv(**technical_config.get("obv", {})),
            "fibonacci": self.fibonacci(**technical_config.get("fibonacci", {})),
            "volume": self.volume_analysis(),
            "candlestick": self.candlestick_patterns(),
            "ma_system": self.ma_system(technical_config.get("ma_periods")),
        }
