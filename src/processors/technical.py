"""Technical indicator engine."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

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
    attrs = dict(getattr(df, "attrs", {}) or {})
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

    # Defensively sanitize malformed OHLCV rows from mixed vendors.
    frame[["open", "high", "low", "close"]] = frame[["open", "high", "low", "close"]].where(
        frame[["open", "high", "low", "close"]] > 0
    )
    frame["high"] = frame[["open", "high", "low", "close"]].max(axis=1)
    frame["low"] = frame[["open", "high", "low", "close"]].min(axis=1)
    frame["volume"] = frame["volume"].clip(lower=0)
    frame["amount"] = frame["amount"].clip(lower=0)

    frame = frame.dropna(subset=["date", "open", "high", "low", "close"])
    frame = frame.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
    normalized = frame[["date", "open", "high", "low", "close", "volume", "amount"]]
    normalized.attrs.update(attrs)
    return normalized


def _last_valid(
    series: pd.Series,
    default: float = 0.0,
    *,
    lower: float | None = None,
    upper: float | None = None,
) -> float:
    value = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if value.empty:
        return default
    result = float(value.iloc[-1])
    if lower is not None:
        result = max(lower, result)
    if upper is not None:
        result = min(upper, result)
    return result


def _wilder_average(values: pd.Series, period: int, *, start: int = 0) -> pd.Series:
    series = pd.to_numeric(values, errors="coerce").astype(float).fillna(0.0)
    result = pd.Series(np.nan, index=series.index, dtype=float)
    seed_end = start + period - 1
    if period <= 0 or len(series) <= seed_end:
        return result

    result.iloc[seed_end] = float(series.iloc[start : seed_end + 1].mean())
    for i in range(seed_end + 1, len(series)):
        prev = float(result.iloc[i - 1])
        result.iloc[i] = ((prev * (period - 1)) + float(series.iloc[i])) / period
    return result


def _seeded_recursive_average(values: pd.Series, alpha: float, seed: float) -> pd.Series:
    series = pd.to_numeric(values, errors="coerce").astype(float).fillna(seed)
    result = pd.Series(index=series.index, dtype=float)
    prev = float(seed)
    for idx, value in series.items():
        prev = (1 - alpha) * prev + alpha * float(value)
        result.loc[idx] = prev
    return result


def _pivot_extrema(series: pd.Series, *, kind: str, order: int = 3, lookback: int = 120) -> List[Tuple[int, float]]:
    values = pd.to_numeric(series, errors="coerce").astype(float).to_numpy()
    if len(values) < max(order * 2 + 1, 10):
        return []

    start = max(order, len(values) - max(lookback, order * 4))
    end = len(values) - order
    pivots: List[Tuple[int, float]] = []

    def _valid(window: np.ndarray) -> np.ndarray:
        return window[~np.isnan(window)]

    for idx in range(start, end):
        current = values[idx]
        if np.isnan(current):
            continue
        left = _valid(values[idx - order : idx])
        right = _valid(values[idx + 1 : idx + order + 1])
        if len(left) < order or len(right) < order:
            continue
        if kind == "high":
            if current > float(left.max()) and current >= float(right.max()):
                pivots.append((idx, float(current)))
        else:
            if current < float(left.min()) and current <= float(right.min()):
                pivots.append((idx, float(current)))

    last_idx = len(values) - 1
    trailing = _valid(values[max(start, last_idx - order) : last_idx])
    last_value = values[last_idx]
    if len(trailing) >= max(1, order - 1) and not np.isnan(last_value):
        if kind == "high" and last_value > float(trailing.max()):
            if not pivots or pivots[-1][0] != last_idx:
                pivots.append((last_idx, float(last_value)))
        if kind == "low" and last_value < float(trailing.min()):
            if not pivots or pivots[-1][0] != last_idx:
                pivots.append((last_idx, float(last_value)))

    return pivots[-6:]


def _indicator_divergence_threshold(indicator: str, previous: float, current: float) -> float:
    name = str(indicator).upper()
    magnitude = max(abs(previous), abs(current), 1e-9)
    if name == "RSI":
        return 2.5
    if name == "MACD":
        return max(0.02, magnitude * 0.12)
    if name == "OBV":
        return max(1.0, magnitude * 0.02)
    return max(0.01, magnitude * 0.05)


def _candle_snapshot(row: pd.Series) -> Dict[str, float | bool]:
    open_price = float(row["open"])
    high = float(row["high"])
    low = float(row["low"])
    close = float(row["close"])
    total_range = max(high - low, 1e-9)
    body = abs(close - open_price)
    upper_shadow = max(0.0, high - max(open_price, close))
    lower_shadow = max(0.0, min(open_price, close) - low)
    midpoint = (open_price + close) / 2
    return {
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "body": body,
        "upper_shadow": upper_shadow,
        "lower_shadow": lower_shadow,
        "range": total_range,
        "body_ratio": body / total_range if total_range > 0 else 0.0,
        "midpoint": midpoint,
        "bullish": close > open_price,
        "bearish": close < open_price,
    }


def _prior_return(close: pd.Series, bars: int, *, window: int = 5) -> float:
    if len(close) <= bars + 1:
        return 0.0
    end = len(close) - bars - 1
    start = max(0, end - window)
    if end <= start:
        return 0.0
    start_price = float(close.iloc[start])
    end_price = float(close.iloc[end])
    if start_price <= 0:
        return 0.0
    return end_price / start_price - 1.0


def _detect_price_indicator_divergence(
    *,
    price_series: pd.Series,
    indicator_series: pd.Series,
    dates: Sequence[pd.Timestamp],
    indicator_name: str,
    mode: str,
    order: int = 3,
    lookback: int = 120,
) -> Optional[Dict[str, Any]]:
    pivot_kind = "low" if mode == "bullish" else "high"
    pivots = _pivot_extrema(price_series, kind=pivot_kind, order=order, lookback=lookback)
    if len(pivots) < 2:
        return None

    prev_idx, prev_price = pivots[-2]
    curr_idx, curr_price = pivots[-1]
    indicator_values = pd.to_numeric(indicator_series, errors="coerce")
    prev_indicator = float(indicator_values.iloc[prev_idx]) if pd.notna(indicator_values.iloc[prev_idx]) else np.nan
    curr_indicator = float(indicator_values.iloc[curr_idx]) if pd.notna(indicator_values.iloc[curr_idx]) else np.nan
    if np.isnan(prev_indicator) or np.isnan(curr_indicator):
        return None

    price_threshold = max(abs(prev_price), abs(curr_price), 1e-9) * 0.005
    indicator_threshold = _indicator_divergence_threshold(indicator_name, prev_indicator, curr_indicator)
    if mode == "bullish":
        price_condition = curr_price < prev_price - price_threshold
        indicator_condition = curr_indicator > prev_indicator + indicator_threshold
    else:
        price_condition = curr_price > prev_price + price_threshold
        indicator_condition = curr_indicator < prev_indicator - indicator_threshold
    if not (price_condition and indicator_condition):
        return None

    prev_date = pd.Timestamp(dates[prev_idx]).date().isoformat()
    curr_date = pd.Timestamp(dates[curr_idx]).date().isoformat()
    price_phrase = "价格低点下移" if mode == "bullish" else "价格高点抬升"
    indicator_phrase = "低点抬高" if mode == "bullish" else "高点回落"
    return {
        "indicator": indicator_name,
        "mode": mode,
        "previous_index": prev_idx,
        "current_index": curr_idx,
        "previous_price": float(prev_price),
        "current_price": float(curr_price),
        "previous_indicator": float(prev_indicator),
        "current_indicator": float(curr_indicator),
        "previous_date": prev_date,
        "current_date": curr_date,
        "detail": f"{prev_date} -> {curr_date} {price_phrase}，但 {indicator_name} {indicator_phrase}",
    }


class TechnicalAnalyzer:
    """技术指标计算。"""

    def __init__(self, df: pd.DataFrame):
        self.df = normalize_ohlcv_frame(df)
        if len(self.df) < 30:
            raise ValueError("At least 30 rows are required for technical analysis")

    def _macd_series(self, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, pd.Series]:
        close = self.df["close"]
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        dif = ema_fast - ema_slow
        dea = dif.ewm(span=signal, adjust=False).mean()
        hist = 2 * (dif - dea)
        return {"DIF": dif, "DEA": dea, "HIST": hist}

    def _rsi_series(self, period: int = 14) -> pd.Series:
        close = self.df["close"]
        delta = close.diff()
        gain = delta.clip(lower=0).fillna(0.0)
        loss = (-delta.clip(upper=0)).fillna(0.0)
        avg_gain = _wilder_average(gain, period, start=1)
        avg_loss = _wilder_average(loss, period, start=1)
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = (100 - (100 / (1 + rs))).clip(lower=0, upper=100)
        return rsi.fillna(50.0)

    def _bollinger_series(self, period: int = 20, std: int = 2) -> Dict[str, pd.Series]:
        close = self.df["close"]
        mid = close.rolling(period).mean()
        std_dev = close.rolling(period).std(ddof=0)
        upper = mid + std * std_dev
        lower = mid - std * std_dev
        width = (upper - lower).replace(0, np.nan)
        pct_b = ((close - lower) / width).replace([np.inf, -np.inf], np.nan)
        return {
            "MID": mid,
            "UPPER": upper,
            "LOWER": lower,
            "%B": pct_b.fillna(0.5),
        }

    def _kdj_series(self, period: int = 9, smooth_k: int = 3, smooth_d: int = 3) -> Dict[str, pd.Series]:
        high_n = self.df["high"].rolling(period).max()
        low_n = self.df["low"].rolling(period).min()
        denominator = (high_n - low_n).replace(0, np.nan)
        rsv = ((self.df["close"] - low_n) / denominator * 100).clip(lower=0, upper=100).fillna(50)
        k = _seeded_recursive_average(rsv, alpha=1 / max(smooth_k, 1), seed=50.0).clip(lower=0, upper=100)
        d = _seeded_recursive_average(k, alpha=1 / max(smooth_d, 1), seed=50.0).clip(lower=0, upper=100)
        j = 3 * k - 2 * d
        return {"K": k, "D": d, "J": j}

    def _dmi_series(self, period: int = 14) -> Dict[str, pd.Series]:
        high = self.df["high"]
        low = self.df["low"]
        close = self.df["close"]

        plus_move = high.diff()
        minus_move = -low.diff()
        plus_dm = plus_move.where((plus_move > minus_move) & (plus_move > 0), 0.0).fillna(0.0)
        minus_dm = minus_move.where((minus_move > plus_move) & (minus_move > 0), 0.0).fillna(0.0)
        tr_components = pd.concat(
            [high - low, (high - close.shift()).abs(), (low - close.shift()).abs()],
            axis=1,
        )
        tr = tr_components.max(axis=1).fillna(0.0)

        atr = _wilder_average(tr, period)
        plus_dm_smoothed = _wilder_average(plus_dm, period)
        minus_dm_smoothed = _wilder_average(minus_dm, period)
        plus_di = (100 * (plus_dm_smoothed / atr.replace(0, np.nan))).clip(lower=0, upper=100)
        minus_di = (100 * (minus_dm_smoothed / atr.replace(0, np.nan))).clip(lower=0, upper=100)
        dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)).clip(lower=0, upper=100)

        adx = pd.Series(np.nan, index=dx.index, dtype=float)
        first_dx = period - 1
        adx_seed_end = first_dx + period - 1
        if len(dx) > adx_seed_end:
            adx.iloc[adx_seed_end] = float(dx.iloc[first_dx : adx_seed_end + 1].dropna().mean())
            for i in range(adx_seed_end + 1, len(dx)):
                prev = float(adx.iloc[i - 1])
                current = float(dx.iloc[i]) if pd.notna(dx.iloc[i]) else prev
                adx.iloc[i] = ((prev * (period - 1)) + current) / period
        adx = adx.clip(lower=0, upper=100)
        return {"DI+": plus_di.fillna(0.0), "DI-": minus_di.fillna(0.0), "ADX": adx.fillna(0.0)}

    def _obv_series(self, period: int = 20) -> Dict[str, pd.Series]:
        close = self.df["close"]
        volume = self.df["volume"].fillna(0)
        direction = np.sign(close.diff().fillna(0))
        obv = (direction * volume).cumsum()
        obv_ma = obv.rolling(period).mean()
        slope_5d = obv.diff(5).fillna(0)
        return {"OBV": obv, "MA": obv_ma, "slope_5d": slope_5d}

    def _atr_series(self, period: int = 14) -> pd.Series:
        high = self.df["high"]
        low = self.df["low"]
        close = self.df["close"]
        tr_components = pd.concat(
            [high - low, (high - close.shift()).abs(), (low - close.shift()).abs()],
            axis=1,
        )
        tr = tr_components.max(axis=1).fillna(0.0)
        return _wilder_average(tr, period).fillna(0.0)

    def indicator_series(self, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        technical_config = config or {}
        macd_cfg = technical_config.get("macd", {})
        rsi_cfg = technical_config.get("rsi", {})
        boll_cfg = technical_config.get("bollinger", {})
        kdj_cfg = technical_config.get("kdj", {})
        dmi_cfg = technical_config.get("dmi", {})
        obv_cfg = technical_config.get("obv", {})
        macd = self._macd_series(fast=macd_cfg.get("fast", 12), slow=macd_cfg.get("slow", 26), signal=macd_cfg.get("signal", 9))
        kdj = self._kdj_series(period=kdj_cfg.get("period", 9), smooth_k=kdj_cfg.get("smooth_k", 3), smooth_d=kdj_cfg.get("smooth_d", 3))
        dmi = self._dmi_series(period=dmi_cfg.get("period", 14))
        boll = self._bollinger_series(period=boll_cfg.get("period", 20), std=boll_cfg.get("std", 2))
        obv = self._obv_series(period=obv_cfg.get("period", 20))
        atr = self._atr_series(period=technical_config.get("atr", {}).get("period", 14))
        return {
            "date": self.df["date"],
            "close": self.df["close"],
            "macd_dif": macd["DIF"],
            "macd_dea": macd["DEA"],
            "macd_hist": macd["HIST"],
            "kdj_k": kdj["K"],
            "kdj_d": kdj["D"],
            "kdj_j": kdj["J"],
            "rsi": self._rsi_series(period=rsi_cfg.get("period", 14)),
            "adx": dmi["ADX"],
            "plus_di": dmi["DI+"],
            "minus_di": dmi["DI-"],
            "boll_mid": boll["MID"],
            "boll_upper": boll["UPPER"],
            "boll_lower": boll["LOWER"],
            "obv": obv["OBV"],
            "obv_ma": obv["MA"],
            "atr": atr,
        }

    def macd(self, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, float]:
        series = self._macd_series(fast=fast, slow=slow, signal=signal)
        dif = series["DIF"]
        dea = series["DEA"]
        hist = series["HIST"]
        return {
            "DIF": _last_valid(dif),
            "DEA": _last_valid(dea),
            "HIST": _last_valid(hist),
            "signal": "bullish" if dif.iloc[-1] > dea.iloc[-1] else "bearish",
        }

    def rsi(self, period: int = 14, overbought: int = 70, oversold: int = 30) -> Dict[str, float]:
        rsi = self._rsi_series(period=period)
        value = _last_valid(rsi, default=50.0, lower=0.0, upper=100.0)
        if value > overbought:
            signal = "overbought"
        elif value < oversold:
            signal = "oversold"
        else:
            signal = "neutral"
        return {"RSI": value, "signal": signal}

    def bollinger(self, period: int = 20, std: int = 2) -> Dict[str, float]:
        boll = self._bollinger_series(period=period, std=std)
        mid = boll["MID"]
        upper = boll["UPPER"]
        lower = boll["LOWER"]
        pct_b = _last_valid(boll["%B"], default=0.5)
        signal = "near_upper" if pct_b > 0.8 else "near_lower" if pct_b < 0.2 else "neutral"
        return {
            "MID": _last_valid(mid),
            "UPPER": _last_valid(upper),
            "LOWER": _last_valid(lower),
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
        kdj = self._kdj_series(period=period, smooth_k=smooth_k, smooth_d=smooth_d)
        k_latest = _last_valid(kdj["K"], default=50.0, lower=0.0, upper=100.0)
        d_latest = _last_valid(kdj["D"], default=50.0, lower=0.0, upper=100.0)
        j_latest = _last_valid(kdj["J"], default=50.0)
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
        dmi = self._dmi_series(period=period)
        plus_di = dmi["DI+"]
        minus_di = dmi["DI-"]
        adx = dmi["ADX"]
        plus_latest = _last_valid(plus_di, default=0.0, lower=0.0, upper=100.0)
        minus_latest = _last_valid(minus_di, default=0.0, lower=0.0, upper=100.0)
        adx_latest = _last_valid(adx, default=0.0, lower=0.0, upper=100.0)

        if adx_latest > adx_strong and plus_latest > minus_latest:
            signal = "bullish_trend"
        elif adx_latest > adx_strong and plus_latest < minus_latest:
            signal = "bearish_trend"
        else:
            signal = "weak_trend"

        return {
            "DI+": plus_latest,
            "DI-": minus_latest,
            "ADX": adx_latest,
            "signal": signal,
        }

    def obv(self, period: int = 20) -> Dict[str, float]:
        obv_data = self._obv_series(period=period)
        latest_obv = _last_valid(obv_data["OBV"])
        latest_ma = _last_valid(obv_data["MA"], default=latest_obv)
        latest_slope = _last_valid(obv_data["slope_5d"])

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

    def divergence_analysis(self, order: int = 3, lookback: int = 120) -> Dict[str, Any]:
        price_high = self.df["high"].astype(float)
        price_low = self.df["low"].astype(float)
        dates = list(self.df["date"])
        macd = self._macd_series()
        rsi = self._rsi_series()
        obv = self._obv_series()

        bullish_hits = [
            hit
            for hit in [
                _detect_price_indicator_divergence(
                    price_series=price_low,
                    indicator_series=rsi,
                    dates=dates,
                    indicator_name="RSI",
                    mode="bullish",
                    order=order,
                    lookback=lookback,
                ),
                _detect_price_indicator_divergence(
                    price_series=price_low,
                    indicator_series=macd["DIF"],
                    dates=dates,
                    indicator_name="MACD",
                    mode="bullish",
                    order=order,
                    lookback=lookback,
                ),
                _detect_price_indicator_divergence(
                    price_series=price_low,
                    indicator_series=obv["OBV"],
                    dates=dates,
                    indicator_name="OBV",
                    mode="bullish",
                    order=order,
                    lookback=lookback,
                ),
            ]
            if hit
        ]
        bearish_hits = [
            hit
            for hit in [
                _detect_price_indicator_divergence(
                    price_series=price_high,
                    indicator_series=rsi,
                    dates=dates,
                    indicator_name="RSI",
                    mode="bearish",
                    order=order,
                    lookback=lookback,
                ),
                _detect_price_indicator_divergence(
                    price_series=price_high,
                    indicator_series=macd["DIF"],
                    dates=dates,
                    indicator_name="MACD",
                    mode="bearish",
                    order=order,
                    lookback=lookback,
                ),
                _detect_price_indicator_divergence(
                    price_series=price_high,
                    indicator_series=obv["OBV"],
                    dates=dates,
                    indicator_name="OBV",
                    mode="bearish",
                    order=order,
                    lookback=lookback,
                ),
            ]
            if hit
        ]

        if bullish_hits and len(bullish_hits) >= len(bearish_hits):
            indicators = [str(item["indicator"]) for item in bullish_hits]
            return {
                "signal": "bullish",
                "kind": "底背离",
                "label": f"价格低点下移，但 {' / '.join(indicators)} 低点抬高（底背离）",
                "indicators": indicators,
                "strength": len(indicators),
                "detail": "；".join(str(item["detail"]) for item in bullish_hits[:3]),
                "hits": bullish_hits,
            }
        if bearish_hits:
            indicators = [str(item["indicator"]) for item in bearish_hits]
            return {
                "signal": "bearish",
                "kind": "顶背离",
                "label": f"价格高点抬升，但 {' / '.join(indicators)} 未同步创新高（顶背离）",
                "indicators": indicators,
                "strength": len(indicators),
                "detail": "；".join(str(item["detail"]) for item in bearish_hits[:3]),
                "hits": bearish_hits,
            }
        return {
            "signal": "neutral",
            "kind": "无明确背离",
            "label": "未识别到明确顶/底背离",
            "indicators": [],
            "strength": 0,
            "detail": "当前按最近两组确认摆点检查 RSI / MACD / OBV，未识别到明确背离。",
            "hits": [],
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
        close = self.df["close"].astype(float)
        ma5 = vol.rolling(5).mean()
        ma20 = vol.rolling(20).mean()
        amount = self.df["amount"]
        amount_ma20 = amount.rolling(20).mean()
        price_ma20 = close.rolling(20).mean()
        prev_20d_high = self.df["high"].shift(1).rolling(20).max()
        prev_20d_low = self.df["low"].shift(1).rolling(20).min()
        denominator = ma5.iloc[-1]
        ratio = 1.0 if pd.isna(denominator) or denominator == 0 else float(vol.iloc[-1] / denominator)
        denominator_20 = ma20.iloc[-1]
        ratio_20 = 1.0 if pd.isna(denominator_20) or denominator_20 == 0 else float(vol.iloc[-1] / denominator_20)
        amount_denominator_20 = amount_ma20.iloc[-1]
        amount_ratio_20 = (
            float(amount.iloc[-1] / amount_denominator_20)
            if pd.notna(amount.iloc[-1]) and pd.notna(amount_denominator_20) and amount_denominator_20 > 0
            else np.nan
        )
        latest_return_1d = float(close.pct_change().iloc[-1]) if len(close) > 1 and pd.notna(close.pct_change().iloc[-1]) else 0.0
        latest_return_5d = float(close.pct_change(5).iloc[-1]) if len(close) > 5 and pd.notna(close.pct_change(5).iloc[-1]) else 0.0
        latest_close = float(close.iloc[-1])
        latest_ma20 = float(price_ma20.iloc[-1]) if pd.notna(price_ma20.iloc[-1]) else latest_close
        breakout_20d = bool(pd.notna(prev_20d_high.iloc[-1]) and prev_20d_high.iloc[-1] > 0 and latest_close >= float(prev_20d_high.iloc[-1]) * 0.995)
        breakdown_20d = bool(pd.notna(prev_20d_low.iloc[-1]) and prev_20d_low.iloc[-1] > 0 and latest_close <= float(prev_20d_low.iloc[-1]) * 1.005)

        if breakout_20d and ratio_20 >= 1.2:
            structure = "放量突破"
        elif latest_return_1d > 0.005 and ratio_20 >= 1.2:
            structure = "放量上攻"
        elif latest_return_1d >= 0 and ratio < 0.8:
            structure = "缩量上涨"
        elif latest_return_1d <= 0 and ratio < 0.8 and latest_close >= latest_ma20 * 0.98:
            structure = "缩量回调"
        elif abs(latest_return_1d) <= 0.01 and ratio_20 >= 1.5:
            structure = "放量滞涨"
        elif latest_return_1d < 0 and ratio_20 >= 1.2:
            structure = "放量下跌"
        elif breakdown_20d and ratio_20 >= 1.1:
            structure = "跌破平台"
        else:
            structure = "量价中性"
        signal = structure
        return {
            "volume": _last_valid(vol),
            "MA5": _last_valid(ma5),
            "MA20": _last_valid(ma20),
            "vol_ratio": ratio,
            "vol_ratio_20": ratio_20,
            "amount_ratio_20": float(amount_ratio_20) if pd.notna(amount_ratio_20) else np.nan,
            "price_change_1d": latest_return_1d,
            "price_change_5d": latest_return_5d,
            "breakout_20d": breakout_20d,
            "breakdown_20d": breakdown_20d,
            "structure": structure,
            "signal": signal,
        }

    def volatility_profile(self, atr_period: int = 14, lookback: int = 60) -> Dict[str, float]:
        close = self.df["close"].astype(float)
        atr = self._atr_series(period=atr_period)
        natr = atr / close.replace(0, np.nan)
        natr_ma20 = natr.rolling(20).mean()
        latest_natr = _last_valid(natr, default=0.0, lower=0.0)
        latest_natr_ma20 = _last_valid(natr_ma20, default=latest_natr if latest_natr > 0 else 1.0, lower=1e-9)
        atr_ratio = latest_natr / latest_natr_ma20 if latest_natr_ma20 > 0 else 1.0

        boll = self._bollinger_series(period=20, std=2)
        width = ((boll["UPPER"] - boll["LOWER"]) / boll["MID"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
        latest_width = _last_valid(width, default=0.0, lower=0.0)
        width_window = width.tail(max(lookback, 20)).dropna()
        if width_window.empty:
            width_percentile = 0.5
        else:
            width_percentile = float((width_window <= latest_width).mean())

        if atr_ratio <= 0.9 and width_percentile <= 0.35:
            signal = "compressed"
        elif atr_ratio >= 1.15 and width_percentile >= 0.65:
            signal = "expanding"
        else:
            signal = "neutral"

        return {
            "ATR": _last_valid(atr, default=0.0, lower=0.0),
            "NATR": latest_natr,
            "atr_ratio_20": float(atr_ratio),
            "boll_width": latest_width,
            "boll_width_percentile": width_percentile,
            "signal": signal,
        }

    def candlestick_patterns(self) -> list:
        """识别最近 1-3 根 K 线的常见形态特征。"""
        close = self.df["close"]
        latest = _candle_snapshot(self.df.iloc[-1])
        patterns: list[str] = []

        def _append(name: str) -> None:
            if name not in patterns:
                patterns.append(name)

        single_trend = _prior_return(close, 1, window=5)
        two_bar_trend = _prior_return(close, 2, window=5)
        three_bar_trend = _prior_return(close, 3, window=5)

        if len(self.df) >= 3:
            first = _candle_snapshot(self.df.iloc[-3])
            second = _candle_snapshot(self.df.iloc[-2])
            third = latest
            first_body_high = max(float(first["open"]), float(first["close"]))
            first_body_low = min(float(first["open"]), float(first["close"]))
            second_body_high = max(float(second["open"]), float(second["close"]))
            second_body_low = min(float(second["open"]), float(second["close"]))
            third_body_high = max(float(third["open"]), float(third["close"]))
            third_body_low = min(float(third["open"]), float(third["close"]))
            equal_body_tolerance = max(float(first["range"]), float(second["range"]), float(third["range"])) * 0.08
            if (
                three_bar_trend <= -0.02
                and first["bearish"]
                and float(first["body_ratio"]) >= 0.45
                and float(second["body_ratio"]) <= 0.35
                and third["bullish"]
                and float(third["body_ratio"]) >= 0.35
                and float(third["close"]) > float(first["midpoint"])
            ):
                _append("morning_star")
            if (
                three_bar_trend >= 0.02
                and first["bullish"]
                and float(first["body_ratio"]) >= 0.45
                and float(second["body_ratio"]) <= 0.35
                and third["bearish"]
                and float(third["body_ratio"]) >= 0.35
                and float(third["close"]) < float(first["midpoint"])
            ):
                _append("evening_star")
            if (
                all(bar["bullish"] for bar in (first, second, third))
                and float(first["body_ratio"]) >= 0.35
                and float(second["body_ratio"]) >= 0.35
                and float(third["body_ratio"]) >= 0.35
                and float(first["close"]) < float(second["close"]) < float(third["close"])
                and float(first["open"]) <= float(second["open"]) <= float(first["close"])
                and float(second["open"]) <= float(third["open"]) <= float(second["close"])
            ):
                _append("three_white_soldiers")
            if (
                all(bar["bearish"] for bar in (first, second, third))
                and float(first["body_ratio"]) >= 0.35
                and float(second["body_ratio"]) >= 0.35
                and float(third["body_ratio"]) >= 0.35
                and float(first["close"]) > float(second["close"]) > float(third["close"])
                and float(first["close"]) <= float(second["open"]) <= float(first["open"])
                and float(second["close"]) <= float(third["open"]) <= float(second["open"])
            ):
                _append("three_black_crows")
            if (
                three_bar_trend <= -0.02
                and first["bearish"]
                and float(first["body_ratio"]) >= 0.45
                and second_body_low >= first_body_low
                and second_body_high <= first_body_high
                and float(second["body"]) <= float(first["body"]) * 0.65
                and third["bullish"]
                and float(third["close"]) > first_body_high
            ):
                _append("three_inside_up")
            if (
                three_bar_trend >= 0.02
                and first["bullish"]
                and float(first["body_ratio"]) >= 0.45
                and second_body_low >= first_body_low
                and second_body_high <= first_body_high
                and float(second["body"]) <= float(first["body"]) * 0.65
                and third["bearish"]
                and float(third["close"]) < first_body_low
            ):
                _append("three_inside_down")
            if (
                three_bar_trend <= -0.015
                and first["bearish"]
                and second["bearish"]
                and abs(float(first["low"]) - float(second["low"])) <= equal_body_tolerance
                and third["bullish"]
                and float(third["close"]) > max(float(second["open"]), float(second["close"]))
            ):
                _append("tweezer_bottom")
            if (
                three_bar_trend >= 0.015
                and first["bullish"]
                and second["bullish"]
                and abs(float(first["high"]) - float(second["high"])) <= equal_body_tolerance
                and third["bearish"]
                and float(third["close"]) < min(float(second["open"]), float(second["close"]))
            ):
                _append("tweezer_top")

        if len(self.df) >= 2:
            previous = _candle_snapshot(self.df.iloc[-2])
            current = latest
            prev_body_high = max(float(previous["open"]), float(previous["close"]))
            prev_body_low = min(float(previous["open"]), float(previous["close"]))
            curr_body_high = max(float(current["open"]), float(current["close"]))
            curr_body_low = min(float(current["open"]), float(current["close"]))
            equal_wick_tolerance = max(float(previous["range"]), float(current["range"])) * 0.08
            if (
                two_bar_trend <= -0.01
                and previous["bearish"]
                and current["bullish"]
                and float(current["open"]) <= float(previous["close"])
                and float(current["close"]) >= float(previous["open"])
                and float(current["body"]) >= float(previous["body"]) * 0.9
            ):
                _append("bullish_engulfing")
            if (
                two_bar_trend >= 0.01
                and previous["bullish"]
                and current["bearish"]
                and float(current["open"]) >= float(previous["close"])
                and float(current["close"]) <= float(previous["open"])
                and float(current["body"]) >= float(previous["body"]) * 0.9
            ):
                _append("bearish_engulfing")
            if (
                two_bar_trend <= -0.015
                and previous["bearish"]
                and float(previous["body_ratio"]) >= 0.45
                and current["bullish"]
                and float(current["open"]) <= float(previous["close"]) + float(previous["range"]) * 0.15
                and float(current["close"]) > float(previous["midpoint"])
                and float(current["close"]) < float(previous["open"])
            ):
                _append("piercing_line")
            if (
                two_bar_trend >= 0.015
                and previous["bullish"]
                and float(previous["body_ratio"]) >= 0.45
                and current["bearish"]
                and float(current["open"]) >= float(previous["close"]) - float(previous["range"]) * 0.15
                and float(current["close"]) < float(previous["midpoint"])
                and float(current["close"]) > float(previous["open"])
            ):
                _append("dark_cloud_cover")
            if (
                two_bar_trend <= -0.01
                and previous["bearish"]
                and float(previous["body_ratio"]) >= 0.45
                and current["bullish"]
                and curr_body_low >= prev_body_low
                and curr_body_high <= prev_body_high
                and float(current["body"]) <= float(previous["body"]) * 0.65
            ):
                _append("bullish_harami")
            if (
                two_bar_trend >= 0.01
                and previous["bullish"]
                and float(previous["body_ratio"]) >= 0.45
                and current["bearish"]
                and curr_body_low >= prev_body_low
                and curr_body_high <= prev_body_high
                and float(current["body"]) <= float(previous["body"]) * 0.65
            ):
                _append("bearish_harami")
            if (
                two_bar_trend <= -0.015
                and previous["bearish"]
                and current["bullish"]
                and abs(float(previous["low"]) - float(current["low"])) <= equal_wick_tolerance
                and float(current["close"]) > float(previous["close"])
            ):
                _append("tweezer_bottom")
            if (
                two_bar_trend >= 0.015
                and previous["bullish"]
                and current["bearish"]
                and abs(float(previous["high"]) - float(current["high"])) <= equal_wick_tolerance
                and float(current["close"]) < float(previous["close"])
            ):
                _append("tweezer_top")

        if float(latest["range"]) > 0:
            if (
                single_trend <= -0.01
                and float(latest["lower_shadow"]) >= max(float(latest["body"]) * 2.2, float(latest["range"]) * 0.45)
                and float(latest["upper_shadow"]) <= max(float(latest["body"]) * 0.6, float(latest["range"]) * 0.15)
            ):
                _append("hammer")
            if (
                single_trend <= -0.01
                and float(latest["upper_shadow"]) >= max(float(latest["body"]) * 2.2, float(latest["range"]) * 0.45)
                and float(latest["lower_shadow"]) <= max(float(latest["body"]) * 0.6, float(latest["range"]) * 0.15)
            ):
                _append("inverted_hammer")
            if (
                single_trend >= 0.01
                and float(latest["upper_shadow"]) >= max(float(latest["body"]) * 2.2, float(latest["range"]) * 0.45)
                and float(latest["lower_shadow"]) <= max(float(latest["body"]) * 0.6, float(latest["range"]) * 0.15)
            ):
                _append("shooting_star")
            if (
                single_trend >= 0.01
                and float(latest["lower_shadow"]) >= max(float(latest["body"]) * 2.2, float(latest["range"]) * 0.45)
                and float(latest["upper_shadow"]) <= max(float(latest["body"]) * 0.6, float(latest["range"]) * 0.15)
            ):
                _append("hanging_man")
            if float(latest["body"]) <= float(latest["range"]) * 0.1:
                _append("doji")
            if (
                float(latest["body"]) >= float(latest["range"]) * 0.8
                and float(latest["upper_shadow"]) <= float(latest["range"]) * 0.1
                and float(latest["lower_shadow"]) <= float(latest["range"]) * 0.1
            ):
                _append("bullish_marubozu" if bool(latest["bullish"]) else "bearish_marubozu")
                _append("marubozu")
        return patterns

    def ma_system(self, periods: Optional[Iterable[int]] = None) -> Dict[str, Any]:
        ma_periods = list(periods or [5, 10, 20, 30, 60])
        mas = {}
        for period in ma_periods:
            series = self.df["close"].rolling(period).mean()
            if series.notna().any():
                mas[f"MA{period}"] = _last_valid(series)
        price = float(self.df["close"].iloc[-1])
        above_count = sum(1 for value in mas.values() if price > value)
        if mas and above_count >= max(len(mas) - 1, 1):
            signal = "bullish"
        elif mas and above_count <= 1:
            signal = "bearish"
        else:
            signal = "neutral"
        alignment = {"bullish": "bullish", "bearish": "bearish"}.get(signal, "mixed")
        return {"mas": mas, "alignment": alignment, "signal": signal}

    def setup_analysis(self, lookback: int = 60) -> Dict[str, Any]:
        """识别当前价量 setup 类型：假突破/失败突破、支撑失效分流、压缩启动 vs 情绪追价。"""
        close = self.df["close"].astype(float)
        high = self.df["high"].astype(float)
        low = self.df["low"].astype(float)
        volume = self.df["volume"].fillna(0).astype(float)

        window = min(lookback, len(self.df) - 1)
        recent_high = float(high.iloc[-(window + 1) : -1].max()) if window > 0 else float(high.iloc[-1])
        recent_low = float(low.iloc[-(window + 1) : -1].min()) if window > 0 else float(low.iloc[-1])

        latest_close = float(close.iloc[-1])
        latest_high = float(high.iloc[-1])
        latest_low = float(low.iloc[-1])
        prev_close = float(close.iloc[-2]) if len(close) >= 2 else latest_close

        vol_ma20 = volume.rolling(20).mean()
        latest_vol = float(volume.iloc[-1])
        latest_vol_ma20 = float(vol_ma20.iloc[-1]) if pd.notna(vol_ma20.iloc[-1]) and float(vol_ma20.iloc[-1]) > 0 else 1.0
        vol_ratio_20 = latest_vol / latest_vol_ma20

        # --- 假突破 / 失败突破 ---
        # 日内突破近期高点但收盘回落到突破位下方（看涨假突破）
        # 或日内跌破近期低点但收盘回升到跌破位上方（看跌假突破）
        breakout_threshold = max(recent_high * 0.002, 0.01)
        breakdown_threshold = max(recent_low * 0.002, 0.01)

        bullish_false_break = (
            latest_high >= recent_high - breakout_threshold  # 日内触及或突破近期高点
            and latest_close < recent_high - breakout_threshold  # 但收盘回落到突破位下方
            and latest_close < prev_close * 1.005  # 收盘没有明显上涨
        )
        bearish_false_break = (
            latest_low <= recent_low + breakdown_threshold  # 日内触及或跌破近期低点
            and latest_close > recent_low + breakdown_threshold  # 但收盘回升到跌破位上方
            and latest_close > prev_close * 0.995  # 收盘没有明显下跌
        )

        if bullish_false_break:
            false_break_kind = "bullish_false_break"
            false_break_label = "看涨假突破：日内触及近期高点但收盘回落，多头未能守住突破位"
        elif bearish_false_break:
            false_break_kind = "bearish_false_break"
            false_break_label = "看跌假突破：日内触及近期低点但收盘回升，空头未能守住跌破位"
        else:
            false_break_kind = "none"
            false_break_label = "未识别到明确假突破形态"

        # --- 支撑失效后的 setup 分流 ---
        # 判断当前是否处于支撑失效后的两种状态：
        # 1. 跌破支撑后反弹但未能收复 → 失效确认，偏空
        # 2. 跌破支撑后继续下行 → 趋势延续，偏空
        # 支撑位用近期低点代理
        #
        # IMPORTANT: support_level must be computed from bars BEFORE bar -2 (the potential
        # breakdown bar). normalize_ohlcv_frame recomputes low = min(OHLC), so a breakdown
        # bar's normalized low will always be ≤ its close (= prev_close). Including bar -2
        # in the support window would make support_level ≤ prev_close always, making the
        # condition structurally impossible. Use low.iloc[-(window+1):-2] instead.
        _support_history = low.iloc[-(window + 1) : -2]
        support_level = float(_support_history.min()) if not _support_history.empty else float(low.iloc[-2])
        support_broken = prev_close < support_level  # strictly below historical floor

        if support_broken:
            rebound_from_break = (latest_close - prev_close) / max(abs(prev_close), 1e-9)
            if rebound_from_break >= 0.01 and latest_close < support_level:
                # 反弹但未能收复支撑位 → 失效确认
                support_setup = "failed_recovery"
                support_label = "支撑失效后反弹未收复：跌破支撑后出现反弹但未能重新站上，偏空分流"
            elif rebound_from_break < 0:
                # 继续下行
                support_setup = "breakdown_continuation"
                support_label = "支撑失效后继续下行：跌破支撑后未见反弹，趋势延续偏空"
            else:
                support_setup = "breakdown_watching"
                support_label = "支撑失效观察中：跌破支撑后量价尚未给出明确分流信号"
        else:
            support_setup = "support_intact"
            support_label = "支撑位完整：当前价格未跌破近期低点支撑区"

        # --- 压缩后放量启动 vs 情绪追价 ---
        # 需要结合波动率压缩状态和当前量价行为
        boll = self._bollinger_series(period=20, std=2)
        width = ((boll["UPPER"] - boll["LOWER"]) / boll["MID"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
        width_window = width.tail(max(lookback, 20)).dropna()
        latest_width = _last_valid(width, default=0.0, lower=0.0)
        width_percentile = float((width_window <= latest_width).mean()) if not width_window.empty else 0.5

        atr = self._atr_series(period=14)
        natr = atr / close.replace(0, np.nan)
        natr_ma20 = natr.rolling(20).mean()
        latest_natr = _last_valid(natr, default=0.0, lower=0.0)
        latest_natr_ma20 = _last_valid(natr_ma20, default=latest_natr if latest_natr > 0 else 1.0, lower=1e-9)
        atr_ratio = latest_natr / latest_natr_ma20 if latest_natr_ma20 > 0 else 1.0

        was_compressed = atr_ratio <= 1.05 and width_percentile <= 0.40
        price_change_1d = (latest_close - prev_close) / max(abs(prev_close), 1e-9) if len(close) >= 2 else 0.0

        if was_compressed and vol_ratio_20 >= 1.5 and price_change_1d >= 0.015:
            # 压缩后放量上涨 → 真启动信号
            compression_setup = "compression_breakout"
            compression_label = "压缩后放量启动：波动收敛后出现放量上涨，更像筹码收敛后的真启动"
        elif not was_compressed and vol_ratio_20 >= 1.5 and price_change_1d >= 0.02:
            # 波动已扩张阶段的放量上涨 → 情绪追价风险
            compression_setup = "momentum_chase"
            compression_label = "情绪追价区：波动已扩张阶段出现放量上涨，更像情绪释放而非低吸区"
        elif was_compressed and vol_ratio_20 < 0.8:
            # 压缩中缩量 → 仍在蓄势
            compression_setup = "still_compressing"
            compression_label = "仍在压缩蓄势：波动收敛且成交量萎缩，尚未出现启动信号"
        else:
            compression_setup = "neutral"
            compression_label = "量价压缩状态中性：当前未识别出明确的压缩启动或情绪追价特征"

        # 综合 setup 信号
        bullish_setups = sum([
            false_break_kind == "bearish_false_break",  # 空头假突破 → 多头机会
            support_setup == "support_intact",
            compression_setup == "compression_breakout",
        ])
        bearish_setups = sum([
            false_break_kind == "bullish_false_break",  # 多头假突破 → 空头机会
            support_setup in {"failed_recovery", "breakdown_continuation"},
            compression_setup == "momentum_chase",
        ])

        if bullish_setups > bearish_setups:
            overall_signal = "bullish"
        elif bearish_setups > bullish_setups:
            overall_signal = "bearish"
        else:
            overall_signal = "neutral"

        return {
            "signal": overall_signal,
            "false_break": {
                "kind": false_break_kind,
                "label": false_break_label,
            },
            "support_setup": {
                "kind": support_setup,
                "label": support_label,
                "support_level": float(support_level),
            },
            "compression_setup": {
                "kind": compression_setup,
                "label": compression_label,
                "was_compressed": was_compressed,
                "vol_ratio_20": float(vol_ratio_20),
                "width_percentile": float(width_percentile),
            },
        }

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
            "volatility": self.volatility_profile(**technical_config.get("atr", {})),
            "candlestick": self.candlestick_patterns(),
            "ma_system": self.ma_system(technical_config.get("ma_periods")),
            "divergence": self.divergence_analysis(**technical_config.get("divergence", {})),
            "setup": self.setup_analysis(**technical_config.get("setup", {})),
        }
