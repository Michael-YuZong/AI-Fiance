"""Shared market data helpers for commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import yfinance as yf

from src.collectors import ChinaMarketCollector, CommodityCollector, HongKongMarketCollector, USMarketCollector
from src.processors.technical import normalize_ohlcv_frame
from src.utils.data import load_watchlist


@dataclass
class AssetContext:
    symbol: str
    name: str
    asset_type: str
    source_symbol: str
    metadata: Dict[str, Any]


def get_asset_context(symbol: str, asset_type: str, config: Dict[str, Any]) -> AssetContext:
    watchlist = {item["symbol"]: item for item in load_watchlist()}
    metadata = dict(watchlist.get(symbol, {}))
    name = metadata.get("name", symbol)
    source_symbol = metadata.get("proxy_symbol", symbol)
    return AssetContext(
        symbol=symbol,
        name=name,
        asset_type=asset_type,
        source_symbol=source_symbol,
        metadata=metadata,
    )


def fetch_asset_history(
    symbol: str,
    asset_type: str,
    config: Dict[str, Any],
    period: str = "3y",
    interval: str = "1d",
) -> pd.DataFrame:
    context = get_asset_context(symbol, asset_type, config)
    if asset_type == "cn_etf":
        return ChinaMarketCollector(config).get_etf_daily(context.source_symbol)
    if asset_type in {"hk", "hk_index"}:
        return HongKongMarketCollector(config).get_history(context.source_symbol, period=period, interval=interval)
    if asset_type == "us":
        return USMarketCollector(config).get_history(context.source_symbol, period=period, interval=interval)
    if asset_type == "futures":
        return CommodityCollector(config).get_main_contract(context.source_symbol)
    raise ValueError(f"Unsupported asset type: {asset_type}")


def fetch_intraday_history(symbol: str, asset_type: str, config: Dict[str, Any]) -> pd.DataFrame:
    context = get_asset_context(symbol, asset_type, config)
    if asset_type == "cn_etf":
        from src.collectors.intraday import IntradayCollector

        return IntradayCollector(config).get_intraday_chart(context.source_symbol)
    ticker = yf.Ticker(context.source_symbol)
    frame = ticker.history(period="1d", interval="1m", auto_adjust=False)
    if frame.empty:
        frame = ticker.history(period="5d", interval="5m", auto_adjust=False)
    return frame


def latest_close(frame: pd.DataFrame) -> float:
    normalized = normalize_ohlcv_frame(frame)
    return float(normalized["close"].iloc[-1])


def compute_history_metrics(frame: pd.DataFrame) -> Dict[str, float]:
    normalized = normalize_ohlcv_frame(frame)
    close = normalized["close"].astype(float)
    volume = normalized["volume"].fillna(0.0).astype(float)
    amount = normalized["amount"].fillna(np.nan).astype(float)

    def _period_return(days: int) -> float:
        if len(close) <= days:
            return np.nan
        return float(close.iloc[-1] / close.iloc[-(days + 1)] - 1)

    returns = close.pct_change().dropna()
    rolling_max = close.cummax()
    drawdown = close / rolling_max - 1
    amount_tail = amount.tail(20).dropna()
    if not amount_tail.empty and float(amount_tail.abs().sum()) > 0:
        avg_amount = float(amount_tail.mean())
    else:
        avg_amount = float((close * volume).tail(20).mean())
    return {
        "last_close": float(close.iloc[-1]),
        "return_1d": _period_return(1),
        "return_5d": _period_return(5),
        "return_20d": _period_return(20),
        "return_60d": _period_return(60),
        "volatility_20d": float(returns.tail(20).std() * np.sqrt(252)) if len(returns) >= 2 else 0.0,
        "max_drawdown_1y": float(drawdown.tail(min(252, len(drawdown))).min()),
        "avg_turnover_20d": avg_amount,
        "price_percentile_1y": float((close.tail(min(252, len(close))) <= close.iloc[-1]).mean()),
    }


def intraday_metrics(frame: pd.DataFrame) -> Dict[str, float]:
    normalized = normalize_ohlcv_frame(frame)
    latest = normalized.iloc[-1]
    day_open = float(normalized["open"].iloc[0])
    day_high = float(normalized["high"].max())
    day_low = float(normalized["low"].min())
    current = float(latest["close"])
    volume = normalized["volume"].fillna(0.0)

    if "amount" in normalized.columns and normalized["amount"].notna().any():
        amount = normalized["amount"].fillna(0.0)
        vwap = float(amount.sum() / max(volume.sum(), 1))
    else:
        typical_price = (normalized["high"] + normalized["low"] + normalized["close"]) / 3
        vwap = float((typical_price * volume).sum() / max(volume.sum(), 1))
    if current > 0 and (vwap / current > 10 or current / max(vwap, 1e-9) > 10):
        typical_price = (normalized["high"] + normalized["low"] + normalized["close"]) / 3
        vwap = float((typical_price * volume).sum() / max(volume.sum(), 1))

    range_pos = 0.5 if day_high == day_low else float((current - day_low) / (day_high - day_low))
    return {
        "current": current,
        "open": day_open,
        "high": day_high,
        "low": day_low,
        "change_pct": float(current / day_open - 1) if day_open else 0.0,
        "vwap": vwap,
        "volume": float(volume.sum()),
        "range_position": range_pos,
    }


def format_pct(value: float) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "N/A"
    return f"{value * 100:+.2f}%"


def market_regime_proxy() -> Dict[str, Any]:
    tickers = {
        "vix": "^VIX",
        "dxy": "DX-Y.NYB",
        "gold": "GC=F",
        "copper": "HG=F",
        "cny": "CNY=X",
    }
    frames = {}
    for key, ticker in tickers.items():
        frame = yf.Ticker(ticker).history(period="3mo", auto_adjust=False)
        if frame.empty:
            continue
        frames[key] = frame

    result: Dict[str, Any] = {}
    if "vix" in frames:
        result["vix"] = float(frames["vix"]["Close"].iloc[-1])
    if "dxy" in frames:
        dxy = frames["dxy"]["Close"]
        result["dxy"] = float(dxy.iloc[-1])
        result["dxy_20d_change"] = float(dxy.iloc[-1] / dxy.iloc[-min(len(dxy), 21)] - 1) if len(dxy) > 20 else 0.0
    if "gold" in frames and "copper" in frames:
        copper = float(frames["copper"]["Close"].iloc[-1])
        gold = float(frames["gold"]["Close"].iloc[-1])
        result["copper_gold_ratio"] = (copper * 100.0) / gold if gold else 0.0
    if "cny" in frames:
        result["cny"] = float(frames["cny"]["Close"].iloc[-1])
    return result
