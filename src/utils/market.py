"""Shared market data helpers for commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

import numpy as np
import pandas as pd
import yfinance as yf

from src.collectors import ChinaMarketCollector, CommodityCollector, HongKongMarketCollector, USMarketCollector
from src.processors.technical import normalize_ohlcv_frame
from src.utils.config import resolve_project_path
from src.utils.data import load_asset_aliases, load_watchlist


@dataclass
class AssetContext:
    symbol: str
    name: str
    asset_type: str
    source_symbol: str
    metadata: Dict[str, Any]


def get_asset_context(symbol: str, asset_type: str, config: Dict[str, Any]) -> AssetContext:
    watchlist = {item["symbol"]: item for item in load_watchlist()}
    alias_path = resolve_project_path(config.get("asset_aliases_file", "config/asset_aliases.yaml"))
    aliases = {item["symbol"]: item for item in load_asset_aliases(alias_path)}
    metadata = dict(aliases.get(symbol, {}))
    metadata.update(dict(watchlist.get(symbol, {})))
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
    if asset_type == "cn_stock":
        return ChinaMarketCollector(config).get_stock_daily(context.source_symbol)
    if asset_type == "cn_index":
        return ChinaMarketCollector(config).get_index_daily(context.symbol, proxy_symbol=context.source_symbol)
    if asset_type == "cn_fund":
        return ChinaMarketCollector(config).get_open_fund_daily(context.symbol, proxy_symbol=context.source_symbol)
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


def _to_float(value: Any) -> Optional[float]:
    series = pd.to_numeric(pd.Series([value]), errors="coerce")
    number = series.iloc[0]
    if pd.isna(number):
        return None
    return float(number)


def _to_timestamp(value: Any) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    try:
        stamp = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(stamp):
        return None
    if stamp.tzinfo is not None:
        try:
            stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
        except TypeError:
            stamp = stamp.tz_localize(None)
    return stamp


def fetch_cn_etf_realtime_row(symbol: str, config: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        frame = ChinaMarketCollector(dict(config)).get_etf_realtime()
    except Exception:
        return {}
    if frame is None or frame.empty:
        return {}
    code_col = "代码" if "代码" in frame.columns else "基金代码" if "基金代码" in frame.columns else None
    if not code_col:
        return {}
    matched = frame[frame[code_col].astype(str) == str(symbol)]
    if matched.empty:
        return {}
    row = matched.iloc[0]
    current = _to_float(row.get("最新价"))
    prev_close = _to_float(row.get("昨收"))
    change_pct = _to_float(row.get("涨跌幅"))
    if change_pct is not None:
        change_pct /= 100.0
    elif current is not None and prev_close:
        change_pct = current / prev_close - 1
    return {
        "name": str(row.get("名称", row.get("基金简称", ""))).strip(),
        "current": current,
        "open": _to_float(row.get("开盘价", row.get("今开"))),
        "high": _to_float(row.get("最高价", row.get("最高"))),
        "low": _to_float(row.get("最低价", row.get("最低"))),
        "prev_close": prev_close,
        "change_pct": change_pct,
        "volume": _to_float(row.get("成交量")),
        "amount": _to_float(row.get("成交额")),
        "data_date": _to_timestamp(row.get("数据日期")),
        "updated_at": _to_timestamp(row.get("更新时间")),
    }


def fetch_cn_stock_realtime_row(symbol: str, config: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        frame = ChinaMarketCollector(dict(config)).get_stock_realtime()
    except Exception:
        return {}
    if frame is None or frame.empty or "代码" not in frame.columns:
        return {}
    matched = frame[frame["代码"].astype(str) == str(symbol)]
    if matched.empty:
        return {}
    row = matched.iloc[0]
    current = _to_float(row.get("最新价"))
    prev_close = _to_float(row.get("昨收"))
    change_pct = _to_float(row.get("涨跌幅"))
    if change_pct is not None:
        change_pct /= 100.0
    elif current is not None and prev_close:
        change_pct = current / prev_close - 1
    return {
        "name": str(row.get("名称", "")).strip(),
        "current": current,
        "open": _to_float(row.get("开盘价", row.get("今开"))),
        "high": _to_float(row.get("最高价", row.get("最高"))),
        "low": _to_float(row.get("最低价", row.get("最低"))),
        "prev_close": prev_close,
        "change_pct": change_pct,
        "volume": _to_float(row.get("成交量")),
        "amount": _to_float(row.get("成交额")),
        "data_date": _to_timestamp(row.get("数据日期")),
        "updated_at": _to_timestamp(row.get("更新时间")),
    }


def fetch_cn_stock_auction_row(symbol: str, config: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        frame = ChinaMarketCollector(dict(config)).get_stock_auction(symbol)
    except Exception:
        return {}
    if frame is None or frame.empty:
        return {}
    row = frame.iloc[0]
    price = _to_float(row.get("price"))
    prev_close = _to_float(row.get("pre_close"))
    return {
        "auction_price": price,
        "auction_volume": _to_float(row.get("vol")),
        "auction_amount": _to_float(row.get("amount")),
        "auction_turnover_rate": _to_float(row.get("turnover_rate")),
        "auction_volume_ratio": _to_float(row.get("volume_ratio")),
        "prev_close": prev_close,
        "trade_date": _to_timestamp(row.get("trade_date")),
        "auction_gap": (float(price / prev_close - 1) if price is not None and prev_close else None),
    }


def fetch_cn_stock_limit_row(symbol: str, config: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        frame = ChinaMarketCollector(dict(config)).get_stock_limit(symbol)
    except Exception:
        return {}
    if frame is None or frame.empty:
        return {}
    row = frame.iloc[0]
    return {
        "up_limit": _to_float(row.get("up_limit")),
        "down_limit": _to_float(row.get("down_limit")),
        "trade_date": _to_timestamp(row.get("trade_date")),
    }


def build_snapshot_fallback_history(
    symbol: str,
    asset_type: str,
    config: Mapping[str, Any],
    periods: int = 60,
) -> pd.DataFrame:
    """Build a minimal local history from a realtime ETF snapshot.

    This is only used when the normal daily-history chain fails. It allows the
    report pipeline to degrade into a clearly-marked snapshot analysis instead of
    failing end-to-end. The generated series is intentionally conservative:
    previous rows stay flat at yesterday's close and only the latest row carries
    today's realtime OHLCV snapshot.
    """

    if asset_type not in {"cn_etf", "cn_stock"}:
        return pd.DataFrame()

    realtime = fetch_cn_etf_realtime_row(symbol, config) if asset_type == "cn_etf" else fetch_cn_stock_realtime_row(symbol, config)
    if not realtime:
        return pd.DataFrame()

    current = _to_float(realtime.get("current"))
    prev_close = _to_float(realtime.get("prev_close")) or current
    open_price = _to_float(realtime.get("open")) or current or prev_close
    high_price = _to_float(realtime.get("high")) or max(filter(None, [current, open_price, prev_close]), default=None)
    low_price = _to_float(realtime.get("low")) or min(filter(None, [current, open_price, prev_close]), default=None)
    amount = _to_float(realtime.get("amount"))
    volume = _to_float(realtime.get("volume")) or 0.0
    anchor = _to_timestamp(realtime.get("updated_at") or realtime.get("data_date")) or pd.Timestamp.now()

    if current is None or prev_close is None or open_price is None or high_price is None or low_price is None:
        return pd.DataFrame()

    count = max(int(periods), 30)
    dates = pd.bdate_range(end=anchor.normalize(), periods=count)
    rows: List[Dict[str, Any]] = []
    for trade_date in dates[:-1]:
        rows.append(
            {
                "date": trade_date,
                "open": prev_close,
                "high": prev_close,
                "low": prev_close,
                "close": prev_close,
                "volume": 0.0,
                "amount": np.nan,
            }
        )
    rows.append(
        {
            "date": dates[-1],
            "open": open_price,
            "high": max(high_price, open_price, current, prev_close),
            "low": min(low_price, open_price, current, prev_close),
            "close": current,
            "volume": max(volume, 0.0),
            "amount": amount if amount is not None else np.nan,
        }
    )
    return pd.DataFrame(rows)


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


def infer_previous_close(history: pd.DataFrame, snapshot_time: Any | None = None) -> float:
    normalized = normalize_ohlcv_frame(history)
    if normalized.empty:
        raise ValueError("Price dataframe is empty")
    if len(normalized) == 1:
        return float(normalized["close"].iloc[-1])
    snapshot_stamp = _to_timestamp(snapshot_time)
    if snapshot_stamp is None:
        return float(normalized["close"].iloc[-1])
    latest_stamp = _to_timestamp(normalized["date"].iloc[-1])
    if latest_stamp is None:
        return float(normalized["close"].iloc[-1])
    if latest_stamp.date() < snapshot_stamp.date():
        return float(normalized["close"].iloc[-1])
    if latest_stamp.date() == snapshot_stamp.date():
        return float(normalized["close"].iloc[-2])
    return float(normalized["close"].iloc[-1])


def intraday_metrics(frame: pd.DataFrame) -> Dict[str, float]:
    source_vwap = None
    if "均价" in frame.columns:
        avg_series = pd.to_numeric(frame["均价"], errors="coerce").dropna()
        if not avg_series.empty:
            source_vwap = float(avg_series.iloc[-1])
    normalized = normalize_ohlcv_frame(frame)
    latest = normalized.iloc[-1]
    day_open = float(normalized["open"].iloc[0])
    day_high = float(normalized["high"].max())
    day_low = float(normalized["low"].min())
    current = float(latest["close"])
    volume = normalized["volume"].fillna(0.0)
    start_time = pd.to_datetime(normalized["date"].iloc[0])
    first_30m_cutoff = start_time + pd.Timedelta(minutes=30)
    first_30m = normalized[normalized["date"] <= first_30m_cutoff]
    if first_30m.empty:
        first_30m = normalized.iloc[:1]
    first_30m_close = float(first_30m["close"].iloc[-1])
    first_30m_volume = float(first_30m["volume"].fillna(0.0).sum())

    if source_vwap is not None and source_vwap > 0:
        vwap = source_vwap
    elif "amount" in normalized.columns and normalized["amount"].notna().any():
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
        "first_30m_change_pct": float(first_30m_close / day_open - 1) if day_open else 0.0,
        "first_30m_volume_share": float(first_30m_volume / max(volume.sum(), 1.0)),
        "range_position": range_pos,
    }


def build_intraday_snapshot(
    symbol: str,
    asset_type: str,
    config: Mapping[str, Any],
    history: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    history_frame = normalize_ohlcv_frame(history if history is not None else fetch_asset_history(symbol, asset_type, dict(config)))
    snapshot: Dict[str, Any] = {"enabled": False}
    fallback_mode = False
    snapshot_time: Optional[pd.Timestamp] = None

    try:
        intraday = fetch_intraday_history(symbol, asset_type, dict(config))
        metrics = intraday_metrics(intraday)
        intraday_frame = normalize_ohlcv_frame(intraday)
        if not intraday_frame.empty:
            snapshot_time = _to_timestamp(intraday_frame["date"].iloc[-1])
    except Exception:
        latest = history_frame.iloc[-1:]
        metrics = intraday_metrics(latest)
        metrics["vwap"] = float((metrics["open"] + metrics["high"] + metrics["low"] + metrics["current"]) / 4)
        fallback_mode = True
        if not history_frame.empty:
            snapshot_time = _to_timestamp(history_frame["date"].iloc[-1])

    realtime = (
        fetch_cn_etf_realtime_row(symbol, config)
        if asset_type == "cn_etf"
        else fetch_cn_stock_realtime_row(symbol, config)
        if asset_type == "cn_stock"
        else {}
    )
    if realtime:
        for key in ("current", "open", "high", "low", "volume"):
            if realtime.get(key) is not None:
                metrics[key] = float(realtime[key])
        if metrics.get("open"):
            metrics["change_pct"] = float(metrics["current"] / metrics["open"] - 1) if metrics["open"] else 0.0
        high = float(metrics["high"])
        low = float(metrics["low"])
        metrics["range_position"] = 0.5 if high == low else float((float(metrics["current"]) - low) / (high - low))
        snapshot_time = realtime.get("updated_at") or realtime.get("data_date") or snapshot_time

    auction = fetch_cn_stock_auction_row(symbol, config) if asset_type == "cn_stock" else {}
    limit_row = fetch_cn_stock_limit_row(symbol, config) if asset_type == "cn_stock" else {}

    prev_close = realtime.get("prev_close")
    if prev_close is None:
        prev_close = infer_previous_close(history_frame, snapshot_time)
    else:
        prev_close = float(prev_close)
    vs_prev_close = float(metrics["current"] / prev_close - 1) if prev_close else 0.0
    opening_gap = float(metrics["open"] / prev_close - 1) if prev_close and metrics.get("open") else 0.0

    if metrics["current"] > metrics["vwap"] and metrics["range_position"] > 0.6:
        trend = "偏强"
    elif metrics["current"] < metrics["vwap"] and metrics["range_position"] < 0.4:
        trend = "偏弱"
    else:
        trend = "震荡"

    snapshot.update(
        {
            "enabled": True,
            "fallback_mode": fallback_mode,
            "current": float(metrics["current"]),
            "open": float(metrics["open"]),
            "high": float(metrics["high"]),
            "low": float(metrics["low"]),
            "prev_close": float(prev_close),
            "vwap": float(metrics["vwap"]),
            "range_position": float(metrics["range_position"]),
            "opening_gap": opening_gap,
            "change_vs_prev_close": vs_prev_close,
            "change_vs_open": float(metrics["change_pct"]),
            "first_30m_change": float(metrics.get("first_30m_change_pct", 0.0)),
            "first_30m_volume_share": float(metrics.get("first_30m_volume_share", 0.0)),
            "trend": trend,
            "auction_price": _to_float(auction.get("auction_price")),
            "auction_gap": _to_float(auction.get("auction_gap")),
            "auction_amount": _to_float(auction.get("auction_amount")),
            "auction_volume_ratio": _to_float(auction.get("auction_volume_ratio")),
            "auction_turnover_rate": _to_float(auction.get("auction_turnover_rate")),
            "up_limit": _to_float(limit_row.get("up_limit")),
            "down_limit": _to_float(limit_row.get("down_limit")),
            "commentary": (
                "盘中价格站上 VWAP 且处于日内高位区域，更接近强势承接。"
                if trend == "偏强"
                else "盘中价格弱于 VWAP 且靠近日内低位，更像承接不足。"
                if trend == "偏弱"
                else "盘中价格围绕 VWAP 摆动，今天更像日内震荡而不是单边确认。"
            ),
        }
    )
    if auction:
        snapshot["auction_commentary"] = (
            "集合竞价高开且量比放大，开盘更像主动抢筹。"
            if (snapshot.get("auction_gap") or 0.0) > 0.01 and (snapshot.get("auction_volume_ratio") or 0.0) >= 1.2
            else "集合竞价低开且量比放大，开盘更像主动兑现。"
            if (snapshot.get("auction_gap") or 0.0) < -0.01 and (snapshot.get("auction_volume_ratio") or 0.0) >= 1.2
            else "集合竞价没有出现特别强的方向性信号，更适合等开盘后确认。"
        )
    if snapshot.get("up_limit") is not None and snapshot.get("down_limit") is not None:
        current = float(snapshot["current"])
        up_limit = float(snapshot["up_limit"])
        down_limit = float(snapshot["down_limit"])
        snapshot["limit_distance_up"] = float(up_limit / current - 1) if current and up_limit else None
        snapshot["limit_distance_down"] = float(current / down_limit - 1) if current and down_limit else None
        if current >= up_limit * 0.995:
            snapshot["limit_commentary"] = "当前价格已经非常接近涨停边界，追价更要考虑次日溢价能否延续。"
        elif current <= down_limit * 1.005:
            snapshot["limit_commentary"] = "当前价格已经非常接近跌停边界，短线更多要先看流动性和承接。"
        else:
            snapshot["limit_commentary"] = "涨跌停边界仍有空间，当前更需要结合 VWAP、量价和竞价状态判断执行节奏。"
    return snapshot


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
