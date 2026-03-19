"""Tests for the opportunity engine helpers."""

from __future__ import annotations

from datetime import datetime
import re
import time

import numpy as np
import pandas as pd

from src.collectors.fund_profile import FundProfileCollector
from src.collectors.market_cn import ChinaMarketCollector
from src.collectors.market_drivers import MarketDriversCollector
from src.collectors.valuation import ValuationCollector
from src.collectors.news import NewsCollector
from src.processors.opportunity_engine import PoolItem, _action_plan, _catalyst_dimension, _chips_dimension, _client_safe_issue, _company_forward_events, _direct_company_event_search_terms, _fund_specific_catalyst_profile, _fundamental_dimension, _hard_checks, _is_high_confidence_company_news, _macro_dimension, _preferred_catalyst_sources, _refresh_action_from_signal_confidence, _relative_strength_dimension, _risk_dimension, _seasonality_dimension, _signal_confidence_warning_line, _stock_name_tokens, _technical_dimension, analyze_opportunity, build_default_pool, build_fund_pool, build_market_context, build_stock_pool, discover_fund_opportunities, discover_opportunities, discover_stock_opportunities
from src.processors.opportunity_engine import _asset_note
from src.processors.horizon import build_analysis_horizon_profile
from src.utils.market import compute_history_metrics


def test_compute_history_metrics_prefers_amount_column_for_turnover():
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=25, freq="B"),
            "open": [2.0] * 25,
            "high": [2.1] * 25,
            "low": [1.9] * 25,
            "close": [2.0 + i * 0.01 for i in range(25)],
            "volume": [1_000_000] * 25,
            "amount": [120_000_000.0] * 25,
        }
    )
    metrics = compute_history_metrics(frame)
    assert metrics["avg_turnover_20d"] == 120_000_000.0


def test_client_safe_issue_hides_raw_exception_details() -> None:
    message = _client_safe_issue("全球代理数据缺失", RuntimeError("Too Many Requests. Rate limited. Try after a while."))
    assert "Too Many Requests" not in message
    assert "限流" in message


def test_build_default_pool_prefers_tushare_etf_universe(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_universe_snapshot",
        lambda self: pd.DataFrame(
            [
                {
                    "symbol": "510880",
                    "name": "红利ETF",
                    "benchmark": "上证红利指数收益率",
                    "invest_type": "被动指数型",
                    "fund_type": "股票型",
                    "management": "华泰柏瑞基金",
                    "trade_date": "2026-03-12",
                    "list_date": "2007-01-18",
                    "delist_date": "",
                    "amount": 238_000_000.0,
                },
                {
                    "symbol": "511010",
                    "name": "国债ETF",
                    "benchmark": "上证5年期国债指数收益率",
                    "invest_type": "被动指数型",
                    "fund_type": "债券型",
                    "management": "某基金",
                    "trade_date": "2026-03-12",
                    "list_date": "2013-03-25",
                    "delist_date": "",
                    "amount": 500_000_000.0,
                },
            ]
        ),
    )
    monkeypatch.setattr("src.processors.opportunity_engine.load_watchlist", lambda: [])
    pool, warnings = build_default_pool({"opportunity": {"min_turnover": 50_000_000, "max_scan_candidates": 30}})
    assert warnings == []
    assert [item.symbol for item in pool] == ["510880"]
    assert pool[0].source == "tushare_etf_universe"


def test_build_default_pool_theme_filter_uses_current_filtered_index(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_universe_snapshot",
        lambda self: pd.DataFrame(
            [
                {
                    "symbol": "510880",
                    "name": "红利ETF",
                    "benchmark": "上证红利指数收益率",
                    "invest_type": "被动指数型",
                    "fund_type": "股票型",
                    "management": "华泰柏瑞基金",
                    "trade_date": "2026-03-12",
                    "list_date": "2007-01-18",
                    "delist_date": "",
                    "amount": 238_000_000.0,
                },
                {
                    "symbol": "159981",
                    "name": "能源化工ETF",
                    "benchmark": "易盛郑商所能源化工指数A收益率",
                    "invest_type": "商品型",
                    "fund_type": "商品型",
                    "management": "建信基金",
                    "trade_date": "2026-03-12",
                    "list_date": "2019-12-13",
                    "delist_date": "",
                    "amount": 498_000_000.0,
                },
            ]
        ),
    )
    monkeypatch.setattr("src.processors.opportunity_engine.load_watchlist", lambda: [])
    pool, warnings = build_default_pool(
        {"opportunity": {"min_turnover": 50_000_000, "max_scan_candidates": 30}},
        theme_filter="红利",
    )
    assert warnings == []
    assert [item.symbol for item in pool] == ["510880"]


def test_build_default_pool_watchlist_fallback_keeps_only_cn_etf(monkeypatch):
    def broken_snapshot(self):  # noqa: ANN001
        raise RuntimeError("snapshot offline")

    def broken_realtime(self):  # noqa: ANN001
        raise RuntimeError("realtime offline")

    monkeypatch.setattr(ChinaMarketCollector, "get_etf_universe_snapshot", broken_snapshot)
    monkeypatch.setattr(ChinaMarketCollector, "get_etf_realtime", broken_realtime)
    monkeypatch.setattr(
        "src.processors.opportunity_engine.load_watchlist",
        lambda: [
            {"symbol": "QQQM", "name": "Invesco NASDAQ 100 ETF", "asset_type": "us", "sector": "科技"},
            {"symbol": "513120", "name": "港股创新药ETF", "asset_type": "cn_etf", "sector": "医药"},
            {"symbol": "561380", "name": "电网ETF", "asset_type": "cn_etf", "sector": "电网"},
        ],
    )

    pool, warnings = build_default_pool({"opportunity": {"min_turnover": 50_000_000, "max_scan_candidates": 30}})

    assert any("watchlist" in item for item in warnings)
    assert [item.symbol for item in pool] == ["513120", "561380"]
    assert all(item.asset_type == "cn_etf" for item in pool)


def test_build_default_pool_dedupes_same_benchmark_by_turnover(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_universe_snapshot",
        lambda self: pd.DataFrame(
            [
                {
                    "symbol": "510880",
                    "name": "红利ETF",
                    "benchmark": "上证红利指数收益率",
                    "invest_type": "被动指数型",
                    "fund_type": "股票型",
                    "management": "华泰柏瑞基金",
                    "trade_date": "2026-03-12",
                    "list_date": "2007-01-18",
                    "delist_date": "",
                    "amount": 238_000_000.0,
                },
                {
                    "symbol": "515180",
                    "name": "红利低波ETF",
                    "benchmark": "上证红利指数收益率",
                    "invest_type": "被动指数型",
                    "fund_type": "股票型",
                    "management": "某基金",
                    "trade_date": "2026-03-12",
                    "list_date": "2022-01-18",
                    "delist_date": "",
                    "amount": 120_000_000.0,
                },
            ]
        ),
    )
    monkeypatch.setattr("src.processors.opportunity_engine.load_watchlist", lambda: [])

    pool, warnings = build_default_pool({"opportunity": {"min_turnover": 50_000_000, "max_scan_candidates": 30}})

    assert warnings == []
    assert [item.symbol for item in pool] == ["510880"]


def test_build_default_pool_falls_back_to_realtime_universe_before_watchlist(monkeypatch):
    monkeypatch.setattr(ChinaMarketCollector, "get_etf_universe_snapshot", lambda self: pd.DataFrame())
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_realtime",
        lambda self: pd.DataFrame(
            [
                {
                    "代码": "513120",
                    "名称": "港股创新药ETF",
                    "成交额": 1_230_000_000.0,
                    "数据日期": "2026-03-12",
                },
                {
                    "代码": "159981",
                    "名称": "能源化工ETF建信",
                    "成交额": 4_609_059_453.95,
                    "数据日期": "2026-03-12",
                },
            ]
        ),
    )
    monkeypatch.setattr(
        ChinaMarketCollector,
        "_ts_fund_basic_snapshot",
        lambda self, market="E": pd.DataFrame(  # noqa: ARG005
            [
                {
                    "ts_code": "513120.SH",
                    "name": "港股创新药ETF",
                    "management": "广发基金",
                    "fund_type": "股票型",
                    "found_date": "20220701",
                    "list_date": "20220708",
                    "benchmark": "中证香港创新药指数收益率(人民币计价)",
                    "status": "L",
                    "invest_type": "被动指数型",
                    "delist_date": None,
                },
                {
                    "ts_code": "159981.SZ",
                    "name": "能源化工ETF建信",
                    "management": "建信基金",
                    "fund_type": "商品型",
                    "found_date": "20191213",
                    "list_date": "20191224",
                    "benchmark": "易盛郑商所能源化工指数A收益率",
                    "status": "L",
                    "invest_type": "商品型",
                    "delist_date": None,
                },
            ]
        ),
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine.load_watchlist",
        lambda: [{"symbol": "QQQM", "name": "Invesco NASDAQ 100 ETF", "asset_type": "us", "sector": "科技"}],
    )

    pool, warnings = build_default_pool({"opportunity": {"min_turnover": 50_000_000, "max_scan_candidates": 30}})

    assert warnings == []
    assert [item.symbol for item in pool] == ["159981", "513120"]
    assert all(item.source == "realtime_etf_universe" for item in pool)


def test_seasonality_dimension_handles_range_index_history():
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=260, freq="B"),
            "open": [2.0 + i * 0.002 for i in range(260)],
            "high": [2.1 + i * 0.002 for i in range(260)],
            "low": [1.9 + i * 0.002 for i in range(260)],
            "close": [2.0 + i * 0.002 for i in range(260)],
            "volume": [1_000_000] * 260,
            "amount": [200_000_000.0] * 260,
        }
    ).reset_index(drop=True)
    dimension = _seasonality_dimension({"sector": "电网"}, history, {})
    assert dimension["score"] is not None
    assert dimension["name"] == "季节/日历"
    assert any(factor["name"] == "指数调整" and factor["display_score"] != "缺失" for factor in dimension["factors"])


def test_fundamental_dimension_prefers_real_index_valuation(monkeypatch):
    def fake_snapshot(self, keywords):  # noqa: ANN001
        assert "芯片" in keywords
        return {"index_code": "980017", "index_name": "国证芯片", "pe_ttm": 99.9}

    monkeypatch.setattr(ValuationCollector, "get_cn_index_snapshot", fake_snapshot)
    dimension = _fundamental_dimension(
        "512480",
        "cn_etf",
        {"name": "半导体ETF", "sector": "科技", "chain_nodes": ["半导体"]},
        {"price_percentile_1y": 0.83},
        {},
    )
    assert dimension["valuation_snapshot"]["index_name"] == "国证芯片"
    assert dimension["valuation_extreme"] is True
    assert any(factor["name"] == "真实指数估值" for factor in dimension["factors"])


def test_fundamental_dimension_uses_financial_proxy_and_sector_flow(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_snapshot",
        lambda self, keywords: {"index_code": "931160", "index_name": "中证军工", "pe_ttm": 45.0},  # noqa: ARG005
    )
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_value_history",
        lambda self, index_code: pd.DataFrame(  # noqa: ARG005
            {
                "日期": pd.date_range("2025-01-01", periods=4, freq="QE"),
                "市盈率2": [30.0, 32.0, 40.0, 45.0],
                "股息率2": [1.1, 1.0, 0.9, 0.8],
            }
        ),
    )
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_financial_proxies",
        lambda self, index_code, top_n=5: {  # noqa: ARG005
            "revenue_yoy": 18.0,
            "profit_yoy": 22.0,
            "roe": 13.5,
            "gross_margin": 27.0,
            "coverage_weight": 41.2,
            "report_date": "2025-09-30",
            "top_concentration": 39.5,
        },
    )
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_fund_flow",
        lambda self, symbol: (_ for _ in ()).throw(RuntimeError("no etf flow")),  # noqa: ARG005
    )
    monkeypatch.setattr(
        MarketDriversCollector,
        "collect",
        lambda self: {  # noqa: ARG005
            "industry_fund_flow": pd.DataFrame(
                [{"名称": "国防军工", "今日主力净流入-净额": 88_000_000, "今日主力净流入-净占比": 1.6}]
            ),
            "concept_fund_flow": pd.DataFrame(),
        },
    )
    dimension = _fundamental_dimension(
        "512660",
        "cn_etf",
        {"name": "军工ETF", "sector": "军工", "chain_nodes": ["军工", "地缘风险"]},
        {"price_percentile_1y": 0.92},
        {},
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert factors["盈利增速"]["display_score"] != "缺失"
    assert factors["ROE"]["display_score"] != "缺失"
    assert factors["毛利率"]["display_score"] != "缺失"
    assert factors["PEG 代理"]["display_score"] != "缺失"
    assert factors["资金承接"]["display_score"] != "缺失"


def test_fundamental_dimension_for_cn_stock_prefers_financial_proxy_pe_ttm(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_financial_proxy",
        lambda self, symbol: {  # noqa: ARG005
            "pe_ttm": 24.5,
            "pb": 3.1,
            "revenue_yoy": 18.0,
            "profit_yoy": 22.0,
            "roe": 15.0,
            "gross_margin": 28.0,
            "report_date": "2025-12-31",
        },
    )
    monkeypatch.setattr(MarketDriversCollector, "collect", lambda self: {})  # noqa: ARG005
    dimension = _fundamental_dimension(
        "300750",
        "cn_stock",
        {
            "name": "宁德时代",
            "sector": "新能源",
            "chain_nodes": ["动力电池"],
            "pe_ttm": 204.1,
            "pe_dynamic": 204.1,
        },
        {"price_percentile_1y": 0.55},
        {},
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert dimension["valuation_snapshot"]["pe_ttm"] == 24.5
    assert dimension["valuation_extreme"] is False


def test_technical_dimension_core_signal_uses_adx_not_di_values():
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=40, freq="B"),
            "open": [10.0 + i * 0.1 for i in range(40)],
            "high": [10.5 + i * 0.1 for i in range(40)],
            "low": [9.5 + i * 0.1 for i in range(40)],
            "close": [10.0 + i * 0.1 for i in range(40)],
            "volume": [1_000_000] * 40,
            "amount": [100_000_000.0] * 40,
        }
    )
    technical = {
        "macd": {"DIF": -0.2, "DEA": -0.1},
        "dmi": {"ADX": 29.4, "DI+": 55.3, "DI-": 18.2},
        "rsi": {"RSI": 47.0},
        "fibonacci": {"levels": {"0.382": 11.0, "0.500": 10.8, "0.618": 10.6}},
        "candlestick": [],
        "volume_ratio": 0.94,
        "ma_system": {"mas": {"MA5": 13.5, "MA20": 12.4, "MA60": 10.7}, "signal": "bullish"},
        "bollinger": {"position": "neutral"},
    }
    dimension = _technical_dimension(history, technical)
    adx_factor = next(f for f in dimension["factors"] if f["name"] == "ADX")
    assert adx_factor["signal"] == "ADX 29.4 · 多头占优"
    assert "55.3" not in dimension["core_signal"]


def test_technical_dimension_does_not_reward_bearish_adx_trend():
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=40, freq="B"),
            "open": [10.0 + i * 0.1 for i in range(40)],
            "high": [10.5 + i * 0.1 for i in range(40)],
            "low": [9.5 + i * 0.1 for i in range(40)],
            "close": [10.0 + i * 0.1 for i in range(40)],
            "volume": [1_000_000] * 40,
            "amount": [100_000_000.0] * 40,
        }
    )
    technical = {
        "macd": {"DIF": -0.2, "DEA": -0.1},
        "dmi": {"ADX": 33.9, "DI+": 24.2, "DI-": 31.1},
        "rsi": {"RSI": 44.7},
        "fibonacci": {"levels": {"0.382": 11.0, "0.500": 10.8, "0.618": 10.6}},
        "candlestick": ["marubozu"],
        "volume": {"vol_ratio": 0.94},
        "ma_system": {"mas": {"MA5": 13.5, "MA20": 12.4, "MA60": 10.7}, "signal": "bullish"},
        "bollinger": {"signal": "neutral"},
    }
    dimension = _technical_dimension(history, technical)
    adx_factor = next(f for f in dimension["factors"] if f["name"] == "ADX")
    assert adx_factor["display_score"] == "0/20"
    assert "空头占优" in adx_factor["signal"]


def test_technical_dimension_rewards_volume_structure_obv_kdj_and_compression():
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=40, freq="B"),
            "open": [10.0 + i * 0.05 for i in range(40)],
            "high": [10.4 + i * 0.05 for i in range(40)],
            "low": [9.7 + i * 0.05 for i in range(40)],
            "close": [10.1 + i * 0.05 for i in range(40)],
            "volume": [1_000_000] * 40,
            "amount": [100_000_000.0] * 40,
        }
    )
    technical = {
        "macd": {"DIF": 0.2, "DEA": 0.1},
        "dmi": {"ADX": 31.0, "DI+": 33.5, "DI-": 18.1},
        "kdj": {"K": 31.0, "D": 24.0, "J": 45.0, "cross": "golden_cross", "zone": "oversold"},
        "obv": {"OBV": 12_000_000, "MA": 11_000_000, "slope_5d": 800_000, "signal": "bullish"},
        "rsi": {"RSI": 43.0},
        "fibonacci": {"levels": {"0.382": 11.8, "0.500": 11.4, "0.618": 11.0}},
        "candlestick": ["hammer"],
        "volume": {
            "vol_ratio": 1.65,
            "vol_ratio_20": 1.42,
            "amount_ratio_20": 1.38,
            "price_change_1d": 0.031,
            "structure": "放量突破",
        },
        "ma_system": {"mas": {"MA5": 11.7, "MA20": 11.3, "MA60": 10.8}, "signal": "bullish"},
        "bollinger": {"signal": "neutral"},
        "volatility": {"NATR": 0.022, "atr_ratio_20": 0.84, "boll_width_percentile": 0.22, "signal": "compressed"},
    }

    dimension = _technical_dimension(history, technical)
    factors = {factor["name"]: factor for factor in dimension["factors"]}

    assert factors["KDJ"]["display_score"] == "10/10"
    assert factors["OBV"]["display_score"] == "10/10"
    assert factors["量价结构"]["display_score"] == "15/15"
    assert "放量突破" in factors["量价结构"]["signal"]
    assert factors["波动压缩"]["display_score"] == "10/10"


def test_technical_dimension_penalizes_bearish_divergence():
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=40, freq="B"),
            "open": [10.0 + i * 0.05 for i in range(40)],
            "high": [10.4 + i * 0.05 for i in range(40)],
            "low": [9.7 + i * 0.05 for i in range(40)],
            "close": [10.1 + i * 0.05 for i in range(40)],
            "volume": [1_000_000] * 40,
            "amount": [100_000_000.0] * 40,
        }
    )
    technical = {
        "macd": {"DIF": 0.2, "DEA": 0.1},
        "dmi": {"ADX": 28.0, "DI+": 31.0, "DI-": 19.0},
        "kdj": {"K": 68.0, "D": 62.0, "J": 80.0, "cross": "golden_cross", "zone": "overbought"},
        "obv": {"OBV": 12_000_000, "MA": 11_500_000, "slope_5d": 300_000, "signal": "bullish"},
        "divergence": {
            "signal": "bearish",
            "kind": "顶背离",
            "label": "价格高点抬升，但 RSI / MACD 未同步创新高（顶背离）",
            "indicators": ["RSI", "MACD"],
            "strength": 2,
            "detail": "最近两组高点里价格创新高，但 RSI / MACD 没有同步创新高。",
        },
        "rsi": {"RSI": 63.0},
        "fibonacci": {"levels": {"0.382": 11.8, "0.500": 11.4, "0.618": 11.0}},
        "candlestick": [],
        "volume": {"vol_ratio": 1.1, "vol_ratio_20": 1.0, "price_change_1d": 0.008, "structure": "量价中性"},
        "ma_system": {"mas": {"MA5": 11.7, "MA20": 11.3, "MA60": 10.8}, "signal": "bullish"},
        "bollinger": {"signal": "near_upper"},
        "volatility": {"NATR": 0.026, "atr_ratio_20": 1.02, "boll_width_percentile": 0.55, "signal": "neutral"},
    }

    dimension = _technical_dimension(history, technical)
    divergence_factor = next(f for f in dimension["factors"] if f["name"] == "量价/动量背离")

    assert divergence_factor["display_score"] == "-8/10"
    assert "顶背离" in divergence_factor["signal"]


def test_technical_dimension_surfaces_nearby_pressure_levels():
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=40, freq="B"),
            "open": [10.0 + i * 0.04 for i in range(40)],
            "high": [10.3 + i * 0.04 for i in range(39)] + [11.82],
            "low": [9.8 + i * 0.04 for i in range(40)],
            "close": [10.1 + i * 0.04 for i in range(39)] + [11.55],
            "volume": [1_000_000] * 40,
            "amount": [100_000_000.0] * 40,
        }
    )
    technical = {
        "macd": {"DIF": 0.18, "DEA": 0.12},
        "dmi": {"ADX": 26.0, "DI+": 28.0, "DI-": 18.0},
        "kdj": {"K": 58.0, "D": 53.0, "J": 68.0, "cross": "neutral", "zone": "neutral"},
        "obv": {"OBV": 11_500_000, "MA": 11_200_000, "slope_5d": 200_000, "signal": "bullish"},
        "divergence": {"signal": "neutral", "label": "未识别到明确顶/底背离", "detail": "无", "strength": 0},
        "rsi": {"RSI": 57.0},
        "fibonacci": {"levels": {"0.382": 11.0, "0.500": 11.2, "0.618": 11.35, "0.786": 11.72}, "swing_high": 11.82},
        "candlestick": [],
        "volume": {"vol_ratio": 1.0, "vol_ratio_20": 1.0, "price_change_1d": 0.006, "structure": "量价中性"},
        "ma_system": {"mas": {"MA5": 11.45, "MA20": 11.70, "MA60": 10.80}, "signal": "bullish"},
        "bollinger": {"signal": "neutral"},
        "volatility": {"NATR": 0.024, "atr_ratio_20": 0.98, "boll_width_percentile": 0.48, "signal": "neutral"},
    }

    dimension = _technical_dimension(history, technical)
    pressure_factor = next(f for f in dimension["factors"] if f["name"] == "压力位")

    assert pressure_factor["awarded"] < 0
    assert "上方存在近端压力" in pressure_factor["signal"]
    assert any(token in pressure_factor["signal"] for token in ("MA20", "近20日高点", "摆动前高", "斐波那契 0.786"))


def test_technical_dimension_rewards_bullish_multi_candle_pattern():
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=40, freq="B"),
            "open": [10.0 + i * 0.05 for i in range(40)],
            "high": [10.4 + i * 0.05 for i in range(40)],
            "low": [9.7 + i * 0.05 for i in range(40)],
            "close": [10.1 + i * 0.05 for i in range(40)],
            "volume": [1_000_000] * 40,
            "amount": [100_000_000.0] * 40,
        }
    )
    technical = {
        "macd": {"DIF": 0.1, "DEA": 0.05},
        "dmi": {"ADX": 24.0, "DI+": 30.0, "DI-": 19.0},
        "kdj": {"K": 52.0, "D": 50.0, "J": 56.0, "cross": "neutral", "zone": "neutral"},
        "obv": {"OBV": 10_000_000, "MA": 9_900_000, "slope_5d": 120_000, "signal": "bullish"},
        "divergence": {"signal": "neutral", "label": "未识别到明确顶/底背离", "detail": "无", "strength": 0},
        "rsi": {"RSI": 48.0},
        "fibonacci": {"levels": {"0.382": 11.8, "0.500": 11.4, "0.618": 11.0}},
        "candlestick": ["morning_star", "hammer"],
        "volume": {"vol_ratio": 1.0, "vol_ratio_20": 1.0, "price_change_1d": 0.008, "structure": "量价中性"},
        "ma_system": {"mas": {"MA5": 11.7, "MA20": 11.3, "MA60": 10.8}, "signal": "bullish"},
        "bollinger": {"signal": "neutral"},
        "volatility": {"NATR": 0.025, "atr_ratio_20": 0.95, "boll_width_percentile": 0.42, "signal": "neutral"},
    }

    dimension = _technical_dimension(history, technical)
    candle_factor = next(f for f in dimension["factors"] if f["name"] == "K线形态")

    assert candle_factor["display_score"] == "10/10"
    assert "早晨之星" in candle_factor["signal"]


def test_technical_dimension_penalizes_bearish_multi_candle_pattern():
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=40, freq="B"),
            "open": [10.0 + i * 0.05 for i in range(40)],
            "high": [10.4 + i * 0.05 for i in range(40)],
            "low": [9.7 + i * 0.05 for i in range(40)],
            "close": [10.1 + i * 0.05 for i in range(40)],
            "volume": [1_000_000] * 40,
            "amount": [100_000_000.0] * 40,
        }
    )
    technical = {
        "macd": {"DIF": 0.1, "DEA": 0.05},
        "dmi": {"ADX": 24.0, "DI+": 30.0, "DI-": 19.0},
        "kdj": {"K": 70.0, "D": 67.0, "J": 76.0, "cross": "golden_cross", "zone": "overbought"},
        "obv": {"OBV": 10_000_000, "MA": 9_900_000, "slope_5d": 120_000, "signal": "bullish"},
        "divergence": {"signal": "neutral", "label": "未识别到明确顶/底背离", "detail": "无", "strength": 0},
        "rsi": {"RSI": 66.0},
        "fibonacci": {"levels": {"0.382": 11.8, "0.500": 11.4, "0.618": 11.0}},
        "candlestick": ["evening_star"],
        "volume": {"vol_ratio": 1.0, "vol_ratio_20": 1.0, "price_change_1d": -0.012, "structure": "量价中性"},
        "ma_system": {"mas": {"MA5": 11.7, "MA20": 11.3, "MA60": 10.8}, "signal": "bullish"},
        "bollinger": {"signal": "near_upper"},
        "volatility": {"NATR": 0.028, "atr_ratio_20": 1.08, "boll_width_percentile": 0.62, "signal": "neutral"},
    }

    dimension = _technical_dimension(history, technical)
    candle_factor = next(f for f in dimension["factors"] if f["name"] == "K线形态")

    assert candle_factor["display_score"] == "-10/10"
    assert "黄昏之星" in candle_factor["signal"]


def test_relative_strength_dimension_penalizes_underperformance_and_weak_breadth():
    dates = pd.date_range("2026-02-01", periods=20, freq="B")
    asset_returns = pd.Series([-0.006] * 20, index=dates)
    benchmark_returns = pd.Series([0.002] * 20, index=dates)
    context = {
        "benchmark_returns": {"cn_etf": benchmark_returns},
        "drivers": {
            "industry_spot": pd.DataFrame(
                [
                    {
                        "板块名称": "科技",
                        "涨跌幅": -1.8,
                        "上涨家数": 8,
                        "下跌家数": 30,
                    }
                ]
            )
        },
        "day_theme": {},
        "regime": {"preferred_assets": []},
    }
    dimension = _relative_strength_dimension(
        "512480",
        "cn_etf",
        {"name": "半导体ETF", "sector": "科技", "chain_nodes": ["半导体"]},
        {"return_5d": -0.040, "return_20d": -0.120},
        asset_returns,
        context,
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert factors["超额拐点"]["awarded"] < 0
    assert factors["板块扩散"]["awarded"] < 0
    assert factors["行业宽度"]["awarded"] < 0
    assert "相对基准" in dimension["core_signal"]


def test_analyze_opportunity_retries_cn_history_before_snapshot_fallback(monkeypatch):
    sample_history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=80, freq="B"),
            "open": np.linspace(10.0, 12.0, 80),
            "high": np.linspace(10.3, 12.4, 80),
            "low": np.linspace(9.8, 11.7, 80),
            "close": np.linspace(10.1, 12.2, 80),
            "volume": np.linspace(1_000_000, 1_500_000, 80),
            "amount": np.linspace(100_000_000.0, 150_000_000.0, 80),
        }
    )
    sample_history.attrs["history_source"] = "tushare"
    sample_history.attrs["history_source_label"] = "Tushare 日线"
    attempts = {"count": 0}

    def fake_fetch_asset_history(symbol, asset_type, config):  # noqa: ANN001
        attempts["count"] += 1
        raise RuntimeError("temporary first-pass failure")

    monkeypatch.setattr("src.processors.opportunity_engine.fetch_asset_history", fake_fetch_asset_history)
    monkeypatch.setattr("src.processors.opportunity_engine._retry_china_history_after_failure", lambda symbol, asset_type, config: sample_history)  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", lambda config, relevant_asset_types=None: {})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._collect_fund_profile", lambda *args, **kwargs: {})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._safe_history", lambda *args, **kwargs: None)  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._intraday_snapshot", lambda *args, **kwargs: {"enabled": False})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._correlation_to_watchlist", lambda *args, **kwargs: None)  # noqa: ARG005

    analysis = analyze_opportunity("300750", "cn_stock", {})

    assert attempts["count"] == 1
    assert analysis["history_fallback_mode"] is False
    assert analysis["metadata"]["history_source"] == "tushare"
    assert any("重试中国市场主链" in note for note in analysis["notes"])


def test_macro_dimension_uses_leading_macro_indicators_for_growth_sector():
    dimension = _macro_dimension(
        {"sector": "科技"},
        {
            "china_macro": {
                "pmi": 50.8,
                "pmi_new_orders": 51.2,
                "pmi_production": 51.6,
                "demand_state": "improving",
                "ppi_yoy": -0.5,
                "price_state": "reflation",
                "credit_impulse": "expanding",
                "m1_m2_spread": -2.1,
                "social_financing_3m_avg_text": "2.58 万亿元",
            },
            "regime": {"current_regime": "recovery"},
            "monitor_rows": [
                {"name": "布伦特原油", "return_5d": -0.03},
                {"name": "美国10Y收益率", "return_5d": -0.01},
                {"name": "美元指数", "return_20d": -0.02},
                {"name": "USDCNY", "return_20d": -0.01},
            ],
        },
    )

    assert dimension["score"] is not None
    assert dimension["score"] >= 25
    assert any(factor["name"] == "景气方向" and "新订单" in factor["signal"] for factor in dimension["factors"])
    assert any(factor["name"] == "信用脉冲" and "社融" in factor["signal"] for factor in dimension["factors"])


def test_fundamental_dimension_for_us_stock_uses_single_stock_labels(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_yf_fundamental",
        lambda self, symbol, asset_type: {  # noqa: ARG005
            "pe_ttm": 28.0,
            "revenue_yoy": 23.8,
            "roe": 30.2,
            "gross_margin": 82.0,
            "report_date": "2025-12-31",
        },
    )

    dimension = _fundamental_dimension(
        "META",
        "us",
        {"name": "Meta", "sector": "科技", "asset_type": "us"},
        {"price_percentile_1y": 0.43},
        {},
    )

    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert "个股增速" in factors["盈利增速"]["signal"]
    assert "个股 ROE" in factors["ROE"]["signal"]
    assert "个股毛利率" in factors["毛利率"]["signal"]
    assert "个股增速代理" in factors["PEG 代理"]["detail"]


def test_fundamental_dimension_for_cn_fund_prefers_holdings_proxy_and_benchmark_keywords(monkeypatch):
    def fake_snapshot(self, keywords):  # noqa: ANN001
        assert "战略新兴" in keywords
        return {"index_code": "931637", "index_name": "中国战略新兴产业", "pe_ttm": 42.0}

    monkeypatch.setattr(ValuationCollector, "get_cn_index_snapshot", fake_snapshot)
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_value_history",
        lambda self, index_code: pd.DataFrame(  # noqa: ARG005
            {
                "日期": pd.date_range("2025-01-01", periods=4, freq="QE"),
                "市盈率2": [28.0, 30.0, 36.0, 42.0],
                "股息率2": [0.8, 0.7, 0.6, 0.5],
            }
        ),
    )
    monkeypatch.setattr(
        ValuationCollector,
        "get_weighted_stock_financial_proxies",
        lambda self, holdings, **kwargs: {  # noqa: ARG005
            "revenue_yoy": 24.0,
            "profit_yoy": 31.0,
            "roe": 16.5,
            "gross_margin": 29.0,
            "coverage_weight": 42.3,
            "report_date": "2025-12-31",
            "top_concentration": 44.3,
        },
    )
    monkeypatch.setattr(
        MarketDriversCollector,
        "collect",
        lambda self: {  # noqa: ARG005
            "industry_fund_flow": pd.DataFrame(
                [{"名称": "科技", "今日主力净流入-净额": 120_000_000, "今日主力净流入-净占比": 2.8}]
            ),
            "concept_fund_flow": pd.DataFrame(),
        },
    )
    fund_profile = {
        "overview": {
            "基金简称": "永赢科技智选混合发起C",
            "业绩比较基准": "中国战略新兴产业成份指数收益率*70%+恒生科技指数收益率*10%",
        },
        "top_holdings": [
            {"股票代码": "600183", "股票名称": "生益科技", "占净值比例": 9.11},
            {"股票代码": "300308", "股票名称": "中际旭创", "占净值比例": 8.85},
            {"股票代码": "002463", "股票名称": "沪电股份", "占净值比例": 8.84},
        ],
        "industry_allocation": [{"行业类别": "科技", "占净值比例": 45.2}],
    }
    dimension = _fundamental_dimension(
        "022365",
        "cn_fund",
        {"name": "永赢科技智选混合发起C", "sector": "科技", "chain_nodes": ["科技", "成长股估值修复"]},
        {"price_percentile_1y": 0.84},
        {},
        fund_profile,
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert dimension["valuation_snapshot"]["index_name"] == "中国战略新兴产业"
    assert "前五大重仓股加权增速代理" in factors["盈利增速"]["signal"]
    assert "前五大重仓股" in factors["ROE"]["signal"]
    assert factors["PEG 代理"]["display_score"] != "缺失"


def test_fundamental_dimension_for_cn_etf_prefers_fund_profile_benchmark_and_holdings_proxy(monkeypatch):
    def fake_snapshot(self, keywords):  # noqa: ANN001
        assert "中证人工智能主题指数" in keywords or "人工智能" in keywords
        return {"index_code": "930713", "index_name": "中证人工智能主题指数", "pe_ttm": 58.0}

    monkeypatch.setattr(ValuationCollector, "get_cn_index_snapshot", fake_snapshot)
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_value_history",
        lambda self, index_code: pd.DataFrame(  # noqa: ARG005
            {
                "日期": pd.date_range("2025-01-01", periods=4, freq="QE"),
                "市盈率2": [40.0, 43.0, 51.0, 58.0],
                "股息率2": [0.6, 0.5, 0.4, 0.3],
            }
        ),
    )
    monkeypatch.setattr(
        ValuationCollector,
        "get_weighted_stock_financial_proxies",
        lambda self, holdings, **kwargs: {  # noqa: ARG005
            "revenue_yoy": 28.0,
            "profit_yoy": 34.0,
            "roe": 15.8,
            "gross_margin": 31.0,
            "coverage_weight": 48.3,
            "report_date": "2025-12-31",
            "top_concentration": 46.1,
        },
    )
    monkeypatch.setattr(
        MarketDriversCollector,
        "collect",
        lambda self: {  # noqa: ARG005
            "industry_fund_flow": pd.DataFrame(
                [{"名称": "信息技术", "今日主力净流入-净额": 210_000_000, "今日主力净流入-净占比": 3.2}]
            ),
            "concept_fund_flow": pd.DataFrame(),
        },
    )
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_fund_flow",
        lambda self, symbol: (_ for _ in ()).throw(RuntimeError("no etf flow")),  # noqa: ARG005
    )
    fund_profile = {
        "overview": {
            "基金简称": "人工智能ETF易方达",
            "业绩比较基准": "中证人工智能主题指数收益率",
        },
        "top_holdings": [
            {"股票代码": "300308", "股票名称": "中际旭创", "占净值比例": 10.57},
            {"股票代码": "300502", "股票名称": "新易盛", "占净值比例": 10.52},
            {"股票代码": "688256", "股票名称": "寒武纪", "占净值比例": 9.44},
        ],
        "industry_allocation": [{"行业类别": "信息技术", "占净值比例": 88.0}],
    }
    dimension = _fundamental_dimension(
        "159819",
        "cn_etf",
        {"name": "人工智能ETF易方达", "sector": "科技", "chain_nodes": ["AI算力", "半导体"]},
        {"price_percentile_1y": 0.86},
        {},
        fund_profile,
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert dimension["valuation_snapshot"]["index_name"] == "中证人工智能主题指数"
    assert "前五大持仓/成分股加权增速代理" in factors["盈利增速"]["signal"]
    assert "前五大持仓/成分股" in factors["ROE"]["signal"]
    assert factors["PEG 代理"]["display_score"] != "缺失"


def test_fundamental_dimension_for_commodity_etf_uses_product_structure_not_stock_pe(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_snapshot",
        lambda self, keywords: (_ for _ in ()).throw(AssertionError("commodity ETF should not query stock index valuation")),  # noqa: ARG005
    )
    monkeypatch.setattr(
        ValuationCollector,
        "get_weighted_stock_financial_proxies",
        lambda self, holdings, **kwargs: (_ for _ in ()).throw(AssertionError("commodity ETF should not query stock financial proxy")),  # noqa: ARG005
    )
    fund_profile = {
        "overview": {
            "基金简称": "能源化工ETF",
            "基金类型": "商品型 / 能源化工期货型",
            "业绩比较基准": "易盛郑商所能源化工指数A收益率",
            "跟踪标的": "易盛郑商所能源化工指数A收益率",
            "净资产规模": "15.95亿元（截止至：2025年12月31日）",
        },
        "style": {
            "sector": "能源",
            "tags": ["能源主题", "被动跟踪", "保留机动仓位"],
            "cash_ratio": 92.32,
        },
        "top_holdings": [],
        "industry_allocation": [],
    }
    dimension = _fundamental_dimension(
        "159981",
        "cn_etf",
        {"name": "能源化工ETF", "sector": "能源", "benchmark": "易盛郑商所能源化工指数A收益率", "fund_style_tags": ["能源主题", "被动跟踪", "保留机动仓位"]},
        {"price_percentile_1y": 0.73},
        {},
        fund_profile,
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert dimension["valuation_snapshot"] is None
    assert dimension["financial_proxy"] == {}
    assert "产品结构评估" in dimension["summary"] or "产品结构" in dimension["summary"]
    assert factors["产品类型"]["display_score"] != "缺失"
    assert factors["跟踪标的"]["display_score"] != "缺失"
    assert factors["产品规模"]["display_score"] != "缺失"
    assert factors["结构缓冲"]["display_score"] != "缺失"
    assert "股票 PE" in factors["价格位置"]["detail"]


def test_asset_note_for_passive_cn_fund_uses_exposure_wording():
    note = _asset_note({"sector": "黄金", "is_passive_fund": True}, "cn_fund")
    assert "被动暴露" in note
    assert "基金经理" not in note


def test_fund_specific_catalyst_profile_keeps_strict_keywords_product_specific():
    profile = _fund_specific_catalyst_profile(
        {"sector": "科技", "name": "人工智能ETF易方达"},
        {
            "overview": {"业绩比较基准": "中证人工智能主题指数收益率"},
            "top_holdings": [
                {"股票名称": "中际旭创"},
                {"股票名称": "新易盛"},
                {"股票名称": "寒武纪"},
            ],
            "industry_allocation": [{"行业类别": "信息传输、软件和信息技术服务业"}],
        },
    )
    strict_keywords = profile["strict_keywords"]
    assert "中际旭创" in strict_keywords
    assert "中证人工智能主题指数" in strict_keywords
    assert "科技" not in strict_keywords


def test_chips_dimension_uses_board_level_proxies(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_snapshot",
        lambda self, keywords: {"index_code": "931160", "index_name": "中证军工", "pe_ttm": 45.0},  # noqa: ARG005
    )
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_financial_proxies",
        lambda self, index_code, top_n=5: {  # noqa: ARG005
            "top_concentration": 37.8,
            "coverage_weight": 42.0,
        },
    )
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_fund_flow",
        lambda self, symbol: (_ for _ in ()).throw(RuntimeError("no etf flow")),  # noqa: ARG005
    )
    context = {
        "drivers": {
            "industry_fund_flow": pd.DataFrame(
                [{"名称": "国防军工", "今日主力净流入-净额": 120_000_000, "今日主力净流入-净占比": 2.1}]
            ),
            "concept_fund_flow": pd.DataFrame(),
            "northbound_industry": {"frame": pd.DataFrame([{"名称": "国防军工", "北向资金今日增持估计-市值": 180_000_000}])},
            "northbound_concept": {"frame": pd.DataFrame()},
            "hot_rank": pd.DataFrame([{"名称": "军工ETF", "排名": 18}]),
        }
    }
    dimension = _chips_dimension(
        "512660",
        "cn_etf",
        {"name": "军工ETF", "sector": "军工", "chain_nodes": ["军工", "地缘风险"]},
        context,
        {},
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert factors["公募/热度代理"]["display_score"] != "缺失"
    assert factors["北向/南向"]["display_score"] != "缺失"
    assert factors["机构资金承接"]["display_score"] != "缺失"
    assert factors["机构集中度代理"]["display_score"] != "缺失"


def test_chips_dimension_falls_back_to_global_northbound_flow(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_snapshot",
        lambda self, keywords: {"index_code": "980017", "index_name": "国证芯片", "pe_ttm": 52.0},  # noqa: ARG005
    )
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_financial_proxies",
        lambda self, index_code, top_n=5: {  # noqa: ARG005
            "top_concentration": 35.0,
            "coverage_weight": 40.0,
        },
    )
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_fund_flow",
        lambda self, symbol: (_ for _ in ()).throw(RuntimeError("no etf flow")),  # noqa: ARG005
    )
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_north_south_flow",
        lambda self: pd.DataFrame([{"日期": "2026-03-10", "北向资金净流入": 600_000_000.0, "南向资金净流入": 0.0}]),  # noqa: ARG005
    )
    context = {
        "drivers": {
            "industry_fund_flow": pd.DataFrame(
                [{"名称": "科技", "今日主力净流入-净额": 120_000_000, "今日主力净流入-净占比": 2.8}]
            ),
            "concept_fund_flow": pd.DataFrame(),
            "northbound_industry": {"frame": pd.DataFrame()},
            "northbound_concept": {"frame": pd.DataFrame()},
            "hot_rank": pd.DataFrame([{"名称": "半导体ETF", "排名": 12}]),
        }
    }
    dimension = _chips_dimension(
        "512480",
        "cn_etf",
        {"name": "半导体ETF", "sector": "科技", "chain_nodes": ["半导体", "AI算力"]},
        context,
        {},
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert factors["北向/南向"]["display_score"] != "缺失"
    assert "6.00亿" in factors["北向/南向"]["signal"]


def test_chips_dimension_for_cn_stock_does_not_award_global_northbound_fallback(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_north_south_flow",
        lambda self: pd.DataFrame([{"日期": "2026-03-10", "北向资金净流入": 3_037_210_000.0, "南向资金净流入": 0.0}]),  # noqa: ARG005
    )
    context = {
        "drivers": {
            "industry_fund_flow": pd.DataFrame(),
            "concept_fund_flow": pd.DataFrame(),
            "northbound_industry": {"frame": pd.DataFrame()},
            "northbound_concept": {"frame": pd.DataFrame()},
            "hot_rank": pd.DataFrame(),
        }
    }
    dimension = _chips_dimension(
        "300308",
        "cn_stock",
        {"name": "中际旭创", "sector": "科技", "chain_nodes": ["光模块"]},
        context,
        {},
    )
    factor = next(f for f in dimension["factors"] if f["name"] == "北向/南向")
    assert factor["display_score"] == "信息项"
    assert "未再回退全市场总量" in factor["signal"]


def test_chips_dimension_for_commodity_etf_avoids_northbound_and_stock_concentration(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_fund_flow",
        lambda self, symbol: pd.DataFrame([{"净流入": 120_000_000.0}, {"净流入": 80_000_000.0}, {"净流入": -20_000_000.0}]),  # noqa: ARG005
    )
    context = {
        "fund_profile": {
            "overview": {
                "基金类型": "商品型 / 能源化工期货型",
                "业绩比较基准": "易盛郑商所能源化工指数A收益率",
            },
            "style": {
                "tags": ["能源主题", "被动跟踪", "保留机动仓位"],
            },
        },
        "drivers": {
            "industry_fund_flow": pd.DataFrame(
                [{"名称": "石油石化", "今日主力净流入-净额": 220_000_000, "今日主力净流入-净占比": 2.8}]
            ),
            "concept_fund_flow": pd.DataFrame(),
            "northbound_industry": {"frame": pd.DataFrame([{"名称": "石油石化", "北向资金今日增持估计-市值": 300_000_000}])},
            "northbound_concept": {"frame": pd.DataFrame()},
            "hot_rank": pd.DataFrame([{"名称": "石油ETF", "排名": 10}]),
        },
    }
    dimension = _chips_dimension(
        "159981",
        "cn_etf",
        {
            "name": "能源化工ETF",
            "sector": "能源",
            "benchmark": "易盛郑商所能源化工指数A收益率",
            "fund_style_tags": ["能源主题", "被动跟踪", "保留机动仓位"],
        },
        context,
        {},
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert factors["北向/南向"]["display_score"] == "不适用"
    assert factors["机构集中度代理"]["display_score"] == "不适用"
    assert factors["机构资金承接"]["display_score"] != "缺失"
    assert "ETF" in factors["机构资金承接"]["detail"]


def test_chips_dimension_penalizes_outflow_and_crowding(monkeypatch):
    monkeypatch.setattr(ValuationCollector, "get_cn_index_snapshot", lambda self, keywords: None)  # noqa: ARG005
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_fund_flow",
        lambda self, symbol: pd.DataFrame([{"净流入": -80_000_000.0}, {"净流入": -60_000_000.0}, {"净流入": -50_000_000.0}]),  # noqa: ARG005
    )
    context = {
        "drivers": {
            "industry_fund_flow": pd.DataFrame(
                [{"名称": "科技", "今日主力净流入-净额": -180_000_000, "今日主力净流入-净占比": -2.2}]
            ),
            "concept_fund_flow": pd.DataFrame(),
            "northbound_industry": {"frame": pd.DataFrame([{"名称": "科技", "北向资金今日增持估计-市值": -260_000_000}])},
            "northbound_concept": {"frame": pd.DataFrame()},
            "hot_rank": pd.DataFrame([{"名称": "半导体ETF", "排名": 6}]),
        }
    }
    dimension = _chips_dimension(
        "512480",
        "cn_etf",
        {"name": "半导体ETF", "sector": "科技", "chain_nodes": ["半导体"]},
        context,
        {},
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert factors["公募/热度代理"]["awarded"] < 0
    assert factors["北向/南向"]["awarded"] < 0
    assert factors["机构资金承接"]["awarded"] < 0
    assert "流出" in factors["机构资金承接"]["signal"]


def test_catalyst_dimension_uses_sector_profile_mapping(tmp_path):
    profile_path = tmp_path / "catalyst_profiles.yaml"
    profile_path.write_text(
        """
profiles:
  半导体:
    themes: ["半导体", "芯片"]
    keywords: ["半导体", "芯片", "semiconductor", "chip"]
    policy_keywords: ["集成电路", "自主可控"]
    domestic_leaders: ["中芯国际"]
    overseas_leaders: ["TSMC", "台积电"]
    earnings_keywords: ["earnings", "guidance", "capex"]
    event_keywords: ["财报", "扩产", "资本开支"]
""".strip(),
        encoding="utf-8",
    )
    context = {
        "config": {"catalyst_profiles_file": str(profile_path)},
        "news_report": {
            "all_items": [
                {"title": "两会提出加快集成电路自主可控", "category": "china_macro_domestic", "source": "财联社"},
                {"title": "中芯国际宣布扩产并上修全年指引", "category": "china_market_domestic", "source": "证券时报"},
                {"title": "TSMC raises capex as AI chip demand surges", "category": "semiconductor", "source": "Reuters"},
            ]
        },
        "events": [{"title": "台积电财报", "note": "资本开支与AI芯片需求"}],
    }
    dimension = _catalyst_dimension(
        {"name": "半导体ETF", "sector": "科技", "chain_nodes": ["半导体", "AI算力"]},
        context,
    )
    assert dimension["score"] is not None
    assert dimension["score"] >= 70
    assert dimension["profile_name"] == "半导体"
    signals = {factor["name"]: factor["signal"] for factor in dimension["factors"]}
    assert "集成电路" in signals["政策催化"]
    assert "中芯国际" in signals["龙头公告/业绩"]
    assert "TSMC" in signals["海外映射"]


def test_catalyst_dimension_surfaces_theme_headwind_for_etf():
    context = {
        "config": {},
        "news_report": {
            "all_items": [
                {
                    "title": "半导体库存高企且价格战加剧，芯片链景气承压 - Reuters",
                    "category": "semiconductor",
                    "source": "Reuters",
                    "configured_source": "Reuters",
                    "must_include": False,
                    "published_at": "2026-03-10",
                    "link": "",
                }
            ]
        },
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "512480", "name": "半导体ETF", "asset_type": "cn_etf", "sector": "科技", "chain_nodes": ["半导体", "芯片"]},
        context,
    )
    negative_factor = next(f for f in dimension["factors"] if f["name"] == "主题逆风")
    assert negative_factor["display_score"] == "-10"
    assert "价格战" in negative_factor["signal"] or "库存高企" in negative_factor["signal"]
    assert "价格战" in dimension["core_signal"] or "库存高企" in dimension["core_signal"]


def test_catalyst_dimension_uses_derived_profile_and_dynamic_topic_search(monkeypatch):
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [  # noqa: ARG005
            {"title": "国防预算增长带动军工订单预期", "category": "topic_search", "source": "Reuters"},
            {"title": "中航沈飞获新型战机批产订单", "category": "topic_search", "source": "财联社"},
        ],
    )
    context = {
        "config": {},
        "news_report": {"all_items": []},
        "events": [],
        "preferred_sources": ["Reuters", "财联社"],
    }
    dimension = _catalyst_dimension(
        {"symbol": "512660", "name": "军工ETF", "sector": "军工", "chain_nodes": ["军工", "地缘风险"]},
        context,
    )
    assert dimension["profile_name"] == "军工"
    assert dimension["score"] is not None
    assert dimension["score"] > 0
    signals = {factor["name"]: factor["signal"] for factor in dimension["factors"]}
    assert "订单" in signals["龙头公告/业绩"] or "军工" in signals["政策催化"]


def test_company_forward_events_do_not_scan_generic_disclosures_for_etf():
    hits = _company_forward_events(
        {"symbol": "159981", "name": "能源化工ETF", "asset_type": "cn_etf"},
        {"as_of": "2026-03-11 10:00:00"},
        news_items=[
            {
                "title": "Porsche CEO plans product overhaul to sharpen margins after 2025 tailspin - Reuters",
                "published_at": "2026-03-20",
                "source": "Reuters",
            }
        ],
    )
    assert hits == []


def test_catalyst_dimension_for_cn_fund_filters_generic_ai_noise():
    fund_profile = {
        "overview": {
            "基金简称": "永赢科技智选混合发起C",
            "业绩比较基准": "中国战略新兴产业成份指数收益率*70%+恒生科技指数收益率*10%",
        },
        "top_holdings": [
            {"股票代码": "600183", "股票名称": "生益科技", "占净值比例": 9.11},
            {"股票代码": "300308", "股票名称": "中际旭创", "占净值比例": 8.85},
            {"股票代码": "002463", "股票名称": "沪电股份", "占净值比例": 8.84},
        ],
        "industry_allocation": [{"行业类别": "科技", "占净值比例": 45.2}],
    }
    context = {
        "config": {},
        "news_report": {
            "mode": "live",
            "all_items": [
                {"title": "【早报】深圳出手，“AI龙虾”支持政策来了", "category": "ai", "source": "财联社"},
                {"title": "中际旭创拟扩产800G光模块产能", "category": "china_market_domestic", "source": "证券时报"},
                {"title": "TSMC raises capex as AI server demand surges", "category": "semiconductor", "source": "Reuters"},
            ]
        },
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "022365", "name": "永赢科技智选混合发起C", "sector": "科技", "chain_nodes": ["科技", "成长股估值修复"]},
        context,
        fund_profile,
    )
    signals = {factor["name"]: factor["signal"] for factor in dimension["factors"]}
    assert "龙虾" not in "".join(signals.values())
    assert "中际旭创" in signals["龙头公告/业绩"] or "TSMC" in signals["海外映射"]


def test_catalyst_dimension_for_grid_etf_ignores_generic_ai_noise(monkeypatch):
    captured_keywords = []
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: (  # noqa: ARG005
            captured_keywords.extend(keywords) or []
        ),
    )
    context = {
        "config": {},
        "news_report": {
            "all_items": [
                {"title": "【早报】深圳出手，“AI龙虾”支持政策来了", "category": "ai", "source": "财联社"},
                {"title": "周末要闻汇总：证监会发布短线交易监管规定；AI“养龙虾”火了", "category": "china_market_domestic", "source": "财联社"},
            ]
        },
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "561380", "name": "电网 ETF", "asset_type": "cn_etf", "sector": "电网", "chain_nodes": ["AI算力", "电力需求", "电网设备", "铜铝"]},
        context,
    )
    signals = {factor["name"]: factor["signal"] for factor in dimension["factors"]}
    assert signals["政策催化"] == "近 7 日未命中直接政策催化"
    assert signals["龙头公告/业绩"] == "未命中直接龙头公告"
    assert not any(str(keyword).lower() in {"ai", "算力", "人工智能"} for keyword in captured_keywords)


def test_hard_checks_use_fund_scale_for_cn_fund():
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=90, freq="B"),
            "open": [1.0] * 90,
            "high": [1.0] * 90,
            "low": [1.0] * 90,
            "close": [1.0 + 0.001 * i for i in range(90)],
            "volume": [0.0] * 90,
            "amount": [None] * 90,
        }
    )
    metrics = {"avg_turnover_20d": 0.0, "price_percentile_1y": 0.4, "return_5d": -0.01}
    technical = {"rsi": {"RSI": 50.0}}
    context = {"config": {"opportunity": {}}}
    fundamental_dimension = {"valuation_snapshot": {}, "valuation_extreme": False}
    checks, exclusion_reasons, _warnings = _hard_checks(
        "cn_fund",
        {"name": "测试基金"},
        history,
        metrics,
        technical,
        context,
        20,
        None,
        fundamental_dimension,
        {"overview": {"净资产规模": "12.50亿元（截止至：2025年12月31日）"}},
    )
    check_map = {item["name"]: item for item in checks}
    assert check_map["基金规模"]["status"] == "✅"
    assert "日均成交额低于 5000 万" not in exclusion_reasons


def test_hard_checks_for_us_stock_do_not_claim_etf_proxy_floor() -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=120, freq="B"),
            "open": [100.0] * 120,
            "high": [101.0] * 120,
            "low": [99.0] * 120,
            "close": [100.0 + 0.2 * i for i in range(120)],
            "volume": [1_000_000] * 120,
            "amount": [800_000_000.0] * 120,
        }
    )
    metrics = {"avg_turnover_20d": 8e8, "price_percentile_1y": 0.43, "return_5d": -0.01}
    technical = {"rsi": {"RSI": 50.0}}
    context = {"config": {"opportunity": {}}}
    fundamental_dimension = {"valuation_snapshot": {"index_name": "Meta", "pe_ttm": 27.9}, "valuation_extreme": False}

    checks, _exclusion_reasons, _warnings = _hard_checks(
        "us",
        {"name": "Meta"},
        history,
        metrics,
        technical,
        context,
        10,
        None,
        fundamental_dimension,
        None,
    )

    check_map = {item["name"]: item for item in checks}
    assert "ETF / 行业代理" not in check_map["基本面底线"]["detail"]


def test_hard_checks_for_cn_stock_pass_fundamental_floor_when_financial_snapshot_available(monkeypatch) -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=120, freq="B"),
            "open": [100.0] * 120,
            "high": [101.0] * 120,
            "low": [99.0] * 120,
            "close": [100.0 + 0.2 * i for i in range(120)],
            "volume": [1_000_000] * 120,
            "amount": [800_000_000.0] * 120,
        }
    )
    metrics = {"avg_turnover_20d": 8e8, "price_percentile_1y": 0.43, "return_5d": -0.01}
    technical = {"rsi": {"RSI": 50.0}}
    context = {"config": {"opportunity": {}}}
    fundamental_dimension = {
        "valuation_snapshot": {"index_name": "中际旭创", "pe_ttm": 28.0},
        "valuation_extreme": False,
        "financial_proxy": {
            "report_date": "2025-12-31",
            "profit_yoy": 22.0,
            "roe": 18.0,
            "gross_margin": 31.5,
            "cfps": 1.8,
            "debt_to_assets": 36.0,
        },
    }

    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_unlock_pressure",
        lambda self, symbol, as_of="", lookahead_days=90: {  # noqa: ARG005
            "status": "✅",
            "detail": "未来 90 日未见明确限售股解禁安排",
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_pledge_risk_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "✅",
            "detail": "当前未见明显股权质押风险",
        },
    )

    checks, exclusion_reasons, _warnings = _hard_checks(
        "cn_stock",
        {"symbol": "300308", "name": "中际旭创"},
        history,
        metrics,
        technical,
        context,
        10,
        None,
        fundamental_dimension,
        None,
    )

    check_map = {item["name"]: item for item in checks}
    assert check_map["基本面底线"]["status"] == "✅"
    assert "未见明显底线失守项" in check_map["基本面底线"]["detail"]
    assert "基本面底线失守" not in exclusion_reasons


def test_hard_checks_for_cn_stock_use_unlock_pressure(monkeypatch) -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=120, freq="B"),
            "open": [100.0] * 120,
            "high": [101.0] * 120,
            "low": [99.0] * 120,
            "close": [100.0 + 0.2 * i for i in range(120)],
            "volume": [1_000_000] * 120,
            "amount": [800_000_000.0] * 120,
        }
    )
    metrics = {"avg_turnover_20d": 8e8, "price_percentile_1y": 0.43, "return_5d": -0.01}
    technical = {"rsi": {"RSI": 50.0}}
    context = {"config": {"opportunity": {}}}
    fundamental_dimension = {"valuation_snapshot": {"index_name": "新易盛", "pe_ttm": 52.0}, "valuation_extreme": False}

    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_unlock_pressure",
        lambda self, symbol, as_of="", lookahead_days=90: {  # noqa: ARG005
            "status": "❌",
            "detail": "未来 30 日预计解禁约 5.30%（约 0.92 亿股）；最近一次在 2026-03-20，主要为定向增发机构配售股份",
        },
    )

    checks, exclusion_reasons, warnings = _hard_checks(
        "cn_stock",
        {"symbol": "300502", "name": "新易盛"},
        history,
        metrics,
        technical,
        context,
        10,
        None,
        fundamental_dimension,
        None,
    )

    check_map = {item["name"]: item for item in checks}
    assert check_map["解禁压力"]["status"] == "❌"
    assert "未来 30 日存在大额解禁压力" in exclusion_reasons
    assert any("大额限售股解禁" in item for item in warnings)


def test_hard_checks_for_cn_stock_use_pledge_risk(monkeypatch) -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=120, freq="B"),
            "open": [100.0] * 120,
            "high": [101.0] * 120,
            "low": [99.0] * 120,
            "close": [100.0 + 0.2 * i for i in range(120)],
            "volume": [1_000_000] * 120,
            "amount": [800_000_000.0] * 120,
        }
    )
    metrics = {"avg_turnover_20d": 8e8, "price_percentile_1y": 0.43, "return_5d": -0.01}
    technical = {"rsi": {"RSI": 50.0}}
    context = {"config": {"opportunity": {}}}
    fundamental_dimension = {"valuation_snapshot": {"index_name": "新易盛", "pe_ttm": 52.0}, "valuation_extreme": False}

    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_unlock_pressure",
        lambda self, symbol, as_of="", lookahead_days=90: {  # noqa: ARG005
            "status": "✅",
            "detail": "未来 90 日未见明确限售股解禁安排",
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_pledge_risk_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "❌",
            "detail": "2026-03-06 质押比例约 16.20%，仍有 3 条未释放质押，单一股东最高质押占其持股约 75.0%",
        },
    )

    checks, exclusion_reasons, warnings = _hard_checks(
        "cn_stock",
        {"symbol": "300502", "name": "新易盛"},
        history,
        metrics,
        technical,
        context,
        10,
        None,
        fundamental_dimension,
        None,
    )

    check_map = {item["name"]: item for item in checks}
    assert check_map["质押风险"]["status"] == "❌"
    assert "股权质押风险较高" in exclusion_reasons
    assert any("股权质押比例偏高" in item for item in warnings)


def test_relative_strength_dimension_uses_concept_spot_for_breadth_and_leader_confirmation() -> None:
    dates = pd.date_range("2025-01-01", periods=30, freq="B")
    asset_returns = pd.Series([0.01] * 30, index=dates)
    benchmark_returns = pd.Series([0.002] * 30, index=dates)
    context = {
        "benchmark_returns": {"cn_stock": benchmark_returns},
        "drivers": {
            "industry_spot": pd.DataFrame(),
            "concept_spot": pd.DataFrame(
                [
                    {
                        "板块名称": "光模块",
                        "涨跌幅": 2.4,
                        "上涨家数": 8,
                        "下跌家数": 2,
                    }
                ]
            ),
        },
        "day_theme": {"label": "AI算力"},
        "regime": {},
        "global_proxy": {},
    }
    metadata = {"name": "中际旭创", "sector": "科技", "chain_nodes": ["光模块"]}
    metrics = {"return_5d": 0.08, "return_20d": 0.12}

    dimension = _relative_strength_dimension("300308", "cn_stock", metadata, metrics, asset_returns, context)
    factor_map = {factor["name"]: factor for factor in dimension["factors"]}

    assert factor_map["板块扩散"]["signal"] == "板块涨跌幅 +2.40%"
    assert factor_map["行业宽度"]["signal"] == "行业上涨家数比例 80%"
    assert factor_map["龙头确认"]["signal"] == "龙头方向与板块一致，扩散结构健康"


def test_relative_strength_dimension_prefers_breadth_row_with_counts_over_generic_industry_row() -> None:
    dates = pd.date_range("2025-01-01", periods=30, freq="B")
    asset_returns = pd.Series([0.01] * 30, index=dates)
    benchmark_returns = pd.Series([0.002] * 30, index=dates)
    context = {
        "benchmark_returns": {"cn_stock": benchmark_returns},
        "drivers": {
            "industry_spot": pd.DataFrame(
                [
                    {
                        "板块名称": "科技",
                        "涨跌幅": 1.1,
                    }
                ]
            ),
            "concept_spot": pd.DataFrame(
                [
                    {
                        "板块名称": "光模块",
                        "涨跌幅": 2.1,
                        "上涨家数": 9,
                        "下跌家数": 1,
                    }
                ]
            ),
        },
        "day_theme": {"label": "AI算力"},
        "regime": {},
        "global_proxy": {},
    }
    metadata = {"name": "中际旭创", "sector": "科技", "chain_nodes": ["光模块"]}
    metrics = {"return_5d": 0.07, "return_20d": 0.10}

    dimension = _relative_strength_dimension("300308", "cn_stock", metadata, metrics, asset_returns, context)
    factor_map = {factor["name"]: factor for factor in dimension["factors"]}

    assert factor_map["行业宽度"]["display_score"] != "观察提示"
    assert factor_map["行业宽度"]["signal"] == "行业上涨家数比例 90%"
    assert factor_map["龙头确认"]["display_score"] != "观察提示"
    assert factor_map["龙头确认"]["signal"] == "龙头方向与板块一致，扩散结构健康"


def test_chips_dimension_uses_index_concentration_proxy_for_cn_stock(monkeypatch) -> None:
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_snapshot",
        lambda self, keywords: {  # noqa: ARG005
            "index_code": "931160",
            "index_name": "中证光模块主题指数",
        },
    )
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_financial_proxies",
        lambda self, index_code, top_n=5: {  # noqa: ARG005
            "top_concentration": 42.0,
            "coverage_weight": 39.5,
        },
    )

    dimension = _chips_dimension(
        "300308",
        "cn_stock",
        {"name": "中际旭创", "sector": "科技", "chain_nodes": ["光模块"]},
        {"drivers": {}, "fund_profile": None},
        {},
    )
    factor_map = {factor["name"]: factor for factor in dimension["factors"]}

    assert factor_map["机构集中度代理"]["signal"] == "前五大成分股权重合计 42.0%"
    assert "财务覆盖权重约 39.5%" in factor_map["机构集中度代理"]["detail"]


def test_chips_dimension_uses_chain_node_keywords_for_index_proxy(monkeypatch) -> None:
    def _snapshot(self, keywords):  # noqa: ANN001, ARG002
        if "光模块" in keywords or "通信设备" in keywords:
            return {"index_code": "931160", "index_name": "中证光模块主题指数"}
        return None

    monkeypatch.setattr(ValuationCollector, "get_cn_index_snapshot", _snapshot)
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_financial_proxies",
        lambda self, index_code, top_n=5: {  # noqa: ARG005
            "top_concentration": 41.0,
            "coverage_weight": 38.0,
        },
    )

    dimension = _chips_dimension(
        "300308",
        "cn_stock",
        {"name": "中际旭创", "sector": "科技", "chain_nodes": ["光模块"]},
        {"drivers": {}, "fund_profile": None, "runtime_caches": {}},
        {},
    )
    factor_map = {factor["name"]: factor for factor in dimension["factors"]}

    assert factor_map["机构集中度代理"]["display_score"] != "缺失"
    assert factor_map["机构集中度代理"]["signal"] == "前五大成分股权重合计 41.0%"


def test_chips_dimension_falls_back_to_secondary_index_proxy_when_primary_proxy_breaks(monkeypatch) -> None:
    def _snapshot(self, keywords):  # noqa: ANN001, ARG002
        if "人工智能" in keywords:
            return {"index_code": "980087", "index_name": "人工智能精选"}
        if "通信" in keywords:
            return {"index_code": "399389", "index_name": "国证通信"}
        return None

    def _financial(self, index_code, top_n=5):  # noqa: ANN001, ARG002
        if index_code == "980087":
            raise ValueError("broken theme workbook")
        if index_code == "399389":
            return {
                "top_concentration": 35.2,
                "coverage_weight": 35.2,
                "constituents": [{"symbol": "300308", "name": "中际旭创", "weight": 8.1}],
            }
        return {}

    monkeypatch.setattr(ValuationCollector, "get_cn_index_snapshot", _snapshot)
    monkeypatch.setattr(ValuationCollector, "get_cn_index_financial_proxies", _financial)

    dimension = _chips_dimension(
        "300308",
        "cn_stock",
        {"name": "中际旭创", "sector": "通信设备", "chain_nodes": ["AI算力"]},
        {"drivers": {}, "fund_profile": None, "runtime_caches": {}},
        {},
    )
    factor_map = {factor["name"]: factor for factor in dimension["factors"]}

    assert factor_map["机构集中度代理"]["display_score"] != "缺失"
    assert factor_map["机构集中度代理"]["signal"] == "前五大成分股权重合计 35.2%"


def test_relative_strength_dimension_uses_derived_theme_leaders_when_breadth_counts_missing() -> None:
    dates = pd.date_range("2025-01-01", periods=30, freq="B")
    asset_returns = pd.Series([0.01] * 30, index=dates)
    benchmark_returns = pd.Series([0.002] * 30, index=dates)
    context = {
        "benchmark_returns": {"cn_stock": benchmark_returns},
        "drivers": {
            "industry_spot": pd.DataFrame(
                [
                    {
                        "板块名称": "通信设备",
                        "涨跌幅": 0.8,
                    }
                ]
            ),
            "concept_spot": pd.DataFrame(),
        },
        "day_theme": {"label": "AI算力"},
        "regime": {},
        "global_proxy": {},
    }
    metadata = {"name": "中际旭创", "sector": "通信设备", "chain_nodes": ["AI算力", "成长股估值修复"]}
    metrics = {"return_5d": 0.08, "return_20d": 0.12}

    dimension = _relative_strength_dimension("300308", "cn_stock", metadata, metrics, asset_returns, context)
    factor_map = {factor["name"]: factor for factor in dimension["factors"]}

    assert factor_map["龙头确认"]["display_score"] != "观察提示"
    assert factor_map["龙头确认"]["signal"] == "龙头方向与板块一致，扩散结构健康"


def test_catalyst_dimension_cn_stock_includes_structured_event_factor(monkeypatch):
    """cn_stock catalyst should turn direct stock announcements into structured event evidence."""
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_repurchase", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_dividend", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_holder_trades", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(
        NewsCollector,
        "get_stock_news",
        lambda self, symbol, limit=10: [
            {"category": "stock_announcement", "title": "比亚迪获海外大额订单", "source": "东方财富", "configured_source": "东方财富", "must_include": False, "link": ""},
        ],
    )
    context = {
        "config": {},
        "news_report": {"all_items": []},
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "002594", "name": "比亚迪", "asset_type": "cn_stock", "sector": "消费", "chain_nodes": []},
        context,
    )
    factor_names = [f["name"] for f in dimension["factors"]]
    assert "结构化事件" in factor_names
    ann_factor = next(f for f in dimension["factors"] if f["name"] == "结构化事件")
    assert "订单" in ann_factor["signal"]
    assert ann_factor["awarded"] == 15
    # Policy max should be 25 (not 30) for cn_stock
    policy_factor = next(f for f in dimension["factors"] if f["name"] == "政策催化")
    assert policy_factor["display_score"].endswith("/25")


def test_catalyst_and_risk_dimensions_share_cn_stock_news_cache(monkeypatch):
    calls = []

    def fake_stock_news(self, symbol, limit=10):  # noqa: ARG001
        calls.append(symbol)
        return [
            {
                "category": "stock_announcement",
                "title": "中际旭创 预计于 2026-03-31 披露 2025年年报",
                "source": "东方财富",
                "configured_source": "东方财富",
                "must_include": False,
                "link": "",
            }
        ]

    monkeypatch.setattr(NewsCollector, "get_stock_news", fake_stock_news)
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_disclosure_dates", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_holder_trades", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_repurchase", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_dividend", lambda self, symbol: [])  # noqa: ARG005

    history = pd.DataFrame({"close": np.linspace(100.0, 130.0, 120)})
    asset_returns = history["close"].pct_change().dropna()
    context = {
        "config": {},
        "news_report": {"all_items": [], "mode": "live"},
        "events": [],
        "runtime_caches": {},
        "benchmark_returns": {},
        "now": "2026-03-17",
    }
    metadata = {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技", "chain_nodes": ["光模块"]}

    _catalyst_dimension(metadata, context)
    _risk_dimension("300308", "cn_stock", metadata, history, asset_returns, context, None)

    assert calls == ["300308"]


def test_company_forward_events_for_cn_stock_use_tushare_disclosure_date(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_disclosure_dates",
        lambda self, symbol: [  # noqa: ARG005
            {"end_date": "20251231", "pre_date": "2026-03-18", "actual_date": ""},
        ],
    )
    context = {"config": {}, "news_report": {"all_items": []}, "events": [], "now": "2026-03-12"}
    events = _company_forward_events(
        {"symbol": "300502", "name": "新易盛", "asset_type": "cn_stock"},
        context,
    )
    assert events
    assert "预计于 2026-03-18 披露 2025年年报" in events[0]["title"]
    assert events[0]["source"] == "Tushare disclosure_date"


def test_catalyst_dimension_cn_stock_uses_tushare_capital_return_events(monkeypatch):
    monkeypatch.setattr(NewsCollector, "get_stock_news", lambda self, symbol, limit=10: [])  # noqa: ARG005
    monkeypatch.setattr(NewsCollector, "search_by_keywords", lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_disclosure_dates", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_holder_trades", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_repurchase",
        lambda self, symbol: [  # noqa: ARG005
            {"ann_date": "2026-03-04", "proc": "实施"},
        ],
    )
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_dividend",
        lambda self, symbol: [  # noqa: ARG005
            {"ann_date": "2026-03-11", "div_proc": "预案"},
        ],
    )
    context = {"config": {}, "news_report": {"all_items": [], "mode": "live"}, "events": [], "now": "2026-03-12"}
    dimension = _catalyst_dimension(
        {"symbol": "601138", "name": "工业富联", "asset_type": "cn_stock", "sector": "科技", "chain_nodes": ["AI算力"]},
        context,
    )
    structured_factor = next(f for f in dimension["factors"] if f["name"] == "结构化事件")
    assert "披露股份回购" in structured_factor["signal"] or "披露现金分红" in structured_factor["signal"]
    assert any(item["source"] in {"Tushare repurchase", "Tushare dividend"} for item in dimension["evidence"])


def test_catalyst_dimension_cn_stock_stale_dividend_does_not_keep_full_score(monkeypatch):
    monkeypatch.setattr(NewsCollector, "get_stock_news", lambda self, symbol, limit=10: [])  # noqa: ARG005
    monkeypatch.setattr(NewsCollector, "search_by_keywords", lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_disclosure_dates", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_holder_trades", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_repurchase", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_dividend",
        lambda self, symbol: [  # noqa: ARG005
            {"ann_date": "2025-08-27", "div_proc": "预案", "cash_div_tax": 6.5},
        ],
    )
    context = {"config": {}, "news_report": {"all_items": [], "mode": "live"}, "events": [], "now": "2026-03-12"}
    dimension = _catalyst_dimension(
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技", "chain_nodes": ["AI算力"]},
        context,
    )
    structured_factor = next(f for f in dimension["factors"] if f["name"] == "结构化事件")
    assert structured_factor["display_score"] in {"0/15", "信息项"}
    assert "超出结构化事件有效窗口" in structured_factor["detail"]


def test_chips_dimension_cn_stock_uses_holdertrade_snapshot(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_holder_trades",
        lambda self, symbol: [  # noqa: ARG005
            {"ann_date": "2026-03-05", "in_de": "IN", "change_ratio": 0.18},
        ],
    )
    dimension = _chips_dimension(
        "300502",
        "cn_stock",
        {"symbol": "300502", "name": "新易盛", "asset_type": "cn_stock", "sector": "科技"},
        {"config": {}},
        {},
    )
    signals = {factor["name"]: factor["signal"] for factor in dimension["factors"]}
    assert "净增持约 0.18%" in signals["高管增持"]


def test_chips_dimension_cn_stock_labels_sector_northbound_as_proxy(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_holder_trades",
        lambda self, symbol: [],  # noqa: ARG005
    )
    dimension = _chips_dimension(
        "300502",
        "cn_stock",
        {"symbol": "300502", "name": "新易盛", "asset_type": "cn_stock", "sector": "科技"},
        {
            "config": {},
            "drivers": {
                "northbound_industry": {
                    "frame": pd.DataFrame([{"名称": "消费电子", "净流入估算(亿)": 1.77}]),
                },
                "northbound_concept": {"frame": pd.DataFrame()},
            },
        },
        {},
    )
    northbound_factor = next(f for f in dimension["factors"] if f["name"] == "北向/南向")
    assert "所属行业/概念代理" in northbound_factor["signal"]
    assert "不是单一个股的北向持仓变动" in northbound_factor["detail"]


def test_chips_dimension_cn_stock_uses_holder_concentration_snapshot(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_holder_concentration_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "title": "新易盛 最新前十大股东合计约 46.2%，前十大流通股东合计约 18.8%",
            "detail": "最近披露期 2025-09-30；该项只作为筹码稳定性辅助。",
            "total_ratio": 46.2,
            "float_ratio": 18.8,
        },
    )
    dimension = _chips_dimension(
        "300502",
        "cn_stock",
        {"symbol": "300502", "name": "新易盛", "asset_type": "cn_stock", "sector": "科技"},
        {"config": {}},
        {},
    )
    factor = next(item for item in dimension["factors"] if item["name"] == "股东集中度")
    assert factor["awarded"] == 5
    assert "前十大股东合计约 46.2%" in factor["signal"]


def test_catalyst_dimension_hk_us_searches_google_news_when_empty(monkeypatch):
    """HK/US stocks should proactively search Google News RSS when stock_specific_pool is empty."""
    search_called = []
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: (
            search_called.append(keywords)
            or [
                {"title": "Tencent Q4 earnings beat expectations", "category": "topic_search", "source": "Reuters", "configured_source": "Reuters", "must_include": False, "link": ""},
            ]
        ),
    )
    context = {
        "config": {},
        "news_report": {"mode": "live", "all_items": []},
        "events": [],
    }
    _catalyst_dimension(
        {"symbol": "00700", "name": "腾讯控股", "asset_type": "hk", "sector": "科技", "chain_nodes": []},
        context,
    )
    assert len(search_called) >= 1, "Should have called search_by_keywords for HK stock"


def test_catalyst_dimension_hk_us_allows_direct_company_search_when_news_degraded(monkeypatch):
    search_called = []
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: (  # noqa: ARG005
            search_called.append(keywords) or []
        ),
    )
    context = {
        "config": {},
        "news_report": {"mode": "proxy", "all_items": []},
        "events": [],
    }
    _catalyst_dimension(
        {"symbol": "00700", "name": "腾讯控股", "asset_type": "hk", "sector": "科技", "chain_nodes": []},
        context,
    )
    assert len(search_called) >= 1


def test_catalyst_dimension_cn_stock_skips_per_stock_news_when_news_degraded(monkeypatch):
    stock_news_called = []
    monkeypatch.setattr(
        NewsCollector,
        "get_stock_news",
        lambda self, symbol, limit=10: (  # noqa: ARG005
            stock_news_called.append(symbol) or []
        ),
    )
    context = {
        "config": {},
        "news_report": {"mode": "proxy", "all_items": []},
        "events": [],
    }
    _catalyst_dimension(
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技", "chain_nodes": []},
        context,
    )
    assert stock_news_called == []


def test_catalyst_dimension_penalizes_negative_dilution_event():
    context = {
        "config": {},
        "news_report": {
            "mode": "live",
            "all_items": [
                {
                    "title": "小米集团-W 配股募资 425 亿港元",
                    "category": "topic_search",
                    "source": "Reuters",
                    "configured_source": "Reuters",
                    "must_include": False,
                    "published_at": "2026-03-06",
                    "link": "",
                }
            ]
        },
        "as_of": "2026-03-10",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "01810.HK", "name": "小米集团-W", "asset_type": "hk", "sector": "科技", "chain_nodes": []},
        context,
    )
    negative_factor = next(f for f in dimension["factors"] if f["name"] == "负面事件")
    assert "配股" in negative_factor["signal"]
    assert negative_factor["display_score"] == "-15"


def test_catalyst_dimension_core_signal_prefers_stock_specific_titles():
    context = {
        "config": {},
        "news_report": {
            "all_items": [
                {
                    "title": "周末要闻汇总：AI龙虾火了",
                    "category": "china_market_domestic",
                    "source": "财联社",
                    "configured_source": "财联社",
                    "must_include": False,
                    "link": "",
                },
                {
                    "title": "Tencent launches new AI assistant for WeChat merchants",
                    "category": "topic_search",
                    "source": "Reuters",
                    "configured_source": "Reuters",
                    "must_include": False,
                    "link": "",
                },
                {
                    "title": "Tencent Q4 earnings beat expectations",
                    "category": "earnings",
                    "source": "Reuters",
                    "configured_source": "Reuters",
                    "must_include": False,
                    "link": "",
                },
            ]
        },
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "00700.HK", "name": "腾讯控股", "asset_type": "hk", "sector": "科技", "chain_nodes": [], "aliases": ["Tencent"]},
        context,
    )
    assert "Tencent" in dimension["core_signal"]
    assert "AI龙虾" not in dimension["core_signal"]


def test_stock_name_tokens_do_not_add_two_letter_prefix_for_english_names():
    tokens = _stock_name_tokens({"symbol": "META", "name": "Meta", "asset_type": "us"})
    assert "Meta" in tokens
    assert "Me" not in tokens


def test_contains_any_requires_word_boundary_for_short_english_tokens():
    from src.processors.opportunity_engine import _contains_any

    assert not _contains_any("worst snowfall in 30 years", ["SNOW"])
    assert _contains_any("Snow reports earnings next week", ["SNOW"])


def test_catalyst_dimension_penalizes_english_regulatory_event_with_alias_decay(monkeypatch):
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [  # noqa: ARG005
            {
                "title": "US review may force Tencent to divest gaming stakes",
                "category": "topic_search",
                "source": "Reuters",
                "configured_source": "Reuters",
                "must_include": False,
                "published_at": "2026-02-15",
                "link": "",
            }
        ],
    )
    context = {
        "config": {},
        "news_report": {"all_items": []},
        "as_of": "2026-03-10",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "00700.HK", "name": "腾讯控股", "asset_type": "hk", "sector": "科技", "chain_nodes": [], "aliases": ["Tencent"]},
        context,
    )
    negative_factor = next(f for f in dimension["factors"] if f["name"] == "负面事件")
    assert "Tencent" in negative_factor["signal"]
    assert negative_factor["display_score"] == "-8"


def test_catalyst_dimension_us_suppresses_generic_positive_signals_without_company_news(monkeypatch):
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    context = {
        "config": {},
        "news_report": {
            "all_items": [
                {
                    "title": "Egypt raises domestic fuel prices by up to 17% amid global energy turmoil - Reuters",
                    "category": "earnings",
                    "source": "Reuters",
                    "configured_source": "Reuters",
                    "must_include": False,
                    "link": "",
                },
                {
                    "title": "【早报】深圳出手，“AI龙虾”支持政策来了；事关短线交易，证监会发新规 - 财联社",
                    "category": "china_macro",
                    "source": "财联社",
                    "configured_source": "财联社",
                    "must_include": False,
                    "link": "",
                },
            ]
        },
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "META", "name": "Meta", "asset_type": "us", "sector": "科技", "chain_nodes": []},
        context,
    )
    policy_factor = next(f for f in dimension["factors"] if f["name"] == "政策催化")
    leader_factor = next(f for f in dimension["factors"] if f["name"] == "龙头公告/业绩")
    overseas_factor = next(f for f in dimension["factors"] if f["name"] == "海外映射")
    assert policy_factor["awarded"] == 0
    assert leader_factor["awarded"] == 0
    assert overseas_factor["awarded"] == 0
    assert "暂不计分" in policy_factor["signal"]
    assert "暂不计分" in leader_factor["signal"]


def test_catalyst_dimension_us_accepts_high_confidence_company_news(monkeypatch):
    search_calls = []

    def fake_search(self, keywords, preferred_sources=None, limit=6, recent_days=7):  # noqa: ANN001, ARG001
        search_calls.append(recent_days)
        return [
            {
                "title": "Snowflake raises annual product revenue forecast after strong enterprise AI demand - Reuters",
                "category": "topic_search",
                "source": "Reuters",
                "configured_source": "Reuters",
                "must_include": False,
                "published_at": "2026-02-25",
                "link": "https://www.reuters.com/example",
            }
        ]

    monkeypatch.setattr(NewsCollector, "search_by_keywords", fake_search)
    context = {
        "config": {},
        "news_report": {"mode": "live", "all_items": []},
        "as_of": "2026-03-11",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "SNOW", "name": "Snowflake", "asset_type": "us", "sector": "科技", "chain_nodes": []},
        context,
    )
    leader_factor = next(f for f in dimension["factors"] if f["name"] == "龙头公告/业绩")
    density_factor = next(f for f in dimension["factors"] if f["name"] == "研报/新闻密度")
    assert search_calls and max(search_calls) >= 30
    assert leader_factor["awarded"] == 25
    assert "Snowflake raises annual product revenue forecast" in leader_factor["signal"]
    assert density_factor["awarded"] >= 5


def test_catalyst_core_signal_hk_us_hides_weak_titles_without_high_confidence_company_news(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._company_calendar_event_dates",
        lambda symbol, asset_type: ["2026-03-24"],  # noqa: ARG005
    )
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    context = {
        "config": {},
        "news_report": {
            "all_items": [
                {
                    "title": "华泰证券：维持小米集团-W买入评级 目标价47港元 - 财联社",
                    "category": "topic_search",
                    "source": "财联社",
                    "configured_source": "财联社",
                    "must_include": False,
                    "link": "",
                },
                {
                    "title": "小米集团-W获南向资金连续8天净买入 - 东方财富",
                    "category": "topic_search",
                    "source": "东方财富",
                    "configured_source": "东方财富",
                    "must_include": False,
                    "link": "",
                },
            ]
        },
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "01810.HK", "name": "小米集团-W", "asset_type": "hk", "sector": "科技", "chain_nodes": []},
        context,
    )
    assert "财联社" not in dimension["core_signal"]
    assert "东方财富" not in dimension["core_signal"]
    assert "2026-03-24" in dimension["core_signal"]


def test_is_high_confidence_company_news_accepts_ir_and_gov_sources():
    assert _is_high_confidence_company_news(
        {
            "title": "Coinbase Delivers on Q4 Financial Outlook",
            "source": "Coinbase Investor Relations",
            "configured_source": "Coinbase Investor Relations",
            "link": "https://investor.coinbase.com/news/news-details/2026/example/default.aspx",
        }
    )


def test_is_high_confidence_company_news_rejects_quote_pages():
    assert not _is_high_confidence_company_news(
        {
            "title": "(SNOW.N) | Stock Price & Latest News - Reuters",
            "source": "Reuters",
            "configured_source": "Reuters",
            "link": "https://www.reuters.com/markets/companies/SNOW.N/",
        }
    )


def test_catalyst_dimension_us_counts_official_ir_results_news(monkeypatch):
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [  # noqa: ARG005
            {
                "title": "Coinbase Delivers on Q4 Financial Outlook, Doubles Total Trading Volume and Crypto Trading Volume Market Share in 2025",
                "category": "topic_search",
                "source": "Coinbase Investor Relations",
                "configured_source": "Coinbase Investor Relations",
                "must_include": False,
                "published_at": "2026-02-14",
                "link": "https://investor.coinbase.com/news/news-details/2026/example/default.aspx",
            }
        ],
    )
    context = {
        "config": {},
        "news_report": {"mode": "live", "all_items": []},
        "as_of": "2026-03-11",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "COIN", "name": "Coinbase", "asset_type": "us", "sector": "科技", "chain_nodes": []},
        context,
    )
    leader_factor = next(f for f in dimension["factors"] if f["name"] == "龙头公告/业绩")
    density_factor = next(f for f in dimension["factors"] if f["name"] == "研报/新闻密度")
    assert leader_factor["awarded"] == 25
    assert "Coinbase Delivers on Q4 Financial Outlook" in leader_factor["signal"]
    assert density_factor["awarded"] >= 5
    assert _is_high_confidence_company_news(
        {
            "title": "Empire State Development Announces Coinbase Expansion",
            "source": "Empire State Development (ESD) (.gov)",
            "configured_source": "Empire State Development (ESD) (.gov)",
            "link": "https://esd.ny.gov/example",
        }
    )


def test_catalyst_dimension_penalizes_tencent_gaming_stakes_headline():
    context = {
        "config": {},
        "news_report": {
            "all_items": [
                {
                    "title": "Trump administration debates allowing Tencent to keep its gaming stakes, FT reports - Reuters",
                    "category": "topic_search",
                    "source": "Reuters",
                    "configured_source": "Reuters",
                    "must_include": False,
                    "published_at": "2026-03-04",
                    "link": "",
                }
            ]
        },
        "as_of": "2026-03-11",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "00700.HK", "name": "腾讯控股", "asset_type": "hk", "sector": "科技", "chain_nodes": [], "aliases": ["Tencent"]},
        context,
    )
    negative_factor = next(f for f in dimension["factors"] if f["name"] == "负面事件")
    assert "gaming" in negative_factor["signal"].lower()
    assert negative_factor["display_score"] == "-15"
    assert "gaming stakes" in str(dimension["core_signal"]).lower()


def test_preferred_catalyst_sources_for_hk_us_keep_high_confidence_sources_only():
    us_sources = _preferred_catalyst_sources({"region": "US"}, {"profile_name": "科技"})
    hk_sources = _preferred_catalyst_sources({"region": "HK"}, {"profile_name": "科技"})
    assert us_sources == ["Reuters", "Investor Relations", "SEC", "Bloomberg", "Financial Times"]
    assert hk_sources == ["Reuters", "HKEXnews", "Investor Relations", "Bloomberg", "Financial Times"]


def test_direct_company_event_search_terms_are_capped_for_runtime():
    groups = _direct_company_event_search_terms(
        {"asset_type": "hk", "name": "腾讯控股", "symbol": "00700.HK", "aliases": ["Tencent"]},
        {"earnings_keywords": ["财报", "业绩"], "event_keywords": ["合作", "回购"]},
    )
    assert len(groups) <= 3
    assert groups[0] == ["腾讯控股"]


def test_direct_company_negative_search_terms_include_regulatory_phrases():
    from src.processors.opportunity_engine import _direct_company_negative_search_terms

    groups = _direct_company_negative_search_terms(
        {"asset_type": "hk", "name": "腾讯控股", "symbol": "00700.HK", "aliases": ["Tencent"]}
    )
    flattened = [" ".join(group) for group in groups]
    assert any("gaming stakes" in item or "cfius" in item or "national security" in item for item in flattened)


def test_catalyst_dimension_uses_company_calendar_for_forward_event(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._company_calendar_event_dates",
        lambda symbol, asset_type: ("2026-03-18",),  # noqa: ARG005
    )
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._search_high_confidence_company_news",
        lambda metadata, profile, config, recent_days=45: [],  # noqa: ARG005
    )
    context = {
        "config": {},
        "news_report": {"mode": "live", "all_items": []},
        "as_of": "2026-03-11",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "00700.HK", "name": "腾讯控股", "asset_type": "hk", "sector": "科技", "chain_nodes": [], "aliases": ["Tencent"]},
        context,
    )
    forward_factor = next(f for f in dimension["factors"] if f["name"] == "前瞻催化")
    structured_factor = next(f for f in dimension["factors"] if f["name"] == "结构化事件")
    assert "2026-03-18" in forward_factor["signal"]
    assert forward_factor["awarded"] == 5
    assert structured_factor["awarded"] == 5
    assert "结构化事件已出现" in dimension["summary"]


def test_catalyst_dimension_calendar_does_not_block_high_confidence_company_news(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._company_calendar_event_dates",
        lambda symbol, asset_type: ("2026-03-18",),  # noqa: ARG005
    )
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [  # noqa: ARG005
            {
                "title": "Tencent quarterly results beat estimates on gaming recovery - Reuters",
                "category": "topic_search",
                "source": "Reuters",
                "configured_source": "Reuters",
                "must_include": False,
                "published_at": "2026-03-10",
                "link": "https://www.reuters.com/example",
            }
        ],
    )
    context = {
        "config": {},
        "news_report": {"all_items": []},
        "as_of": "2026-03-11",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "00700.HK", "name": "腾讯控股", "asset_type": "hk", "sector": "科技", "chain_nodes": [], "aliases": ["Tencent"]},
        context,
    )
    forward_factor = next(f for f in dimension["factors"] if f["name"] == "前瞻催化")
    leader_factor = next(f for f in dimension["factors"] if f["name"] == "龙头公告/业绩")
    assert forward_factor["awarded"] == 5
    assert leader_factor["awarded"] == 25
    assert "Tencent quarterly results beat estimates" in leader_factor["signal"]
    assert any("Tencent quarterly results beat estimates" in item["title"] for item in dimension["evidence"])


def test_chips_dimension_caps_proxy_only_individual_stock_score():
    context = {
        "drivers": {
            "industry_fund_flow": pd.DataFrame(),
            "concept_fund_flow": pd.DataFrame(),
            "northbound_industry": {"frame": pd.DataFrame()},
            "northbound_concept": {"frame": pd.DataFrame()},
            "hot_rank": pd.DataFrame([{"名称": "腾讯控股", "排名": 88}]),
        }
    }
    dimension = _chips_dimension(
        "00700.HK",
        "hk",
        {"name": "腾讯控股", "sector": "科技", "chain_nodes": ["互联网"]},
        context,
        {},
    )
    assert dimension["score"] <= 55
    assert "行业/市场级筹码代理" in dimension["summary"]


def test_catalyst_dimension_treats_missing_news_as_information_gap_not_negative(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._company_calendar_event_dates",
        lambda symbol, asset_type: tuple(),  # noqa: ARG005
    )
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    context = {
        "config": {},
        "news_report": {"all_items": []},
        "as_of": "2026-03-11",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "AMD", "name": "AMD", "asset_type": "us", "sector": "科技", "chain_nodes": []},
        context,
    )
    structured_factor = next(f for f in dimension["factors"] if f["name"] == "结构化事件")
    assert structured_factor["awarded"] == 0
    assert "信息不足" in structured_factor["detail"]
    assert "信息不足" in dimension["summary"]


def test_catalyst_dimension_ignores_non_positive_company_statement_in_positive_awards(monkeypatch):
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_repurchase", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_dividend", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_disclosure_dates", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_holder_trades", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(NewsCollector, "get_stock_news", lambda self, symbol, limit=10: [])  # noqa: ARG005
    monkeypatch.setattr(NewsCollector, "search_by_keywords", lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [])  # noqa: ARG005
    context = {
        "config": {},
        "news_report": {
            "mode": "live",
            "all_items": [
                {
                    "title": "中际旭创：公司目前未发布任何业绩指引",
                    "category": "topic_search",
                    "source": "Reuters",
                    "configured_source": "Reuters",
                    "must_include": False,
                    "published_at": "2026-03-08",
                    "link": "https://example.com/no-guidance",
                }
            ],
        },
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技", "chain_nodes": []},
        context,
    )
    leader_factor = next(f for f in dimension["factors"] if f["name"] == "龙头公告/业绩")
    structured_factor = next(f for f in dimension["factors"] if f["name"] == "结构化事件")
    assert leader_factor["awarded"] == 0
    assert structured_factor["awarded"] == 0


def test_catalyst_dimension_does_not_penalize_unscoped_negative_headline():
    context = {
        "config": {},
        "news_report": {
            "all_items": [
                {
                    "title": "市场调查显示月底或临波动",
                    "category": "topic_search",
                    "source": "财联社",
                    "configured_source": "财联社",
                    "must_include": False,
                    "link": "",
                }
            ]
        },
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "01810.HK", "name": "01810.HK", "asset_type": "hk", "sector": "科技", "chain_nodes": []},
        context,
    )
    negative_factor = next(f for f in dimension["factors"] if f["name"] == "负面事件")
    assert negative_factor["display_score"] == "信息项"


def test_catalyst_dimension_ignores_stale_negative_event_beyond_lookback(monkeypatch):
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [  # noqa: ARG005
            {
                "title": "Tencent faces antitrust review in overseas market",
                "category": "topic_search",
                "source": "Reuters",
                "configured_source": "Reuters",
                "must_include": False,
                "published_at": "2026-01-15",
                "link": "",
            }
        ],
    )
    context = {
        "config": {},
        "news_report": {"all_items": []},
        "as_of": "2026-03-10",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "00700.HK", "name": "腾讯控股", "asset_type": "hk", "sector": "科技", "chain_nodes": [], "aliases": ["Tencent"]},
        context,
    )
    negative_factor = next(f for f in dimension["factors"] if f["name"] == "负面事件")
    assert negative_factor["display_score"] == "信息项"


def test_catalyst_dimension_ignores_unrelated_negative_search_hit(monkeypatch):
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [  # noqa: ARG005
            {
                "title": "Sony fighting $2.7 billion UK lawsuit over PlayStation Store prices - Reuters",
                "category": "topic_search",
                "source": "Reuters",
                "configured_source": "Reuters",
                "must_include": False,
                "published_at": "2026-03-10",
                "link": "",
            }
        ],
    )
    context = {
        "config": {},
        "news_report": {"all_items": []},
        "as_of": "2026-03-12",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "00700.HK", "name": "腾讯控股", "asset_type": "hk", "sector": "科技", "chain_nodes": [], "aliases": ["Tencent"]},
        context,
    )
    negative_factor = next(f for f in dimension["factors"] if f["name"] == "负面事件")
    assert negative_factor["display_score"] == "信息项"


def test_seasonality_dimension_penalizes_poor_same_month_history():
    current_month = datetime.now().month
    dates = pd.date_range("2021-01-31", periods=72, freq="ME")
    level = 100.0
    closes = []
    for date in dates:
        if date.month == current_month:
            level *= 0.90
        else:
            level *= 1.01
        closes.append(level)
    history = pd.DataFrame({"date": dates, "close": closes})
    dimension = _seasonality_dimension({"sector": "科技", "asset_type": "cn_stock"}, history, {})
    factor = next(f for f in dimension["factors"] if f["name"] == "月度胜率")
    assert factor["awarded"] < 0
    assert "同月胜率" in factor["signal"]


def test_fundamental_dimension_hk_us_uses_yfinance(monkeypatch):
    """HK/US stocks should use yfinance data and break through the 55 cap."""
    monkeypatch.setattr(
        ValuationCollector,
        "get_yf_fundamental",
        lambda self, symbol, asset_type: {
            "pe_ttm": 18.5,
            "roe": 22.0,
            "revenue_yoy": 15.0,
            "gross_margin": 35.0,
        },
    )
    metrics = {"price_percentile_1y": 0.4, "avg_turnover_20d": 100_000_000}
    dimension = _fundamental_dimension("00700", "hk", {"name": "腾讯控股", "sector": "科技"}, metrics, {})
    assert dimension["score"] is not None
    # With real PE + ROE + revenue + margin data, available should be >= 80
    # so the score should NOT be capped at 55
    assert dimension["available_max"] >= 60
    assert "ETF/行业代理视角" not in dimension["summary"]


def test_risk_dimension_penalizes_disclosure_window_from_stock_news(monkeypatch):
    monkeypatch.setattr(
        NewsCollector,
        "get_stock_news",
        lambda self, symbol, limit=10: [  # noqa: ARG005
            {
                "category": "stock_announcement",
                "title": "寒武纪-U将于3月13日披露2025年年报",
                "source": "东方财富",
                "configured_source": "东方财富",
                "must_include": False,
                "link": "",
            }
        ],
    )
    history = _make_simple_history()
    asset_returns = history["close"].pct_change().dropna()
    dimension = _risk_dimension(
        "688256",
        "cn_stock",
        {"symbol": "688256", "name": "寒武纪-U", "asset_type": "cn_stock", "sector": "科技"},
        history,
        asset_returns,
        {"config": {}, "as_of": "2026-03-10", "news_report": {"all_items": []}, "benchmark_returns": {}},
        None,
    )
    disclosure_factor = next(f for f in dimension["factors"] if f["name"] == "披露窗口")
    assert "3月13日" in disclosure_factor["signal"]
    assert disclosure_factor["display_score"] == "-15"


def test_risk_dimension_penalizes_recent_result_disclosure_from_stock_news(monkeypatch):
    monkeypatch.setattr(
        NewsCollector,
        "get_stock_news",
        lambda self, symbol, limit=10: [  # noqa: ARG005
            {
                "category": "stock_announcement",
                "title": "工业富联：2025年净利润同比增长51.99% 拟派发129亿元现金红利",
                "source": "东方财富",
                "configured_source": "东方财富",
                "must_include": False,
                "published_at": "2026-03-10",
                "link": "",
            }
        ],
    )
    history = _make_simple_history()
    asset_returns = history["close"].pct_change().dropna()
    dimension = _risk_dimension(
        "601138",
        "cn_stock",
        {"symbol": "601138", "name": "工业富联", "asset_type": "cn_stock", "sector": "科技"},
        history,
        asset_returns,
        {"config": {}, "as_of": "2026-03-11", "news_report": {"all_items": []}, "benchmark_returns": {}},
        None,
    )
    disclosure_factor = next(f for f in dimension["factors"] if f["name"] == "披露窗口")
    assert "工业富联" in disclosure_factor["signal"]
    assert disclosure_factor["display_score"] == "-15"


def test_risk_dimension_ignores_unrelated_disclosure_headline(monkeypatch):
    history = _make_simple_history()
    asset_returns = history["close"].pct_change().dropna()
    context = {
        "config": {},
        "as_of": "2026-03-11",
        "benchmark_returns": {},
        "news_report": {
            "all_items": [
                {
                    "title": "TSMC Sales Grow 30% on Sustained Global Demand for AI Hardware - Bloomberg.com",
                    "category": "earnings",
                    "source": "Bloomberg",
                    "configured_source": "Bloomberg",
                    "must_include": False,
                    "published_at": "2026-03-10",
                    "link": "",
                }
            ]
        },
    }
    dimension = _risk_dimension(
        "300502",
        "cn_stock",
        {"symbol": "300502", "name": "新易盛", "asset_type": "cn_stock", "sector": "科技"},
        history,
        asset_returns,
        context,
        None,
    )
    disclosure_factor = next(f for f in dimension["factors"] if f["name"] == "披露窗口")
    assert disclosure_factor["display_score"] == "信息项"


def test_risk_dimension_uses_company_calendar_for_hk_disclosure_window(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._company_calendar_event_dates",
        lambda symbol, asset_type: ("2026-03-18",),  # noqa: ARG005
    )
    history = _make_simple_history()
    asset_returns = history["close"].pct_change().dropna()
    dimension = _risk_dimension(
        "00700.HK",
        "hk",
        {"symbol": "00700.HK", "name": "腾讯控股", "asset_type": "hk", "sector": "科技", "aliases": ["Tencent"]},
        history,
        asset_returns,
        {"config": {}, "as_of": "2026-03-11", "news_report": {"all_items": []}, "benchmark_returns": {}},
        None,
    )
    disclosure_factor = next(f for f in dimension["factors"] if f["name"] == "披露窗口")
    assert "2026-03-18" in disclosure_factor["signal"]
    assert disclosure_factor["display_score"] == "-15"


def test_risk_dimension_uses_recent_high_recovery_signal():
    close = [100.0] * 80 + [98.0, 95.0, 90.0, 85.0, 80.0, 75.0, 70.0, 68.0, 66.0, 64.0, 62.0, 60.0]
    close += [61.0 + i * 0.5 for i in range(120)]
    size = len(close)
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=size, freq="B"),
            "open": close,
            "high": [value + 1.0 for value in close],
            "low": [value - 1.0 for value in close],
            "close": close,
            "volume": [1_000_000] * size,
            "amount": [10_000_000] * size,
        }
    )
    asset_returns = history["close"].pct_change().dropna()
    dimension = _risk_dimension(
        "01024.HK",
        "hk",
        {"symbol": "01024.HK", "name": "快手-W", "asset_type": "hk", "sector": "科技"},
        history,
        asset_returns,
        {"config": {}, "as_of": "2026-03-10", "news_report": {"all_items": []}, "benchmark_returns": {}},
        None,
    )
    recovery_factor = next(f for f in dimension["factors"] if f["name"] == "回撤恢复")
    assert "近一年高点后" in recovery_factor["signal"]
    assert "999" not in recovery_factor["signal"]
    assert recovery_factor["awarded"] > 0


def _make_action_plan_analysis(
    rating_rank,
    tech,
    risk,
    relative,
    catalyst,
    macro_reverse=False,
    asset_type="cn_stock",
    fundamental=50,
):
    return {
        "rating": {"rank": rating_rank},
        "asset_type": asset_type,
        "dimensions": {
            "technical": {"score": tech},
            "risk": {"score": risk},
            "relative_strength": {"score": relative},
            "catalyst": {"score": catalyst},
            "macro": {"score": 20, "macro_reverse": macro_reverse},
            "fundamental": {"score": fundamental},
            "chips": {"score": None},
            "seasonality": {"score": 40},
        },
    }


def _make_simple_history():
    return pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=90, freq="B"),
        "open": [10.0 + 0.01 * i for i in range(90)],
        "high": [10.5 + 0.01 * i for i in range(90)],
        "low": [9.5 + 0.01 * i for i in range(90)],
        "close": [10.0 + 0.01 * i for i in range(90)],
        "volume": [1_000_000] * 90,
        "amount": [10_000_000] * 90,
    })


def test_action_plan_differentiated_when_risk_high_relative_high():
    """rating <= 1 but risk >= 70 and relative >= 60 should NOT be '暂不出手'."""
    analysis = _make_action_plan_analysis(rating_rank=1, tech=35, risk=75, relative=65, catalyst=20)
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 50.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA60": 10.0}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.4})
    assert "暂不出手" not in result["position"]
    assert "试探" in result["position"] or "2%" in result["position"]
    assert result["direction"] == "观望偏多"


def test_action_plan_still_hold_when_no_signals():
    """rating <= 1 with low risk and relative should remain '暂不出手'."""
    analysis = _make_action_plan_analysis(rating_rank=0, tech=30, risk=40, relative=35, catalyst=10)
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 50.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA60": 10.0}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.5})
    assert result["position"] == "暂不出手"
    assert result["direction"] == "回避"


def test_action_plan_portfolio_management_fields():
    """Action plan should include portfolio management fields."""
    analysis = _make_action_plan_analysis(rating_rank=3, tech=70, risk=75, relative=65, catalyst=55)
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 50.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA60": 10.0}}}
    result = _action_plan(analysis, history, technical, ("SH510050", 0.85), {"volatility_percentile_1y": 0.2})
    assert "max_portfolio_exposure" in result
    assert "≤10%" in result["max_portfolio_exposure"]
    assert "scaling_plan" in result
    assert "分" in result["scaling_plan"]
    assert result["stop_loss_pct"] == "-5%"
    assert "0.85" in result["correlated_warning"]
    assert result["horizon"]["code"] in {"position_trade", "swing"}
    assert result["horizon"]["fit_reason"]


def test_action_plan_uses_watchful_bullish_direction_when_odds_are_low():
    analysis = _make_action_plan_analysis(rating_rank=3, tech=62, risk=55, relative=65, catalyst=30, asset_type="cn_etf")
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 75.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.0, "MA60": 9.8}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.4, "price_percentile_1y": 0.95})
    assert result["direction"] == "观望偏多"


def test_action_plan_warns_when_price_runs_far_above_ma20():
    analysis = _make_action_plan_analysis(rating_rank=3, tech=60, risk=50, relative=55, catalyst=55)
    history = _make_simple_history()
    history.loc[:, "close"] = [10.0] * (len(history) - 1) + [11.0]
    technical = {"rsi": {"RSI": 52.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.2, "MA60": 10.0}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.5, "return_5d": 0.08})
    assert "抬离 MA20" in result["entry"]
    assert "不追高" in result["entry"]


def test_action_plan_warns_when_five_day_rally_is_too_fast():
    analysis = _make_action_plan_analysis(rating_rank=3, tech=62, risk=50, relative=55, catalyst=55)
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 54.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.6, "MA60": 10.0}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.5, "return_5d": 0.08})
    assert "近 5 日拉升较快" in result["entry"]
    assert "不追高" in result["entry"]


def test_action_plan_uses_near_ma20_language_when_not_extended():
    analysis = _make_action_plan_analysis(rating_rank=3, tech=60, risk=50, relative=55, catalyst=55)
    history = _make_simple_history()
    history.loc[:, "close"] = [10.0] * (len(history) - 1) + [10.1]
    technical = {"rsi": {"RSI": 48.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.0, "MA60": 9.9}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.5, "return_5d": 0.01})
    assert "接近 MA20" in result["entry"]


def test_action_plan_validates_stop_and_target_against_current_price():
    analysis = _make_action_plan_analysis(rating_rank=3, tech=62, risk=50, relative=60, catalyst=55, asset_type="us")
    history = _make_simple_history()
    history.loc[:, "close"] = [50.0] * (len(history) - 1) + [50.0]
    history.loc[:, "high"] = [49.5] * (len(history) - 1) + [50.5]
    history.loc[:, "low"] = [48.0] * len(history)
    technical = {
        "rsi": {"RSI": 55.0},
        "fibonacci": {"levels": {"0.382": 70.0, "0.500": 65.0, "0.618": 60.0, "1.000": 52.0}},
        "ma_system": {"mas": {"MA20": 49.0, "MA60": 48.5}},
    }
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.5, "return_5d": 0.02})
    stop_ref = float(re.search(r"跌破 ([0-9.]+)", result["stop"]).group(1))
    target_ref = float(re.search(r"高点 ([0-9.]+)", result["target"]).group(1))
    assert stop_ref < 50.0
    assert target_ref > 50.0
    assert stop_ref <= 49.0


def test_action_plan_marks_long_term_when_fundamental_and_risk_are_strong():
    analysis = _make_action_plan_analysis(
        rating_rank=4,
        tech=52,
        risk=72,
        relative=58,
        catalyst=48,
        fundamental=78,
        asset_type="cn_etf",
    )
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 49.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.0, "MA60": 9.8}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.4, "return_5d": 0.01})
    assert result["horizon"]["code"] == "long_term_allocation"
    assert "长线配置" in result["horizon"]["label"]
    assert "不适合按纯短线追价" in result["horizon"]["misfit_reason"]


def test_refresh_action_from_signal_confidence_differentiates_watch_horizon_and_warning():
    analysis = _make_action_plan_analysis(rating_rank=0, tech=30, risk=25, relative=22, catalyst=12, fundamental=8)
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 48.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.0, "MA60": 9.8}}}
    analysis["action"] = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.5})
    analysis["narrative"] = {"judgment": {"state": "观察为主"}}
    analysis["conclusion"] = "技术面有亮点，但还没有形成满配共振。"
    analysis["signal_confidence"] = {
        "available": True,
        "stop_hit_rate": 0.71,
        "target_hit_rate": 0.41,
        "win_rate_20d": 0.29,
        "confidence_score": 34,
    }

    _refresh_action_from_signal_confidence(analysis)

    assert "止损频率偏高" in analysis["conclusion"]
    assert "止损触发率偏高" in analysis["action"]["horizon"]["fit_reason"]
    assert analysis["action"]["scaling_plan"] == "观察名单阶段，不预设加仓"
    assert any("止损频率偏高" in item for item in analysis["rating"]["warnings"])


def test_signal_confidence_warning_line_supports_single_sided_high_stop_warning():
    warning = _signal_confidence_warning_line(
        {
            "available": True,
            "stop_hit_rate": 0.61,
            "target_hit_rate": 0.41,
        }
    )
    assert "61%" in warning
    assert "止损频率偏高" in warning


def test_build_market_context_can_skip_global_proxy_and_market_monitor(monkeypatch):
    monkeypatch.setattr("src.processors.opportunity_engine.load_china_macro_snapshot", lambda cfg: {"pmi": 50.0})
    monkeypatch.setattr("src.processors.opportunity_engine.load_watchlist", lambda: [])
    monkeypatch.setattr("src.processors.opportunity_engine.load_global_proxy_snapshot", lambda: (_ for _ in ()).throw(AssertionError("should skip global proxy")))
    monkeypatch.setattr("src.processors.opportunity_engine.MarketMonitorCollector.collect", lambda self: (_ for _ in ()).throw(AssertionError("should skip market monitor")))
    monkeypatch.setattr("src.processors.opportunity_engine.derive_regime_inputs", lambda *args, **kwargs: {})
    monkeypatch.setattr("src.processors.opportunity_engine.RegimeDetector.detect_regime", lambda self: {"current_regime": "recovery", "preferred_assets": []})
    monkeypatch.setattr("src.processors.opportunity_engine.NewsCollector.collect", lambda self, **kwargs: {"mode": "proxy", "items": [], "lines": [], "note": ""})
    monkeypatch.setattr("src.processors.opportunity_engine.EventsCollector.collect", lambda self, mode="daily": [])
    monkeypatch.setattr("src.processors.opportunity_engine.MarketDriversCollector.collect", lambda self: {})
    monkeypatch.setattr("src.processors.opportunity_engine.MarketPulseCollector.collect", lambda self: {})

    context = build_market_context(
        {
            "market_context": {
                "skip_global_proxy": True,
                "skip_market_monitor": True,
            }
        },
        relevant_asset_types=["cn_stock"],
    )

    assert context["global_proxy"] == {}
    assert context["monitor_rows"] == []
    assert any("全球代理数据已按运行配置关闭" in note for note in context["notes"])
    assert any("宏观资产监控已按运行配置关闭" in note for note in context["notes"])


def test_build_market_context_prefetches_independent_sections_in_parallel(monkeypatch):
    def _sleep_and_return(payload):  # noqa: ANN001
        time.sleep(0.08)
        return payload

    monkeypatch.setattr("src.processors.opportunity_engine.load_watchlist", lambda: [])
    monkeypatch.setattr("src.processors.opportunity_engine._safe_history", lambda *args, **kwargs: None)  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine.load_china_macro_snapshot", lambda cfg: _sleep_and_return({"pmi": 50.0}))  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine.derive_regime_inputs", lambda *args, **kwargs: {})
    monkeypatch.setattr("src.processors.opportunity_engine.RegimeDetector.detect_regime", lambda self: {"current_regime": "recovery", "preferred_assets": []})
    monkeypatch.setattr("src.processors.opportunity_engine.NewsCollector.collect", lambda self, **kwargs: _sleep_and_return({"mode": "proxy", "items": [], "lines": [], "note": ""}))  # noqa: ARG005,E501
    monkeypatch.setattr("src.processors.opportunity_engine.EventsCollector.collect", lambda self, mode="daily": _sleep_and_return([]))
    monkeypatch.setattr("src.processors.opportunity_engine.MarketDriversCollector.collect", lambda self: _sleep_and_return({}))
    monkeypatch.setattr("src.processors.opportunity_engine.MarketPulseCollector.collect", lambda self: _sleep_and_return({}))

    start = time.perf_counter()
    context = build_market_context(
        {
            "market_context": {
                "skip_global_proxy": True,
                "skip_market_monitor": True,
            }
        },
        relevant_asset_types=["cn_etf", "futures"],
    )
    elapsed = time.perf_counter() - start

    assert context["drivers"] == {}
    assert context["pulse"] == {}
    assert elapsed < 0.30


def test_discover_opportunities_analyzes_candidates_in_parallel(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_market_context",
        lambda config, relevant_asset_types=None: {"notes": [], "regime": {}, "day_theme": {}, "runtime_caches": {}},  # noqa: ARG005
    )
    pool = [
        PoolItem(symbol="513120", name="港股创新药ETF", asset_type="cn_etf", sector="医药", chain_nodes=["医药"], region="CN", source="watchlist"),
        PoolItem(symbol="561380", name="电网ETF", asset_type="cn_etf", sector="电网", chain_nodes=["电网"], region="CN", source="watchlist"),
        PoolItem(symbol="512480", name="半导体ETF", asset_type="cn_etf", sector="科技", chain_nodes=["半导体"], region="CN", source="watchlist"),
    ]
    monkeypatch.setattr("src.processors.opportunity_engine.build_default_pool", lambda config, theme_filter="", preferred_sectors=None: (pool, []))  # noqa: ARG005

    def _slow_analysis(symbol, asset_type, config, context=None, metadata_override=None):  # noqa: ANN001, ARG001
        time.sleep(0.08)
        return {
            "symbol": symbol,
            "name": metadata_override.get("name", symbol),
            "asset_type": asset_type,
            "generated_at": "2026-03-18 10:00:00",
            "excluded": False,
            "rating": {"rank": 1},
            "dimensions": {
                "technical": {"score": 40},
                "fundamental": {"score": 40},
                "catalyst": {"score": 40},
                "relative_strength": {"score": 40},
                "risk": {"score": 40},
            },
        }

    monkeypatch.setattr("src.processors.opportunity_engine.analyze_opportunity", _slow_analysis)

    start = time.perf_counter()
    payload = discover_opportunities({"opportunity": {"analysis_workers": 3}}, top_n=3)
    elapsed = time.perf_counter() - start

    assert payload["scan_pool"] == 3
    assert payload["passed_pool"] == 3
    assert len(payload["coverage_analyses"]) == 3
    assert elapsed < 0.20


def test_discover_stock_opportunities_limits_context_asset_types_by_market(monkeypatch):
    captured: list[list[str]] = []

    def fake_context(config, preferred_sources=None, relevant_asset_types=None):  # noqa: ANN001
        captured.append(list(relevant_asset_types or []))
        return {"notes": [], "regime": {}, "day_theme": {}, "config": dict(config)}

    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", fake_context)
    monkeypatch.setattr("src.processors.opportunity_engine.build_stock_pool", lambda config, market="all", sector_filter="": ([], []))

    result = discover_stock_opportunities({}, market="cn", top_n=5)

    assert result["top"] == []
    assert captured == [["cn_stock", "cn_etf", "futures"]]


def test_discover_stock_opportunities_reuses_provided_context(monkeypatch):
    shared_context = {"notes": [], "regime": {"current_regime": "deflation"}, "day_theme": {"label": "利率驱动成长修复"}}

    def fail_context(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("build_market_context should not be called when context is provided")

    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", fail_context)
    monkeypatch.setattr("src.processors.opportunity_engine.build_stock_pool", lambda config, market="all", sector_filter="": ([], []))

    result = discover_stock_opportunities({}, market="cn", top_n=5, context=shared_context)

    assert result["top"] == []
    assert result["regime"]["current_regime"] == "deflation"
    assert result["day_theme"]["label"] == "利率驱动成长修复"


def test_build_analysis_horizon_profile_varies_swing_contract_by_driver():
    fundamental_led = build_analysis_horizon_profile(
        rating=3,
        asset_type="cn_stock",
        technical_score=40,
        fundamental_score=83,
        catalyst_score=53,
        relative_score=50,
        risk_score=30,
        macro_reverse=False,
        trade_state="等右侧确认",
        direction="观望偏多",
        position="首次建仓 ≤3%，等结构进一步确认后再加仓",
    )
    momentum_led = build_analysis_horizon_profile(
        rating=2,
        asset_type="cn_stock",
        technical_score=30,
        fundamental_score=80,
        catalyst_score=65,
        relative_score=71,
        risk_score=30,
        macro_reverse=False,
        trade_state="等右侧确认",
        direction="观望",
        position="先不超过 5% 试错",
    )
    repair_led = build_analysis_horizon_profile(
        rating=2,
        asset_type="cn_stock",
        technical_score=29,
        fundamental_score=68,
        catalyst_score=65,
        relative_score=46,
        risk_score=52,
        macro_reverse=False,
        trade_state="风险释放前不宜激进",
        direction="观望",
        position="先不超过 5% 试错",
    )

    assert fundamental_led["code"] == "swing"
    assert momentum_led["code"] == "swing"
    assert repair_led["code"] == "swing"
    assert "基本面和主线没有坏" in fundamental_led["fit_reason"]
    assert "催化和相对强弱都在线" in momentum_led["fit_reason"]
    assert "风险收益比相对不差" in repair_led["fit_reason"]


def test_action_plan_varies_watch_scaling_by_driver_mix():
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 48.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.0, "MA60": 9.8}}}

    defensive = _make_action_plan_analysis(rating_rank=0, tech=35, risk=85, relative=54, catalyst=15, fundamental=61)
    weak_fundamental = _make_action_plan_analysis(rating_rank=0, tech=50, risk=25, relative=71, catalyst=58, fundamental=21)
    plain_watch = _make_action_plan_analysis(rating_rank=0, tech=28, risk=5, relative=2, catalyst=25, fundamental=20)

    defensive_action = _action_plan(defensive, history, technical, None, {"volatility_percentile_1y": 0.4})
    weak_fundamental_action = _action_plan(weak_fundamental, history, technical, None, {"volatility_percentile_1y": 0.5})
    plain_watch_action = _action_plan(plain_watch, history, technical, None, {"volatility_percentile_1y": 0.7})

    assert defensive_action["scaling_plan"] == "先按防守观察仓理解，等催化补齐后再讨论第二笔"
    assert weak_fundamental_action["scaling_plan"] == "先盯基本面约束能否缓解，再决定是否给第二笔"
    assert plain_watch_action["scaling_plan"] == "不抢反弹，不预设加仓"


def test_build_stock_pool_warns_when_cn_snapshot_missing_required_columns(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_stock_realtime",
        lambda self: pd.DataFrame([{"代码": "000001", "最新价": 10.0}]),  # noqa: ARG005
    )
    pool, warnings = build_stock_pool({}, market="cn")
    assert pool == []
    assert any("A 股实时快照缺少必要列" in warning for warning in warnings)


def test_build_fund_pool_prefers_full_open_fund_universe_and_dedupes_share_classes(monkeypatch):
    monkeypatch.setattr(
        FundProfileCollector,
        "get_fund_basic",
        lambda self, market="O": pd.DataFrame(  # noqa: ARG005
            [
                {
                    "ts_code": "021739.OF",
                    "name": "前海开源黄金ETF联接A",
                    "management": "前海开源基金",
                    "fund_type": "商品型",
                    "invest_type": "黄金现货合约",
                    "found_date": "20240115",
                    "issue_amount": 6.0,
                    "benchmark": "上海黄金交易所Au99.99现货实盘合约收盘价收益率*90%+人民币活期存款利率(税后)*10%",
                    "status": "L",
                },
                {
                    "ts_code": "021740.OF",
                    "name": "前海开源黄金ETF联接C",
                    "management": "前海开源基金",
                    "fund_type": "商品型",
                    "invest_type": "黄金现货合约",
                    "found_date": "20240115",
                    "issue_amount": 6.0,
                    "benchmark": "上海黄金交易所Au99.99现货实盘合约收盘价收益率*90%+人民币活期存款利率(税后)*10%",
                    "status": "L",
                },
                {
                    "ts_code": "022365.OF",
                    "name": "永赢科技智选混合发起C",
                    "management": "永赢基金",
                    "fund_type": "混合型",
                    "invest_type": "混合型",
                    "found_date": "20240801",
                    "issue_amount": 4.2,
                    "benchmark": "中国战略新兴产业成份指数收益率*70%+恒生科技指数收益率*10%",
                    "status": "L",
                },
                {
                    "ts_code": "019999.OF",
                    "name": "某纯债A",
                    "management": "某基金",
                    "fund_type": "债券型",
                    "invest_type": "债券型",
                    "found_date": "20220101",
                    "issue_amount": 30.0,
                    "benchmark": "中债综合财富指数收益率",
                    "status": "L",
                },
                {
                    "ts_code": "029999.OF",
                    "name": "某新基金C",
                    "management": "某基金",
                    "fund_type": "股票型",
                    "invest_type": "股票型",
                    "found_date": "20260201",
                    "issue_amount": 5.0,
                    "benchmark": "中证人工智能指数收益率",
                    "status": "L",
                },
            ]
        ),
    )
    monkeypatch.setattr("src.processors.opportunity_engine.load_watchlist", lambda: [])
    pool, warnings = build_fund_pool({}, preferred_sectors=["黄金"], max_candidates=5)
    assert warnings == []
    assert [item.symbol for item in pool] == ["021740", "022365"]
    assert all(item.source == "tushare_open_fund_basic" for item in pool)
    assert pool[0].sector == "黄金"
    assert (pool[0].metadata or {})["invest_type"] == "黄金现货合约"


def test_discover_fund_opportunities_uses_full_universe_pool(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_market_context",
        lambda config, relevant_asset_types=None: {"day_theme": {"label": "地缘风险升温"}, "regime": {"current_regime": "recovery"}},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_fund_pool",
        lambda config, theme_filter="", preferred_sectors=None, max_candidates=None: (  # noqa: ARG005
            [
                type(
                    "PoolItemStub",
                    (),
                    {
                        "symbol": "021740",
                        "asset_type": "cn_fund",
                        "name": "前海开源黄金ETF联接C",
                        "sector": "黄金",
                        "chain_nodes": ["黄金"],
                        "region": "CN",
                        "in_watchlist": False,
                        "metadata": {"benchmark": "黄金现货"},
                    },
                )(),
                type(
                    "PoolItemStub",
                    (),
                    {
                        "symbol": "022365",
                        "asset_type": "cn_fund",
                        "name": "永赢科技智选混合发起C",
                        "sector": "科技",
                        "chain_nodes": ["科技"],
                        "region": "CN",
                        "in_watchlist": False,
                        "metadata": {"benchmark": "战略新兴"},
                    },
                )(),
            ],
            [],
        ),
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine.analyze_opportunity",
        lambda symbol, asset_type, config, context=None, metadata_override=None: {  # noqa: ARG005
            "symbol": symbol,
            "name": "前海开源黄金ETF联接C" if symbol == "021740" else "永赢科技智选混合发起C",
            "asset_type": asset_type,
            "rating": {"rank": 2 if symbol == "021740" else 1, "label": "储备机会", "stars": "⭐⭐"},
            "dimensions": {
                "technical": {"score": 36 if symbol == "021740" else 44},
                "fundamental": {"score": 80 if symbol == "021740" else 62},
                "catalyst": {"score": 70 if symbol == "021740" else 55},
                "relative_strength": {"score": 55 if symbol == "021740" else 51},
                "chips": {"score": None},
                "risk": {"score": 75 if symbol == "021740" else 58},
                "seasonality": {"score": 30 if symbol == "021740" else 25},
                "macro": {"score": 20 if symbol == "021740" else 18},
            },
            "excluded": False,
        },
    )
    payload = discover_fund_opportunities({}, top_n=2, max_candidates=6)
    assert payload["scan_pool"] == 2
    assert payload["passed_pool"] == 2
    assert payload["preferred_sectors"][:2] == ["黄金", "高股息"]
    assert payload["top"][0]["symbol"] == "021740"


def test_build_stock_pool_prefers_cn_ttm_pe_column(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_stock_realtime",
        lambda self: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "代码": "300476",
                    "名称": "胜宏科技",
                    "成交额": 120_000_000.0,
                    "总市值": 18_000_000_000.0,
                    "行业": "电子",
                    "市盈率(动态)": 204.1,
                    "市盈率TTM": 64.8,
                    "市净率": 6.2,
                }
            ]
        ),
    )
    monkeypatch.setattr("src.processors.opportunity_engine.detect_asset_type", lambda symbol, config: "cn_stock")
    pool, warnings = build_stock_pool({}, market="cn", max_candidates=5)
    assert warnings == []
    assert len(pool) == 1
    metadata = pool[0].metadata or {}
    assert metadata["pe_ttm"] == 64.8
    assert metadata["pe_dynamic"] == 204.1


def test_build_stock_pool_carries_bak_daily_enrichment(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_stock_realtime",
        lambda self: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "代码": "300502",
                    "名称": "新易盛",
                    "成交额": 120_000_000.0,
                    "总市值": 18_000_000_000.0,
                    "行业": "通信设备",
                    "市盈率(动态)": 137.7,
                    "市盈率TTM": 52.0,
                    "市净率": 26.9,
                    "强弱度": 2.31,
                    "活跃度": 3988.0,
                    "攻击度": 3.42,
                    "振幅": 4.79,
                    "地域": "四川",
                }
            ]
        ),
    )
    monkeypatch.setattr("src.processors.opportunity_engine.detect_asset_type", lambda symbol, config: "cn_stock")
    pool, warnings = build_stock_pool({}, market="cn", max_candidates=5)
    assert warnings == []
    metadata = pool[0].metadata or {}
    assert metadata["bak_strength"] == 2.31
    assert metadata["bak_activity"] == 3988.0
    assert metadata["bak_attack"] == 3.42
    assert metadata["bak_swing"] == 4.79
    assert metadata["area"] == "四川"


def test_discover_stock_opportunities_includes_watch_positive(monkeypatch):
    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", lambda config, relevant_asset_types=None: {})  # noqa: ARG005
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_stock_pool",
        lambda config, market="all", sector_filter="": (  # noqa: ARG005
            [
                type(
                    "PoolItemStub",
                    (),
                    {
                        "symbol": "300750",
                        "asset_type": "cn_stock",
                        "name": "宁德时代",
                        "sector": "新能源",
                        "chain_nodes": [],
                        "region": "CN",
                        "in_watchlist": False,
                        "metadata": {},
                    },
                )()
            ],
            [],
        ),
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine.analyze_opportunity",
        lambda symbol, asset_type, config, context=None, metadata_override=None: {  # noqa: ARG005
            "symbol": symbol,
            "name": "宁德时代",
            "asset_type": asset_type,
            "rating": {"rank": 0, "label": "无信号", "stars": "—"},
            "dimensions": {
                "technical": {"score": 38},
                "fundamental": {"score": 75},
                "catalyst": {"score": 35},
                "relative_strength": {"score": 55},
                "chips": {"score": 0},
                "risk": {"score": 65},
                "seasonality": {"score": 0},
                "macro": {"score": 30},
            },
            "excluded": False,
        },
    )
    payload = discover_stock_opportunities({}, top_n=5, market="cn")
    assert len(payload["coverage_analyses"]) == 1
    assert len(payload["watch_positive"]) == 1
    assert payload["watch_positive"][0]["symbol"] == "300750"


def test_discover_opportunities_builds_prepick_candidate_layers(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_market_context",
        lambda config, relevant_asset_types=None: {  # noqa: ARG005
            "day_theme": {"label": "黄金避险升温"},
            "regime": {"current_regime": "risk_off"},
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_default_pool",
        lambda config, theme_filter="", preferred_sectors=None: (  # noqa: ARG005
            [
                type(
                    "PoolItemStub",
                    (),
                    {
                        "symbol": "518880",
                        "asset_type": "cn_etf",
                        "name": "黄金ETF",
                        "sector": "黄金",
                        "chain_nodes": ["黄金"],
                        "region": "CN",
                        "in_watchlist": False,
                        "source": "tushare_etf_universe",
                    },
                )(),
                type(
                    "PoolItemStub",
                    (),
                    {
                        "symbol": "561380",
                        "asset_type": "cn_etf",
                        "name": "电网ETF",
                        "sector": "电网",
                        "chain_nodes": ["电网设备"],
                        "region": "CN",
                        "in_watchlist": True,
                        "source": "watchlist",
                    },
                )(),
            ],
            ["全市场 ETF 扫描池部分数据降级。"],
        ),
    )

    def _analysis(symbol, asset_type, config, context=None, metadata_override=None):  # noqa: ARG001
        if symbol == "518880":
            return {
                "symbol": symbol,
                "name": "黄金ETF",
                "asset_type": asset_type,
                "metadata": {"sector": "黄金"},
                "day_theme": {"label": "黄金避险升温"},
                "dimensions": {
                    "technical": {"score": 42, "max_score": 100, "summary": "技术结构一般，更多看防守承接。"},
                    "fundamental": {"score": 78, "max_score": 100, "summary": "产品结构清晰。"},
                    "catalyst": {"score": 72, "max_score": 100, "summary": "避险交易和央行增持构成催化。", "coverage": {"news_mode": "proxy", "degraded": True}},
                    "relative_strength": {"score": 46, "max_score": 100, "summary": "相对强弱中性。"},
                    "chips": {"score": None, "max_score": 100, "summary": "缺失"},
                    "risk": {"score": 82, "max_score": 100, "summary": "防守属性和回撤控制更突出。"},
                    "seasonality": {"score": 30, "max_score": 100, "summary": "一般"},
                    "macro": {"score": 26, "max_score": 40, "summary": "宏观偏防守。"},
                },
                "rating": {"rank": 2, "label": "储备机会", "stars": "⭐⭐", "meaning": "单维度亮灯但还未形成共振。", "warnings": []},
                "action": {"timeframe": "短线交易(1-2周)", "direction": "观望", "entry": "等确认", "position": "≤2%", "stop": "跌破支撑重评"},
                "narrative": {"cautions": ["技术确认不足。"], "phase": {"label": "防守轮动"}},
                "notes": ["当前只拿到代理型新闻覆盖。"],
                "history_fallback_mode": False,
                "excluded": False,
            }
        return {
            "symbol": symbol,
            "name": "电网ETF",
            "asset_type": asset_type,
            "metadata": {"sector": "电网"},
            "day_theme": {"label": "黄金避险升温"},
            "dimensions": {
                "technical": {"score": 48, "max_score": 100, "summary": "技术还没完全转强。"},
                "fundamental": {"score": 63, "max_score": 100, "summary": "底层暴露清晰。"},
                "catalyst": {"score": 28, "max_score": 100, "summary": "当前缺少新增催化。", "coverage": {"news_mode": "live", "degraded": False}},
                "relative_strength": {"score": 40, "max_score": 100, "summary": "轮动一般。"},
                "chips": {"score": None, "max_score": 100, "summary": "缺失"},
                "risk": {"score": 58, "max_score": 100, "summary": "风险中性。"},
                "seasonality": {"score": 35, "max_score": 100, "summary": "一般"},
                "macro": {"score": 18, "max_score": 40, "summary": "宏观没有额外顺风。"},
            },
            "rating": {"rank": 1, "label": "有信号但不充分", "stars": "⭐", "meaning": "只有单一维度足够亮，其余不足以支持动作。", "warnings": []},
            "action": {"timeframe": "等待更好窗口", "direction": "观望", "entry": "继续观察", "position": "暂不出手", "stop": "等待确认"},
            "narrative": {"cautions": ["催化不足。"], "phase": {"label": "震荡整理"}},
            "notes": ["该标的已在 watchlist 中，本次分析更偏复核而不是首次发现。"],
            "history_fallback_mode": False,
            "excluded": False,
        }

    monkeypatch.setattr("src.processors.opportunity_engine.analyze_opportunity", _analysis)

    payload = discover_opportunities({}, top_n=3, theme_filter="黄金")

    assert payload["scan_pool"] == 2
    assert payload["passed_pool"] == 2
    assert payload["ready_candidates"][0]["symbol"] == "518880"
    assert payload["ready_candidates"][0]["discovery"]["driver_type"] == "防守驱动"
    assert any(step["command"] == "python -m src.commands.scan 518880" for step in payload["ready_candidates"][0]["discovery"]["next_steps"])
    assert any("fund_pick --theme 黄金" in step["command"] for step in payload["ready_candidates"][0]["discovery"]["next_steps"])
    assert payload["observation_candidates"][0]["symbol"] == "561380"
    assert payload["observation_candidates"][0]["discovery"]["bucket"] == "observe"
    assert payload["pool_summary"]["source_rows"] == [["Tushare 全市场 ETF 快照", "1"], ["watchlist 回退池", "1"]]
    assert any("主题过滤 `黄金`" in item for item in payload["pool_summary"]["filter_rules"])
    assert payload["data_coverage"]["degraded"] is True
    assert any("代理型新闻覆盖" in item for item in payload["ready_candidates"][0]["discovery"]["data_notes"])


# ---------------------------------------------------------------------------
# J-3: Breadth / chips factor tests
# ---------------------------------------------------------------------------

def test_j3_sector_breadth_detail_returns_structure_when_no_data():
    """_sector_breadth_detail 在无数据时应返回合法结构，不抛异常。"""
    from src.processors.opportunity_engine import _sector_breadth_detail
    result = _sector_breadth_detail({"sector": "科技"}, {})
    assert "advance_ratio" in result
    assert "sector_move" in result
    assert "leader_up" in result
    assert "proxy_level" in result
    assert result["proxy_level"] == "sector_proxy"


def test_j3_sector_breadth_detail_extracts_advance_ratio():
    """_sector_breadth_detail 应从 industry_spot 中提取上涨家数比例。"""
    import pandas as pd
    import pytest
    from src.processors.opportunity_engine import _sector_breadth_detail
    industry_spot = pd.DataFrame([
        {"板块名称": "科技", "涨跌幅": 1.5, "上涨家数": 60, "下跌家数": 40},
    ])
    result = _sector_breadth_detail({"sector": "科技"}, {"industry_spot": industry_spot})
    assert result["advance_ratio"] == 0.6
    assert result["sector_move"] == pytest.approx(0.015)


def test_j3_relative_strength_dimension_includes_breadth_factors():
    """相对强弱维度应包含行业宽度和龙头确认因子。"""
    import pandas as pd
    from src.processors.opportunity_engine import _relative_strength_dimension
    history = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=30, freq="B"),
        "close": [10.0 + i * 0.05 for i in range(30)],
    })
    metrics = {"return_5d": 0.02, "return_20d": 0.05}
    asset_returns = pd.Series([0.001] * 30)
    dim = _relative_strength_dimension("600519", "cn_stock", {"sector": "消费"}, metrics, asset_returns, {})
    factor_names = {f["name"] for f in dim["factors"]}
    assert "行业宽度" in factor_names
    assert "龙头确认" in factor_names


def test_j3_chips_dimension_includes_crowding_risk():
    """筹码结构维度应包含拥挤度风险因子（observation_only）。"""
    from src.processors.opportunity_engine import _chips_dimension
    dim = _chips_dimension("600519", "cn_stock", {"sector": "消费", "name": "贵州茅台"}, {}, {})
    factor_names = {f["name"] for f in dim["factors"]}
    assert "拥挤度风险" in factor_names
    crowding_factor = next(f for f in dim["factors"] if f["name"] == "拥挤度风险")
    # 拥挤度风险是 observation_only，不进入主评分
    assert crowding_factor["display_score"] == "观察提示"
    assert crowding_factor["awarded"] == 0


def test_j3_breadth_proxy_level_disclosed():
    """行业宽度因子的 detail 必须包含代理层级说明。"""
    import pandas as pd
    from src.processors.opportunity_engine import _relative_strength_dimension
    history = pd.DataFrame({
        "date": pd.date_range("2025-01-01", periods=30, freq="B"),
        "close": [10.0 + i * 0.05 for i in range(30)],
    })
    metrics = {"return_5d": 0.02, "return_20d": 0.05}
    asset_returns = pd.Series([0.001] * 30)
    dim = _relative_strength_dimension("600519", "cn_stock", {"sector": "消费"}, metrics, asset_returns, {})
    breadth_factor = next((f for f in dim["factors"] if f["name"] == "行业宽度"), None)
    assert breadth_factor is not None
    # detail 必须包含代理层级说明
    assert "行业级" in breadth_factor["detail"] or "代理" in breadth_factor["detail"]


# ---------------------------------------------------------------------------
# J-2: Seasonal / calendar / event window factor tests
# ---------------------------------------------------------------------------

def _make_seasonal_history(years: int = 5, start: str = "2020-01-01") -> pd.DataFrame:
    """Build multi-year monthly history for seasonality tests."""
    periods = years * 252  # approx trading days
    dates = pd.date_range(start, periods=periods, freq="B")
    close = 10.0 + pd.Series(range(periods)) * 0.01
    return pd.DataFrame({
        "date": dates,
        "open": close - 0.05,
        "high": close + 0.1,
        "low": close - 0.1,
        "close": close.values,
        "volume": [1_000_000] * periods,
        "amount": [200_000_000.0] * periods,
    })


def test_j2_seasonality_discloses_sample_size_in_monthly_win_rate():
    """月度胜率因子必须在 signal 中披露样本年数。"""
    history = _make_seasonal_history(years=5)
    dim = _seasonality_dimension({"sector": "科技"}, history, {})
    monthly_factor = next((f for f in dim["factors"] if f["name"] == "月度胜率"), None)
    assert monthly_factor is not None
    # 样本足够时，signal 中应包含样本年数
    if monthly_factor["display_score"] != "缺失":
        assert "年样本" in monthly_factor["signal"] or "样本" in monthly_factor["signal"]


def test_j2_seasonality_degrades_when_insufficient_samples():
    """样本不足 3 年时，月度胜率因子应降级为观察提示（display_score=缺失）。"""
    # 只有 1 年历史，同月样本必然不足 3 年
    history = _make_seasonal_history(years=1)
    dim = _seasonality_dimension({"sector": "科技"}, history, {})
    monthly_factor = next((f for f in dim["factors"] if f["name"] == "月度胜率"), None)
    assert monthly_factor is not None
    assert monthly_factor["display_score"] == "缺失"
    assert monthly_factor["awarded"] == 0


def test_j2_seasonality_earnings_window_covers_all_sectors():
    """财报窗口因子应覆盖所有行业，不只是科技。"""
    import unittest.mock as mock
    # 模拟当前月份为 4 月（财报密集期）
    with mock.patch("src.processors.opportunity_engine.datetime") as mock_dt:
        mock_dt.now.return_value = mock.Mock(month=4)
        history = _make_seasonal_history(years=3)
        for sector in ["科技", "医药", "消费", "军工", "高股息"]:
            dim = _seasonality_dimension({"sector": sector}, history, {})
            earnings_factor = next((f for f in dim["factors"] if f["name"] == "财报窗口"), None)
            assert earnings_factor is not None
            # 4 月是财报密集期，所有行业都应该有非零加分
            assert earnings_factor["awarded"] > 0, f"sector={sector} should have earnings award in month 4"


def test_j2_seasonality_policy_event_window_is_observation_only():
    """政策事件窗因子必须是 observation_only，不进入主评分（awarded=0，display_score=观察提示）。"""
    import unittest.mock as mock
    # 模拟 3 月（两会窗口）
    with mock.patch("src.processors.opportunity_engine.datetime") as mock_dt:
        mock_dt.now.return_value = mock.Mock(month=3)
        history = _make_seasonal_history(years=3)
        dim = _seasonality_dimension({"sector": "科技"}, history, {})
        policy_factor = next((f for f in dim["factors"] if f["name"] == "政策事件窗"), None)
        assert policy_factor is not None
        assert policy_factor["display_score"] == "观察提示"
        assert policy_factor["awarded"] == 0  # 不进入主评分


def test_j2_seasonality_holiday_window_applies_to_consumer_sector():
    """节假日窗口因子应对消费行业在节假日月份加分。"""
    import unittest.mock as mock
    # 模拟 10 月（十一黄金周）
    with mock.patch("src.processors.opportunity_engine.datetime") as mock_dt:
        mock_dt.now.return_value = mock.Mock(month=10)
        history = _make_seasonal_history(years=3)
        dim = _seasonality_dimension({"sector": "消费"}, history, {})
        holiday_factor = next((f for f in dim["factors"] if f["name"] == "节假日窗口"), None)
        assert holiday_factor is not None
        assert holiday_factor["awarded"] > 0


def test_j2_seasonality_commodity_window_applies_to_energy_sector():
    """商品季节性窗口因子应对能源行业在冬季月份加分。"""
    import unittest.mock as mock
    # 模拟 11 月（冬季取暖需求）
    with mock.patch("src.processors.opportunity_engine.datetime") as mock_dt:
        mock_dt.now.return_value = mock.Mock(month=11)
        history = _make_seasonal_history(years=3)
        dim = _seasonality_dimension({"sector": "能源"}, history, {})
        commodity_factor = next((f for f in dim["factors"] if f["name"] == "商品季节性"), None)
        assert commodity_factor is not None
        assert commodity_factor["awarded"] > 0


def test_j2_seasonality_index_rebalance_window_june():
    """指数调整因子在 6 月应给出最高加分。"""
    import unittest.mock as mock
    with mock.patch("src.processors.opportunity_engine.datetime") as mock_dt:
        mock_dt.now.return_value = mock.Mock(month=6)
        history = _make_seasonal_history(years=3)
        dim = _seasonality_dimension({"sector": "宽基"}, history, {})
        rebalance_factor = next((f for f in dim["factors"] if f["name"] == "指数调整"), None)
        assert rebalance_factor is not None
        assert rebalance_factor["awarded"] == 15  # 最高加分


def test_j2_seasonality_dimension_structure():
    """季节/日历维度应包含所有必要因子，且结构完整。"""
    history = _make_seasonal_history(years=4)
    dim = _seasonality_dimension({"sector": "科技"}, history, {})
    assert dim["name"] == "季节/日历"
    assert dim["score"] is not None or dim["missing"]
    factor_names = {f["name"] for f in dim["factors"]}
    required = {"月度胜率", "旺季前置", "财报窗口", "指数调整", "节假日窗口", "商品季节性", "政策事件窗", "分红窗口"}
    assert required.issubset(factor_names), f"Missing factors: {required - factor_names}"


# ---------------------------------------------------------------------------
# J-5: ETF / 基金专属因子
# ---------------------------------------------------------------------------

def _make_etf_fund_profile(
    *,
    benchmark: str = "沪深300ETF",
    sector: str = "宽基",
    top5: float = 42.5,
    tenure_days: float = 1500.0,
    fee_rate: str = "0.15%（每年）",
    tags: list | None = None,
    consistency: str = "",
) -> dict:
    return {
        "overview": {"业绩比较基准": benchmark, "管理费率": fee_rate},
        "style": {
            "sector": sector,
            "tags": tags if tags is not None else ["被动跟踪"],
            "top5_concentration": top5,
            "benchmark_note": benchmark,
            "consistency": consistency,
        },
        "manager": {"tenure_days": tenure_days},
        "rating": {},
    }


def test_j5_etf_tracking_benchmark_clarity_scores_full_when_clear():
    """被动 ETF 业绩基准清晰、无实际跟踪误差数据时，跟踪误差因子应以代理评分（6 分）。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(benchmark="中证科技50指数收益率", sector="科技")
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {"is_passive_fund": True}, fp)
    track_factor = next((f for f in factors if f["name"] == "跟踪误差"), None)
    assert track_factor is not None
    assert track_factor["factor_id"] == "j5_tracking_error"
    # 无实际 tracking_error 数据，降级为基准清晰度代理，最多 6 分
    assert track_factor["awarded"] <= 6


def test_j5_etf_tracking_benchmark_missing_scores_zero():
    """ETF 业绩基准未披露时，跟踪误差因子应得 0 分。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(benchmark="")
    fp["style"]["benchmark_note"] = ""
    fp["overview"]["业绩比较基准"] = ""
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {}, fp)
    track_factor = next((f for f in factors if f["name"] == "跟踪误差"), None)
    assert track_factor is not None
    assert track_factor["awarded"] == 0


def test_j5_component_concentration_moderate_scores_ten():
    """前五大重仓占比 30%-69% 时，成分集中度应得 10 分。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(top5=45.0)
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {}, fp)
    conc_factor = next((f for f in factors if f["name"] == "成分集中度"), None)
    assert conc_factor is not None
    assert conc_factor["awarded"] == 10


def test_j5_component_concentration_high_scores_five():
    """前五大重仓超 70% 时，成分集中度应得 5 分（高度集中风险）。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(top5=75.0)
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {}, fp)
    conc_factor = next((f for f in factors if f["name"] == "成分集中度"), None)
    assert conc_factor is not None
    assert conc_factor["awarded"] == 5


def test_j5_theme_purity_sector_clear_passive_benchmark_match():
    """主题纯度：被动基金行业与基准匹配时应得满分。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(benchmark="中证科技50指数", sector="科技", tags=["科技主题", "被动跟踪"])
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {}, fp)
    purity_factor = next((f for f in factors if f["name"] == "主题纯度"), None)
    assert purity_factor is not None
    assert purity_factor["awarded"] == 10


def test_j5_theme_purity_sector_undefined_scores_zero():
    """主题纯度：行业为综合/未识别时应得 0 分。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(sector="综合")
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {}, fp)
    purity_factor = next((f for f in factors if f["name"] == "主题纯度"), None)
    assert purity_factor is not None
    assert purity_factor["awarded"] == 0


def test_j5_manager_stability_senior_scores_ten():
    """经理在职 5 年以上应得满分 10 分。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(tenure_days=2000.0)  # ~5.5 years
    factors, raw, available = _j5_etf_fund_factors("cn_fund", {}, fp)
    mgr_factor = next((f for f in factors if f["name"] == "经理稳定性"), None)
    assert mgr_factor is not None
    assert mgr_factor["awarded"] == 10


def test_j5_manager_stability_new_scores_low():
    """经理在职不足 1 年应得低分（1 分）。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(tenure_days=200.0)  # < 1 year
    factors, raw, available = _j5_etf_fund_factors("cn_fund", {}, fp)
    mgr_factor = next((f for f in factors if f["name"] == "经理稳定性"), None)
    assert mgr_factor is not None
    assert mgr_factor["awarded"] == 1


def test_j5_fee_rate_passive_low_scores_ten():
    """管理费率 ≤ 0.5% 时费率结构应得满分 10 分。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(fee_rate="0.15%（每年）")
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {}, fp)
    fee_factor = next((f for f in factors if f["name"] == "费率结构"), None)
    assert fee_factor is not None
    assert fee_factor["awarded"] == 10


def test_j5_fee_rate_active_high_scores_zero():
    """管理费率 > 1.5% 时费率结构应得 0 分。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(fee_rate="2.00%（每年）")
    factors, raw, available = _j5_etf_fund_factors("cn_fund", {}, fp)
    fee_factor = next((f for f in factors if f["name"] == "费率结构"), None)
    assert fee_factor is not None
    assert fee_factor["awarded"] == 0


def test_j5_fund_benchmark_disclosure_clear_scores_ten():
    """场外基金业绩基准清晰披露（>5字符）应得 10 分。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(benchmark="沪深300指数收益率×60% + 中债新综合指数收益率×40%", tags=[])
    factors, raw, available = _j5_etf_fund_factors("cn_fund", {}, fp)
    bm_factor = next((f for f in factors if f["name"] == "业绩基准披露"), None)
    assert bm_factor is not None
    assert bm_factor["awarded"] == 10


def test_j5_fund_benchmark_disclosure_missing_scores_zero():
    """场外基金业绩基准未披露应得 0 分。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(benchmark="", tags=[])
    fp["style"]["benchmark_note"] = ""
    factors, raw, available = _j5_etf_fund_factors("cn_fund", {}, fp)
    bm_factor = next((f for f in factors if f["name"] == "业绩基准披露"), None)
    assert bm_factor is not None
    assert bm_factor["awarded"] == 0


def test_j5_active_fund_style_drift_stable_tag_scores_ten():
    """主动基金带 '风格稳定' 标签时，风格漂移评估应得 10 分。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(tags=["消费主题", "风格稳定"])
    factors, raw, available = _j5_etf_fund_factors("cn_fund", {}, fp)
    drift_factor = next((f for f in factors if f["name"] == "风格漂移评估"), None)
    assert drift_factor is not None
    assert drift_factor["awarded"] == 10


def test_j5_cn_stock_returns_no_j5_factors():
    """cn_stock 资产类型不应产生任何 J-5 专属因子。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    factors, raw, available = _j5_etf_fund_factors("cn_stock", {}, None)
    assert factors == []
    assert raw == 0
    assert available == 0


def test_j5_etf_factors_all_have_proxy_disclosure():
    """所有 J-5 ETF 因子的 detail 字段应包含数据源说明。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(benchmark="沪深300ETF指数", sector="宽基", top5=40.0, tenure_days=1500.0)
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {}, fp)
    for f in factors:
        # Skip observation-only / missing data items — they don't need a full data source note
        if f["display_score"] in ("信息项", "缺失"):
            continue
        assert "数据源" in f["detail"] or "lag" in f["detail"] or "direct" in f["detail"], \
            f"J-5 factor '{f['name']}' detail is missing data source disclosure: {f['detail']}"


def test_j5_etf_share_change_positive_scores_nonzero():
    """ETF 份额净创设为正时，ETF 份额申赎因子应得分 > 0。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(benchmark="中证500", sector="宽基")
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {"etf_share_change": 8.5}, fp)
    sc_factor = next((f for f in factors if f["name"] == "ETF 份额申赎"), None)
    assert sc_factor is not None
    assert sc_factor["factor_id"] == "j5_etf_share_change"
    assert sc_factor["awarded"] == 10
    assert "净创设" in sc_factor["signal"]


def test_j5_etf_share_change_negative_scores_zero():
    """ETF 份额净赎回时，ETF 份额申赎因子应得 0 分。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(benchmark="中证500", sector="宽基")
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {"etf_share_change": -5.0}, fp)
    sc_factor = next((f for f in factors if f["name"] == "ETF 份额申赎"), None)
    assert sc_factor is not None
    assert sc_factor["awarded"] == 0
    assert "赎回" in sc_factor["signal"]


def test_j5_etf_share_change_missing_shows_info():
    """ETF 份额申赎数据缺失时，应显示为信息项。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(benchmark="中证500", sector="宽基")
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {}, fp)
    sc_factor = next((f for f in factors if f["name"] == "ETF 份额申赎"), None)
    assert sc_factor is not None
    assert sc_factor["display_score"] == "信息项"
    assert sc_factor["factor_id"] == "j5_etf_share_change"


def test_j5_tracking_error_actual_data_scores_correctly():
    """实际年化跟踪误差数据可用时，应基于数值评分（< 0.3% 得满分）。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(benchmark="沪深300", sector="宽基")
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {"tracking_error": 0.2}, fp)
    te_factor = next((f for f in factors if f["name"] == "跟踪误差"), None)
    assert te_factor is not None
    assert te_factor["factor_id"] == "j5_tracking_error"
    assert te_factor["awarded"] == 10
    assert "0.20%" in te_factor["signal"]


def test_j5_tracking_error_high_scores_low():
    """年化跟踪误差 ≥ 1% 时应得低分。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(benchmark="沪深300", sector="宽基")
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {"tracking_error": 1.5}, fp)
    te_factor = next((f for f in factors if f["name"] == "跟踪误差"), None)
    assert te_factor is not None
    assert te_factor["awarded"] == 2


def test_j5_cn_stock_has_no_share_change_factor():
    """cn_stock 不应产生 ETF 份额申赎或跟踪误差因子。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    factors, raw, available = _j5_etf_fund_factors("cn_stock", {"etf_share_change": 5.0, "tracking_error": 0.1}, None)
    names = [f["name"] for f in factors]
    assert "ETF 份额申赎" not in names
    assert "跟踪误差" not in names
    assert raw == 0


def test_j5_factor_ids_all_registered():
    """_j5_etf_fund_factors 输出中所有非 None 的 factor_id 都应在 FACTOR_REGISTRY 中。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    from src.processors.factor_meta import FACTOR_REGISTRY
    fp = _make_etf_fund_profile(benchmark="中证科技", sector="科技", top5=50.0, tenure_days=2000.0)
    metadata = {"etf_share_change": 3.0, "tracking_error": 0.4}
    factors, _, _ = _j5_etf_fund_factors("cn_etf", metadata, fp)
    for f in factors:
        fid = f.get("factor_id")
        if fid is not None:
            assert fid in FACTOR_REGISTRY, f"factor_id '{fid}' not found in FACTOR_REGISTRY"


# ---------------------------------------------------------------------------
# J-4: 质量 / 盈利修正 / 估值协同 — 补充因子
# ---------------------------------------------------------------------------

def test_j4_cashflow_quality_positive_cfps_scores_full():
    """每股经营现金流为正时，现金流质量应得满分 10 分。"""
    monkeypatch = None  # pure unit test via monkeypatching in function
    import pytest
    from src.processors.opportunity_engine import _fundamental_dimension

    class _FakeValuation:
        def get_cn_stock_financial_proxy(self, symbol):  # noqa: ARG002
            return {
                "pe_ttm": 20.0, "roe": 15.0, "gross_margin": 35.0,
                "revenue_yoy": 12.0, "profit_yoy": 10.0,
                "cfps": 2.5, "debt_to_assets": 30.0, "current_ratio": 2.0,
                "report_date": "2025-09-30",
            }
        def get_cn_index_snapshot(self, *a, **kw): return None  # noqa: ARG002
        def get_cn_index_value_history(self, *a, **kw): return __import__("pandas").DataFrame()  # noqa: ARG002
        def get_cn_index_financial_proxies(self, *a, **kw): return {}  # noqa: ARG002

    import unittest.mock as mock
    with mock.patch("src.processors.opportunity_engine.ValuationCollector", return_value=_FakeValuation()), \
         mock.patch("src.processors.opportunity_engine.MarketDriversCollector") as md:
        md.return_value.collect.return_value = {}
        dim = _fundamental_dimension("600519", "cn_stock", {"name": "茅台", "sector": "消费"}, {"price_percentile_1y": 0.4}, {})
    factor_names = {f["name"] for f in dim["factors"]}
    assert "现金流质量" in factor_names
    cf_factor = next(f for f in dim["factors"] if f["name"] == "现金流质量")
    assert cf_factor["awarded"] == 10


def test_j4_cashflow_quality_negative_cfps_scores_penalty():
    """每股经营现金流明显为负时，现金流质量应给出拖累分。"""
    import unittest.mock as mock
    from src.processors.opportunity_engine import _fundamental_dimension

    class _FakeV:
        def get_cn_stock_financial_proxy(self, symbol):  # noqa: ARG002
            return {"pe_ttm": 20.0, "cfps": -1.5, "report_date": "2025-09-30"}
        def get_cn_index_snapshot(self, *a, **kw): return None  # noqa: ARG002
        def get_cn_index_value_history(self, *a, **kw): return __import__("pandas").DataFrame()  # noqa: ARG002
        def get_cn_index_financial_proxies(self, *a, **kw): return {}  # noqa: ARG002

    with mock.patch("src.processors.opportunity_engine.ValuationCollector", return_value=_FakeV()), \
         mock.patch("src.processors.opportunity_engine.MarketDriversCollector") as md:
        md.return_value.collect.return_value = {}
        dim = _fundamental_dimension("600519", "cn_stock", {"name": "茅台", "sector": "消费"}, {"price_percentile_1y": 0.4}, {})
    cf_factor = next((f for f in dim["factors"] if f["name"] == "现金流质量"), None)
    assert cf_factor is not None
    assert cf_factor["awarded"] < 0


def test_j4_leverage_low_debt_ratio_scores_full():
    """资产负债率 < 40% 时，杠杆压力应得满分 10 分。"""
    import unittest.mock as mock
    from src.processors.opportunity_engine import _fundamental_dimension

    class _FakeV:
        def get_cn_stock_financial_proxy(self, symbol):  # noqa: ARG002
            return {"pe_ttm": 18.0, "debt_to_assets": 25.0, "current_ratio": 3.0, "report_date": "2025-09-30"}
        def get_cn_index_snapshot(self, *a, **kw): return None  # noqa: ARG002
        def get_cn_index_value_history(self, *a, **kw): return __import__("pandas").DataFrame()  # noqa: ARG002
        def get_cn_index_financial_proxies(self, *a, **kw): return {}  # noqa: ARG002

    with mock.patch("src.processors.opportunity_engine.ValuationCollector", return_value=_FakeV()), \
         mock.patch("src.processors.opportunity_engine.MarketDriversCollector") as md:
        md.return_value.collect.return_value = {}
        dim = _fundamental_dimension("600519", "cn_stock", {"name": "茅台", "sector": "消费"}, {"price_percentile_1y": 0.4}, {})
    lev_factor = next((f for f in dim["factors"] if f["name"] == "杠杆压力"), None)
    assert lev_factor is not None
    assert lev_factor["awarded"] == 10


def test_j4_leverage_high_debt_ratio_scores_penalty():
    """资产负债率 ≥ 80% 时，杠杆压力应给出拖累分。"""
    import unittest.mock as mock
    from src.processors.opportunity_engine import _fundamental_dimension

    class _FakeV:
        def get_cn_stock_financial_proxy(self, symbol):  # noqa: ARG002
            return {"pe_ttm": 18.0, "debt_to_assets": 85.0, "report_date": "2025-09-30"}
        def get_cn_index_snapshot(self, *a, **kw): return None  # noqa: ARG002
        def get_cn_index_value_history(self, *a, **kw): return __import__("pandas").DataFrame()  # noqa: ARG002
        def get_cn_index_financial_proxies(self, *a, **kw): return {}  # noqa: ARG002

    with mock.patch("src.processors.opportunity_engine.ValuationCollector", return_value=_FakeV()), \
         mock.patch("src.processors.opportunity_engine.MarketDriversCollector") as md:
        md.return_value.collect.return_value = {}
        dim = _fundamental_dimension("600519", "cn_stock", {"name": "茅台", "sector": "消费"}, {"price_percentile_1y": 0.4}, {})
    lev_factor = next((f for f in dim["factors"] if f["name"] == "杠杆压力"), None)
    assert lev_factor is not None
    assert lev_factor["awarded"] < 0


def test_fundamental_dimension_penalizes_expensive_and_weak_quality_snapshot(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_yf_fundamental",
        lambda self, symbol, asset_type: {  # noqa: ARG005
            "pe_ttm": 120.0,
            "revenue_yoy": -6.0,
            "roe": 3.5,
            "gross_margin": 8.0,
            "cfps": -1.2,
            "debt_to_assets": 86.0,
            "report_date": "2025-12-31",
        },
    )

    dimension = _fundamental_dimension(
        "BAD",
        "us",
        {"name": "BadCo", "sector": "科技"},
        {"price_percentile_1y": 0.92},
        {},
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}
    valuation_factor = next(f for f in dimension["factors"] if "估值" in f["name"] or "PE" in f["signal"])
    assert valuation_factor["awarded"] < 0
    assert factors["盈利增速"]["awarded"] < 0
    assert factors["ROE"]["awarded"] < 0
    assert factors["毛利率"]["awarded"] < 0
    assert factors["现金流质量"]["awarded"] < 0
    assert factors["杠杆压力"]["awarded"] < 0
    assert dimension["core_signal"]


def test_macro_dimension_penalizes_growth_sector_under_macro_headwind():
    dimension = _macro_dimension(
        {"sector": "科技"},
        {
            "china_macro": {
                "pmi": 48.6,
                "pmi_new_orders": 48.1,
                "pmi_production": 48.9,
                "demand_state": "weakening",
                "ppi_yoy": -2.6,
                "price_state": "disinflation",
                "credit_impulse": "contracting",
                "m1_m2_spread": -7.2,
                "social_financing_3m_avg_text": "2.01 万亿元",
            },
            "regime": {"current_regime": "stagflation"},
            "monitor_rows": [
                {"name": "布伦特原油", "return_5d": 0.07},
                {"name": "美国10Y收益率", "return_5d": 0.05},
                {"name": "美元指数", "return_20d": 0.03},
                {"name": "USDCNY", "return_20d": 0.02},
            ],
        },
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert factors["敏感度向量"]["awarded"] < 0
    assert factors["景气方向"]["awarded"] < 0
    assert factors["信用脉冲"]["awarded"] < 0
    assert dimension["score"] is not None
    assert dimension["score"] <= 15


def test_j4_earnings_momentum_always_observation_only():
    """盈利动量因子始终为 observation_only（display_score 为 观察提示，不进入评分）。"""
    import unittest.mock as mock
    from src.processors.opportunity_engine import _fundamental_dimension

    class _FakeV:
        def get_cn_stock_financial_proxy(self, symbol):  # noqa: ARG002
            return {
                "pe_ttm": 20.0, "profit_yoy": 15.0, "profit_dedt_yoy": 20.0, "report_date": "2025-09-30",
            }
        def get_cn_index_snapshot(self, *a, **kw): return None  # noqa: ARG002
        def get_cn_index_value_history(self, *a, **kw): return __import__("pandas").DataFrame()  # noqa: ARG002
        def get_cn_index_financial_proxies(self, *a, **kw): return {}  # noqa: ARG002

    with mock.patch("src.processors.opportunity_engine.ValuationCollector", return_value=_FakeV()), \
         mock.patch("src.processors.opportunity_engine.MarketDriversCollector") as md:
        md.return_value.collect.return_value = {}
        dim = _fundamental_dimension("600519", "cn_stock", {"name": "茅台", "sector": "消费"}, {"price_percentile_1y": 0.4}, {})
    ep_factor = next((f for f in dim["factors"] if f["name"] == "盈利动量"), None)
    assert ep_factor is not None
    assert ep_factor["display_score"] == "观察提示"
    assert ep_factor["awarded"] == 0  # observation_only: 不进入评分
    assert ep_factor["max"] == 0      # max=0 表示该因子不贡献分数


def test_fundamental_dimension_uses_context_drivers_without_recollect(monkeypatch):
    class _FakeV:
        def get_cn_stock_financial_proxy(self, symbol):  # noqa: ARG002
            return {"pe_ttm": 18.0, "profit_yoy": 12.0, "roe": 18.0, "report_date": "2025-09-30"}

        def get_cn_index_snapshot(self, *a, **kw):  # noqa: ARG002
            return None

        def get_cn_index_value_history(self, *a, **kw):  # noqa: ARG002
            return pd.DataFrame()

        def get_cn_index_financial_proxies(self, *a, **kw):  # noqa: ARG002
            return {}

    monkeypatch.setattr("src.processors.opportunity_engine.ValuationCollector", lambda config: _FakeV())  # noqa: ARG005
    monkeypatch.setattr(
        "src.processors.opportunity_engine.MarketDriversCollector.collect",
        lambda self: (_ for _ in ()).throw(AssertionError("should reuse context drivers")),
    )

    dimension = _fundamental_dimension(
        "600519",
        "cn_stock",
        {"name": "贵州茅台", "sector": "消费"},
        {"price_percentile_1y": 0.4},
        {},
        context={"drivers": {}, "runtime_caches": {}},
    )

    assert dimension["score"] is not None


def test_chips_dimension_reuses_index_proxy_cache_within_context(monkeypatch):
    snapshot_calls = []
    proxy_calls = []

    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_snapshot",
        lambda self, keywords: snapshot_calls.append(tuple(keywords)) or {  # noqa: ARG005
            "index_code": "931160",
            "index_name": "中证光模块主题指数",
        },
    )
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_financial_proxies",
        lambda self, index_code, top_n=5: proxy_calls.append((index_code, top_n)) or {  # noqa: ARG005
            "top_concentration": 42.0,
            "coverage_weight": 39.5,
        },
    )

    context = {"drivers": {}, "fund_profile": None, "runtime_caches": {}}
    metadata = {"name": "中际旭创", "sector": "科技", "chain_nodes": ["光模块"]}

    _chips_dimension("300308", "cn_stock", metadata, context, {})
    _chips_dimension("300308", "cn_stock", metadata, context, {})

    assert len(snapshot_calls) == 1
    assert len(proxy_calls) == 1


def test_j4_cn_etf_does_not_include_j4_factors():
    """ETF 资产类型不应产生 J-4 现金流/杠杆/盈利动量因子（只有股票才有）。"""
    import unittest.mock as mock
    from src.processors.opportunity_engine import _fundamental_dimension

    class _FakeV:
        def get_cn_index_snapshot(self, *a, **kw): return None  # noqa: ARG002
        def get_cn_index_value_history(self, *a, **kw): return __import__("pandas").DataFrame()  # noqa: ARG002
        def get_cn_index_financial_proxies(self, *a, **kw): return {}  # noqa: ARG002
        def get_weighted_stock_financial_proxies(self, *a, **kw): return {}  # noqa: ARG002

    with mock.patch("src.processors.opportunity_engine.ValuationCollector", return_value=_FakeV()), \
         mock.patch("src.processors.opportunity_engine.MarketDriversCollector") as md, \
         mock.patch("src.processors.opportunity_engine.ChinaMarketCollector") as cm:
        md.return_value.collect.return_value = {}
        cm.return_value.get_etf_fund_flow.return_value = __import__("pandas").DataFrame()
        dim = _fundamental_dimension(
            "510300", "cn_etf",
            {"name": "沪深300ETF", "sector": "宽基", "is_passive_fund": True},
            {"price_percentile_1y": 0.4}, {},
            fund_profile={
                "overview": {"业绩比较基准": "沪深300指数收益率", "管理费率": "0.15%（每年）"},
                "style": {"sector": "宽基", "tags": ["被动跟踪"], "top5_concentration": 20.0, "benchmark_note": "沪深300指数收益率"},
                "manager": {}, "rating": {}, "top_holdings": [],
            },
        )
    factor_names = {f["name"] for f in dim["factors"]}
    assert "现金流质量" not in factor_names
    assert "杠杆压力" not in factor_names
    assert "盈利动量" not in factor_names


def test_j4_lag_disclosure_in_cashflow_and_leverage_details():
    """现金流质量和杠杆压力 detail 字段应包含 lag 披露（T+45 天）。"""
    import unittest.mock as mock
    from src.processors.opportunity_engine import _fundamental_dimension

    class _FakeV:
        def get_cn_stock_financial_proxy(self, symbol):  # noqa: ARG002
            return {
                "pe_ttm": 20.0, "cfps": 1.2, "debt_to_assets": 40.0,
                "report_date": "2025-09-30",
            }
        def get_cn_index_snapshot(self, *a, **kw): return None  # noqa: ARG002
        def get_cn_index_value_history(self, *a, **kw): return __import__("pandas").DataFrame()  # noqa: ARG002
        def get_cn_index_financial_proxies(self, *a, **kw): return {}  # noqa: ARG002

    with mock.patch("src.processors.opportunity_engine.ValuationCollector", return_value=_FakeV()), \
         mock.patch("src.processors.opportunity_engine.MarketDriversCollector") as md:
        md.return_value.collect.return_value = {}
        dim = _fundamental_dimension("600519", "cn_stock", {"name": "茅台", "sector": "消费"}, {"price_percentile_1y": 0.4}, {})
    cf_factor = next((f for f in dim["factors"] if f["name"] == "现金流质量"), None)
    lev_factor = next((f for f in dim["factors"] if f["name"] == "杠杆压力"), None)
    assert cf_factor is not None and "lag" in cf_factor["detail"]
    assert lev_factor is not None and "lag" in lev_factor["detail"]
