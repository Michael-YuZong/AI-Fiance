"""Tests for the opportunity engine helpers."""

from __future__ import annotations

import re

import pandas as pd

from src.collectors.fund_profile import FundProfileCollector
from src.collectors.market_cn import ChinaMarketCollector
from src.collectors.market_drivers import MarketDriversCollector
from src.collectors.valuation import ValuationCollector
from src.collectors.news import NewsCollector
from src.processors.opportunity_engine import _action_plan, _catalyst_dimension, _chips_dimension, _client_safe_issue, _company_forward_events, _direct_company_event_search_terms, _fund_specific_catalyst_profile, _fundamental_dimension, _hard_checks, _is_high_confidence_company_news, _macro_dimension, _preferred_catalyst_sources, _risk_dimension, _seasonality_dimension, _stock_name_tokens, _technical_dimension, build_default_pool, build_fund_pool, build_stock_pool, discover_fund_opportunities, discover_opportunities, discover_stock_opportunities
from src.processors.opportunity_engine import _asset_note
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


def _make_action_plan_analysis(rating_rank, tech, risk, relative, catalyst, macro_reverse=False, asset_type="cn_stock"):
    return {
        "rating": {"rank": rating_rank},
        "asset_type": asset_type,
        "dimensions": {
            "technical": {"score": tech},
            "risk": {"score": risk},
            "relative_strength": {"score": relative},
            "catalyst": {"score": catalyst},
            "macro": {"score": 20, "macro_reverse": macro_reverse},
            "fundamental": {"score": 50},
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
