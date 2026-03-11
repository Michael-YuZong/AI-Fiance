"""Tests for the opportunity engine helpers."""

from __future__ import annotations

import re

import pandas as pd

from src.collectors.market_cn import ChinaMarketCollector
from src.collectors.market_drivers import MarketDriversCollector
from src.collectors.valuation import ValuationCollector
from src.collectors.news import NewsCollector
from src.processors.opportunity_engine import _action_plan, _catalyst_dimension, _chips_dimension, _fundamental_dimension, _hard_checks, _is_high_confidence_company_news, _risk_dimension, _seasonality_dimension, _stock_name_tokens, build_stock_pool, discover_stock_opportunities
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
    assert "24.5x" in factors["个股估值"]["signal"]


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


def test_catalyst_dimension_cn_stock_includes_stock_announcement_factor(monkeypatch):
    """cn_stock catalyst should have a '个股公告/事件' factor with redistributed weights."""
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
    assert "个股公告/事件" in factor_names
    ann_factor = next(f for f in dimension["factors"] if f["name"] == "个股公告/事件")
    assert "订单" in ann_factor["signal"]
    # Policy max should be 25 (not 30) for cn_stock
    policy_factor = next(f for f in dimension["factors"] if f["name"] == "政策催化")
    assert policy_factor["display_score"].endswith("/25")


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
        "news_report": {"all_items": []},
        "events": [],
    }
    _catalyst_dimension(
        {"symbol": "00700", "name": "腾讯控股", "asset_type": "hk", "sector": "科技", "chain_nodes": []},
        context,
    )
    assert len(search_called) >= 1, "Should have called search_by_keywords for HK stock"


def test_catalyst_dimension_penalizes_negative_dilution_event():
    context = {
        "config": {},
        "news_report": {
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
        "news_report": {"all_items": []},
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
        "news_report": {"all_items": []},
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


def test_catalyst_dimension_uses_company_calendar_for_forward_event(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._company_calendar_event_dates",
        lambda symbol, asset_type: ("2026-03-18",),  # noqa: ARG005
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
    assert "2026-03-18" in forward_factor["signal"]
    assert forward_factor["awarded"] == 5


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
