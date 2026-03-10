"""Tests for the opportunity engine helpers."""

from __future__ import annotations

import pandas as pd

from src.collectors.market_cn import ChinaMarketCollector
from src.collectors.market_drivers import MarketDriversCollector
from src.collectors.valuation import ValuationCollector
from src.collectors.news import NewsCollector
from src.processors.opportunity_engine import _action_plan, _catalyst_dimension, _chips_dimension, _fundamental_dimension, _hard_checks, _seasonality_dimension
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


def _make_action_plan_analysis(rating_rank, tech, risk, relative, catalyst, macro_reverse=False):
    return {
        "rating": {"rank": rating_rank},
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
