"""Tests for the opportunity engine helpers."""

from __future__ import annotations

from datetime import datetime
import re
import threading
import time
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from src.collectors.fund_profile import FundProfileCollector
from src.collectors.market_cn import ChinaMarketCollector
from src.collectors.market_drivers import MarketDriversCollector
from src.collectors.valuation import ValuationCollector
from src.collectors.news import NewsCollector
from src.processors.opportunity_engine import PoolItem, _action_guidance_hint, _action_plan, _analysis_theme_playbook_context, _board_keywords, _build_narrative, _catalyst_dimension, _catalyst_search_groups, _chips_dimension, _client_safe_issue, _cn_holdertrade_snapshot, _cn_pledge_risk_snapshot, _cn_stock_board_action_snapshot, _cn_stock_broker_recommend_snapshot, _cn_stock_capital_flow_snapshot, _cn_stock_chip_snapshot, _cn_stock_margin_snapshot, _cn_stock_regulatory_risk_snapshot, _cn_stock_unlock_pressure_snapshot, _collect_fund_profile, _company_forward_events, _context_index_topic_bundle, _correlation_to_watchlist, _direct_company_event_search_terms, _discover_driver_type, _discover_next_step_commands, _discover_ready_for_next_step, _discover_today_reason_lines, _formal_scaling_plan_from_setup, _fund_specific_catalyst_profile, _fundamental_dimension, _hard_checks, _is_high_confidence_company_news, _macro_dimension, _map_industry_to_sector, _market_context_watch_hint_lines, _market_event_rows_from_context, _merge_metadata, _nearest_support_reference, _normalize_sector, _northbound_sector_snapshot, _preferred_catalyst_sources, _preferred_fund_sectors, _rating_from_dimensions, _refresh_action_from_signal_confidence, _relative_strength_dimension, _risk_dimension, _seasonality_dimension, _sector_flow_snapshot, _signal_confidence_warning_line, _stock_name_tokens, _technical_dimension, _theme_alignment, _today_theme, _trim_market_event_rows, _valuation_keywords, analyze_opportunity, build_default_pool, build_fund_pool, build_market_context, build_stock_pool, discover_fund_opportunities, discover_opportunities, discover_stock_opportunities, summarize_proxy_contracts_from_analyses
from src.processors.opportunity_engine import _asset_note
from src.processors.horizon import build_analysis_horizon_profile
from src.utils.market import compute_history_metrics


@pytest.fixture(autouse=True)
def _stub_company_calendar_event_dates(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._company_calendar_event_dates",
        lambda symbol, asset_type: tuple(),  # noqa: ARG005
    )


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


def test_formal_scaling_plan_from_setup_contextualizes_formal_entries() -> None:
    line = _formal_scaling_plan_from_setup(
        trade_state="等右侧确认",
        entry="等创新药情绪和价格强度继续共振",
        buy_range="51.200 - 52.600",
        target="先看前高/近 60 日高点 58.300 附近的承压与突破情况",
        technical_score=31,
        catalyst_score=22,
        relative_score=39,
    )

    assert "51.200 - 52.600" in line
    assert "等创新药情绪和价格强度继续共振" in line
    assert "轮动承接还不算充足" in line
    assert "分 2-3 批建仓，每次确认后加仓" not in line


def test_map_industry_to_sector_keeps_financial_subindustries_under_financials() -> None:
    assert _map_industry_to_sector("证券Ⅱ", "中信证券") == ("金融", ["券商"])
    assert _map_industry_to_sector("保险", "中国平安") == ("金融", ["保险"])
    assert _map_industry_to_sector("银行", "招商银行") == ("金融", ["银行"])


def test_board_keywords_prioritize_financial_subindustry_before_generic_financial_aliases() -> None:
    keywords = _board_keywords(
        {
            "name": "中信证券",
            "sector": "金融",
            "industry_framework_label": "券商",
            "chain_nodes": ["券商", "证券Ⅲ", "证券Ⅱ"],
        }
    )
    assert keywords[:3] == ["券商", "证券Ⅲ", "证券Ⅱ"]
    assert "银行" not in keywords


def test_cn_stock_board_action_snapshot_hides_raw_exception_path(monkeypatch) -> None:
    class _BrokenPulseCollector:
        def __init__(self, _config):  # noqa: ANN001
            pass

        def get_stock_board_action_snapshot(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN201
            raise RuntimeError("Operation not permitted: '/Users/bilibili/tk.csv'")

    monkeypatch.setattr("src.processors.opportunity_engine.MarketPulseCollector", _BrokenPulseCollector)
    snapshot = _cn_stock_board_action_snapshot(
        {"asset_type": "cn_stock", "symbol": "300274", "name": "阳光电源"},
        {"config": {}},
    )

    assert "tk.csv" not in snapshot["detail"]
    assert "/Users/bilibili" not in snapshot["detail"]
    assert "已按可用数据降级处理" in snapshot["detail"]


def test_merge_metadata_normalizes_generic_industry_bucket_to_engine_sector(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine.get_asset_context",
        lambda symbol, asset_type, context: SimpleNamespace(metadata={"name": "农发种业", "sector": "综合"}, name="农发种业"),  # noqa: ARG005
    )

    merged = _merge_metadata(
        "600313",
        "cn_stock",
        {"name": "农发种业", "sector": "农林牧渔"},
        config={},
    )

    assert merged["sector"] == "农业"
    assert "粮食安全" in merged["chain_nodes"]


def test_merge_metadata_normalizes_narrow_industry_to_yaml_sector_bucket(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine.get_asset_context",
        lambda symbol, asset_type, context: SimpleNamespace(metadata={"name": "中国铝业", "sector": "综合"}, name="中国铝业"),  # noqa: ARG005
    )

    merged = _merge_metadata(
        "601600",
        "cn_stock",
        {"name": "中国铝业", "sector": "铝"},
        config={},
    )

    assert merged["sector"] == "有色"
    assert "铜铝" in merged["chain_nodes"]


def test_merge_metadata_adds_power_equipment_chain_nodes_for_a_share_bucket(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine.get_asset_context",
        lambda symbol, asset_type, context: SimpleNamespace(metadata={"name": "阳光电源", "sector": "电气设备"}, name="阳光电源"),  # noqa: ARG005
    )

    merged = _merge_metadata(
        "300274",
        "cn_stock",
        {"name": "阳光电源", "sector": "电气设备"},
        config={},
    )

    assert merged["sector"] == "电气设备"
    assert merged["chain_nodes"] == ["光伏主链", "储能", "电网设备"]


def test_merge_metadata_uses_tushare_company_profile_text_for_chain_nodes(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine.get_asset_context",
        lambda symbol, asset_type, context: SimpleNamespace(  # noqa: ARG005
            metadata={
                "name": "阳光电源",
                "sector": "综合",
                "main_business": "主营业务是太阳能光伏逆变器、储能系统和风能变流器。",
                "business_scope": "新能源发电设备、储能电源及相关电力电子设备。",
            },
            name="阳光电源",
        ),
    )

    merged = _merge_metadata(
        "300274",
        "cn_stock",
        {"name": "阳光电源", "sector": "综合"},
        config={},
    )

    assert merged["sector"] == "电力设备"
    assert merged["industry_framework_label"] == "光伏主链"
    assert merged["chain_nodes"] == ["光伏主链", "储能", "电网设备"]


def test_merge_metadata_prefers_stock_industry_over_company_profile_cross_sector_noise(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine.get_asset_context",
        lambda symbol, asset_type, context: SimpleNamespace(  # noqa: ARG005
            metadata={
                "name": "盐湖股份",
                "sector": "农药化肥",
                "industry": "农药化肥",
                "main_business": "主要产品:氯化钾、碳酸锂。",
                "business_scope": "化肥销售、旅游业务、餐饮服务、电池制造、软件开发。",
                "company_intro": "氯化钾主要应用于农业领域，锂也是新能源和人工智能产业关键材料。",
            },
            name="盐湖股份",
        ),
    )

    merged = _merge_metadata(
        "000792",
        "cn_stock",
        {"name": "盐湖股份", "sector": "农药化肥", "industry": "农药化肥"},
        config={},
    )

    assert merged["sector"] == "农业"
    assert merged["industry"] == "农药化肥"
    assert merged["industry_framework_label"] == "农药化肥"
    assert merged["chain_nodes"][:3] == ["粮食安全", "种业", "农化"]


def test_merge_metadata_uses_tracked_index_before_chain_node_for_etf_framework_label(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine.get_asset_context",
        lambda symbol, asset_type, context: SimpleNamespace(  # noqa: ARG005
            metadata={
                "name": "国泰恒生A股电网设备ETF",
                "sector": "电网",
                "benchmark": "恒生A股电网设备指数",
                "chain_nodes": ["AI算力", "电力需求", "电网设备"],
            },
            name="国泰恒生A股电网设备ETF",
        ),
    )

    merged = _merge_metadata(
        "561380",
        "cn_etf",
        {
            "name": "国泰恒生A股电网设备ETF",
            "sector": "电网",
            "benchmark": "恒生A股电网设备指数",
            "chain_nodes": ["AI算力", "电力需求", "电网设备"],
        },
        config={},
    )

    assert merged["industry_framework_label"] == "恒生A股电网设备指数"


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


def test_build_default_pool_preserves_sector_breadth_before_hitting_candidate_cap(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_universe_snapshot",
        lambda self: pd.DataFrame(
            [
                {
                    "symbol": "512480",
                    "name": "半导体ETF",
                    "benchmark": "中证半导体指数收益率",
                    "invest_type": "被动指数型",
                    "fund_type": "股票型",
                    "management": "某基金",
                    "trade_date": "2026-03-12",
                    "list_date": "2020-01-18",
                    "delist_date": "",
                    "amount": 420_000_000.0,
                },
                {
                    "symbol": "159819",
                    "name": "人工智能ETF",
                    "benchmark": "中证人工智能主题指数收益率",
                    "invest_type": "被动指数型",
                    "fund_type": "股票型",
                    "management": "某基金",
                    "trade_date": "2026-03-12",
                    "list_date": "2021-01-18",
                    "delist_date": "",
                    "amount": 380_000_000.0,
                },
                {
                    "symbol": "515880",
                    "name": "消费电子ETF",
                    "benchmark": "中证消费电子主题指数收益率",
                    "invest_type": "被动指数型",
                    "fund_type": "股票型",
                    "management": "某基金",
                    "trade_date": "2026-03-12",
                    "list_date": "2022-01-18",
                    "delist_date": "",
                    "amount": 360_000_000.0,
                },
                {
                    "symbol": "515230",
                    "name": "算力ETF",
                    "benchmark": "中证人工智能算力主题指数收益率",
                    "invest_type": "被动指数型",
                    "fund_type": "股票型",
                    "management": "某基金",
                    "trade_date": "2026-03-12",
                    "list_date": "2023-01-18",
                    "delist_date": "",
                    "amount": 340_000_000.0,
                },
                {
                    "symbol": "518880",
                    "name": "黄金ETF",
                    "benchmark": "上海金ETF价格收益率",
                    "invest_type": "商品型",
                    "fund_type": "商品型",
                    "management": "某基金",
                    "trade_date": "2026-03-12",
                    "list_date": "2013-01-18",
                    "delist_date": "",
                    "amount": 220_000_000.0,
                },
                {
                    "symbol": "561380",
                    "name": "电网ETF",
                    "benchmark": "中证电网设备指数收益率",
                    "invest_type": "被动指数型",
                    "fund_type": "股票型",
                    "management": "某基金",
                    "trade_date": "2026-03-12",
                    "list_date": "2024-01-18",
                    "delist_date": "",
                    "amount": 180_000_000.0,
                },
            ]
        ),
    )
    monkeypatch.setattr("src.processors.opportunity_engine.load_watchlist", lambda: [])

    pool, warnings = build_default_pool({"opportunity": {"min_turnover": 50_000_000, "max_scan_candidates": 4}})

    assert warnings == []
    assert len(pool) == 4
    sectors = [item.sector for item in pool]
    assert "科技" in sectors
    assert "黄金" in sectors
    assert "电网" in sectors
    assert len(set(sectors)) >= 3


def test_build_default_pool_uses_specific_taxonomy_buckets_for_hardtech_and_media_etfs(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_universe_snapshot",
        lambda self: pd.DataFrame(
            [
                {
                    "symbol": "515880",
                    "name": "通信ETF",
                    "benchmark": "中证全指通信设备指数收益率",
                    "invest_type": "被动指数型",
                    "fund_type": "股票型",
                    "management": "某基金",
                    "trade_date": "2026-04-22",
                    "list_date": "2020-01-18",
                    "delist_date": "",
                    "amount": 420_000_000.0,
                },
                {
                    "symbol": "512480",
                    "name": "半导体ETF",
                    "benchmark": "中证全指半导体产品与设备指数收益率",
                    "invest_type": "被动指数型",
                    "fund_type": "股票型",
                    "management": "某基金",
                    "trade_date": "2026-04-22",
                    "list_date": "2020-01-18",
                    "delist_date": "",
                    "amount": 410_000_000.0,
                },
                {
                    "symbol": "159869",
                    "name": "游戏ETF",
                    "benchmark": "中证动漫游戏指数收益率",
                    "invest_type": "被动指数型",
                    "fund_type": "股票型",
                    "management": "某基金",
                    "trade_date": "2026-04-22",
                    "list_date": "2021-01-18",
                    "delist_date": "",
                    "amount": 180_000_000.0,
                },
            ]
        ),
    )
    monkeypatch.setattr("src.processors.opportunity_engine.load_watchlist", lambda: [])

    pool, warnings = build_default_pool({"opportunity": {"min_turnover": 50_000_000, "max_scan_candidates": 10}})

    assert warnings == []
    sector_map = {item.symbol: item.sector for item in pool}
    assert sector_map["515880"] == "通信"
    assert sector_map["512480"] == "半导体"
    assert sector_map["159869"] == "传媒"


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
    innovation = next(item for item in pool if item.symbol == "513120")
    assert innovation.chain_nodes == ["创新药", "港股医药", "FDA"]


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


def test_fundamental_dimension_etf_light_profile_reuses_prefetched_index_bundle(monkeypatch):
    def _unexpected(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("heavy valuation chain should be skipped for light ETF discovery")

    monkeypatch.setattr(ValuationCollector, "get_cn_index_snapshot", _unexpected)
    monkeypatch.setattr(ValuationCollector, "get_cn_index_value_history", _unexpected)
    monkeypatch.setattr(ValuationCollector, "get_cn_index_financial_proxies", _unexpected)
    monkeypatch.setattr("src.processors.opportunity_engine._fund_financial_proxy", _unexpected)
    monkeypatch.setattr(ChinaMarketCollector, "get_etf_fund_flow", _unexpected)
    monkeypatch.setattr("src.processors.opportunity_engine._sector_flow_snapshot", lambda metadata, drivers: {})  # noqa: ARG005

    dimension = _fundamental_dimension(
        "513310",
        "cn_etf",
        {
            "name": "中韩半导体ETF",
            "sector": "科技",
            "chain_nodes": ["半导体"],
            "index_topic_bundle": {
                "index_snapshot": {
                    "index_code": "931790.CSI",
                    "index_name": "中韩半导体",
                    "pe_ttm": 21.5,
                    "display_label": "真实指数估值",
                    "metric_label": "滚动PE",
                    "match_quality": "exact",
                    "match_note": "已命中真实指数。",
                }
            },
        },
        {"price_percentile_1y": 0.42},
        {},
        {
            "profile_mode": "light",
            "overview": {"业绩比较基准": "中证韩交所中韩半导体指数"},
            "style": {},
            "etf_snapshot": {"index_code": "931790.CSI", "index_name": "中证韩交所中韩半导体指数"},
        },
        {"drivers": {}},
    )

    assert dimension["valuation_snapshot"]["index_code"] == "931790.CSI"
    assert dimension["valuation_snapshot"]["pe_ttm"] == 21.5
    assert any(factor["name"] == "真实指数估值" for factor in dimension["factors"])


def test_chips_dimension_etf_light_profile_skips_heavy_flow_and_concentration_calls(monkeypatch):
    def _unexpected(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("heavy ETF chip chain should be skipped in light discovery mode")

    monkeypatch.setattr("src.processors.opportunity_engine._sector_flow_snapshot", lambda metadata, drivers: {"name": "半导体", "main_flow": 8.2})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._northbound_sector_snapshot", lambda metadata, drivers: {})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._hot_rank_snapshot", lambda metadata, drivers: {})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._context_cn_index_concentration_proxy", _unexpected)
    monkeypatch.setattr(ChinaMarketCollector, "get_etf_fund_flow", _unexpected)
    monkeypatch.setattr(ChinaMarketCollector, "get_north_south_flow", _unexpected)

    dimension = _chips_dimension(
        "513310",
        "cn_etf",
        {"name": "中韩半导体ETF", "sector": "科技", "chain_nodes": ["半导体"]},
        {"fund_profile": {"profile_mode": "light"}, "drivers": {}},
        {},
    )

    institution_factor = next(factor for factor in dimension["factors"] if factor["name"] == "机构资金承接")
    assert "行业资金流代理承接方向" in institution_factor["detail"]
    assert institution_factor["awarded"] in {10, -5, 0}


def test_chips_dimension_etf_full_profile_reuses_prefetched_index_topic_weights(monkeypatch):
    def _unexpected(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("slow index proxy path should not run when index bundle already has weights")

    monkeypatch.setattr(ValuationCollector, "get_cn_index_snapshot", _unexpected)
    monkeypatch.setattr(ValuationCollector, "get_cn_index_financial_proxies", _unexpected)
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_north_south_flow",
        lambda self: pd.DataFrame([{"日期": "2026-03-10", "北向资金净流入": 600_000_000.0, "南向资金净流入": 0.0}]),  # noqa: ARG005
    )
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_fund_flow",
        lambda self, symbol: pd.DataFrame([{"日期": "2026-03-10", "净流入": 120_000_000.0}]),  # noqa: ARG005
    )

    weights = pd.DataFrame(
        [
            {"symbol": "688072", "name": "拓荆科技", "weight": 10.8},
            {"symbol": "688120", "name": "华海清科", "weight": 10.7},
            {"symbol": "688012", "name": "中微公司", "weight": 10.0},
            {"symbol": "688361", "name": "中科飞测", "weight": 8.5},
            {"symbol": "688126", "name": "沪硅产业", "weight": 8.4},
        ]
    )
    dimension = _chips_dimension(
        "588170",
        "cn_etf",
        {
            "name": "科创半导体ETF",
            "sector": "科技",
            "chain_nodes": ["半导体"],
            "index_topic_bundle": {
                "index_snapshot": {"index_code": "950125.CSI", "index_name": "科创半导体材料设备"},
                "constituent_weights": weights,
                "fallback": "none",
                "as_of": "2026-04-02 19:43:16",
            },
        },
        {
            "fund_profile": {"profile_mode": "full"},
            "drivers": {
                "industry_fund_flow": pd.DataFrame([{"名称": "半导体", "今日主力净流入-净额": 88_000_000.0, "今日主力净流入-净占比": 1.3}]),
                "concept_fund_flow": pd.DataFrame(),
                "northbound_industry": {"frame": pd.DataFrame()},
                "northbound_concept": {"frame": pd.DataFrame()},
                "hot_rank": pd.DataFrame(),
            },
        },
        {},
    )

    factor = next(item for item in dimension["factors"] if item["name"] == "机构集中度代理")
    assert factor["display_score"] != "缺失"
    assert "48.4%" in factor["signal"]
    assert "直接复用已命中的指数成分权重主链" in factor["detail"]


def test_chips_dimension_etf_full_profile_falls_back_to_sector_flow_when_etf_flow_empty(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_fund_flow",
        lambda self, symbol: pd.DataFrame(columns=["日期", "净流入"]),  # noqa: ARG005
    )
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_north_south_flow",
        lambda self: pd.DataFrame([{"日期": "2026-03-10", "北向资金净流入": 600_000_000.0, "南向资金净流入": 0.0}]),  # noqa: ARG005
    )

    dimension = _chips_dimension(
        "512480",
        "cn_etf",
        {
            "name": "半导体ETF",
            "sector": "科技",
            "chain_nodes": ["半导体"],
            "index_topic_bundle": {
                "index_snapshot": {"index_code": "931790.CSI", "index_name": "中韩半导体"},
                "constituent_weights": pd.DataFrame([{"symbol": "688072", "name": "拓荆科技", "weight": 10.8}]),
            },
        },
        {
            "fund_profile": {"profile_mode": "full"},
            "drivers": {
                "industry_fund_flow": pd.DataFrame([{"名称": "半导体", "今日主力净流入-净额": 88_000_000.0, "今日主力净流入-净占比": 1.3}]),
                "concept_fund_flow": pd.DataFrame(),
                "northbound_industry": {"frame": pd.DataFrame()},
                "northbound_concept": {"frame": pd.DataFrame()},
                "hot_rank": pd.DataFrame(),
            },
        },
        {},
    )

    factor = next(item for item in dimension["factors"] if item["name"] == "机构资金承接")
    assert factor["display_score"] != "缺失"
    assert "主力净流入" in factor["signal"]
    assert "空表" in factor["detail"]


def test_j4_convertible_bond_snapshot_uses_basic_daily_and_factor_pro(monkeypatch) -> None:
    from src.processors.opportunity_engine import _cn_stock_convertible_bond_snapshot

    calls: list[tuple[str, str, str]] = []

    def fake_basic(self, exchange):  # noqa: ANN001
        if exchange == "SH":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "113519.SH",
                        "stk_code": "600519",
                        "stk_short_name": "贵州茅台",
                        "bond_short_name": "茅台转债",
                        "remain_size": 2_400_000_000.0,
                        "list_date": "2024-01-05",
                    }
                ]
            )
        return pd.DataFrame()

    def fake_daily(self, ts_code, trade_date):  # noqa: ANN001
        calls.append(("daily", ts_code, trade_date))
        return pd.DataFrame(
            [
                {
                    "trade_date": "2026-04-03",
                    "close": 115.2,
                    "cb_over_rate": 12.8,
                }
            ]
        )

    def fake_factor(self, ts_code, trade_date):  # noqa: ANN001
        calls.append(("factor", ts_code, trade_date))
        return pd.DataFrame(
            [
                {
                    "trade_date": "2026-04-03",
                    "ma_bfq_5": 113.0,
                    "ma_bfq_20": 110.0,
                    "macd_dif_bfq": 1.2,
                    "macd_dea_bfq": 0.8,
                    "rsi_bfq_6": 65.0,
                }
            ]
        )

    monkeypatch.setattr(ChinaMarketCollector, "get_cb_basic", fake_basic)
    monkeypatch.setattr(ChinaMarketCollector, "get_cb_daily", fake_daily)
    monkeypatch.setattr(ChinaMarketCollector, "get_cb_factor_pro", fake_factor)

    snapshot = _cn_stock_convertible_bond_snapshot(
        {"asset_type": "cn_stock", "symbol": "600519", "name": "贵州茅台"},
        {"config": {}, "as_of": "2026-04-03"},
    )

    assert snapshot["status"] == "matched"
    assert snapshot["diagnosis"] == "live"
    assert snapshot["bond_code"] == "113519.SH"
    assert snapshot["premium_rate"] == pytest.approx(12.8)
    assert snapshot["trend_label"] == "趋势偏强"
    assert snapshot["momentum_label"] == "动能改善"
    assert snapshot["is_fresh"] is True
    assert ("daily", "113519.SH", "20260403") in calls
    assert ("factor", "113519.SH", "20260403") in calls


def test_fundamental_dimension_surfaces_convertible_bond_factor_for_cn_stock(monkeypatch) -> None:
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
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_convertible_bond_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "matched",
            "diagnosis": "live",
            "is_fresh": True,
            "latest_date": "2026-04-03",
            "bond_code": "113519.SH",
            "detail": "贵州茅台 对应转债 茅台转债；转债 趋势偏强；动能改善；转股溢价约 +12.80%；余额约 24.0 亿",
            "disclosure": "Tushare cb_basic / cb_daily / cb_factor_pro 已接入；空表、未匹配或非当期时不伪装成 fresh。",
            "source": "tushare.cb_basic+tushare.cb_daily+tushare.cb_factor_pro",
            "trend_label": "趋势偏强",
            "momentum_label": "动能改善",
            "premium_rate": 12.8,
            "remain_size_yi": 24.0,
        },
    )

    dimension = _fundamental_dimension(
        "600519",
        "cn_stock",
        {"name": "贵州茅台", "sector": "消费", "chain_nodes": ["白酒"]},
        {"price_percentile_1y": 0.55},
        {},
    )
    factor_map = {factor["name"]: factor for factor in dimension["factors"]}

    cb_factor = next(factor for factor in dimension["factors"] if factor.get("factor_id") == "j4_convertible_bond_proxy")
    assert cb_factor["display_score"] != "观察"
    assert "转股溢价" in cb_factor["signal"]
    assert "余额约" in cb_factor["signal"]


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


def test_nearest_support_reference_ignores_levels_above_current_price():
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=40, freq="B"),
            "open": [0.86] * 40,
            "high": [0.87] * 40,
            "low": [0.78] * 39 + [0.79],
            "close": [0.86] * 39 + [0.80],
            "volume": [1_000_000] * 40,
            "amount": [80_000_000.0] * 40,
        }
    )
    technical = {
        "ma_system": {"mas": {"MA20": 0.842, "MA60": 0.935}},
        "fibonacci": {"levels": {"0.382": 0.845, "0.500": 0.889, "0.618": 0.935}},
    }

    label, level = _nearest_support_reference(history, technical)

    assert label == "近20日低点"
    assert level == 0.78
    assert level < float(history["close"].iloc[-1])


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


def test_technical_dimension_uses_stk_factor_pro_snapshot_in_score_and_meta():
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-03-01", periods=40, freq="B"),
            "open": [20.0 + i * 0.1 for i in range(40)],
            "high": [20.5 + i * 0.1 for i in range(40)],
            "low": [19.5 + i * 0.1 for i in range(40)],
            "close": [20.1 + i * 0.1 for i in range(40)],
            "volume": [1_200_000] * 40,
            "amount": [120_000_000.0] * 40,
        }
    )
    technical = {
        "macd": {"DIF": 0.2, "DEA": 0.1},
        "dmi": {"ADX": 29.0, "DI+": 34.0, "DI-": 17.0},
        "kdj": {"K": 54.0, "D": 50.0, "J": 62.0, "cross": "neutral", "zone": "neutral"},
        "obv": {"OBV": 12_000_000, "MA": 11_500_000, "slope_5d": 260_000, "signal": "bullish"},
        "divergence": {"signal": "neutral", "label": "未识别到明确顶/底背离", "detail": "无", "strength": 0},
        "rsi": {"RSI": 56.0},
        "fibonacci": {"levels": {"0.382": 21.8, "0.500": 21.2, "0.618": 20.9}},
        "candlestick": [],
        "volume": {"vol_ratio": 1.35, "vol_ratio_20": 1.10, "price_change_1d": 0.024, "structure": "放量上攻"},
        "ma_system": {"mas": {"MA5": 21.7, "MA20": 21.2, "MA60": 20.6}, "signal": "bullish"},
        "bollinger": {"signal": "neutral"},
        "volatility": {"NATR": 0.021, "atr_ratio_20": 0.91, "boll_width_percentile": 0.31, "signal": "compressed"},
    }
    expected_as_of = history["date"].iloc[-1].strftime("%Y-%m-%d")

    class _FakeValuationCollector:
        def __init__(self, config=None):  # noqa: ARG002
            pass

        def get_cn_stock_factor_snapshot(self, symbol, *, as_of="", lookback_days=90):  # noqa: ARG002
            assert symbol == "300750"
            assert as_of == expected_as_of
            assert lookback_days == 90
            return {
                "status": "matched",
                "diagnosis": "live",
                "is_fresh": True,
                "latest_date": expected_as_of,
                "source": "tushare.stk_factor_pro",
                "trend_label": "趋势偏强",
                "momentum_label": "动能改善",
                "signal_strength": "高",
                "detail": "收盘复权价 22.80 / BBI 22.10 / BIAS1 +1.80% / ADX 29.0 / 量比 1.35",
                "disclosure": "Tushare stk_factor_pro 股票每日技术面因子快照。",
            }

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("src.processors.opportunity_engine.ValuationCollector", _FakeValuationCollector)
        dimension = _technical_dimension(
            history,
            technical,
            symbol="300750",
            asset_type="cn_stock",
            metadata={"name": "宁德时代"},
            config={},
        )

    factors = {factor["name"]: factor for factor in dimension["factors"]}
    stock_factor = factors["股票技术面状态"]
    assert stock_factor["factor_id"] == "j1_stk_factor_pro"
    assert stock_factor["display_score"] == "12/12"
    assert "趋势偏强" in stock_factor["signal"]
    assert dimension["stock_factor_snapshot"]["latest_date"] == expected_as_of
    assert dimension["stock_factor_snapshot"]["is_fresh"] is True
    assert "stk_factor_pro" in dimension["summary"]
    assert stock_factor["factor_meta"]["source_as_of"] == expected_as_of
    assert stock_factor["factor_meta"]["state"] == "production_factor"


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


def test_technical_dimension_expires_stale_bullish_divergence() -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-02-20", periods=40, freq="B"),
            "open": [10.0 + i * 0.03 for i in range(40)],
            "high": [10.3 + i * 0.03 for i in range(40)],
            "low": [9.8 + i * 0.03 for i in range(40)],
            "close": [10.1 + i * 0.03 for i in range(40)],
            "volume": [1_000_000] * 40,
            "amount": [100_000_000.0] * 40,
        }
    )
    technical = {
        "macd": {"DIF": 0.2, "DEA": 0.1},
        "dmi": {"ADX": 28.0, "DI+": 31.0, "DI-": 19.0},
        "kdj": {"K": 38.0, "D": 32.0, "J": 50.0, "cross": "golden_cross", "zone": "oversold"},
        "obv": {"OBV": 12_000_000, "MA": 11_500_000, "slope_5d": 300_000, "signal": "bullish"},
        "divergence": {
            "signal": "bullish",
            "kind": "底背离",
            "label": "价格低点下移，但 RSI / OBV 低点抬高（底背离）",
            "indicators": ["RSI", "OBV"],
            "strength": 2,
            "detail": "2026-03-23 -> 2026-03-31 价格低点下移，但 RSI / OBV 低点抬高。",
            "hits": [
                {"indicator": "RSI", "current_date": "2026-03-31"},
                {"indicator": "OBV", "current_date": "2026-03-31"},
            ],
        },
        "rsi": {"RSI": 46.0},
        "fibonacci": {"levels": {"0.382": 10.8, "0.500": 10.6, "0.618": 10.4}},
        "candlestick": [],
        "volume": {"vol_ratio": 1.1, "vol_ratio_20": 1.0, "price_change_1d": 0.008, "structure": "量价中性"},
        "ma_system": {"mas": {"MA5": 10.9, "MA20": 10.7, "MA60": 10.4}, "signal": "bullish"},
        "bollinger": {"signal": "neutral"},
        "volatility": {"NATR": 0.026, "atr_ratio_20": 1.02, "boll_width_percentile": 0.55, "signal": "neutral"},
    }

    dimension = _technical_dimension(history, technical)
    divergence_factor = next(f for f in dimension["factors"] if f["name"] == "量价/动量背离")

    assert divergence_factor["display_score"] == "0/10"
    assert "已过背离触发窗口" in divergence_factor["detail"]


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


def test_relative_strength_dimension_caps_rebound_only_etf_with_weak_peer_rank() -> None:
    dates = pd.date_range("2025-11-05", periods=103, freq="B")
    asset_returns = pd.Series([0.001] * 103, index=dates)
    benchmark_returns = pd.Series([0.0004] * 103, index=dates)
    context = {
        "benchmark_returns": {"cn_etf": benchmark_returns},
        "drivers": {"industry_spot": pd.DataFrame()},
        "fund_profile": {
            "achievement": {
                "近3月": {
                    "return_pct": -0.1191,
                    "peer_rank": "3633/3873",
                }
            }
        },
        "day_theme": {},
        "regime": {"preferred_assets": []},
        "global_proxy": {},
    }
    dimension = _relative_strength_dimension(
        "159131",
        "cn_etf",
        {"name": "AI算力ETF", "sector": "科技", "chain_nodes": ["AI算力"]},
        {"return_5d": 0.030, "return_20d": -0.002},
        asset_returns,
        context,
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}

    assert factors["同类业绩校验"]["awarded"] < 0
    assert dimension["cross_check_failed"] is True
    assert dimension["score"] <= 65


def test_relative_strength_dimension_surfaces_benchmark_name() -> None:
    dates = pd.date_range("2026-02-01", periods=20, freq="B")
    asset_returns = pd.Series([0.004] * 20, index=dates)
    benchmark_returns = pd.Series([0.001] * 20, index=dates)
    context = {
        "benchmark_returns": {"cn_stock": benchmark_returns},
        "drivers": {"industry_spot": pd.DataFrame()},
        "day_theme": {},
        "regime": {"preferred_assets": []},
    }
    dimension = _relative_strength_dimension(
        "600519",
        "cn_stock",
        {"name": "贵州茅台", "sector": "消费", "chain_nodes": ["白酒"]},
        {"return_5d": 0.030, "return_20d": 0.080},
        asset_returns,
        context,
    )
    factor = next(item for item in dimension["factors"] if item["name"] == "超额拐点")
    assert "沪深300ETF" in factor["signal"]
    assert dimension["benchmark_name"] == "沪深300ETF"


def test_relative_strength_dimension_surfaces_ah_comparison_factor(monkeypatch):
    dates = pd.date_range("2026-02-01", periods=20, freq="B")
    asset_returns = pd.Series([0.004] * 20, index=dates)
    benchmark_returns = pd.Series([0.001] * 20, index=dates)
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_ah_comparison_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "matched",
            "is_fresh": True,
            "latest_date": "2026-04-03",
            "premium_rate": 18.4,
            "detail": "600519 A/H 比价快照；贵州茅台 vs HK；溢价/折价约 +18.40%",
        },
    )
    context = {
        "benchmark_returns": {"cn_stock": benchmark_returns},
        "drivers": {"industry_spot": pd.DataFrame()},
        "day_theme": {},
        "regime": {"preferred_assets": []},
    }
    dimension = _relative_strength_dimension(
        "600519",
        "cn_stock",
        {"name": "贵州茅台", "sector": "消费", "chain_nodes": ["白酒"]},
        {"return_5d": 0.030, "return_20d": 0.080},
        asset_returns,
        context,
    )
    factor = next(item for item in dimension["factors"] if item["name"] == "跨市场比价")
    assert factor["factor_id"] == "j3_ah_comparison"
    assert "A/H 比价溢价 +18.40%" in factor["signal"]
    assert factor["awarded"] == 10


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


def test_correlation_to_watchlist_skips_constant_return_series() -> None:
    dates = pd.date_range("2026-01-01", periods=30, freq="B")
    asset_returns = pd.Series([0.01] * 30, index=dates)
    context = {
        "watchlist_returns": {
            "CONST": pd.Series([0.02] * 30, index=dates),
            "VAR": pd.Series(np.linspace(-0.01, 0.02, 30), index=dates),
        }
    }

    pair = _correlation_to_watchlist("300308", asset_returns, context)

    assert pair is None


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
    assert "偏顺风" in dimension["summary"]
    assert any(factor["name"] == "景气方向" and "新订单" in factor["signal"] for factor in dimension["factors"])
    assert any(factor["name"] == "信用脉冲" and "社融" in factor["signal"] for factor in dimension["factors"])


def test_normalize_sector_promotes_specific_theme_buckets_before_broad_technology() -> None:
    assert _normalize_sector("光模块ETF")[0] == "通信"
    assert _normalize_sector("5GETF")[0] == "通信"
    assert _normalize_sector("半导体ETF")[0] == "半导体"
    assert _normalize_sector("游戏ETF")[0] == "传媒"
    assert _normalize_sector("国证商用卫星通信产业ETF")[1] == ["卫星通信", "卫星互联网", "商业航天"]
    assert _normalize_sector("国泰中证半导体材料设备主题ETF")[1] == ["半导体设备", "半导体材料", "国产替代"]
    assert _normalize_sector("嘉实上证科创板芯片ETF")[1] == ["芯片", "半导体", "国产替代"]
    assert _normalize_sector("智能电网ETF")[1] == ["特高压", "智能电网", "电网设备"]
    assert _normalize_sector("储能并网ETF")[1] == ["储能并网", "新型储能", "电力设备"]
    assert _normalize_sector("医疗器械ETF")[1] == ["医疗器械", "设备更新", "老龄化"]
    assert _normalize_sector("CXO ETF")[1] == ["CXO", "CRO/CDMO", "医药外包"]


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
    assert dimension["display_name"] == "产品质量/基本面代理"
    assert dimension["valuation_snapshot"]["index_name"] == "中国战略新兴产业"
    assert "前五大重仓股加权增速代理" in factors["盈利增速"]["signal"]
    assert "前五大重仓股" in factors["ROE"]["signal"]
    assert factors["PEG 代理"]["display_score"] != "缺失"
    assert "不直接等同于底层行业基本面已经确认" in dimension["summary"]


def test_enrich_metadata_with_fund_profile_preserves_etf_snapshot_fields() -> None:
    from src.processors.opportunity_engine import _enrich_metadata_with_fund_profile

    enriched = _enrich_metadata_with_fund_profile(
        {"name": "A500ETF", "sector": "宽基"},
        {
            "overview": {
                "基金简称": "A500ETF华泰柏瑞",
                "业绩比较基准": "中证A500指数",
            },
            "style": {"sector": "宽基", "chain_nodes": ["宽基", "大盘蓝筹"]},
            "etf_snapshot": {
                "index_code": "000510.SH",
                "index_name": "中证A500指数",
                "exchange": "SH",
                "list_status": "L",
                "etf_type": "境内",
                "total_share": 3_091_998.74,
                "total_size": 3_818_927.64,
                "share_as_of": "2026-03-31",
                "etf_share_change": 2.58,
            },
        },
    )

    assert enriched["name"] == "A500ETF华泰柏瑞"
    assert enriched["benchmark"] == "中证A500指数"
    assert enriched["index_code"] == "000510.SH"
    assert enriched["index_name"] == "中证A500指数"
    assert enriched["benchmark_name"] == "中证A500指数"
    assert enriched["exchange"] == "SH"
    assert enriched["list_status"] == "L"
    assert enriched["etf_type"] == "境内"
    assert enriched["total_share"] == 3_091_998.74
    assert enriched["total_size"] == 3_818_927.64
    assert enriched["etf_share_change"] == 2.58
    assert enriched["share_as_of"] == "2026-03-31"


def test_enrich_metadata_with_fund_profile_keeps_more_specific_chain_nodes() -> None:
    from src.processors.opportunity_engine import _enrich_metadata_with_fund_profile

    enriched = _enrich_metadata_with_fund_profile(
        {"name": "港股创新药ETF", "sector": "医药", "chain_nodes": ["创新药", "港股医药", "FDA"]},
        {
            "style": {
                "sector": "医药",
                "chain_nodes": ["医药", "老龄化"],
            }
        },
    )

    assert enriched["chain_nodes"] == ["创新药", "港股医药", "FDA"]


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
    assert dimension["display_name"] == "产品质量/基本面代理"
    assert dimension["valuation_snapshot"]["index_name"] == "中证人工智能主题指数"
    assert "前五大持仓/成分股加权增速代理" in factors["盈利增速"]["signal"]
    assert "前五大持仓/成分股" in factors["ROE"]["signal"]
    assert factors["PEG 代理"]["display_score"] != "缺失"
    assert "不直接等同于底层行业基本面已经确认" in dimension["summary"]


def test_fundamental_dimension_for_hk_tech_etf_uses_exact_benchmark_holdings_pe_proxy(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_snapshot",
        lambda self, keywords: (  # noqa: ARG005
            {
                "index_code": "987008",
                "index_name": "港股通科技",
                "match_quality": "exact_no_pe",
                "display_label": "真实指数估值",
                "match_note": "估值库已命中基准指数，但当前缺少可用滚动PE。",
            }
            if "港股通科技" in keywords
            else (_ for _ in ()).throw(AssertionError(keywords))
        ),
    )
    monkeypatch.setattr(ValuationCollector, "get_cn_index_value_history", lambda self, index_code: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_weighted_stock_financial_proxies", lambda self, holdings, **kwargs: {})  # noqa: ARG005
    monkeypatch.setattr(
        ValuationCollector,
        "get_weighted_market_financial_proxies",
        lambda self, holdings, asset_type, **kwargs: {  # noqa: ARG005
            "pe_ttm": 21.5,
            "roe": 15.2,
            "revenue_yoy": 9.8,
            "gross_margin": 38.4,
            "coverage_weight": 60.0,
            "coverage_ratio": 1.0,
            "coverage_count": 5,
            "top_concentration": 60.0,
        },
    )
    monkeypatch.setattr(MarketDriversCollector, "collect", lambda self: {})  # noqa: ARG005
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_fund_flow",
        lambda self, symbol: (_ for _ in ()).throw(RuntimeError("no etf flow")),  # noqa: ARG005
    )
    fund_profile = {
        "overview": {
            "基金简称": "港股通恒生科技ETF",
            "业绩比较基准": "恒生港股通科技主题指数收益率(经汇率调整)",
        },
        "top_holdings": [
            {"股票代码": "0700", "股票名称": "腾讯控股", "占净值比例": 14.90},
            {"股票代码": "9988", "股票名称": "阿里巴巴-W", "占净值比例": 13.96},
            {"股票代码": "1810", "股票名称": "小米集团-W", "占净值比例": 13.90},
            {"股票代码": "3690", "股票名称": "美团-W", "占净值比例": 11.03},
            {"股票代码": "0981", "股票名称": "中芯国际", "占净值比例": 6.24},
        ],
        "industry_allocation": [{"行业类别": "信息技术", "占净值比例": 90.0}],
    }

    dimension = _fundamental_dimension(
        "520840",
        "cn_etf",
        {"name": "港股通恒生科技ETF", "sector": "科技", "chain_nodes": ["港股科技"]},
        {"price_percentile_1y": 0.01},
        {},
        fund_profile,
    )

    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert dimension["valuation_snapshot"]["index_name"] == "港股通科技"
    assert dimension["valuation_snapshot"]["pe_ttm"] == 21.5
    assert dimension["valuation_snapshot"]["match_quality"] == "exact_holdings_proxy"
    assert "前五大重仓加权PE代理" in dimension["valuation_snapshot"]["match_note"]
    assert factors["真实基准重仓股PE代理"]["signal"] == "港股通科技 PE 21.5x"
    assert factors["ROE"]["display_score"] != "缺失"


def test_fundamental_dimension_for_cn_agriculture_etf_uses_exact_benchmark_holdings_pe_proxy(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_snapshot",
        lambda self, keywords: (  # noqa: ARG005
            {
                "index_code": "399365",
                "index_name": "国证粮食",
                "match_quality": "exact_no_pe",
                "display_label": "真实指数估值",
                "match_note": "估值库已命中基准指数，但当前缺少可用滚动PE。",
            }
            if "国证粮食" in keywords or "粮食" in keywords
            else (_ for _ in ()).throw(AssertionError(keywords))
        ),
    )
    monkeypatch.setattr(ValuationCollector, "get_cn_index_value_history", lambda self, index_code: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        ValuationCollector,
        "get_weighted_stock_financial_proxies",
        lambda self, holdings, **kwargs: {  # noqa: ARG005
            "pe_ttm": 18.4,
            "roe": 9.6,
            "revenue_yoy": 6.8,
            "gross_margin": 24.1,
            "coverage_weight": 35.4,
            "coverage_ratio": 1.0,
            "coverage_count": 5,
            "top_concentration": 35.4,
        },
    )
    monkeypatch.setattr(MarketDriversCollector, "collect", lambda self: {})  # noqa: ARG005
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_fund_flow",
        lambda self, symbol: (_ for _ in ()).throw(RuntimeError("no etf flow")),  # noqa: ARG005
    )
    fund_profile = {
        "overview": {
            "基金简称": "粮食ETF",
            "业绩比较基准": "国证粮食产业指数收益率",
        },
        "top_holdings": [
            {"股票代码": "002385", "股票名称": "大北农", "占净值比例": 10.06},
            {"股票代码": "000998", "股票名称": "隆平高科", "占净值比例": 7.73},
            {"股票代码": "600598", "股票名称": "北大荒", "占净值比例": 7.25},
            {"股票代码": "300087", "股票名称": "荃银高科", "占净值比例": 5.63},
            {"股票代码": "300189", "股票名称": "神农种业", "占净值比例": 4.75},
        ],
        "industry_allocation": [{"行业类别": "农、林、牧、渔业", "占净值比例": 39.91}],
        "style": {"sector": "农业"},
    }

    dimension = _fundamental_dimension(
        "159698",
        "cn_etf",
        {"name": "粮食ETF", "sector": "农业", "chain_nodes": ["粮食安全", "种业"]},
        {"price_percentile_1y": 0.90},
        {},
        fund_profile,
    )

    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert dimension["valuation_snapshot"]["index_name"] == "国证粮食"
    assert dimension["valuation_snapshot"]["pe_ttm"] == 18.4
    assert dimension["valuation_snapshot"]["match_quality"] == "exact_holdings_proxy"
    assert "前五大重仓加权PE代理" in dimension["valuation_snapshot"]["match_note"]
    assert "国证零售" not in dimension["summary"]
    assert factors["真实基准重仓股PE代理"]["signal"] == "国证粮食 PE 18.4x"


def test_fundamental_dimension_for_a500_fund_blocks_theme_proxy_and_uses_holdings_proxy(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_index_snapshot",
        lambda self, keywords: (  # noqa: ARG005
            {
                "index_code": "",
                "index_name": "中证A500",
                "match_quality": "benchmark_no_proxy",
                "display_label": "真实指数估值",
                "match_note": "估值库未直接命中 `中证A500`；为避免错配，不再回退到其他主题指数代理。",
            }
            if "中证A500" in keywords
            else (_ for _ in ()).throw(AssertionError(keywords))
        ),
    )
    monkeypatch.setattr(ValuationCollector, "get_cn_index_value_history", lambda self, index_code: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        ValuationCollector,
        "get_weighted_stock_financial_proxies",
        lambda self, holdings, **kwargs: {  # noqa: ARG005
            "pe_ttm": 15.7,
            "roe": 18.9,
            "revenue_yoy": 11.2,
            "gross_margin": 31.5,
            "coverage_weight": 48.0,
            "coverage_ratio": 1.0,
            "coverage_count": 5,
            "top_concentration": 48.0,
        },
    )
    monkeypatch.setattr(MarketDriversCollector, "collect", lambda self: {})  # noqa: ARG005
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_fund_flow",
        lambda self, symbol: (_ for _ in ()).throw(RuntimeError("no etf flow")),  # noqa: ARG005
    )
    fund_profile = {
        "overview": {
            "基金简称": "招商中证A500ETF联接C",
            "业绩比较基准": "中证A500指数收益率*95%+中国人民银行人民币活期存款利率(税后)*5%",
        },
        "top_holdings": [
            {"股票代码": "600519", "股票名称": "贵州茅台", "占净值比例": 10.1},
            {"股票代码": "300750", "股票名称": "宁德时代", "占净值比例": 9.8},
            {"股票代码": "601318", "股票名称": "中国平安", "占净值比例": 9.6},
            {"股票代码": "600036", "股票名称": "招商银行", "占净值比例": 9.3},
            {"股票代码": "000333", "股票名称": "美的集团", "占净值比例": 9.2},
        ],
        "industry_allocation": [{"行业类别": "制造业", "占净值比例": 80.0}],
        "style": {"sector": "宽基"},
    }

    dimension = _fundamental_dimension(
        "022456",
        "cn_fund",
        {"name": "招商中证A500ETF联接C", "sector": "宽基", "chain_nodes": ["宽基"], "is_passive_fund": True},
        {"price_percentile_1y": 0.68},
        {},
        fund_profile,
    )

    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert dimension["valuation_snapshot"]["index_name"] == "中证A500"
    assert dimension["valuation_snapshot"]["pe_ttm"] == 15.7
    assert dimension["valuation_snapshot"]["match_quality"] == "benchmark_holdings_proxy"
    assert "重仓加权PE代理" in dimension["valuation_snapshot"]["match_note"]
    assert factors["真实基准重仓股PE代理"]["signal"] == "中证A500 PE 15.7x"
    assert "创业软件" not in dimension["summary"]
    assert "绿色电力" not in dimension["summary"]


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
    assert dimension["display_name"] == "产品质量/基本面代理"
    assert dimension["valuation_snapshot"] is None
    assert dimension["financial_proxy"] == {}
    assert "产品结构评估" in dimension["summary"] or "产品结构" in dimension["summary"]
    assert factors["产品类型"]["display_score"] != "缺失"
    assert factors["跟踪标的"]["display_score"] != "缺失"
    assert factors["产品规模"]["display_score"] != "缺失"
    assert factors["结构缓冲"]["display_score"] != "缺失"
    assert "股票 PE" in factors["价格位置"]["detail"]


def test_fundamental_dimension_for_gold_fund_adds_sge_anchor_and_fund_sales_info(monkeypatch):
    monkeypatch.setattr(ValuationCollector, "get_cn_index_snapshot", lambda self, keywords: {})  # noqa: ARG005
    frame = pd.DataFrame([{"trade_date": "2026-04-01", "close": 580.0}, {"trade_date": "2026-04-02", "close": 586.0}])
    frame.attrs["source"] = "tushare.sge_daily"
    frame.attrs["latest_date"] = "2026-04-02"
    frame.attrs["is_fresh"] = True
    frame.attrs["disclosure"] = "黄金现货日线来自 Tushare sge_daily。"
    monkeypatch.setattr("src.processors.opportunity_engine.CommodityCollector.get_gold", lambda self: frame)  # noqa: ARG005

    fund_profile = {
        "overview": {
            "基金简称": "前海开源黄金ETF联接C",
            "基金类型": "商品型 / 黄金现货合约",
            "业绩比较基准": "黄金现货合约收益率",
            "跟踪标的": "黄金现货合约",
            "净资产规模": "12.30亿元（截止至：2025年12月31日）",
        },
        "style": {"sector": "黄金", "cash_ratio": 12.0},
        "sales_ratio_snapshot": {
            "latest_year": "2025",
            "lead_channel": "商业银行",
            "lead_ratio": 41.2,
            "summary": "2025年渠道保有结构：商业银行占比最高，约 41.20% 。",
        },
    }

    dimension = _fundamental_dimension(
        "021740",
        "cn_fund",
        {"name": "前海开源黄金ETF联接C", "sector": "黄金"},
        {"price_percentile_1y": 0.52},
        {},
        fund_profile,
    )

    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert "现货锚定" in factors
    assert "Au99.95" in factors["现货锚定"]["signal"]
    assert "公募渠道环境" in factors
    assert "商业银行占比最高" in factors["公募渠道环境"]["signal"]


def test_fundamental_dimension_for_cn_stock_adds_convertible_bond_proxy(monkeypatch):
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_financial_proxy", lambda self, symbol: {})  # noqa: ARG005
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_convertible_bond_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "matched",
            "is_fresh": True,
            "latest_date": "2026-04-03",
            "detail": "寒武纪对应转债 测试转债；转债 趋势偏强；动能改善；转股溢价约 +12.50%；余额约 8.6 亿",
            "row": {"bond_short_name": "测试转债"},
            "premium_rate": 12.5,
            "trend_label": "趋势偏强",
            "momentum_label": "动能改善",
            "remain_size_yi": 8.6,
            "disclosure": "Tushare cb_basic / cb_daily / cb_factor_pro 已接入。",
        },
    )

    dimension = _fundamental_dimension(
        "688256",
        "cn_stock",
        {"name": "寒武纪", "sector": "科技"},
        {"price_percentile_1y": 0.61},
        {},
        None,
    )

    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert "可转债映射" in factors
    assert "测试转债" in factors["可转债映射"]["signal"]
    assert "转股溢价 +12.50%" in factors["可转债映射"]["signal"]
    assert factors["可转债映射"]["factor_id"] == "j4_convertible_bond_proxy"


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
    assert dimension["display_name"] == "筹码结构（辅助项）"
    assert factors["北向/南向"]["display_score"] == "不适用"
    assert factors["机构集中度代理"]["display_score"] == "不适用"
    assert factors["机构资金承接"]["display_score"] != "缺失"
    assert "ETF" in factors["机构资金承接"]["detail"]
    assert "辅助判断" in dimension["summary"] or "主排序未使用" in dimension["summary"]


def test_fundamental_dimension_caps_proxy_only_etf_without_true_valuation(monkeypatch):
    monkeypatch.setattr(ValuationCollector, "get_cn_index_snapshot", lambda self, keywords: None)  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_index_value_history", lambda self, index_code: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        ValuationCollector,
        "get_weighted_stock_financial_proxies",
        lambda self, holdings, **kwargs: {  # noqa: ARG005
            "revenue_yoy": 25.0,
            "profit_yoy": 28.0,
            "roe": 18.0,
            "gross_margin": 35.0,
            "coverage_weight": 20.0,
            "report_date": "2025-12-31",
        },
    )
    monkeypatch.setattr(
        MarketDriversCollector,
        "collect",
        lambda self: {"industry_fund_flow": pd.DataFrame(), "concept_fund_flow": pd.DataFrame()},  # noqa: ARG005
    )
    fund_profile = {
        "overview": {"基金简称": "人工智能ETF", "业绩比较基准": "中证人工智能主题指数收益率"},
        "top_holdings": [{"股票代码": "300308", "股票名称": "中际旭创", "占净值比例": 10.57}],
        "industry_allocation": [{"行业类别": "信息技术", "占净值比例": 88.0}],
    }
    dimension = _fundamental_dimension(
        "159819",
        "cn_etf",
        {"name": "人工智能ETF", "sector": "科技", "chain_nodes": ["AI算力"]},
        {"price_percentile_1y": 0.84},
        {},
        fund_profile,
    )

    assert dimension["score"] <= 68
    assert "产品质量、跟踪机制和主题代理" in dimension["summary"]


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


def test_chips_dimension_cn_etf_market_northbound_fallback_is_info_only(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_north_south_flow",
        lambda self: pd.DataFrame([{"日期": "2026-03-26", "北向资金净流入": 2495.51 * 100000000}]),  # noqa: ARG005
    )
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_etf_fund_flow",
        lambda self, symbol: pd.DataFrame([{"净流入": 0.0}, {"净流入": 0.0}]),  # noqa: ARG005
    )
    dimension = _chips_dimension(
        "512480",
        "cn_etf",
        {"symbol": "512480", "name": "半导体ETF", "asset_type": "cn_etf", "sector": "科技"},
        {
            "config": {},
            "drivers": {
                "industry_fund_flow": pd.DataFrame(),
                "concept_fund_flow": pd.DataFrame(),
                "northbound_industry": {"frame": pd.DataFrame()},
                "northbound_concept": {"frame": pd.DataFrame()},
                "hot_rank": pd.DataFrame(),
            },
        },
        {},
    )
    northbound_factor = next(f for f in dimension["factors"] if f["name"] == "北向/南向")
    assert northbound_factor["display_score"] == "信息项"
    assert northbound_factor["awarded"] == 0
    assert "全市场方向代理" in northbound_factor["detail"]


def test_fundamental_dimension_etf_missing_flow_is_conservative_penalty(monkeypatch):
    monkeypatch.setattr(ValuationCollector, "get_cn_index_snapshot", lambda self, keywords: None)  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_index_value_history", lambda self, index_code: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        ValuationCollector,
        "get_weighted_stock_financial_proxies",
        lambda self, holdings, **kwargs: {  # noqa: ARG005
            "revenue_yoy": 18.0,
            "profit_yoy": 20.0,
            "roe": 12.0,
            "gross_margin": 30.0,
            "coverage_weight": 42.0,
            "report_date": "2025-12-31",
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
            "industry_fund_flow": pd.DataFrame(),
            "concept_fund_flow": pd.DataFrame(),
            "northbound_industry": {"frame": pd.DataFrame()},
            "northbound_concept": {"frame": pd.DataFrame()},
            "hot_rank": pd.DataFrame(),
        },
    )
    dimension = _fundamental_dimension(
        "512480",
        "cn_etf",
        {"name": "半导体ETF", "sector": "科技", "chain_nodes": ["半导体"]},
        {"price_percentile_1y": 0.75},
        {},
        {
            "overview": {"基金简称": "半导体ETF", "业绩比较基准": "中证全指半导体产品与设备指数收益率"},
            "top_holdings": [{"股票代码": "688256", "股票名称": "寒武纪", "占净值比例": 9.0}],
            "industry_allocation": [{"行业类别": "信息技术", "占净值比例": 95.0}],
        },
    )
    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert factors["资金承接"]["awarded"] == 0
    assert factors["资金承接"]["display_score"] == "观察"
    assert "缺失" in factors["资金承接"]["signal"]


def test_fundamental_dimension_for_etf_keeps_price_position_and_flow_as_info_only_when_valuation_missing(monkeypatch):
    monkeypatch.setattr(ValuationCollector, "get_cn_index_snapshot", lambda self, keywords: None)  # noqa: ARG005
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
                [{"名称": "通信设备", "今日主力净流入-净额": -150_000_000, "今日主力净流入-净占比": -1.8}]
            ),
            "concept_fund_flow": pd.DataFrame(),
        },
    )

    dimension = _fundamental_dimension(
        "515880",
        "cn_etf",
        {
            "name": "通信ETF",
            "sector": "通信",
            "chain_nodes": ["CPO", "光模块", "通信设备"],
            "etf_share_change": 2.8,
        },
        {"price_percentile_1y": 1.0},
        {},
        {
            "overview": {
                "基金简称": "通信ETF",
                "业绩比较基准": "中证全指通信设备指数收益率",
                "管理费率": "0.50%",
            },
            "style": {
                "profile_mode": "full",
                "tags": ["被动跟踪"],
                "sector": "通信",
                "benchmark_note": "中证全指通信设备指数",
                "top5_concentration": 46.0,
            },
            "fund_factor_snapshot": {
                "trend_label": "修复中",
                "momentum_label": "动能改善",
                "latest_date": "2026-04-21",
            },
        },
    )

    factors = {factor["name"]: factor for factor in dimension["factors"]}
    assert factors["估值代理分位"]["awarded"] == 0
    assert factors["估值代理分位"]["display_score"] == "观察"
    assert factors["资金承接"]["awarded"] == 0
    assert factors["资金承接"]["display_score"] == "观察"
    assert dimension["score"] >= 35


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
                "published_at": "2026-03-17",
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
    assert signals["政策催化"] == "产品级直接政策情报偏弱"
    assert signals["龙头公告/业绩"] == "产品级直接业绩/公告情报偏弱"
    assert not any(str(keyword).lower() in {"ai", "算力", "人工智能"} for keyword in captured_keywords)


def test_catalyst_dimension_for_cn_etf_scores_directional_catalyst_from_holdings():
    fund_profile = {
        "overview": {
            "基金简称": "人工智能ETF",
            "业绩比较基准": "中证人工智能主题指数收益率",
        },
        "top_holdings": [
            {"股票代码": "300308", "股票名称": "中际旭创", "占净值比例": 8.85},
            {"股票代码": "002463", "股票名称": "沪电股份", "占净值比例": 8.84},
        ],
        "industry_allocation": [{"行业类别": "科技", "占净值比例": 88.0}],
    }
    context = {
        "config": {},
        "news_report": {
            "mode": "live",
            "all_items": [
                {"title": "【早报】AI 龙虾概念再起，地方支持政策引关注", "category": "ai", "source": "财联社"},
                {"title": "人工智能ETF跟踪指数核心成分中际旭创上修光模块出货指引，AI 光互连景气延续", "category": "china_market_domestic", "source": "证券时报"},
            ],
        },
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "159819", "name": "人工智能ETF", "asset_type": "cn_etf", "sector": "科技", "chain_nodes": ["AI算力", "光模块"]},
        context,
        fund_profile,
    )
    directional_factor = next(f for f in dimension["factors"] if f["name"] == "产品/跟踪方向催化")
    assert directional_factor["factor_id"] == "j5_directional_catalyst"
    assert directional_factor["awarded"] > 0
    assert "中际旭创" in directional_factor["signal"]
    assert "龙虾" not in directional_factor["signal"]


def test_catalyst_dimension_for_cn_etf_does_not_score_peer_etf_or_holding_only_headlines():
    fund_profile = {
        "overview": {
            "基金简称": "国泰中证半导体材料设备主题ETF",
            "业绩比较基准": "中证半导体材料设备主题指数收益率",
        },
        "top_holdings": [
            {"股票代码": "002371", "股票名称": "北方华创", "占净值比例": 13.0},
            {"股票代码": "688012", "股票名称": "中微公司", "占净值比例": 14.9},
        ],
        "industry_allocation": [{"行业类别": "半导体", "占净值比例": 88.0}],
    }
    context = {
        "config": {},
        "news_report": {
            "mode": "live",
            "all_items": [
                {
                    "title": "半导体设备ETF易方达（159558）交易时段获1800万份净申购",
                    "category": "topic_search",
                    "source": "财联社",
                },
                {
                    "title": "北方华创股价连续3天上涨，嘉实基金旗下11只基金合计持有浮盈扩大",
                    "category": "china_market_domestic",
                    "source": "新浪财经",
                },
            ],
        },
        "events": [],
    }

    dimension = _catalyst_dimension(
        {
            "symbol": "159516",
            "name": "国泰中证半导体材料设备主题ETF",
            "asset_type": "cn_etf",
            "sector": "科技",
            "chain_nodes": ["半导体"],
        },
        context,
        fund_profile,
    )

    directional_factor = next(f for f in dimension["factors"] if f["name"] == "产品/跟踪方向催化")
    density_factor = next(f for f in dimension["factors"] if f["name"] == "研报/新闻密度")
    heat_factor = next(f for f in dimension["factors"] if f["name"] == "新闻热度")
    assert directional_factor["awarded"] == 0
    assert density_factor["awarded"] == 0
    assert heat_factor["awarded"] == 0
    assert dimension["score"] == 0
    assert not list(dimension.get("evidence") or [])
    assert dimension["coverage"]["directional_catalyst_hit"] is False
    assert dimension["coverage"]["direct_news_count"] == 0


def test_catalyst_dimension_for_cn_etf_uses_recent_theme_catalyst_window() -> None:
    context = {
        "config": {},
        "news_report": {
            "mode": "proxy",
            "all_items": [
                {
                    "title": "医药赛道局部活跃，港股通创新药ETF标的指数逆势走强",
                    "category": "topic_search",
                    "source": "财联社",
                    "configured_source": "财联社",
                    "published_at": "2026-04-02",
                    "link": "https://example.com/biotech-etf",
                },
                {
                    "title": "创新药企海外授权合作持续活跃，港股创新药方向继续受益",
                    "category": "topic_search",
                    "source": "证券时报",
                    "configured_source": "证券时报",
                    "published_at": "2026-04-01",
                    "link": "https://example.com/license-out",
                },
            ],
        },
        "as_of": "2026-04-06",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {
            "symbol": "513120",
            "name": "港股创新药ETF",
            "asset_type": "cn_etf",
            "sector": "医药",
            "chain_nodes": ["创新药", "医药"],
        },
        context,
        {"overview": {"基金简称": "港股创新药ETF"}},
    )
    coverage = dict(dimension["coverage"] or {})
    directional_factor = next(f for f in dimension["factors"] if f["name"] == "产品/跟踪方向催化")
    density_factor = next(f for f in dimension["factors"] if f["name"] == "研报/新闻密度")
    heat_factor = next(f for f in dimension["factors"] if f["name"] == "新闻热度")

    assert coverage["fresh_news_pool_count"] == 0
    assert coverage["recent_theme_news_pool_count"] >= 2
    assert directional_factor["awarded"] > 0
    assert density_factor["awarded"] > 0
    assert heat_factor["awarded"] > 0
    assert "覆盖源" in heat_factor["signal"]
    assert "近 7 日主题/跟踪方向情报仍在延续" in dimension["summary"]


def test_catalyst_dimension_for_cn_etf_falls_back_to_full_theme_pool_when_recent_items_are_noise() -> None:
    context = {
        "config": {},
        "news_report": {
            "mode": "proxy",
            "all_items": [
                {
                    "title": "14只ETF融资余额上月减少超亿元",
                    "category": "china_market_domestic",
                    "source": "证券时报",
                    "published_at": "2026-04-01",
                },
                {
                    "title": "两市ETF融券余额环比增加1.82亿元",
                    "category": "china_market_domestic",
                    "source": "证券时报",
                    "published_at": "2026-03-31",
                },
                {
                    "title": "行业ETF风向标丨港股创新药ETF广发（513120）半日成交66亿元",
                    "category": "topic_search",
                    "source": "每日经济新闻",
                    "published_at": "2026-04-02",
                },
                {
                    "title": "港股创新药ETF批量涨超7%，创新药方向迎来密集催化",
                    "category": "topic_search",
                    "source": "界面新闻",
                    "published_at": "2026-04-01",
                },
            ],
        },
        "as_of": "2026-04-06",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {
            "symbol": "513120",
            "name": "港股创新药ETF",
            "asset_type": "cn_etf",
            "sector": "医药",
            "chain_nodes": ["创新药", "港股医药"],
        },
        context,
        {"overview": {"基金简称": "港股创新药ETF", "业绩比较基准": "中证香港创新药指数"}},
    )
    directional_factor = next(f for f in dimension["factors"] if f["name"] == "产品/跟踪方向催化")
    assert directional_factor["awarded"] > 0
    assert "创新药" in directional_factor["signal"]


def test_catalyst_dimension_for_cn_etf_promotes_sector_live_news_to_directional_catalyst() -> None:
    context = {
        "config": {},
        "news_report": {
            "mode": "live",
            "all_items": [
                {
                    "title": "国务院重磅部署！事关AI、算力、6G、卫星互联网等",
                    "category": "topic_search",
                    "source": "财联社",
                    "published_at": "2026-04-22",
                },
                {
                    "title": "午评：创业板指涨0.63%，CPO概念大涨",
                    "category": "topic_search",
                    "source": "证券时报",
                    "published_at": "2026-04-22",
                },
            ],
        },
        "as_of": "2026-04-22",
        "events": [],
    }

    dimension = _catalyst_dimension(
        {
            "symbol": "515880",
            "name": "通信ETF",
            "asset_type": "cn_etf",
            "sector": "通信",
            "benchmark": "中证全指通信设备指数",
            "chain_nodes": ["CPO", "光模块", "通信设备", "AI算力"],
        },
        context,
        {"overview": {"基金简称": "通信ETF", "业绩比较基准": "中证全指通信设备指数收益率"}},
    )

    directional_factor = next(f for f in dimension["factors"] if f["name"] == "产品/跟踪方向催化")

    assert directional_factor["awarded"] > 0
    assert not any(f["name"] == "主题级背景催化" for f in dimension["factors"])
    assert dimension["coverage"]["theme_news_count"] >= 1
    assert dimension["coverage"]["theme_background_support"] is False
    assert dimension["coverage"]["directional_catalyst_hit"] is True
    assert dimension["coverage"]["direct_news_count"] >= 1
    assert dimension["coverage"]["fresh_direct_news_count"] >= 1
    assert "背景催化不是空白" in dimension["summary"]
    assert "还缺直接、强、可执行" in dimension["summary"]


def test_catalyst_search_groups_prioritize_chain_nodes_for_theme_etf() -> None:
    groups = _catalyst_search_groups(
        {
            "name": "广发中证香港创新药(QDII-ETF)",
            "sector": "医药",
            "chain_nodes": ["创新药", "港股医药", "FDA"],
        },
        {
            "profile_name": "医药基金",
            "keywords": ["医药", "创新药", "中证香港创新药指数"],
            "search_terms": ["中证香港创新药指数", "医药", "biotech"],
        },
    )
    assert groups[0] == ["广发中证香港创新药(QDII-ETF)"]
    assert groups[1] == ["广发中证香港创新药(QDII-ETF)", "医药"]
    assert groups[2] == ["创新药", "港股医药"]


def test_relative_strength_for_cross_border_etf_does_not_use_domestic_sector_penalty() -> None:
    asset_returns = pd.Series([0.01] * 30)
    context = {
        "benchmark_returns": {"cn_etf": pd.Series([0.002] * 30)},
        "drivers": {
            "industry_spot": pd.DataFrame(
                [{"板块名称": "医疗器械", "涨跌幅": -2.36}]
            )
        },
        "fund_profile": {
            "style": {
                "benchmark_note": "中证香港创新药指数",
                "taxonomy": {"exposure_scope": "跨境"},
            }
        },
        "day_theme": {},
        "regime": {},
        "global_proxy": {},
    }
    dimension = _relative_strength_dimension(
        "513120",
        "cn_etf",
        {
            "symbol": "513120",
            "name": "港股创新药ETF",
            "asset_type": "cn_etf",
            "sector": "医药",
            "chain_nodes": ["创新药", "港股医药"],
        },
        {"return_5d": 0.08, "return_20d": 0.12},
        asset_returns,
        context,
    )
    board_factor = next(f for f in dimension["factors"] if f["name"] == "板块扩散")
    breadth_factor = next(f for f in dimension["factors"] if f["name"] == "行业宽度")
    leader_factor = next(f for f in dimension["factors"] if f["name"] == "龙头确认")
    assert board_factor["awarded"] == 0
    assert board_factor["display_score"] == "观察提示"
    assert "跨境/海外底层" in board_factor["signal"]
    assert breadth_factor["display_score"] == "观察提示"
    assert leader_factor["display_score"] == "观察提示"


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


def test_hard_checks_treat_watchlist_correlation_as_warning_only() -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=120, freq="B"),
            "open": [1.0] * 120,
            "high": [1.02] * 120,
            "low": [0.98] * 120,
            "close": [1.0 + 0.002 * i for i in range(120)],
            "volume": [5_000_000] * 120,
            "amount": [800_000_000.0] * 120,
        }
    )
    metrics = {"avg_turnover_20d": 8.0e8, "price_percentile_1y": 0.55, "return_5d": 0.02}
    technical = {"rsi": {"RSI": 58.0}}
    context = {"config": {"opportunity": {}}}
    fundamental_dimension = {"valuation_snapshot": {}, "valuation_extreme": False}

    checks, exclusion_reasons, warnings = _hard_checks(
        "cn_etf",
        {"symbol": "588200", "name": "科创芯片ETF"},
        history,
        metrics,
        technical,
        context,
        12,
        ("512480", 0.95),
        fundamental_dimension,
        None,
    )

    check_map = {item["name"]: item for item in checks}
    assert check_map["相关性"]["status"] == "❌"
    assert exclusion_reasons == []
    assert any("相关性过高" in item for item in warnings)


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


def test_hard_checks_for_cn_stock_use_regulatory_risk_snapshot(monkeypatch) -> None:
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
            "status": "✅",
            "detail": "当前未见明显股权质押风险",
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "❌",
            "detail": "当前仍处于 ST 风险警示板，直接抬高退市与交易约束风险。",
            "active_st": True,
            "high_shock_count": 1,
            "alert_count": 1,
            "active_alert_count": 1,
            "components": {
                "stock_st": {"status": "❌", "detail": "2026-03-12 仍在 风险警示板 名单内"},
                "st": {"status": "❌", "detail": "2026-03-04 最近一次 ST 变更为 `ST`"},
                "stk_high_shock": {"status": "⚠️", "detail": "2026-03-10 命中严重异常波动"},
                "stk_alert": {"status": "⚠️", "detail": "2026-03-04 起被列入交易所重点提示证券"},
            },
        },
    )

    checks, exclusion_reasons, warnings = _hard_checks(
        "cn_stock",
        {"symbol": "000711", "name": "ST京蓝"},
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
    assert check_map["ST 风险"]["status"] == "❌"
    assert check_map["严重异常波动"]["status"] == "⚠️"
    assert check_map["交易所重点提示"]["status"] == "⚠️"
    assert "ST / *ST 股票，退市风险较高" in exclusion_reasons
    assert any("严重异常波动" in item for item in warnings)
    assert any("重点提示证券" in item for item in warnings)


def test_hard_checks_soften_theme_proxy_extreme_valuation_for_etf() -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=120, freq="B"),
            "open": [1.5] * 120,
            "high": [1.6] * 120,
            "low": [1.4] * 120,
            "close": [1.5 + 0.001 * i for i in range(120)],
            "volume": [10_000_000] * 120,
            "amount": [1_200_000_000.0] * 120,
        }
    )
    metrics = {"avg_turnover_20d": 1.2e9, "price_percentile_1y": 0.75, "return_5d": -0.03}
    technical = {"rsi": {"RSI": 42.0}}
    context = {"config": {"opportunity": {}}}
    fundamental_dimension = {
        "valuation_snapshot": {
            "index_name": "国证芯片",
            "pe_ttm": 92.4,
            "metric_label": "滚动PE",
            "display_label": "指数估值代理",
            "match_quality": "theme_proxy",
        },
        "valuation_extreme": True,
    }

    checks, exclusion_reasons, warnings = _hard_checks(
        "cn_etf",
        {"symbol": "512480", "name": "半导体ETF"},
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
    assert check_map["估值极端"]["status"] == "⚠️"
    assert "辅助约束" in "".join(warnings)
    assert not any("极高区间" in item for item in exclusion_reasons)


def test_hard_checks_softens_macro_reverse_and_price_percentile_proxy_for_cn_stock() -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=120, freq="B"),
            "open": [10.0] * 120,
            "high": [10.5] * 120,
            "low": [9.8] * 120,
            "close": [10.0 + 0.01 * i for i in range(120)],
            "volume": [8_000_000] * 120,
            "amount": [1_000_000_000.0] * 120,
        }
    )
    metrics = {"avg_turnover_20d": 1.0e9, "price_percentile_1y": 0.92, "return_5d": 0.01}
    technical = {"rsi": {"RSI": 64.0}}
    context = {"config": {"opportunity": {}}}
    fundamental_dimension = {"valuation_snapshot": {}, "valuation_extreme": False}

    checks, exclusion_reasons, warnings = _hard_checks(
        "cn_stock",
        {"symbol": "600519", "name": "贵州茅台"},
        history,
        metrics,
        technical,
        context,
        0,
        None,
        fundamental_dimension,
        None,
    )

    check_map = {item["name"]: item for item in checks}
    assert check_map["估值极端"]["status"] == "⚠️"
    assert check_map["宏观顺逆风"]["status"] == "⚠️"
    assert "价格位置代理已处于极端高位" not in exclusion_reasons
    assert "宏观敏感度完全逆风" not in exclusion_reasons
    assert any("不再额外做硬排除" in item for item in warnings)


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


def test_relative_strength_dimension_uses_dc_index_breadth_when_primary_spot_lacks_counts() -> None:
    dates = pd.date_range("2025-01-01", periods=30, freq="B")
    asset_returns = pd.Series([0.01] * 30, index=dates)
    benchmark_returns = pd.Series([0.002] * 30, index=dates)
    context = {
        "benchmark_returns": {"cn_stock": benchmark_returns},
        "drivers": {
            "industry_spot": pd.DataFrame(
                [
                    {
                        "板块名称": "半导体",
                        "涨跌幅": 1.45,
                    }
                ]
            ),
            "concept_spot": pd.DataFrame(),
            "dc_index": {
                "frame": pd.DataFrame(
                    [
                        {
                            "板块名称": "集成电路封测",
                            "名称": "集成电路封测",
                            "涨跌幅": 1.87,
                            "上涨家数": 13,
                            "下跌家数": 0,
                            "领涨股票": "汇成股份",
                            "领涨涨跌幅": 7.04,
                            "日期": "2026-04-16",
                        }
                    ]
                )
            },
        },
        "day_theme": {"label": "硬科技"},
        "regime": {},
        "global_proxy": {},
    }
    metadata = {"name": "长电科技", "sector": "半导体", "chain_nodes": ["集成电路封测"]}
    metrics = {"return_5d": 0.06, "return_20d": 0.09}

    dimension = _relative_strength_dimension("600584", "cn_stock", metadata, metrics, asset_returns, context)
    factor_map = {factor["name"]: factor for factor in dimension["factors"]}

    assert dimension["proxy_only"] is False
    assert factor_map["行业宽度"]["signal"] == "行业上涨家数比例 100%"
    assert "汇成股份 +7.04%" in factor_map["行业宽度"]["detail"]
    assert factor_map["龙头确认"]["signal"] == "龙头方向与板块一致，扩散结构健康"


def test_j3_ah_comparison_snapshot_uses_hk_code_and_ratio_to_premium(monkeypatch) -> None:
    from src.processors.opportunity_engine import _cn_stock_ah_comparison_snapshot

    calls: list[dict[str, str]] = []

    def fake_get(self, **kwargs):  # noqa: ANN001
        calls.append(dict(kwargs))
        return pd.DataFrame(
            [
                {
                    "trade_date": "2026-04-03",
                    "a_name": "腾讯控股",
                    "hk_name": "腾讯控股",
                    "comparison_ratio": 1.25,
                }
            ]
        )

    monkeypatch.setattr(ChinaMarketCollector, "get_stk_ah_comparison", fake_get)

    snapshot = _cn_stock_ah_comparison_snapshot(
        {"asset_type": "hk_index", "symbol": "00700", "name": "腾讯控股"},
        {"config": {}, "as_of": "2026-04-03"},
    )

    assert calls and calls[0]["hk_code"] == "00700"
    assert snapshot["status"] == "matched"
    assert snapshot["diagnosis"] == "live"
    assert snapshot["is_fresh"] is True
    assert snapshot["premium_rate"] == pytest.approx(25.0)
    assert snapshot["source"] == "tushare.stk_ah_comparison"


def test_j3_relative_strength_dimension_surfaces_ah_comparison_factor_for_cn_stock(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_ah_comparison_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "matched",
            "diagnosis": "live",
            "is_fresh": True,
            "latest_date": "2026-04-03",
            "premium_rate": 18.0,
            "detail": "贵州茅台 A/H 比价快照；溢价/折价约 +18.00%",
            "disclosure": "Tushare stk_ah_comparison 提供 AH 股比价快照；空表或受限时按缺失处理，不伪装成 fresh。",
            "source": "tushare.stk_ah_comparison",
        },
    )

    dates = pd.date_range("2025-01-01", periods=30, freq="B")
    asset_returns = pd.Series([0.01] * 30, index=dates)
    benchmark_returns = pd.Series([0.002] * 30, index=dates)
    context = {
        "benchmark_returns": {"cn_stock": benchmark_returns},
        "drivers": {},
        "day_theme": {"label": "消费"},
        "regime": {},
        "global_proxy": {},
        "as_of": "2026-04-03",
    }
    metadata = {"name": "贵州茅台", "sector": "消费", "chain_nodes": []}
    metrics = {"return_5d": 0.08, "return_20d": 0.12}

    dimension = _relative_strength_dimension("600519", "cn_stock", metadata, metrics, asset_returns, context)
    factor_map = {factor["name"]: factor for factor in dimension["factors"]}

    ah_factor = next(factor for factor in dimension["factors"] if factor.get("factor_id") == "j3_ah_comparison")
    assert ah_factor["display_score"] != "观察提示"
    assert "A/H 比价" in ah_factor["signal"]


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

    assert factor_map["机构集中度代理"]["display_score"] == "不适用"
    assert factor_map["机构集中度代理"]["signal"] == "个股主链不适用"
    assert "不再用指数成分股集中度代理个股判断" in factor_map["机构集中度代理"]["detail"]


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

    assert factor_map["机构集中度代理"]["display_score"] == "不适用"
    assert factor_map["机构集中度代理"]["signal"] == "个股主链不适用"


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

    assert factor_map["机构集中度代理"]["display_score"] == "不适用"
    assert factor_map["机构集中度代理"]["signal"] == "个股主链不适用"


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


def test_relative_strength_dimension_marks_proxy_only_when_breadth_counts_missing() -> None:
    dates = pd.date_range("2025-01-01", periods=30, freq="B")
    asset_returns = pd.Series([0.01] * 30, index=dates)
    benchmark_returns = pd.Series([0.002] * 30, index=dates)
    context = {
        "benchmark_returns": {"cn_etf": benchmark_returns},
        "drivers": {
            "industry_spot": pd.DataFrame(
                [
                    {
                        "板块名称": "中证500",
                        "涨跌幅": 0.6,
                    }
                ]
            ),
            "concept_spot": pd.DataFrame(),
        },
        "day_theme": {},
        "regime": {},
        "global_proxy": {},
    }
    metadata = {"name": "A500ETF华泰柏瑞", "sector": "中证500", "chain_nodes": ["宽基"]}
    metrics = {"return_5d": 0.02, "return_20d": -0.01}

    dimension = _relative_strength_dimension("563360", "cn_etf", metadata, metrics, asset_returns, context)

    assert dimension["proxy_only"] is True
    assert "行业宽度/龙头确认仍缺失" in dimension["summary"]


def test_relative_strength_dimension_uses_exact_sector_profile_leaders_for_non_generic_bucket() -> None:
    dates = pd.date_range("2025-01-01", periods=30, freq="B")
    asset_returns = pd.Series([0.01] * 30, index=dates)
    benchmark_returns = pd.Series([0.002] * 30, index=dates)
    context = {
        "benchmark_returns": {"cn_stock": benchmark_returns},
        "drivers": {
            "industry_spot": pd.DataFrame(
                [
                    {
                        "板块名称": "铝",
                        "上涨家数": 21,
                        "下跌家数": 8,
                        "涨跌幅": 2.4,
                    }
                ]
            ),
            "concept_spot": pd.DataFrame(),
        },
        "day_theme": {"label": "有色修复"},
        "regime": {},
        "global_proxy": {},
    }
    metadata = {"name": "中国铝业", "sector": "有色", "chain_nodes": ["铜铝", "顺周期"]}
    metrics = {"return_5d": 0.08, "return_20d": 0.12}

    dimension = _relative_strength_dimension("601600", "cn_stock", metadata, metrics, asset_returns, context)
    factor_map = {factor["name"]: factor for factor in dimension["factors"]}

    assert "紫金矿业/江西铜业" in factor_map["行业宽度"]["detail"]
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
    assert ann_factor["awarded"] == 20
    # Generic cn_stock sector profiles should rebalance maxima instead of falling back to 30.
    policy_factor = next(f for f in dimension["factors"] if f["name"] == "政策催化")
    assert policy_factor["display_score"].endswith("/15")


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
    context = {"config": {}, "news_report": {"all_items": [], "mode": "proxy"}, "events": [], "now": "2026-03-12"}
    dimension = _catalyst_dimension(
        {"symbol": "601138", "name": "工业富联", "asset_type": "cn_stock", "sector": "科技", "chain_nodes": ["AI算力"]},
        context,
    )
    structured_factor = next(f for f in dimension["factors"] if f["name"] == "结构化事件")
    assert "披露股份回购" in structured_factor["signal"] or "披露现金分红" in structured_factor["signal"]
    assert any(item["source"] in {"Tushare repurchase", "Tushare dividend"} for item in dimension["evidence"])


def test_catalyst_dimension_dividend_formats_tushare_per_share_fields_as_per_ten(monkeypatch):
    monkeypatch.setattr(NewsCollector, "get_stock_news", lambda self, symbol, limit=10: [])  # noqa: ARG005
    monkeypatch.setattr(NewsCollector, "search_by_keywords", lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_disclosure_dates", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_holder_trades", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_repurchase", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_dividend",
        lambda self, symbol: [  # noqa: ARG005
            {"ann_date": "2026-04-24", "div_proc": "预案", "cash_div_tax": 1.0, "stk_co_rate": 0.4},
        ],
    )
    context = {"config": {}, "news_report": {"all_items": [], "mode": "live"}, "events": [], "now": "2026-04-24"}

    dimension = _catalyst_dimension(
        {"symbol": "300502", "name": "新易盛", "asset_type": "cn_stock", "sector": "科技", "chain_nodes": ["AI算力"]},
        context,
    )

    structured_factor = next(f for f in dimension["factors"] if f["name"] == "结构化事件")
    assert "每10股派现 10.00 元" in structured_factor["signal"]
    assert "每10股转增 4.00 股" in structured_factor["signal"]


def test_catalyst_dimension_rebalances_forward_event_weight_by_sector(monkeypatch):
    monkeypatch.setattr(NewsCollector, "get_stock_news", lambda self, symbol, limit=10: [])  # noqa: ARG005
    monkeypatch.setattr(NewsCollector, "search_by_keywords", lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [])  # noqa: ARG005
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_disclosure_dates",
        lambda self, symbol: [  # noqa: ARG005
            {"end_date": "20251231", "pre_date": "2026-03-15", "actual_date": ""},
        ],
    )
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_holder_trades", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_repurchase", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_dividend", lambda self, symbol: [])  # noqa: ARG005
    context = {"config": {}, "news_report": {"all_items": [], "mode": "live"}, "events": [], "now": "2026-03-12"}

    medicine = _catalyst_dimension(
        {"symbol": "603259", "name": "药明康德", "asset_type": "cn_stock", "sector": "医药", "chain_nodes": ["CRO"]},
        context,
    )
    tech = _catalyst_dimension(
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技", "chain_nodes": ["AI算力"]},
        context,
    )

    medicine_forward = next(f for f in medicine["factors"] if f["name"] == "前瞻催化")
    medicine_structured = next(f for f in medicine["factors"] if f["name"] == "结构化事件")
    tech_forward = next(f for f in tech["factors"] if f["name"] == "前瞻催化")
    tech_structured = next(f for f in tech["factors"] if f["name"] == "结构化事件")

    assert medicine_forward["display_score"].endswith("/15")
    assert medicine_structured["display_score"].endswith("/20")
    assert tech_forward["display_score"].endswith("/5")
    assert medicine_forward["awarded"] > tech_forward["awarded"]
    assert medicine_structured["awarded"] >= tech_structured["awarded"]


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


def test_catalyst_dimension_cn_stock_irm_qa_is_treated_as_supporting_structured_signal(monkeypatch):
    monkeypatch.setattr(
        NewsCollector,
        "get_stock_news",
        lambda self, symbol, limit=10: [  # noqa: ARG005
            {
                "category": "stock_structured_intelligence",
                "title": "汇洲智能互动平台问答：公司是否和幻方量化有合作；回复称经核查无合作关系…",
                "source": "Tushare",
                "configured_source": "Tushare::irm_qa_sz",
                "source_note": "structured_disclosure",
                "note": "投资者关系/路演纪要",
                "published_at": "2026-03-10T15:02:17",
                "link": "https://irm.cninfo.com.cn/",
            }
        ],
    )
    monkeypatch.setattr(NewsCollector, "search_by_keywords", lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_disclosure_dates", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_holder_trades", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_repurchase", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_dividend", lambda self, symbol: [])  # noqa: ARG005
    context = {"config": {}, "news_report": {"all_items": [], "mode": "live"}, "events": [], "now": "2026-03-12"}

    dimension = _catalyst_dimension(
        {"symbol": "002122", "name": "汇洲智能", "asset_type": "cn_stock", "sector": "科技", "chain_nodes": ["算力"]},
        context,
    )

    structured_factor = next(f for f in dimension["factors"] if f["name"] == "结构化事件")
    assert "互动平台问答" in structured_factor["signal"]
    assert structured_factor["display_score"] == "8/15"
    assert "补充证据处理" in structured_factor["detail"]
    assert any("互动平台问答" in str(item.get("title", "")) for item in dimension["evidence"])


def test_catalyst_dimension_theme_profile_can_override_factor_maxima(monkeypatch):
    monkeypatch.setattr(NewsCollector, "get_stock_news", lambda self, symbol, limit=10: [])  # noqa: ARG005
    monkeypatch.setattr(NewsCollector, "search_by_keywords", lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [])  # noqa: ARG005
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_disclosure_dates",
        lambda self, symbol: [  # noqa: ARG005
            {"end_date": "20251231", "pre_date": "2026-03-15", "actual_date": ""},
        ],
    )
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_holder_trades", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_repurchase", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_dividend", lambda self, symbol: [])  # noqa: ARG005
    context = {"config": {}, "news_report": {"all_items": [], "mode": "live"}, "events": [], "now": "2026-03-12"}

    dimension = _catalyst_dimension(
        {"symbol": "600406", "name": "国电南瑞", "asset_type": "cn_stock", "sector": "电网", "chain_nodes": ["电网设备"]},
        context,
    )

    forward_factor = next(f for f in dimension["factors"] if f["name"] == "前瞻催化")
    overseas_factor = next(f for f in dimension["factors"] if f["name"] == "海外映射")
    assert forward_factor["display_score"].endswith("/15")
    assert overseas_factor["display_score"].endswith("/5")


def test_catalyst_dimension_cn_stock_holdertrade_decrease_is_not_positive_structured_event(monkeypatch):
    monkeypatch.setattr(NewsCollector, "get_stock_news", lambda self, symbol, limit=10: [])  # noqa: ARG005
    monkeypatch.setattr(NewsCollector, "search_by_keywords", lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_disclosure_dates", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_holder_trades",
        lambda self, symbol: [  # noqa: ARG005
            {"ann_date": "2026-03-10", "in_de": "DE", "change_ratio": 1.36},
        ],
    )
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_repurchase", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_dividend", lambda self, symbol: [])  # noqa: ARG005
    context = {"config": {}, "news_report": {"all_items": [], "mode": "live"}, "events": [], "now": "2026-03-12"}
    dimension = _catalyst_dimension(
        {"symbol": "603259", "name": "药明康德", "asset_type": "cn_stock", "sector": "医药", "chain_nodes": ["CRO"]},
        context,
    )
    structured_factor = next(f for f in dimension["factors"] if f["name"] == "结构化事件")
    assert structured_factor["awarded"] == 0
    assert all(item["source"] != "Tushare stk_holdertrade" for item in dimension["evidence"])


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


def test_cn_holdertrade_snapshot_reuses_runtime_cache(monkeypatch):
    calls = {"count": 0}

    def _fake_holdertrade(self, symbol):  # noqa: ARG001
        calls["count"] += 1
        return [{"ann_date": "2026-03-05", "in_de": "IN", "change_ratio": 0.18}]

    monkeypatch.setattr(ValuationCollector, "get_cn_stock_holder_trades", _fake_holdertrade)

    metadata = {"symbol": "300502", "name": "新易盛", "asset_type": "cn_stock", "sector": "科技"}
    context = {"config": {}}

    first = _cn_holdertrade_snapshot(metadata, context)
    second = _cn_holdertrade_snapshot(metadata, context)

    assert first["direction"] == "increase"
    assert second["direction"] == "increase"
    assert calls["count"] == 1


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
    assert "相关行业/概念代理" in northbound_factor["signal"]
    assert "不等于单一个股出现了明确北向增持" in northbound_factor["detail"]


def test_chips_dimension_cn_stock_softens_proxy_wording_for_narrow_sector_match(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_holder_trades",
        lambda self, symbol: [],  # noqa: ARG005
    )
    dimension = _chips_dimension(
        "600313",
        "cn_stock",
        {"symbol": "600313", "name": "农发种业", "asset_type": "cn_stock", "sector": "农业"},
        {
            "config": {},
            "drivers": {
                "industry_spot": pd.DataFrame(),
                "concept_spot": pd.DataFrame(),
                "northbound_industry": {"frame": pd.DataFrame()},
                "northbound_concept": {"frame": pd.DataFrame()},
                "industry_fund_flow": pd.DataFrame([{"名称": "饲料", "主力净流入-净额": -138918000.0}]),
                "concept_fund_flow": pd.DataFrame(),
            },
        },
        {},
    )

    chips_factor = next(f for f in dimension["factors"] if f["name"] == "机构资金承接")
    assert "农业（当前命中 饲料）" in chips_factor["signal"]
    assert "低置信代理" in chips_factor["detail"]
    assert "行业/市场级信号" in dimension["summary"]


def test_chips_dimension_cn_stock_does_not_match_generic_consumer_to_consumer_electronics(monkeypatch):
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_holder_trades",
        lambda self, symbol: [],  # noqa: ARG005
    )
    dimension = _chips_dimension(
        "600809",
        "cn_stock",
        {"symbol": "600809", "name": "山西汾酒", "asset_type": "cn_stock", "sector": "消费"},
        {
            "config": {},
            "drivers": {
                "northbound_industry": {
                    "frame": pd.DataFrame(
                        [
                            {"名称": "消费电子", "今日增持估计市值": 1.77},
                            {"名称": "旅游零售Ⅲ", "今日增持估计市值": -3.01},
                        ]
                    ),
                },
                "northbound_concept": {"frame": pd.DataFrame()},
            },
        },
        {},
    )
    northbound_factor = next(f for f in dimension["factors"] if f["name"] == "北向/南向")
    assert "消费电子" not in northbound_factor["signal"]
    assert "旅游零售Ⅲ" not in northbound_factor["signal"]
    assert "缺失" in northbound_factor["signal"] or "北向数据缺失" in northbound_factor["signal"]


def test_semiconductor_board_matching_prefers_semiconductor_over_broad_tech_aliases():
    drivers = {
        "industry_fund_flow": pd.DataFrame(
            [
                {"名称": "通信设备", "今日主力净流入-净额": 800_000_000},
                {"名称": "半导体", "今日主力净流入-净额": 300_000_000},
            ]
        ),
        "concept_fund_flow": pd.DataFrame(),
        "northbound_industry": {
            "frame": pd.DataFrame(
                [
                    {"名称": "通信设备", "今日增持估计市值": 2.5},
                    {"名称": "半导体", "今日增持估计市值": 0.8},
                ]
            )
        },
        "northbound_concept": {"frame": pd.DataFrame()},
    }
    metadata = {"symbol": "512480", "name": "半导体ETF", "asset_type": "cn_etf", "sector": "科技", "chain_nodes": ["半导体"]}
    sector_flow = _sector_flow_snapshot(metadata, drivers)
    northbound = _northbound_sector_snapshot(metadata, drivers)
    assert sector_flow["name"] == "半导体"
    assert northbound["name"] == "半导体"


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


def test_chips_dimension_cn_stock_uses_real_chip_snapshot(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_chip_snapshot",
        lambda metadata, context, history=None: {  # noqa: ARG005
            "status": "matched",
            "is_fresh": True,
            "winner_rate_pct": 71.5,
            "weight_avg": 103.2,
            "price_vs_weight_avg_pct": 0.054,
            "above_price_pct": 21.0,
            "near_price_pct": 34.0,
            "peak_price": 104.0,
            "peak_percent": 18.5,
            "detail": "现价已经回到平均成本上方，上方套牢盘不重。",
        },
    )
    dimension = _chips_dimension(
        "300308",
        "cn_stock",
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技", "chain_nodes": ["光模块"]},
        {"config": {}, "drivers": {}},
        {},
    )

    factors = {item["name"]: item for item in dimension["factors"]}
    assert factors["筹码胜率"]["awarded"] > 0
    assert factors["平均成本位置"]["awarded"] > 0
    assert factors["套牢盘压力"]["awarded"] > 0
    assert "真实筹码分布开始配合价格" in dimension["summary"]


def test_market_event_rows_from_context_include_chip_snapshot_signal(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_drivers",
        lambda context, config: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_chip_snapshot",
        lambda metadata, context, history=None: {  # noqa: ARG005
            "status": "matched",
            "is_fresh": True,
            "winner_rate_pct": 70.2,
            "price_vs_weight_avg_pct": 0.031,
            "above_price_pct": 18.0,
            "detail": "现价已经回到平均成本上方，多数筹码进入盈利区。",
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_theme_membership_snapshot",
        lambda metadata, context: {"items": []},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {"components": {}, "active_st": False, "active_alert_count": 0, "high_shock_count": 0},  # noqa: ARG005
    )

    rows = _market_event_rows_from_context(
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技"},
        {"config": {}, "as_of": "2026-04-01"},
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "筹码确认：中际旭创 胜率约 70.2%" in joined
    assert "筹码分布专题" in joined


def test_market_event_rows_from_context_include_p1_stock_signal_rows(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_drivers",
        lambda context, config: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_industry_index_snapshot",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_index_topic_bundle",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._matched_sector_spot_row",
        lambda metadata, drivers: (None, pd.DataFrame(), ""),  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._hot_rank_snapshot",
        lambda metadata, drivers: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_chip_snapshot",
        lambda metadata, context, history=None: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_theme_membership_snapshot",
        lambda metadata, context: {"items": []},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {"components": {}, "active_st": False, "active_alert_count": 0, "high_shock_count": 0},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_capital_flow_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "matched",
            "is_fresh": True,
            "direct_main_flow": 160_000_000.0,
            "detail": "主力资金开始直接承接，不再只靠题材代理。",
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_margin_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "is_fresh": True,
            "crowding_level": "high",
            "detail": "融资余额和融资买入/偿还比同步抬升，融资盘升温明显。",
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_board_action_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "is_fresh": True,
            "has_positive_signal": True,
            "positive_bits": ["龙虎榜净买入", "竞价高开"],
            "detail": "龙虎榜净买入叠加竞价高开，微观交易结构开始配合。",
        },
    )

    rows = _market_event_rows_from_context(
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技"},
        {"config": {}, "as_of": "2026-04-01"},
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "个股资金流确认：中际旭创 当日主力净流入 1.60亿" in joined
    assert "两融拥挤提示：中际旭创 当前融资盘升温明显" in joined
    assert "打板信号确认：中际旭创 龙虎榜净买入/竞价高开" in joined


def test_chips_dimension_cn_stock_does_not_score_stale_chip_snapshot(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_chip_snapshot",
        lambda metadata, context, history=None: {  # noqa: ARG005
            "status": "matched",
            "is_fresh": False,
            "latest_date": "2026-03-20",
            "detail": "最新可用筹码日期停在 2026-03-20，当前不按 fresh 命中处理。",
        },
    )
    dimension = _chips_dimension(
        "300308",
        "cn_stock",
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技"},
        {"config": {}, "drivers": {}},
        {},
    )

    factor = next(item for item in dimension["factors"] if item["name"] == "真实筹码分布")
    assert factor["display_score"] == "观察"
    assert "非当期" in factor["signal"]
    assert all(item["name"] != "筹码胜率" for item in dimension["factors"])


def test_chips_dimension_cn_stock_prefers_direct_capital_flow_snapshot(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._hot_rank_snapshot",
        lambda metadata, context: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._sector_flow_snapshot",
        lambda metadata, context: {"name": "AI算力", "main_flow": 520_000_000.0},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._northbound_sector_snapshot",
        lambda metadata, context: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_cn_index_concentration_proxy",
        lambda keywords, symbol="", prefetched_bundle=None, context=None, config=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_chip_snapshot",
        lambda metadata, context, history=None: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_holdertrade_snapshot",
        lambda metadata, context: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_holder_concentration_snapshot",
        lambda metadata, context: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_capital_flow_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "matched",
            "is_fresh": True,
            "direct_main_flow": 160_000_000.0,
            "direct_5d_main_flow": 190_000_000.0,
            "detail": "个股级 moneyflow 已命中，当日与近 5 日主力资金都为净流入。",
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_margin_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "is_fresh": True,
            "crowding_level": "medium",
            "detail": "融资盘仍在升温，需防一致性交易。",
        },
    )

    dimension = _chips_dimension(
        "300308",
        "cn_stock",
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技", "chain_nodes": ["光模块"]},
        {"config": {}, "drivers": {}},
        {},
    )

    factors = {item["name"]: item for item in dimension["factors"]}
    assert "个股主力净流入 1.60亿 / 近 5 日累计 1.90亿" in factors["机构资金承接"]["signal"]
    assert factors["机构资金承接"]["awarded"] == 15
    assert factors["两融拥挤度"]["display_score"] == "观察提示"
    assert "融资盘仍在升温" in factors["两融拥挤度"]["signal"]


def test_chips_dimension_cn_stock_accepts_t1_direct_snapshots(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._hot_rank_snapshot",
        lambda metadata, context: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._sector_flow_snapshot",
        lambda metadata, context: {"name": "半导体", "main_flow": -404_000_000.0},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._northbound_sector_snapshot",
        lambda metadata, context: {"name": "半导体", "net_value": -321_000_000.0},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_cn_index_concentration_proxy",
        lambda keywords, symbol="", prefetched_bundle=None, context=None, config=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_chip_snapshot",
        lambda metadata, context, history=None: {  # noqa: ARG005
            "status": "matched",
            "is_fresh": False,
            "trade_gap_days": 1,
            "latest_date": "2026-04-15",
            "winner_rate_pct": 52.25,
            "weight_avg": 43.06,
            "price_vs_weight_avg_pct": 0.016,
            "above_price_pct": 47.8,
            "near_price_pct": 31.0,
            "peak_price": 44.1,
            "peak_percent": 5.95,
            "detail": "最新可用筹码日期停在 2026-04-15（上一交易日，T+1 直连）；当前不把它写成今天盘中的新增资金，但仍可用于判断平均成本和上方套牢盘。",
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_holdertrade_snapshot",
        lambda metadata, context: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_holder_concentration_snapshot",
        lambda metadata, context: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_capital_flow_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "proxy",
            "is_fresh": True,
            "latest_date": "2026-04-15",
            "direct_trade_gap_days": 1,
            "direct_main_flow": 50_494_500.0,
            "direct_5d_main_flow": 155_274_600.0,
            "board_name": "共封装光学(CPO)",
            "board_main_flow": 1_880_000.0,
            "detail": "个股主力资金最新停在 2026-04-15（上一交易日，T+1 直连）；当日先看概念代理：`共封装光学(CPO)` 主力净流入 0.02亿。",
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_margin_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "is_fresh": True,
            "crowding_level": "medium",
            "detail": "融资盘仍在升温，需防一致性交易。",
        },
    )

    dimension = _chips_dimension(
        "600584",
        "cn_stock",
        {"symbol": "600584", "name": "长电科技", "asset_type": "cn_stock", "sector": "半导体"},
        {"config": {}, "drivers": {}},
        {},
    )

    factors = {item["name"]: item for item in dimension["factors"]}
    assert "T+1 直连" in factors["机构资金承接"]["signal"]
    assert "个股主力净流入" in factors["机构资金承接"]["signal"]
    assert "T+1 直连" in factors["平均成本位置"]["signal"]
    assert "真实筹码分布偏谨慎" in dimension["summary"]


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


def test_catalyst_dimension_cn_stock_does_not_use_macro_news_as_leader_announcement(monkeypatch) -> None:
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    context = {
        "config": {},
        "news_report": {
            "mode": "live",
            "all_items": [
                {
                    "title": "【早报】事关货币政策，潘功胜发声；央行连续第16个月增持黄金 - 财联社",
                    "category": "china_macro",
                    "source": "财联社",
                    "configured_source": "财联社",
                    "must_include": False,
                    "link": "",
                }
            ],
        },
        "events": [],
    }
    dimension = _catalyst_dimension(
        {"symbol": "600313", "name": "农发种业", "asset_type": "cn_stock", "sector": "农业", "chain_nodes": ["种业"]},
        context,
    )
    leader_factor = next(f for f in dimension["factors"] if f["name"] == "龙头公告/业绩")
    assert leader_factor["awarded"] == 0
    assert leader_factor["signal"] == "直接龙头公告/业绩情报偏弱"


def test_catalyst_core_signal_hk_us_hides_weak_titles_without_high_confidence_company_news(monkeypatch):
    future_date = (pd.Timestamp.now(tz="Asia/Shanghai").normalize() + pd.Timedelta(days=1)).date().isoformat()
    monkeypatch.setattr(
        "src.processors.opportunity_engine._company_calendar_event_dates",
        lambda symbol, asset_type: [future_date],  # noqa: ARG005
    )
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keyword_groups",
        lambda self, keyword_groups, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
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
    assert future_date in dimension["core_signal"]


def test_is_high_confidence_company_news_accepts_ir_and_gov_sources():
    assert _is_high_confidence_company_news(
        {
            "title": "Coinbase Delivers on Q4 Financial Outlook",
            "source": "Coinbase Investor Relations",
            "configured_source": "Coinbase Investor Relations",
            "link": "https://investor.coinbase.com/news/news-details/2026/example/default.aspx",
        }
    )


def test_is_high_confidence_company_news_accepts_cn_announcements_and_exchange_hosts():
    assert _is_high_confidence_company_news(
        {
            "title": "农发种业披露 2025 年年报",
            "category": "stock_announcement",
            "source": "巨潮资讯",
            "configured_source": "CNINFO",
            "link": "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600313",
        }
    )
    assert _is_high_confidence_company_news(
        {
            "title": "农发种业临时公告",
            "source": "上交所",
            "configured_source": "SSE",
            "link": "https://www.sse.com.cn/disclosure/listedinfo/announcement/",
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


def test_preferred_catalyst_sources_for_cn_stock_prioritize_first_party_sources():
    cn_sources = _preferred_catalyst_sources({"region": "CN", "asset_type": "cn_stock"}, {"profile_name": "农业"})
    assert cn_sources[:4] == ["CNINFO", "SSE", "SZSE", "Investor Relations"]


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
    assert "行业/市场级观察信号" in dimension["summary"]


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


def test_catalyst_dimension_flags_hot_theme_search_gap_for_ai_review(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._company_calendar_event_dates",
        lambda symbol, asset_type: tuple(),  # noqa: ARG005
    )
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keyword_groups",
        lambda self, keyword_groups, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    context = {
        "config": {},
        "news_report": {"mode": "live", "all_items": []},
        "as_of": "2026-03-26",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {
            "symbol": "512480",
            "name": "半导体ETF",
            "asset_type": "cn_etf",
            "sector": "科技",
            "chain_nodes": ["半导体", "芯片"],
        },
        context,
    )
    assert dimension["score"] is None
    assert dimension["missing"] is True
    assert "待 AI 联网复核" in dimension["summary"]
    coverage = dict(dimension["coverage"] or {})
    assert coverage["diagnosis"] == "suspected_search_gap"
    assert coverage["ai_web_search_recommended"] is True


def test_catalyst_dimension_flags_hot_cn_stock_search_gap_for_ai_review(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._company_calendar_event_dates",
        lambda symbol, asset_type: tuple(),  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._company_forward_events",
        lambda metadata, context, news_items=None, extra_items=None: [],  # noqa: ARG005
    )
    monkeypatch.setattr(NewsCollector, "get_stock_news", lambda self, symbol, limit=10: [])  # noqa: ARG005
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keyword_groups",
        lambda self, keyword_groups, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    context = {
        "config": {},
        "news_report": {"mode": "live", "all_items": []},
        "as_of": "2026-03-26",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {
            "symbol": "300502",
            "name": "新易盛",
            "asset_type": "cn_stock",
            "sector": "科技",
            "chain_nodes": ["光模块", "AI算力"],
        },
        context,
    )
    coverage = dict(dimension["coverage"] or {})
    assert coverage["diagnosis"] == "suspected_search_gap"
    assert coverage["ai_web_search_recommended"] is True
    assert dimension["score"] is None


def test_catalyst_dimension_marks_stale_only_theme_news_as_non_new_catalyst(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._company_calendar_event_dates",
        lambda symbol, asset_type: tuple(),  # noqa: ARG005
    )
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keyword_groups",
        lambda self, keyword_groups, preferred_sources=None, limit=6, recent_days=7: [
            {
                "title": "半导体设备需求持续改善 - Reuters",
                "category": "topic_search",
                "source": "Reuters",
                "configured_source": "Reuters",
                "must_include": False,
                "published_at": "2026-03-17",
                "link": "https://example.com/old-chip",
            }
        ],  # noqa: ARG005
    )
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    context = {
        "config": {},
        "news_report": {"mode": "live", "all_items": []},
        "as_of": "2026-03-26",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {
            "symbol": "512480",
            "name": "半导体ETF",
            "asset_type": "cn_etf",
            "sector": "科技",
            "chain_nodes": ["半导体", "芯片"],
        },
        context,
    )
    coverage = dict(dimension["coverage"] or {})
    density_factor = next(f for f in dimension["factors"] if f["name"] == "研报/新闻密度")
    assert coverage["diagnosis"] == "stale_live_only"
    assert coverage["fresh_news_pool_count"] == 0
    assert coverage["stale_news_pool_count"] >= 1
    assert density_factor["awarded"] == 0
    assert "旧闻回放" in dimension["summary"]


def test_catalyst_dimension_exposes_theme_news_field_for_theme_assets(monkeypatch):
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keyword_groups",
        lambda self, keyword_groups, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    context = {
        "config": {},
        "news_report": {
            "mode": "live",
            "all_items": [],
        },
        "as_of": "2026-03-26",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {
            "symbol": "512480",
            "name": "半导体ETF",
            "asset_type": "cn_etf",
            "sector": "科技",
            "chain_nodes": ["半导体", "芯片"],
        },
        context,
    )
    assert "theme_news" in dimension
    assert isinstance(dimension["theme_news"], list)


def test_catalyst_dimension_attempts_theme_search_for_etf_when_news_mode_is_proxy(monkeypatch):
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keyword_groups",
        lambda self, keyword_groups, preferred_sources=None, limit=6, recent_days=7: [  # noqa: ARG005
            {
                "title": "有色金属价格继续修复，工业金属方向走强",
                "category": "topic_search",
                "source": "财联社",
                "configured_source": "财联社",
                "must_include": False,
                "published_at": "2026-03-26",
                "link": "https://example.com/nonferrous",
            }
        ],
    )
    monkeypatch.setattr(
        NewsCollector,
        "search_by_keywords",
        lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [],  # noqa: ARG005
    )
    context = {
        "config": {"news_topic_search_enabled": True},
        "news_report": {
            "mode": "proxy",
            "all_items": [],
        },
        "as_of": "2026-03-26",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {
            "symbol": "512400",
            "name": "有色金属ETF",
            "asset_type": "cn_etf",
            "sector": "有色",
            "chain_nodes": ["有色金属", "工业金属"],
        },
        context,
    )
    coverage = dict(dimension["coverage"] or {})
    assert coverage["search_attempted"] is True
    assert coverage["search_result_count"] >= 1
    assert len(list(coverage["search_groups"] or [])) <= 4
    assert ["有色金属", "工业金属"] in list(coverage["search_groups"] or [])
    assert any(
        "有色金属价格继续修复" in str(item.get("title", ""))
        for item in [*(dimension.get("theme_news") or []), *(dimension.get("evidence") or [])]
    )


def test_today_theme_uses_drivers_and_pulse_to_identify_hardtech() -> None:
    theme = _today_theme(
        {"items": [{"category": "semiconductor", "title": "半导体设备链继续走强"}]},
        [{"name": "VIX波动率", "latest": 18.0}],
        drivers={
            "industry_spot": pd.DataFrame([{"名称": "半导体"}, {"名称": "证券Ⅱ"}]),
            "concept_spot": pd.DataFrame(),
            "hot_rank": pd.DataFrame(),
        },
        pulse={
            "zt_pool": pd.DataFrame(),
            "strong_pool": pd.DataFrame([{"所属行业": "光学光电"}, {"所属行业": "通信服务"}]),
        },
    )

    assert theme["code"] == "ai_semis"
    assert theme["label"] == "硬科技 / AI硬件链"


def test_today_theme_ignores_stale_driver_and_pulse_frames() -> None:
    theme = _today_theme(
        {"items": []},
        [],
        drivers={
            "industry_spot": pd.DataFrame([{"名称": "电力"}]),
            "industry_spot_report": {"is_fresh": False, "frame_empty": False},
        },
        pulse={
            "strong_pool": pd.DataFrame([{"所属行业": "通信服务"}]),
            "strong_pool_report": {"is_fresh": False, "frame_empty": False},
        },
    )

    assert theme["code"] == "macro_background"


def test_today_theme_requires_more_than_single_power_label() -> None:
    theme = _today_theme(
        {"items": []},
        [],
        drivers={
            "industry_spot": pd.DataFrame([{"名称": "电力"}]),
            "industry_spot_report": {"is_fresh": True, "frame_empty": False},
        },
    )

    assert theme["code"] == "macro_background"


def test_today_theme_uses_watchlist_strength_to_surface_cpo_chain() -> None:
    theme = _today_theme(
        {"items": []},
        [],
        watchlist=[
            {
                "symbol": "515880",
                "name": "通信ETF",
                "asset_type": "cn_etf",
                "sector": "通信",
                "chain_nodes": ["CPO", "光模块", "通信设备", "AI算力"],
            }
        ],
        watchlist_returns={
            "515880": pd.Series(
                [0.012, 0.015, 0.009, 0.011, 0.013],
                index=pd.date_range("2026-04-01", periods=5, freq="B"),
            )
        },
    )

    assert theme["code"] == "ai_semis"


def test_today_theme_can_identify_innovation_medicine_from_titles_and_concepts() -> None:
    theme = _today_theme(
        {"items": [{"category": "biotech", "title": "创新药 license-out 持续活跃"}]},
        [{"name": "VIX波动率", "latest": 18.0}],
        drivers={
            "concept_spot": pd.DataFrame([{"名称": "创新药"}, {"名称": "生物医药"}]),
            "concept_spot_report": {"is_fresh": True, "frame_empty": False},
        },
    )

    assert theme["code"] == "innovation_medicine"
    assert theme["label"] == "创新药 / 医药催化"


def test_today_theme_falls_back_to_all_items_when_items_are_empty() -> None:
    theme = _today_theme(
        {
            "items": [],
            "all_items": [{"category": "biotech", "title": "创新药 license-out 持续活跃"}],
        },
        [{"name": "VIX波动率", "latest": 18.0}],
        drivers={
            "concept_spot": pd.DataFrame([{"名称": "创新药"}]),
            "concept_spot_report": {"is_fresh": True, "frame_empty": False},
        },
    )

    assert theme["code"] == "innovation_medicine"
    assert theme["label"] == "创新药 / 医药催化"


def test_market_context_watch_hint_lines_keep_sector_breadth_without_etf_name_noise() -> None:
    hints = _market_context_watch_hint_lines(
        [
            {"symbol": "515880", "name": "通信ETF", "sector": "通信", "chain_nodes": ["CPO", "光模块", "通信设备"]},
            {"symbol": "588200", "name": "科创芯片ETF", "sector": "半导体", "chain_nodes": ["半导体", "国产替代"]},
            {"symbol": "513120", "name": "港股创新药ETF", "sector": "创新药", "chain_nodes": ["创新药", "港股医药"]},
        ],
        {
            "515880": pd.Series([0.01, 0.015, 0.02], index=pd.date_range("2026-04-01", periods=3, freq="B")),
            "588200": pd.Series([0.012, 0.016, 0.018], index=pd.date_range("2026-04-01", periods=3, freq="B")),
            "513120": pd.Series([0.011, 0.018, 0.022], index=pd.date_range("2026-04-01", periods=3, freq="B")),
        },
        limit=2,
    )

    assert len(hints) == 2
    assert any("创新药 港股医药" in item for item in hints)
    assert all("ETF" not in item for item in hints)


def test_today_theme_keeps_parallel_mainline_when_innovation_medicine_is_also_strong() -> None:
    theme = _today_theme(
        {
            "items": [
                {"category": "semiconductor", "title": "半导体设备链继续走强"},
                {"category": "biotech", "title": "创新药 license-out 持续活跃"},
            ]
        },
        [{"name": "VIX波动率", "latest": 18.0}],
        drivers={
            "industry_spot": pd.DataFrame([{"名称": "半导体"}]),
            "concept_spot": pd.DataFrame([{"名称": "创新药"}]),
            "industry_spot_report": {"is_fresh": True, "frame_empty": False},
            "concept_spot_report": {"is_fresh": True, "frame_empty": False},
        },
    )

    assert theme["code"] == "ai_semis"
    assert "innovation_medicine" in list(theme.get("secondary_codes") or [])
    assert "创新药 / 医药催化" in list(theme.get("secondary_labels") or [])


def test_today_theme_uses_market_intelligence_headline_density_for_parallel_secondary_theme() -> None:
    theme = _today_theme(
        {
            "all_items": [
                {"category": "market_intelligence", "title": "光模块景气修复，CPO 链继续走强"},
                {"category": "market_intelligence", "title": "通信设备景气回暖，算力链热度抬升"},
                {"category": "market_intelligence", "title": "AI 服务器需求回暖，光通信链跟随活跃"},
                {"category": "market_intelligence", "title": "国家药监局批准自主研发新药上市"},
                {"category": "market_intelligence", "title": "ASCO 前瞻：创新药临床数据持续更新"},
                {"category": "market_intelligence", "title": "药企通过 FDA 检查，创新药催化延续"},
                {"category": "market_intelligence", "title": "创新药授权交易继续推进，里程碑付款释放"},
            ]
        },
        [{"name": "VIX波动率", "latest": 18.0}],
        watchlist=[
            {"symbol": "515880", "name": "通信ETF", "sector": "通信", "chain_nodes": ["CPO", "光模块"]},
            {"symbol": "513120", "name": "港股创新药ETF", "sector": "医药", "chain_nodes": ["创新药", "港股医药"]},
        ],
        watchlist_returns={
            "515880": pd.Series([0.01, 0.03, 0.05], index=pd.date_range("2026-04-01", periods=3, freq="B")),
            "513120": pd.Series([0.005, 0.01, 0.02], index=pd.date_range("2026-04-01", periods=3, freq="B")),
        },
    )

    assert theme["code"] == "ai_semis"
    assert "innovation_medicine" in list(theme.get("secondary_codes") or [])


def test_preferred_fund_sectors_do_not_leak_grid_into_hardtech_theme() -> None:
    preferred = _preferred_fund_sectors("硬科技 / AI硬件链")

    assert "科技" in preferred
    assert "电网" not in preferred


def test_theme_alignment_requires_primary_exposure_for_hardtech_theme() -> None:
    day_theme = {"code": "ai_semis", "label": "硬科技 / AI硬件链"}

    assert _theme_alignment(
        {
            "name": "国泰中证全指通信设备ETF",
            "sector": "科技",
            "benchmark": "中证全指通信设备指数",
            "chain_nodes": ["CPO", "光模块", "通信设备", "AI算力"],
        },
        day_theme,
    )
    assert not _theme_alignment(
        {
            "name": "恒生A股电网设备ETF",
            "sector": "电网",
            "benchmark": "恒生A股电网设备指数",
            "chain_nodes": ["AI算力", "电力需求", "电网设备"],
        },
        day_theme,
    )
    assert not _theme_alignment(
        {
            "name": "动漫游戏ETF",
            "sector": "科技",
            "benchmark": "中证动漫游戏指数",
            "chain_nodes": ["游戏", "传媒", "AI应用"],
        },
        day_theme,
    )
    assert not _theme_alignment(
        {
            "name": "卫星ETF",
            "sector": "通信",
            "benchmark": "国证商用卫星通信产业指数",
            "chain_nodes": ["卫星通信", "卫星互联网", "商业航天"],
        },
        day_theme,
    )


def test_theme_alignment_accepts_secondary_day_theme_for_innovation_medicine() -> None:
    day_theme = {
        "code": "ai_semis",
        "label": "硬科技 / AI硬件链",
        "secondary_codes": ["innovation_medicine"],
        "secondary_labels": ["创新药 / 医药催化"],
    }

    assert _theme_alignment(
        {
            "name": "港股创新药ETF",
            "sector": "医药",
            "benchmark": "中证香港创新药指数",
            "chain_nodes": ["创新药", "医药", "license-out"],
        },
        day_theme,
    )


def test_discover_driver_type_does_not_match_day_theme_text_itself() -> None:
    driver_type, _ = _discover_driver_type(
        {
            "name": "电网ETF",
            "metadata": {
                "sector": "电网",
                "benchmark": "恒生A股电网设备指数",
                "chain_nodes": ["AI算力", "电力需求", "电网设备"],
            },
            "day_theme": {"code": "ai_semis", "label": "硬科技 / AI硬件链"},
            "dimensions": {
                "technical": {"score": 32},
                "catalyst": {"score": 0},
                "relative_strength": {"score": 87},
                "risk": {"score": 75},
                "macro": {"score": 2},
            },
        },
        preferred_sectors=["科技", "半导体", "通信", "宽基"],
    )

    assert driver_type == "防守驱动"


def test_catalyst_dimension_respects_disabled_topic_search_runtime(monkeypatch):
    def _boom(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("dynamic topic search should stay disabled")

    monkeypatch.setattr(NewsCollector, "search_by_keyword_groups", _boom)
    monkeypatch.setattr(NewsCollector, "search_by_keywords", _boom)
    context = {
        "config": {"news_topic_search_enabled": False},
        "news_report": {
            "mode": "proxy",
            "all_items": [],
        },
        "as_of": "2026-03-26",
        "events": [],
    }
    dimension = _catalyst_dimension(
        {
            "symbol": "512400",
            "name": "有色金属ETF",
            "asset_type": "cn_etf",
            "sector": "有色",
            "chain_nodes": ["有色金属", "工业金属"],
        },
        context,
    )
    coverage = dict(dimension.get("coverage") or {})
    assert coverage["search_attempted"] is False
    assert coverage["search_result_count"] == 0


def test_market_event_rows_from_context_uses_fund_profile_keywords_for_etf_matching() -> None:
    context = {
        "config": {},
        "as_of": "2026-04-01",
        "drivers": {
            "industry_spot": pd.DataFrame(
                [
                    {
                        "名称": "有色金属",
                        "涨跌幅": 2.65,
                        "领涨股票": "紫金矿业",
                    }
                ]
            ),
            "concept_spot": pd.DataFrame(),
            "hot_rank": pd.DataFrame(),
        },
    }
    rows = _market_event_rows_from_context(
        {
            "symbol": "512400",
            "name": "南方中证申万有色金属ETF",
            "asset_type": "cn_etf",
            "sector": "有色",
            "chain_nodes": [],
        },
        context,
        {
            "overview": {"业绩比较基准": "中证申万有色金属指数收益率"},
            "industry_allocation": [{"行业类别": "有色金属", "持仓市值": 1.0}],
        },
    )

    assert rows
    assert "A股行业走强：有色金属（+2.65%）；领涨 紫金矿业" == rows[0][1]


def test_market_event_rows_from_context_includes_standard_stock_industry_framework(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_industry_index_snapshot",
        lambda metadata, context, fund_profile=None: {  # noqa: ARG005
            "status": "matched",
            "items": [
                {
                    "family": "sw",
                    "family_label": "申万",
                    "framework_source": "申万二级行业",
                    "level": "L2",
                    "index_name": "通信设备",
                    "pct_change": 3.6,
                    "signal_strength": "高",
                }
            ],
        },
    )
    monkeypatch.setattr("src.processors.opportunity_engine._cn_stock_theme_membership_snapshot", lambda metadata, context: {"items": []})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot", lambda metadata, context: {})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._cn_stock_chip_snapshot", lambda metadata, context: {})  # noqa: ARG005

    rows = _market_event_rows_from_context(
        {
            "symbol": "300308",
            "name": "中际旭创",
            "asset_type": "cn_stock",
            "sector": "科技",
            "chain_nodes": ["光模块"],
        },
        {"config": {}, "as_of": "2026-04-01", "drivers": {"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()}},
    )

    assert rows[0][1] == "标准行业框架：中际旭创 属于 申万二级行业·通信设备（+3.60%）"
    assert rows[0][2] == "申万行业框架"
    assert rows[0][6] == "标准行业归因"


def test_market_event_rows_from_context_prefers_standard_etf_industry_framework(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_industry_index_snapshot",
        lambda metadata, context, fund_profile=None: {  # noqa: ARG005
            "status": "matched",
            "items": [
                {
                    "family": "sw",
                    "family_label": "申万",
                    "framework_source": "申万二级行业",
                    "level": "L2",
                    "index_name": "有色金属",
                    "pct_change": 2.65,
                    "signal_strength": "中",
                }
            ],
        },
    )

    rows = _market_event_rows_from_context(
        {
            "symbol": "512400",
            "name": "南方中证申万有色金属ETF",
            "asset_type": "cn_etf",
            "sector": "有色",
            "chain_nodes": [],
        },
        {
            "config": {},
            "as_of": "2026-04-01",
            "drivers": {
                "industry_spot": pd.DataFrame(
                    [
                        {
                            "名称": "有色金属",
                            "涨跌幅": 2.65,
                            "领涨股票": "紫金矿业",
                        }
                    ]
                ),
                "concept_spot": pd.DataFrame(),
                "hot_rank": pd.DataFrame(),
            },
        },
        {
            "overview": {"业绩比较基准": "中证申万有色金属指数收益率"},
            "industry_allocation": [{"行业类别": "有色金属", "持仓市值": 1.0}],
        },
    )

    assert rows[0][1] == "跟踪指数/行业框架：南方中证申万有色金属ETF 对应 申万二级行业·有色金属（+2.65%）"
    assert rows[0][2] == "申万行业框架"
    assert not any(str(row[1]).startswith("A股行业走强：有色金属") for row in rows)


def test_market_event_rows_from_context_labels_negative_board_move_as_pressure() -> None:
    rows = _market_event_rows_from_context(
        {
            "symbol": "999999",
            "name": "示例资产",
            "asset_type": "cn_stock",
            "sector": "饰品",
            "chain_nodes": [],
        },
        {
            "config": {},
            "as_of": "2026-04-03",
            "drivers": {
                "industry_spot": pd.DataFrame([{"名称": "饰品", "涨跌幅": -1.51}]),
                "concept_spot": pd.DataFrame(),
                "hot_rank": pd.DataFrame(),
            },
        },
    )

    assert rows
    assert rows[0][1] == "A股行业承压：饰品（-1.51%）"
    assert rows[0][6] == "主线承压"
    assert "继续走弱" in str(rows[0][7])


def test_market_event_rows_from_context_includes_index_topic_bundle_for_etf(monkeypatch) -> None:
    monkeypatch.setattr("src.processors.opportunity_engine._context_industry_index_snapshot", lambda *args, **kwargs: {})  # noqa: ANN001
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_index_topic_bundle",
        lambda metadata, context, fund_profile=None: {  # noqa: ARG005
            "index_snapshot": {
                "index_name": "沪深300",
                "pe_ttm": 12.5,
                "pb": 1.3,
            },
            "technical_snapshot": {
                "status": "matched",
                "pct_change": 1.8,
                "trend_label": "趋势偏强",
                "momentum_label": "动能改善",
                "signal_strength": "中",
                "detail": "收盘 3500 / MA20 3450 / MA60 3380 / MACD +12 / RSI6 61",
            },
            "constituent_weights": pd.DataFrame(
                [
                    {"symbol": "600519", "name": "贵州茅台", "weight": 5.0},
                    {"symbol": "300750", "name": "宁德时代", "weight": 4.0},
                ]
            ),
        },
    )

    rows = _market_event_rows_from_context(
        {
            "symbol": "510300",
            "name": "沪深300ETF",
            "asset_type": "cn_etf",
            "sector": "宽基",
            "chain_nodes": [],
        },
        {"config": {}, "as_of": "2026-04-02", "drivers": {"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()}},
        {"overview": {"业绩比较基准": "沪深300指数收益率"}},
    )

    assert any("跟踪指数框架：沪深300ETF 跟踪 沪深300" in str(row[1]) for row in rows)
    assert any("指数技术面：沪深300 趋势偏强 / 动能改善" == str(row[1]) for row in rows)
    assert any("指数成分权重：前十权重合计 9.0%" in str(row[1]) for row in rows)


def test_market_event_rows_from_context_adds_etf_profile_proxy_rows_when_index_data_is_thin(monkeypatch) -> None:
    monkeypatch.setattr("src.processors.opportunity_engine._context_industry_index_snapshot", lambda *args, **kwargs: {})  # noqa: ANN001
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_index_topic_bundle",
        lambda metadata, context, fund_profile=None: {  # noqa: ARG005
            "index_snapshot": {
                "index_name": "中证韩交所中韩半导体指数",
                "index_code": "931790.CSI",
                "match_quality": "exact_code",
            },
            "technical_snapshot": {"status": "empty"},
            "constituent_weights": pd.DataFrame(),
        },
    )

    rows = _market_event_rows_from_context(
        {
            "symbol": "513310",
            "name": "中韩半导体ETF华泰柏瑞",
            "asset_type": "cn_etf",
            "sector": "科技",
            "chain_nodes": [],
        },
        {"config": {}, "as_of": "2026-04-02", "drivers": {"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()}},
        {
            "overview": {"跟踪标的": "中证韩交所中韩半导体指数"},
            "top_holdings": [
                {"股票名称": "SamsungElectronics", "占净值比例": 16.31},
                {"股票名称": "SK hynix", "占净值比例": 15.45},
                {"股票名称": "寒武纪", "占净值比例": 7.95},
            ],
            "industry_allocation": [
                {"行业类别": "科技", "占净值比例": 50.67},
                {"行业类别": "制造业", "占净值比例": 38.94},
            ],
        },
    )

    assert any("跟踪指数框架：中韩半导体ETF华泰柏瑞 跟踪 中证韩交所中韩半导体指数" in str(row[1]) for row in rows)
    assert any("跟踪成分画像：中韩半导体ETF华泰柏瑞 最近披露持仓集中在 SamsungElectronics 16.3%" in str(row[1]) for row in rows)
    assert any("行业暴露画像：中韩半导体ETF华泰柏瑞 最近披露主要暴露在 科技 50.7%" in str(row[1]) for row in rows)


def test_market_event_rows_from_context_includes_etf_share_flow_signal(monkeypatch) -> None:
    monkeypatch.setattr("src.processors.opportunity_engine._context_industry_index_snapshot", lambda *args, **kwargs: {})  # noqa: ANN001
    monkeypatch.setattr("src.processors.opportunity_engine._context_index_topic_bundle", lambda *args, **kwargs: {})  # noqa: ANN001

    rows = _market_event_rows_from_context(
        {
            "symbol": "563360",
            "name": "A500ETF华泰柏瑞",
            "asset_type": "cn_etf",
            "sector": "宽基",
            "chain_nodes": [],
        },
        {"config": {}, "as_of": "2026-04-02", "drivers": {"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()}},
        {
            "etf_snapshot": {
                "share_as_of": "2026-04-01",
                "etf_share_change": 2.58,
                "etf_share_change_pct": 0.84,
            }
        },
    )

    assert any("份额申赎确认：A500ETF华泰柏瑞 最近净创设 +2.58 亿份" in str(row[1]) for row in rows)


def test_market_event_rows_from_context_includes_index_weekly_and_monthly_rows(monkeypatch) -> None:
    monkeypatch.setattr("src.processors.opportunity_engine._context_industry_index_snapshot", lambda *args, **kwargs: {})  # noqa: ANN001
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_index_topic_bundle",
        lambda metadata, context, fund_profile=None: {  # noqa: ARG005
            "index_snapshot": {"index_name": "沪深300", "pe_ttm": 12.5, "pb": 1.3},
            "technical_snapshot": {
                "status": "matched",
                "pct_change": 1.8,
                "trend_label": "趋势偏强",
                "momentum_label": "动能改善",
                "signal_strength": "中",
            },
            "history_snapshots": {
                "monthly": {
                    "status": "matched",
                    "trend_label": "趋势偏强",
                    "momentum_label": "动能偏强",
                    "signal_strength": "高",
                    "summary": "近 36月 +18.00%，最近一月 +4.20%，趋势偏强，动能偏强",
                },
                "weekly": {
                    "status": "matched",
                    "trend_label": "修复中",
                    "momentum_label": "动能改善",
                    "signal_strength": "中",
                    "summary": "近 156周 +12.00%，最近一周 +1.10%，修复中，动能改善",
                },
            },
            "constituent_weights": pd.DataFrame(
                [
                    {"symbol": "600519", "name": "贵州茅台", "weight": 5.0},
                    {"symbol": "300750", "name": "宁德时代", "weight": 4.0},
                ]
            ),
        },
    )

    rows = _market_event_rows_from_context(
        {
            "symbol": "510300",
            "name": "沪深300ETF",
            "asset_type": "cn_etf",
            "sector": "宽基",
            "chain_nodes": [],
        },
        {"config": {}, "as_of": "2026-04-02", "drivers": {"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()}},
        {"overview": {"业绩比较基准": "沪深300指数收益率"}},
    )

    assert any("指数月线：沪深300" in str(row[1]) for row in rows)
    assert any("指数周线：沪深300" in str(row[1]) for row in rows)
    assert any(str(row[2]) == "指数月线" for row in rows)
    assert any(str(row[2]) == "指数周线" for row in rows)


def test_relative_strength_dimension_includes_tracking_index_technical_state(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_index_topic_bundle",
        lambda metadata, context, fund_profile=None: {  # noqa: ARG005
            "index_snapshot": {"index_name": "沪深300"},
            "technical_snapshot": {
                "status": "matched",
                "pct_change": 1.2,
                "trend_label": "趋势偏强",
                "momentum_label": "动能偏强",
            },
        },
    )
    metrics = {"return_5d": 0.03, "return_20d": 0.08}
    asset_returns = pd.Series([0.01] * 30)

    dimension = _relative_strength_dimension(
        "510300",
        "cn_etf",
        {"symbol": "510300", "asset_type": "cn_etf", "benchmark_name": "沪深300", "sector": "宽基"},
        metrics,
        asset_returns,
        {"fund_profile": {}},
    )

    assert any(str(factor.get("name")) == "跟踪指数技术状态" for factor in dimension["factors"])
    matched = next(factor for factor in dimension["factors"] if str(factor.get("name")) == "跟踪指数技术状态")
    assert matched["awarded"] >= 10
    assert "沪深300 趋势偏强" in matched["signal"]


def test_relative_strength_dimension_includes_index_weekly_and_monthly_structure(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_index_topic_bundle",
        lambda metadata, context, fund_profile=None: {  # noqa: ARG005
            "index_snapshot": {"index_name": "沪深300"},
            "technical_snapshot": {
                "status": "matched",
                "pct_change": 1.2,
                "trend_label": "趋势偏强",
                "momentum_label": "动能偏强",
            },
            "history_snapshots": {
                "weekly": {
                    "status": "matched",
                    "trend_label": "修复中",
                    "momentum_label": "动能改善",
                    "signal_strength": "中",
                    "summary": "近 156周 +12.00%，最近一周 +1.10%，修复中，动能改善",
                },
                "monthly": {
                    "status": "matched",
                    "trend_label": "趋势偏强",
                    "momentum_label": "动能偏强",
                    "signal_strength": "高",
                    "summary": "近 36月 +18.00%，最近一月 +4.20%，趋势偏强，动能偏强",
                },
            },
        },
    )
    metrics = {"return_5d": 0.03, "return_20d": 0.08}
    asset_returns = pd.Series([0.01] * 30)

    dimension = _relative_strength_dimension(
        "510300",
        "cn_etf",
        {"symbol": "510300", "asset_type": "cn_etf", "benchmark_name": "沪深300", "sector": "宽基"},
        metrics,
        asset_returns,
        {"fund_profile": {}},
    )

    factor_names = {str(factor.get("name")) for factor in dimension["factors"]}
    assert "指数月线结构" in factor_names
    assert "指数周线结构" in factor_names
    monthly = next(factor for factor in dimension["factors"] if str(factor.get("name")) == "指数月线结构")
    weekly = next(factor for factor in dimension["factors"] if str(factor.get("name")) == "指数周线结构")
    assert monthly["awarded"] >= weekly["awarded"]
    assert "沪深300" in monthly["signal"]


def test_relative_strength_dimension_skips_index_bundle_for_active_cn_fund(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_index_topic_bundle",
        lambda metadata, context, fund_profile=None: {  # noqa: ARG005
            "index_snapshot": {"index_name": "中证科技50"},
            "technical_snapshot": {
                "status": "matched",
                "pct_change": 1.2,
                "trend_label": "趋势偏强",
                "momentum_label": "动能偏强",
            },
            "history_snapshots": {
                "weekly": {"status": "matched", "trend_label": "修复中", "momentum_label": "动能改善", "summary": "近 156周 +12.00%"},
                "monthly": {"status": "matched", "trend_label": "趋势偏强", "momentum_label": "动能偏强", "summary": "近 36月 +18.00%"},
            },
        },
    )
    metrics = {"return_5d": 0.03, "return_20d": 0.08}
    asset_returns = pd.Series([0.01] * 30)

    dimension = _relative_strength_dimension(
        "022365",
        "cn_fund",
        {"symbol": "022365", "asset_type": "cn_fund", "benchmark_name": "中证科技50", "sector": "科技", "is_passive_fund": False},
        metrics,
        asset_returns,
        {"fund_profile": {"style": {"tags": ["科技主题"]}}},
    )

    factor_names = {str(factor.get("name")) for factor in dimension["factors"]}
    assert "跟踪指数技术状态" not in factor_names
    assert "指数月线结构" not in factor_names
    assert "指数周线结构" not in factor_names


def test_market_event_rows_from_context_includes_stock_theme_membership_and_regulatory_risk(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_theme_membership_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "items": [
                {
                    "board_name": "AI算力",
                    "board_type_label": "概念",
                    "pct_change": 6.4,
                    "signal_strength": "高",
                }
            ]
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "active_st": False,
            "active_alert_count": 1,
            "high_shock_count": 0,
            "components": {
                "stk_alert": {"detail": "2026-04-01 起被列入交易所重点提示证券，参考截至 2026-04-12"},
            },
        },
    )

    rows = _market_event_rows_from_context(
        {
            "symbol": "300308",
            "name": "中际旭创",
            "asset_type": "cn_stock",
            "sector": "科技",
            "chain_nodes": ["AI算力", "光模块"],
        },
        {"config": {}, "as_of": "2026-04-01", "drivers": {"industry_spot": pd.DataFrame(), "concept_spot": pd.DataFrame(), "hot_rank": pd.DataFrame()}},
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "A股概念成员：中际旭创 属于 AI算力（+6.40%）" in joined
    assert "交易所重点提示：中际旭创 当前仍在重点提示证券名单" in joined


def test_risk_dimension_penalizes_exchange_alerted_stock(monkeypatch) -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=120, freq="B"),
            "open": [100.0] * 120,
            "high": [101.0] * 120,
            "low": [99.0] * 120,
            "close": [100.0 + 0.1 * i for i in range(120)],
            "volume": [1_000_000] * 120,
            "amount": [800_000_000.0] * 120,
        }
    )
    asset_returns = history["close"].pct_change().dropna()
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "⚠️",
            "detail": "近窗口命中过严重异常波动与交易所重点提示。",
            "active_alert_count": 1,
            "high_shock_count": 1,
        },
    )

    dimension = _risk_dimension(
        "300308",
        "cn_stock",
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock"},
        history,
        asset_returns,
        {"benchmark_returns": {"cn_stock": asset_returns}, "news_report": {"items": []}, "config": {}},
        None,
    )

    factor = next(item for item in dimension["factors"] if item["name"] == "交易所风险提示")
    assert factor["display_score"] == "-12"
    assert "异常波动" in factor["detail"]


def test_risk_dimension_penalizes_margin_and_board_crowding(monkeypatch) -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=120, freq="B"),
            "open": [100.0] * 120,
            "high": [101.0] * 120,
            "low": [99.0] * 120,
            "close": [100.0 + 0.1 * i for i in range(120)],
            "volume": [1_000_000] * 120,
            "amount": [800_000_000.0] * 120,
        }
    )
    asset_returns = history["close"].pct_change().dropna()
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_margin_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "is_fresh": True,
            "crowding_level": "high",
            "detail": "融资余额和融资买入/偿还比同步抬升，融资盘升温明显。",
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_board_action_snapshot",
        lambda metadata, context, history=None: {  # noqa: ARG005
            "is_fresh": True,
            "has_negative_signal": True,
            "in_dt_pool": True,
            "negative_bits": ["跌停池命中", "竞价转弱"],
            "detail": "跌停池命中叠加竞价转弱，情绪交易风险偏高。",
        },
    )

    dimension = _risk_dimension(
        "300308",
        "cn_stock",
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock"},
        history,
        asset_returns,
        {"benchmark_returns": {"cn_stock": asset_returns}, "news_report": {"items": []}, "config": {}},
        None,
    )

    factors = {item["name"]: item for item in dimension["factors"]}
    assert factors["两融拥挤"]["display_score"] == "-12"
    assert "融资盘升温明显" in factors["两融拥挤"]["signal"]
    assert factors["打板情绪风险"]["display_score"] == "-10"
    assert "情绪交易风险偏高" in factors["打板情绪风险"]["detail"]


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


def test_seasonality_dimension_uses_explicit_earnings_window_label_in_april(monkeypatch):
    class _AprilDateTime:
        @classmethod
        def now(cls):
            return pd.Timestamp("2026-04-01 10:00:00").to_pydatetime()

    monkeypatch.setattr("src.processors.opportunity_engine.datetime", _AprilDateTime)
    history = pd.DataFrame(
        {
            "date": pd.date_range("2021-01-31", periods=72, freq="ME"),
            "close": np.linspace(100.0, 150.0, 72),
        }
    )

    dimension = _seasonality_dimension({"sector": "农业", "asset_type": "cn_stock"}, history, {})
    factor = next(f for f in dimension["factors"] if f["name"] == "财报窗口")

    assert "年报和一季报" in factor["signal"]
    assert "Q2 财报密集期" not in factor["signal"]


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


def test_risk_dimension_penalizes_bottom_peer_rank_for_etf() -> None:
    history = _make_simple_history()
    asset_returns = history["close"].pct_change().dropna()
    dimension = _risk_dimension(
        "159131",
        "cn_etf",
        {"symbol": "159131", "name": "AI算力ETF", "asset_type": "cn_etf", "sector": "科技"},
        history,
        asset_returns,
        {
            "config": {},
            "as_of": "2026-04-17",
            "news_report": {"all_items": []},
            "benchmark_returns": {"cn_etf": asset_returns},
            "fund_profile": {
                "achievement": {
                    "近3月": {
                        "return_pct": -0.1191,
                        "peer_rank": "3633/3873",
                    }
                }
            },
        },
        None,
    )
    factor = next(f for f in dimension["factors"] if f["name"] == "同类业绩风险")

    assert factor["awarded"] < 0
    assert "3633/3873" in factor["signal"]


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


def test_rating_from_dimensions_keeps_resilient_rank_three_under_macro_reverse():
    dimensions = {
        "technical": {"score": 45},
        "fundamental": {"score": 74, "available_max": 100},
        "catalyst": {"score": 59},
        "relative_strength": {"score": 87},
        "risk": {"score": 37},
        "macro": {"score": 3, "macro_reverse": True},
    }

    rating = _rating_from_dimensions(dimensions, [])

    assert rating["rank"] == 3
    assert any("保留 ⭐⭐⭐" in item for item in rating["warnings"])


def test_rating_from_dimensions_still_caps_non_resilient_rank_three_under_macro_reverse():
    dimensions = {
        "technical": {"score": 40},
        "fundamental": {"score": 68, "available_max": 100},
        "catalyst": {"score": 52},
        "relative_strength": {"score": 45},
        "risk": {"score": 37},
        "macro": {"score": 3, "macro_reverse": True},
    }

    rating = _rating_from_dimensions(dimensions, [])

    assert rating["rank"] == 2
    assert any("评级上限已压到 ⭐⭐" in item for item in rating["warnings"])


def test_rating_from_dimensions_promotes_trend_continuation_candidate():
    dimensions = {
        "technical": {"score": 63},
        "fundamental": {"score": 38, "available_max": 100},
        "catalyst": {"score": 42},
        "relative_strength": {"score": 76},
        "risk": {"score": 58},
        "macro": {"score": 18, "macro_reverse": False},
    }

    rating = _rating_from_dimensions(dimensions, [])

    assert rating["rank"] == 3
    assert "趋势和轮动已经形成共振" in rating["meaning"]


def test_rating_from_dimensions_promotes_borderline_logic_candidate_after_calibration():
    dimensions = {
        "technical": {"score": 35},
        "fundamental": {"score": 62, "available_max": 100},
        "catalyst": {"score": 50},
        "relative_strength": {"score": 58},
        "risk": {"score": 32},
        "macro": {"score": 18, "macro_reverse": False},
    }

    rating = _rating_from_dimensions(dimensions, [])

    assert rating["rank"] == 3
    assert "右侧执行仍需一个维度继续确认" in rating["meaning"]


def test_rating_from_dimensions_promotes_borderline_trend_candidate_after_calibration():
    dimensions = {
        "technical": {"score": 55},
        "fundamental": {"score": 35, "available_max": 100},
        "catalyst": {"score": 25},
        "relative_strength": {"score": 65},
        "risk": {"score": 45},
        "macro": {"score": 18, "macro_reverse": False},
    }

    rating = _rating_from_dimensions(dimensions, [])

    assert rating["rank"] == 3
    assert "不必因为赔率还不完美就过度降级" in rating["meaning"]


def test_rating_from_dimensions_caps_sideways_stock_when_hard_gates_fail() -> None:
    dimensions = {
        "technical": {"score": 22},
        "fundamental": {"score": 78, "available_max": 100},
        "catalyst": {"score": 29},
        "relative_strength": {"score": 31},
        "risk": {"score": 18},
        "macro": {"score": 14, "macro_reverse": False},
    }

    rating = _rating_from_dimensions(dimensions, [], asset_type="cn_stock")

    assert rating["rank"] == 0
    assert rating["label"] == "无信号"
    assert any("个股信号硬门槛未过" in item for item in rating["warnings"])


def test_rating_from_dimensions_caps_high_quality_stock_when_signal_gates_fail() -> None:
    dimensions = {
        "technical": {"score": 22},
        "fundamental": {"score": 88, "available_max": 100},
        "catalyst": {"score": 18},
        "relative_strength": {"score": 36},
        "risk": {"score": 14},
        "macro": {"score": 12, "macro_reverse": False},
    }

    rating = _rating_from_dimensions(dimensions, [], asset_type="cn_stock")

    assert rating["rank"] == 0
    assert rating["label"] == "无信号"
    assert any("个股信号硬门槛未过" in item for item in rating["warnings"])


def test_rating_from_dimensions_promotes_defensive_etf_candidate() -> None:
    dimensions = {
        "technical": {"score": 28},
        "fundamental": {"score": 55, "available_max": 100},
        "catalyst": {"score": 8},
        "relative_strength": {"score": 38},
        "risk": {"score": 60},
        "macro": {"score": 12, "macro_reverse": False},
    }

    rating = _rating_from_dimensions(dimensions, [], asset_type="cn_etf")

    assert rating["rank"] == 3
    assert "配置型产品已经具备防守收益比和相对强弱" in rating["meaning"]


def test_rating_from_dimensions_promotes_cross_border_theme_etf_with_continuation_setup() -> None:
    dimensions = {
        "technical": {"score": 39},
        "fundamental": {"score": 44, "available_max": 100},
        "catalyst": {"score": 21},
        "relative_strength": {"score": 51},
        "risk": {"score": 15},
        "macro": {"score": 10, "macro_reverse": False},
    }

    rating = _rating_from_dimensions(dimensions, [], asset_type="cn_etf")

    assert rating["rank"] == 3
    assert "跨境主题产品已经有相对强弱和延续催化" in rating["meaning"]


def test_rating_from_dimensions_caps_structured_event_stock_when_hard_gates_fail() -> None:
    dimensions = {
        "technical": {"score": 18},
        "fundamental": {"score": 72, "available_max": 100},
        "catalyst": {"score": 18, "coverage": {"structured_event": True}},
        "relative_strength": {"score": 28},
        "risk": {"score": 22},
        "macro": {"score": 12, "macro_reverse": False},
    }

    rating = _rating_from_dimensions(dimensions, [], asset_type="cn_stock")

    assert rating["rank"] == 0
    assert rating["label"] == "无信号"
    assert any("个股信号硬门槛未过" in item for item in rating["warnings"])


def test_rating_from_dimensions_hard_gate_overrides_macro_resilient_stock_combo() -> None:
    dimensions = {
        "technical": {"score": 18},
        "fundamental": {"score": 74, "available_max": 100},
        "catalyst": {"score": 22, "coverage": {}},
        "relative_strength": {"score": 31},
        "risk": {"score": 12},
        "macro": {"score": 8, "macro_reverse": True},
    }

    rating = _rating_from_dimensions(dimensions, [], asset_type="cn_stock")

    assert rating["rank"] == 0
    assert rating["label"] == "无信号"
    assert any("个股信号硬门槛未过" in warning for warning in rating["warnings"])


def test_rating_from_dimensions_single_stock_gate_failure_caps_to_observe() -> None:
    dimensions = {
        "technical": {"score": 32},
        "fundamental": {"score": 78, "available_max": 100},
        "catalyst": {"score": 26, "coverage": {"structured_event": True}},
        "relative_strength": {"score": 42},
        "risk": {"score": 16},
        "macro": {"score": 12, "macro_reverse": False},
    }

    rating = _rating_from_dimensions(dimensions, [], asset_type="cn_stock")

    assert rating["rank"] == 1
    assert rating["label"] == "有信号但不充分"
    assert any("结论封顶为观察级" in item for item in rating["warnings"])


def test_rating_from_dimensions_promotes_defensive_etf_in_shaky_market_without_same_day_catalyst() -> None:
    dimensions = {
        "technical": {"score": 28},
        "fundamental": {"score": 77, "available_max": 100},
        "catalyst": {"score": 4, "coverage": {}},
        "relative_strength": {"score": 36},
        "risk": {"score": 55},
        "macro": {"score": 10, "macro_reverse": False},
    }

    rating = _rating_from_dimensions(dimensions, [], asset_type="cn_etf")

    assert rating["rank"] == 3
    assert "配置型产品已经具备防守收益比和相对强弱" in rating["meaning"]


def test_rating_from_dimensions_promotes_etf_with_strength_and_product_support_without_fresh_catalyst() -> None:
    dimensions = {
        "technical": {"score": 30},
        "fundamental": {"score": 67, "available_max": 100},
        "catalyst": {"score": 0, "coverage": {}},
        "relative_strength": {"score": 58},
        "risk": {"score": 0},
        "macro": {"score": 18, "macro_reverse": False},
    }

    rating = _rating_from_dimensions(dimensions, [], asset_type="cn_etf")

    assert rating["rank"] == 3
    assert "产品承接、相对强弱和技术位置已经站住" in rating["meaning"]


def test_rating_from_dimensions_caps_etf_when_relative_strength_cross_check_fails() -> None:
    dimensions = {
        "technical": {"score": 41},
        "fundamental": {"score": 55, "available_max": 100},
        "catalyst": {"score": 9, "coverage": {}},
        "relative_strength": {"score": 98, "cross_check_failed": True},
        "risk": {"score": 55},
        "macro": {"score": 12, "macro_reverse": False},
    }

    rating = _rating_from_dimensions(dimensions, [], asset_type="cn_etf")

    assert rating["rank"] == 2
    assert rating["label"] == "储备机会"
    assert any("观察/储备" in item for item in rating["warnings"])


def test_rating_from_dimensions_caps_thematic_etf_without_confirmation_to_observe() -> None:
    dimensions = {
        "technical": {"score": 41},
        "fundamental": {"score": 55, "available_max": 100},
        "catalyst": {"score": 9, "coverage": {}},
        "relative_strength": {"score": 98},
        "risk": {"score": 55},
        "macro": {"score": 12, "macro_reverse": False},
    }

    rating = _rating_from_dimensions(
        dimensions,
        [],
        asset_type="cn_etf",
        metadata={"primary_chain": "CPO/光模块", "theme_directness": "direct", "theme_role": "AI硬件主链"},
    )

    assert rating["rank"] == 1
    assert rating["label"] == "有信号但不充分"
    assert any("先按观察处理" in item for item in rating["warnings"])


def test_discover_ready_for_next_step_blocks_thematic_etf_without_confirmation() -> None:
    analysis = {
        "asset_type": "cn_etf",
        "rating": {"rank": 2},
        "metadata": {"primary_chain": "创新药", "theme_directness": "non_ai", "theme_role": "医药修复"},
        "dimensions": {
            "technical": {"score": 48},
            "catalyst": {"score": 12},
            "relative_strength": {"score": 66},
            "risk": {"score": 42},
        },
    }

    assert _discover_ready_for_next_step(analysis) is False


def test_discover_ready_for_next_step_blocks_cn_stock_hard_gate() -> None:
    analysis = {
        "asset_type": "cn_stock",
        "rating": {"rank": 3},
        "metadata": {},
        "dimensions": {
            "technical": {"score": 25},
            "catalyst": {"score": 18},
            "relative_strength": {"score": 51},
            "risk": {"score": 45},
        },
    }

    assert _discover_ready_for_next_step(analysis) is False


def test_action_plan_differentiated_when_risk_high_relative_high():
    """rating <= 1 but risk >= 70 and relative >= 60 should NOT be '暂不出手'."""
    analysis = _make_action_plan_analysis(rating_rank=1, tech=35, risk=75, relative=65, catalyst=20)
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 50.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA60": 10.0}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.4})
    assert "暂不出手" not in result["position"]
    assert "试探" in result["position"] or "2%" in result["position"]
    assert result["direction"] == "观望偏多"


def test_action_plan_blocks_cn_stock_hard_gate_even_if_rating_is_stale() -> None:
    analysis = _make_action_plan_analysis(
        rating_rank=3,
        tech=25,
        risk=11,
        relative=51,
        catalyst=18,
        asset_type="cn_stock",
        fundamental=67,
    )
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 50.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.2, "MA60": 10.0}}}

    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.4})

    assert result["direction"] == "回避"
    assert "不设正式建仓" in result["position"]
    assert "硬门槛" in result["entry"]


def test_action_plan_keeps_thematic_etf_as_observation_when_confirmation_missing() -> None:
    analysis = _make_action_plan_analysis(rating_rank=1, tech=49, risk=60, relative=65, catalyst=10, asset_type="cn_etf", fundamental=62)
    analysis["metadata"] = {"primary_chain": "CPO/光模块", "theme_directness": "direct", "theme_role": "AI硬件主链"}
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 56.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.2, "MA60": 10.0}}}

    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.4})

    assert result["direction"] == "观察为主"
    assert "观察仓" in result["position"]
    assert "观察名单" in result["entry"]


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
    assert result["horizon"]["family_code"] in {"position_trade", "swing"}
    assert result["horizon"]["fit_reason"]


def test_action_plan_uses_watchful_bullish_direction_when_odds_are_low():
    analysis = _make_action_plan_analysis(rating_rank=3, tech=62, risk=55, relative=65, catalyst=30, asset_type="cn_etf")
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 75.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.0, "MA60": 9.8}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.4, "price_percentile_1y": 0.95})
    assert result["direction"] == "观望偏多"


def test_action_plan_promotes_rank_three_confirming_setup_to_long() -> None:
    analysis = _make_action_plan_analysis(
        rating_rank=3,
        tech=30,
        risk=56,
        relative=48,
        catalyst=28,
        fundamental=78,
        asset_type="cn_stock",
    )
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 52.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.0, "MA60": 9.8}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.4, "price_percentile_1y": 0.62})

    assert result["direction"] == "做多"
    assert "首次建仓" in result["position"]


def test_action_plan_caps_rank_three_cn_stock_when_tech_hard_gate_fails() -> None:
    analysis = _make_action_plan_analysis(
        rating_rank=3,
        tech=18,
        risk=52,
        relative=28,
        catalyst=20,
        fundamental=72,
        asset_type="cn_stock",
    )
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 49.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.0, "MA60": 9.8}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.4, "price_percentile_1y": 0.58})

    assert result["direction"] == "观望"
    assert "不设正式建仓" in result["position"]


def test_action_plan_keeps_rank_three_cn_etf_in_formal_frame_when_relative_and_risk_stand_up() -> None:
    analysis = _make_action_plan_analysis(
        rating_rank=3,
        tech=24,
        risk=46,
        relative=42,
        catalyst=16,
        fundamental=46,
        asset_type="cn_etf",
    )
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 51.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.0, "MA60": 9.8}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.35, "price_percentile_1y": 0.55})

    assert result["direction"] == "做多"
    assert "首次建仓" in result["position"]


def test_action_plan_relaxes_trend_continuation_etf_when_rank_two():
    analysis = _make_action_plan_analysis(
        rating_rank=2,
        tech=50,
        risk=48,
        relative=72,
        catalyst=42,
        asset_type="cn_etf",
        fundamental=38,
    )
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 57.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.0, "MA60": 9.8}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.35, "price_percentile_1y": 0.58})
    assert result["direction"] == "观望偏多"
    assert result["timeframe"] == "波段跟踪(2-6周)"
    assert "≤3%" in result["position"]
    assert "分 2 批跟踪" in result["scaling_plan"]
    assert result["horizon"]["family_code"] == "swing"
    assert result["horizon"]["code"] == "swing_staged_followthrough"


def test_action_plan_keeps_watchful_bullish_direction_for_macro_reverse_rank_three():
    analysis = _make_action_plan_analysis(rating_rank=3, tech=48, risk=40, relative=82, catalyst=55, macro_reverse=True)
    history = _make_simple_history()
    technical = {"rsi": {"RSI": 54.0}, "fibonacci": {"levels": {}}, "ma_system": {"mas": {"MA20": 10.0, "MA60": 9.8}}}
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.4, "price_percentile_1y": 0.62})
    assert result["direction"] == "观望偏多"
    assert "首次建仓" in result["position"]


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


def test_action_plan_uses_nearest_pressure_as_first_observe_target():
    analysis = _make_action_plan_analysis(
        rating_rank=1,
        tech=22,
        risk=20,
        relative=45,
        catalyst=7,
        asset_type="cn_etf",
        fundamental=80,
    )
    history = _make_simple_history()
    history.loc[:, "close"] = [10.0] * len(history)
    history.loc[:, "high"] = [10.1] * (len(history) - 1) + [10.3]
    technical = {
        "rsi": {"RSI": 74.0},
        "fibonacci": {"levels": {"1.000": 11.6}, "swing_high": 10.3},
        "ma_system": {"mas": {"MA20": 9.8, "MA60": 9.6}},
        "setup": {"false_break": {"kind": "bullish_false_break"}},
    }

    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.5, "price_percentile_1y": 0.95})

    assert result["target_ref"] == 10.3
    assert "近端压力 10.300" in result["target"]
    assert result["trim_low_ref"] < 10.8


def test_action_plan_surfaces_buy_and_trim_ranges():
    analysis = _make_action_plan_analysis(rating_rank=3, tech=62, risk=55, relative=60, catalyst=52, asset_type="cn_stock")
    history = _make_simple_history()
    history.loc[:, "close"] = [20.0] * (len(history) - 1) + [20.0]
    technical = {
        "rsi": {"RSI": 54.0},
        "fibonacci": {"levels": {"0.382": 19.4, "0.500": 19.0, "0.618": 18.6, "1.000": 21.6}},
        "ma_system": {"mas": {"MA20": 19.6, "MA60": 19.1}},
    }
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.45, "return_5d": 0.02})
    assert result["buy_low_ref"] is not None
    assert result["buy_high_ref"] is not None
    assert result["buy_low_ref"] < result["buy_high_ref"] <= 20.1
    assert result["trim_low_ref"] > 20.0
    assert result["trim_high_ref"] > result["trim_low_ref"]
    assert re.search(r"[0-9.]+ - [0-9.]+", result["buy_range"])
    assert re.search(r"[0-9.]+ - [0-9.]+", result["trim_range"])


def test_action_plan_keeps_minimum_gap_between_buy_range_and_stop():
    analysis = _make_action_plan_analysis(rating_rank=3, tech=60, risk=58, relative=62, catalyst=48, asset_type="cn_etf")
    history = _make_simple_history()
    history.loc[:, "close"] = [1.78] * (len(history) - 1) + [1.80]
    history.loc[:, "high"] = [1.81] * len(history)
    history.loc[:, "low"] = [1.77] * len(history)
    technical = {
        "rsi": {"RSI": 51.0},
        "fibonacci": {"levels": {"0.382": 1.79, "0.500": 1.785, "0.618": 1.78, "1.000": 1.92}},
        "ma_system": {"mas": {"MA20": 1.79, "MA60": 1.785}},
    }
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.25, "return_5d": 0.01})
    assert result["buy_low_ref"] is not None
    assert result["buy_high_ref"] is not None
    assert result["buy_low_ref"] > result["stop_ref"]
    assert (result["buy_low_ref"] - result["stop_ref"]) / result["buy_low_ref"] >= 0.0099


def test_action_plan_uses_atr_buffer_for_cn_etf_stop() -> None:
    analysis = _make_action_plan_analysis(rating_rank=3, tech=60, risk=58, relative=62, catalyst=48, asset_type="cn_etf")
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-02-01", periods=40, freq="B"),
            "open": [0.85] * 40,
            "high": [0.87] * 40,
            "low": [0.83] * 40,
            "close": [0.85] * 40,
            "volume": [1_000_000] * 40,
            "amount": [85_000_000.0] * 40,
        }
    )
    technical = {
        "rsi": {"RSI": 51.0},
        "fibonacci": {"levels": {"0.382": 0.848, "0.500": 0.846, "0.618": 0.844, "1.000": 0.920}},
        "ma_system": {"mas": {"MA20": 0.848, "MA60": 0.842}},
        "volatility": {"NATR": 0.0291},
    }
    result = _action_plan(analysis, history, technical, None, {"volatility_percentile_1y": 0.2, "return_5d": 0.01})

    assert (0.85 - result["stop_ref"]) / 0.85 >= 0.058
    assert int(result["stop_loss_pct"].strip("-%")) >= 6
    assert "ATR" in result["stop"]


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
    assert result["horizon"]["family_code"] == "long_term_allocation"
    assert result["horizon"]["code"] == "long_term_core_allocation"
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


def test_build_market_context_can_skip_global_proxy_market_monitor_and_drivers(monkeypatch):
    monkeypatch.setattr("src.processors.opportunity_engine.load_china_macro_snapshot", lambda cfg: {"pmi": 50.0})
    monkeypatch.setattr("src.processors.opportunity_engine.load_watchlist", lambda: [])
    monkeypatch.setattr("src.processors.opportunity_engine.load_global_proxy_snapshot", lambda: (_ for _ in ()).throw(AssertionError("should skip global proxy")))
    monkeypatch.setattr("src.processors.opportunity_engine.MarketMonitorCollector.collect", lambda self: (_ for _ in ()).throw(AssertionError("should skip market monitor")))
    monkeypatch.setattr("src.processors.opportunity_engine.MarketDriversCollector.collect", lambda self: (_ for _ in ()).throw(AssertionError("should skip market drivers")))
    monkeypatch.setattr("src.processors.opportunity_engine.derive_regime_inputs", lambda *args, **kwargs: {})
    monkeypatch.setattr("src.processors.opportunity_engine.RegimeDetector.detect_regime", lambda self: {"current_regime": "recovery", "preferred_assets": []})
    monkeypatch.setattr("src.processors.opportunity_engine.NewsCollector.collect", lambda self, **kwargs: {"mode": "proxy", "items": [], "lines": [], "note": ""})
    monkeypatch.setattr("src.processors.opportunity_engine.EventsCollector.collect", lambda self, mode="daily": [])
    monkeypatch.setattr("src.processors.opportunity_engine.MarketPulseCollector.collect", lambda self: {})

    context = build_market_context(
        {
            "market_context": {
                "skip_global_proxy": True,
                "skip_market_monitor": True,
                "skip_market_drivers": True,
            }
        },
        relevant_asset_types=["cn_stock"],
    )

    assert context["global_proxy"] == {}
    assert context["monitor_rows"] == []
    assert context["drivers"] == {}
    assert any("全球代理数据已按运行配置关闭" in note for note in context["notes"])
    assert any("宏观资产监控已按运行配置关闭" in note for note in context["notes"])
    assert any("板块驱动数据已按运行配置关闭" in note for note in context["notes"])


def test_collect_fund_profile_honors_skip_flag(monkeypatch):
    called = {"value": False}

    class _FakeFundProfileCollector:
        def __init__(self, _config):
            pass

        def collect_profile(self, symbol, asset_type="cn_fund"):  # noqa: ANN001
            called["value"] = True
            return {"overview": {"基金简称": symbol, "资产类型": asset_type}}

    monkeypatch.setattr("src.processors.opportunity_engine.FundProfileCollector", _FakeFundProfileCollector)

    assert _collect_fund_profile("159981", "cn_etf", {"skip_fund_profile": True}) == {}
    assert called["value"] is False


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


def test_build_market_context_closes_yfinance_runtime_caches(monkeypatch):
    closed = {"count": 0}

    monkeypatch.setattr("src.processors.opportunity_engine.load_watchlist", lambda: [])
    monkeypatch.setattr("src.processors.opportunity_engine.load_china_macro_snapshot", lambda cfg: {"pmi": 50.0})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine.derive_regime_inputs", lambda *args, **kwargs: {})
    monkeypatch.setattr("src.processors.opportunity_engine.RegimeDetector.detect_regime", lambda self: {"current_regime": "recovery", "preferred_assets": []})
    monkeypatch.setattr("src.processors.opportunity_engine.NewsCollector.collect", lambda self, **kwargs: {"mode": "proxy", "items": [], "lines": [], "note": ""})  # noqa: ARG005,E501
    monkeypatch.setattr("src.processors.opportunity_engine.EventsCollector.collect", lambda self, mode="daily": [])
    monkeypatch.setattr("src.processors.opportunity_engine.MarketDriversCollector.collect", lambda self: {})
    monkeypatch.setattr("src.processors.opportunity_engine.MarketPulseCollector.collect", lambda self: {})
    monkeypatch.setattr("src.processors.opportunity_engine.close_yfinance_runtime_caches", lambda: closed.__setitem__("count", closed["count"] + 1))

    build_market_context(
        {
            "market_context": {
                "skip_global_proxy": True,
                "skip_market_monitor": True,
            }
        },
        relevant_asset_types=["cn_etf", "futures"],
    )

    assert closed["count"] >= 1


def test_build_market_context_backfills_proxy_news_report_with_shared_intel(monkeypatch):
    captured = {}

    monkeypatch.setattr("src.processors.opportunity_engine.load_watchlist", lambda: [])
    monkeypatch.setattr("src.processors.opportunity_engine.load_china_macro_snapshot", lambda cfg: {"pmi": 50.0})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine.derive_regime_inputs", lambda *args, **kwargs: {})
    monkeypatch.setattr("src.processors.opportunity_engine.RegimeDetector.detect_regime", lambda self: {"current_regime": "recovery", "preferred_assets": []})
    monkeypatch.setattr(
        "src.processors.opportunity_engine.NewsCollector.collect",
        lambda self, **kwargs: {"mode": "proxy", "items": [], "all_items": [], "lines": [], "note": ""},  # noqa: ARG005,E501
    )
    monkeypatch.setattr("src.processors.opportunity_engine.EventsCollector.collect", lambda self, mode="daily": [])
    monkeypatch.setattr(
        "src.processors.opportunity_engine.MarketDriversCollector.collect",
        lambda self: {
            "concept_spot": pd.DataFrame([{"名称": "创新药"}]),
            "concept_spot_report": {"is_fresh": True, "frame_empty": False},
        },
    )
    monkeypatch.setattr("src.processors.opportunity_engine.MarketPulseCollector.collect", lambda self: {})

    def _fake_collect_intel(query, *, config, structured_only=False, **kwargs):  # noqa: ANN001, ARG001
        captured["query"] = query
        captured["structured_only"] = structured_only
        return {
            "mode": "live",
            "items": [
                {
                    "category": "biotech",
                    "title": "创新药 license-out 持续活跃",
                    "source": "财联社",
                    "link": "https://example.com/biotech",
                    "published_at": "2026-04-22 10:00:00",
                }
            ],
            "all_items": [
                {
                    "category": "biotech",
                    "title": "创新药 license-out 持续活跃",
                    "source": "财联社",
                    "link": "https://example.com/biotech",
                    "published_at": "2026-04-22 10:00:00",
                }
            ],
            "summary_lines": ["创新药方向催化回暖。"],
            "note": "共享 intel 回填命中。",
            "disclosure": "共享情报快照",
        }

    monkeypatch.setattr("src.processors.opportunity_engine.collect_intel_news_report", _fake_collect_intel)

    context = build_market_context(
        {
            "market_context": {
                "skip_global_proxy": True,
                "skip_market_monitor": True,
            }
        },
        relevant_asset_types=["cn_etf", "futures"],
    )

    assert "创新药" in captured["query"]
    assert captured["structured_only"] is True
    assert context["news_report"]["fallback"] == "intel_shared_upstream"
    assert context["news_report"]["items"][0]["title"] == "创新药 license-out 持续活跃"
    assert context["day_theme"]["code"] == "innovation_medicine"


def test_build_market_context_merges_parallel_hint_queries_for_shared_intel(monkeypatch):
    captured_queries = []

    monkeypatch.setattr("src.processors.opportunity_engine.load_watchlist", lambda: [])
    monkeypatch.setattr("src.processors.opportunity_engine.load_china_macro_snapshot", lambda cfg: {"pmi": 50.0})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine.derive_regime_inputs", lambda *args, **kwargs: {})
    monkeypatch.setattr("src.processors.opportunity_engine.RegimeDetector.detect_regime", lambda self: {"current_regime": "recovery", "preferred_assets": []})
    monkeypatch.setattr(
        "src.processors.opportunity_engine.NewsCollector.collect",
        lambda self, **kwargs: {"mode": "proxy", "items": [], "all_items": [], "lines": [], "note": ""},  # noqa: ARG005,E501
    )
    monkeypatch.setattr("src.processors.opportunity_engine.EventsCollector.collect", lambda self, mode="daily": [])
    monkeypatch.setattr("src.processors.opportunity_engine.MarketDriversCollector.collect", lambda self: {})
    monkeypatch.setattr("src.processors.opportunity_engine.MarketPulseCollector.collect", lambda self: {})
    monkeypatch.setattr(
        "src.processors.opportunity_engine._market_context_hint_lines",
        lambda *args, **kwargs: ["通信 CPO 光模块", "创新药 港股医药"],  # noqa: ARG005
    )

    def _fake_collect_intel(query, *, config, structured_only=False, **kwargs):  # noqa: ANN001, ARG001
        captured_queries.append(query)
        if "通信" in query or "CPO" in query:
            items = [
                {
                    "category": "ai",
                    "title": "光模块景气修复，通信链热度抬升",
                    "source": "财联社",
                    "link": "https://example.com/cpo-1",
                    "published_at": "2026-04-22 10:00:00",
                },
                {
                    "category": "semiconductor",
                    "title": "CPO 链订单扩张，龙头加速扩产",
                    "source": "证券时报",
                    "link": "https://example.com/cpo-2",
                    "published_at": "2026-04-22 09:30:00",
                },
            ]
        else:
            items = [
                {
                    "category": "biotech",
                    "title": "创新药 ASCO 数据更新，资金继续围绕临床线博弈",
                    "source": "财联社",
                    "link": "https://example.com/biotech-1",
                    "published_at": "2026-04-22 10:05:00",
                },
                {
                    "category": "pharma",
                    "title": "新药临床推进，医药催化维持活跃",
                    "source": "证券时报",
                    "link": "https://example.com/biotech-2",
                    "published_at": "2026-04-22 09:35:00",
                },
            ]
        return {
            "mode": "live",
            "items": items,
            "all_items": items,
            "summary_lines": [f"{query} 命中共享结构化情报。"],
            "note": f"共享 intel 回填：{query}",
            "disclosure": "共享情报快照",
            "source_list": ["财联社", "证券时报"],
        }

    monkeypatch.setattr("src.processors.opportunity_engine.collect_intel_news_report", _fake_collect_intel)

    context = build_market_context(
        {
            "market_context": {
                "skip_global_proxy": True,
                "skip_market_monitor": True,
            }
        },
        relevant_asset_types=["cn_etf", "futures"],
    )

    assert captured_queries == ["通信 CPO 光模块", "创新药 港股医药"]
    titles = [str(item.get("title") or "") for item in list(context["news_report"]["items"] or [])]
    assert any("光模块景气修复" in title for title in titles)
    assert any("创新药 ASCO 数据更新" in title for title in titles)
    assert context["day_theme"]["code"] == "ai_semis"
    assert "innovation_medicine" in list(context["day_theme"].get("secondary_codes") or [])


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
    monkeypatch.setattr("src.processors.opportunity_engine.build_stock_pool", lambda config, market="all", sector_filter="", max_candidates=60: ([], []))

    result = discover_stock_opportunities({}, market="cn", top_n=5)

    assert result["top"] == []
    assert captured == [["cn_stock", "cn_etf", "futures"]]


def test_discover_stock_opportunities_reuses_provided_context(monkeypatch):
    shared_context = {"notes": [], "regime": {"current_regime": "deflation"}, "day_theme": {"label": "利率驱动成长修复"}}

    def fail_context(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("build_market_context should not be called when context is provided")

    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", fail_context)
    monkeypatch.setattr("src.processors.opportunity_engine.build_stock_pool", lambda config, market="all", sector_filter="", max_candidates=60: ([], []))

    result = discover_stock_opportunities({}, market="cn", top_n=5, context=shared_context)

    assert result["top"] == []
    assert result["regime"]["current_regime"] == "deflation"
    assert result["day_theme"]["label"] == "利率驱动成长修复"


def test_discover_stock_opportunities_analyzes_in_parallel(monkeypatch):
    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", lambda config, relevant_asset_types=None: {"notes": [], "regime": {}, "day_theme": {}, "config": dict(config)})  # noqa: ARG005
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_stock_pool",
        lambda config, market="all", sector_filter="", max_candidates=60: (  # noqa: ARG005
            [
                type(
                    "PoolItemStub",
                    (),
                    {
                        "symbol": f"30075{i}",
                        "asset_type": "cn_stock",
                        "name": f"样本{i}",
                        "sector": "新能源",
                        "chain_nodes": [],
                        "region": "CN",
                        "in_watchlist": False,
                        "metadata": {},
                    },
                )()
                for i in range(3)
            ],
            [],
        ),
    )

    def _slow_analysis(symbol, asset_type, config, context=None, metadata_override=None):  # noqa: ANN001,ARG001
        time.sleep(0.08)
        return {
            "symbol": symbol,
            "name": symbol,
            "asset_type": asset_type,
            "rating": {"rank": 1, "label": "储备机会", "stars": "⭐"},
            "dimensions": {
                "technical": {"score": 40},
                "fundamental": {"score": 60},
                "catalyst": {"score": 20},
                "relative_strength": {"score": 30},
                "chips": {"score": 0},
                "risk": {"score": 35},
                "seasonality": {"score": 0},
                "macro": {"score": 12},
            },
            "excluded": False,
        }

    monkeypatch.setattr("src.processors.opportunity_engine.analyze_opportunity", _slow_analysis)
    monkeypatch.setattr("src.processors.opportunity_engine._attach_signal_confidence", lambda analyses, config, limit=0: None)  # noqa: ARG005

    start = time.perf_counter()
    payload = discover_stock_opportunities({"opportunity": {"analysis_workers": 3}}, market="cn", top_n=3)
    elapsed = time.perf_counter() - start

    assert payload["scan_pool"] == 3
    assert payload["passed_pool"] == 3
    assert len(payload["coverage_analyses"]) == 3
    assert elapsed < 0.20


def test_discover_stock_opportunities_captures_candidate_errors_without_hanging(monkeypatch):
    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", lambda config, relevant_asset_types=None: {"notes": [], "regime": {}, "day_theme": {}, "config": dict(config)})  # noqa: ARG005
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_stock_pool",
        lambda config, market="all", sector_filter="", max_candidates=60: (  # noqa: ARG005
            [
                PoolItem(symbol="300750", name="宁德时代", asset_type="cn_stock", region="CN", sector="新能源", chain_nodes=[], source="all_market_stock"),
                PoolItem(symbol="300274", name="阳光电源", asset_type="cn_stock", region="CN", sector="新能源", chain_nodes=[], source="all_market_stock"),
            ],
            [],
        ),
    )

    def _failing_analysis(symbol, asset_type, config, context=None, metadata_override=None):  # noqa: ANN001, ARG001
        raise TimeoutError(f"simulated timeout for {symbol}")

    monkeypatch.setattr("src.processors.opportunity_engine.analyze_opportunity", _failing_analysis)
    monkeypatch.setattr("src.processors.opportunity_engine._attach_signal_confidence", lambda analyses, config, limit=0: None)  # noqa: ARG005

    start = time.perf_counter()
    payload = discover_stock_opportunities({"opportunity": {"analysis_workers": 2}}, market="cn", top_n=3)
    elapsed = time.perf_counter() - start

    assert elapsed < 0.5
    assert payload["passed_pool"] == 0
    assert payload["coverage_analyses"] == []
    assert any("扫描失败" in item for item in payload["blind_spots"])


def test_discover_stock_opportunities_forwards_candidate_limit(monkeypatch):
    captured: list[int] = []

    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", lambda config, relevant_asset_types=None: {"notes": [], "regime": {}, "day_theme": {}, "config": dict(config)})  # noqa: ARG005

    def _fake_pool(config, market="all", sector_filter="", max_candidates=60):  # noqa: ANN001,ARG001
        captured.append(max_candidates)
        return [], []

    monkeypatch.setattr("src.processors.opportunity_engine.build_stock_pool", _fake_pool)

    result = discover_stock_opportunities({}, market="cn", top_n=5, max_candidates=18)

    assert result["candidate_limit"] == 18
    assert captured == [18]


def test_discover_stock_opportunities_can_skip_signal_confidence(monkeypatch):
    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", lambda config, relevant_asset_types=None: {"notes": [], "regime": {}, "day_theme": {}, "config": dict(config)})  # noqa: ARG005
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_stock_pool",
        lambda config, market="all", sector_filter="", max_candidates=60: (  # noqa: ARG005
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
            "rating": {"rank": 2, "label": "储备机会", "stars": "⭐"},
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
            "history": _make_simple_history(),
            "action": {"stop_loss_pct": "-8%", "target_pct": 0.12},
            "excluded": False,
        },
    )

    def _fail_attach(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("_attach_signal_confidence should be skipped")

    monkeypatch.setattr("src.processors.opportunity_engine._attach_signal_confidence", _fail_attach)

    payload = discover_stock_opportunities({}, top_n=5, market="cn", attach_signal_confidence=False)

    assert len(payload["top"]) == 1


def test_discover_stock_opportunities_can_skip_signal_confidence_via_runtime_flag(monkeypatch):
    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", lambda config, relevant_asset_types=None: {"notes": [], "regime": {}, "day_theme": {}, "config": dict(config)})  # noqa: ARG005
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_stock_pool",
        lambda config, market="all", sector_filter="", max_candidates=60: (  # noqa: ARG005
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
            "rating": {"rank": 2, "label": "储备机会", "stars": "⭐"},
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
            "history": _make_simple_history(),
            "action": {"stop_loss_pct": "-8%", "target_pct": 0.12},
            "excluded": False,
        },
    )

    def _fail_attach(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("_attach_signal_confidence should be skipped")

    monkeypatch.setattr("src.processors.opportunity_engine._attach_signal_confidence", _fail_attach)

    payload = discover_stock_opportunities(
        {"skip_signal_confidence_runtime": True},
        top_n=5,
        market="cn",
    )

    assert len(payload["top"]) == 1


def test_build_stock_pool_can_skip_per_symbol_industry_lookup(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_stock_realtime",
        lambda self: pd.DataFrame(
            [
                {
                    "代码": "600000",
                    "名称": "浦发银行",
                    "成交额": 120_000_000.0,
                    "总市值": 10_000_000_000.0,
                }
            ]
        ),
    )

    def _fail_lookup(self, symbol):  # noqa: ANN001
        raise AssertionError("get_stock_industry should be skipped in runtime preview")

    monkeypatch.setattr(ChinaMarketCollector, "get_stock_industry", _fail_lookup)
    monkeypatch.setattr("src.processors.opportunity_engine.detect_asset_type", lambda symbol, config: "cn_stock")  # noqa: ARG005

    pool, warnings = build_stock_pool({"stock_pool_skip_industry_lookup_runtime": True}, market="cn", max_candidates=5)

    assert warnings == []
    assert [item.symbol for item in pool] == ["600000"]


def test_analyze_opportunity_can_skip_proxy_signals_via_runtime_flag(monkeypatch):
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

    monkeypatch.setattr("src.processors.opportunity_engine.fetch_asset_history", lambda symbol, asset_type, config: sample_history)  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", lambda config, relevant_asset_types=None: {})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._collect_fund_profile", lambda *args, **kwargs: {})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._safe_history", lambda *args, **kwargs: None)  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._intraday_snapshot", lambda *args, **kwargs: {"enabled": False})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._correlation_to_watchlist", lambda *args, **kwargs: None)  # noqa: ARG005

    def _fail_proxy(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("_analysis_proxy_signals should be skipped")

    monkeypatch.setattr("src.processors.opportunity_engine._analysis_proxy_signals", _fail_proxy)

    analysis = analyze_opportunity("300750", "cn_stock", {"skip_analysis_proxy_signals_runtime": True})

    assert analysis["proxy_signals"] == {}


def test_runtime_skip_helpers_return_fast_disclosures_for_cn_stock() -> None:
    context = {
        "config": {
            "skip_index_topic_bundle_runtime": True,
            "skip_cn_stock_chip_snapshot_runtime": True,
            "skip_cn_stock_capital_flow_runtime": True,
            "skip_cn_stock_margin_runtime": True,
            "skip_cn_stock_board_action_runtime": True,
            "skip_cn_stock_regulatory_risk_runtime": True,
            "skip_cn_stock_broker_recommend_runtime": True,
            "skip_cn_stock_unlock_pressure_runtime": True,
            "skip_cn_stock_pledge_risk_runtime": True,
        }
    }
    metadata = {"symbol": "300750", "asset_type": "cn_stock", "name": "宁德时代"}

    bundle = _context_index_topic_bundle(metadata, context)
    chip = _cn_stock_chip_snapshot(metadata, context)
    flow = _cn_stock_capital_flow_snapshot(metadata, context)
    margin = _cn_stock_margin_snapshot(metadata, context)
    board = _cn_stock_board_action_snapshot(metadata, context)
    regulatory = _cn_stock_regulatory_risk_snapshot(metadata, context)
    broker = _cn_stock_broker_recommend_snapshot(metadata, context)
    unlock = _cn_stock_unlock_pressure_snapshot(metadata, context)
    pledge = _cn_pledge_risk_snapshot(metadata, context)

    assert bundle["fallback"] == "not_applicable"
    assert bundle["technical_snapshot"]["status"] == "skipped"
    assert bundle["technical_snapshot"]["diagnosis"] == "not_applicable"
    assert chip["diagnosis"] == "runtime_skip"
    assert flow["diagnosis"] == "runtime_skip"
    assert margin["diagnosis"] == "runtime_skip"
    assert board["fallback"] == "runtime_skip"
    assert regulatory["fallback"] == "runtime_skip"
    assert broker["diagnosis"] == "runtime_skip"
    assert unlock["fallback"] == "runtime_skip"
    assert pledge["fallback"] == "runtime_skip"


def test_analyze_opportunity_skips_index_topic_bundle_for_cn_stock(monkeypatch) -> None:
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

    monkeypatch.setattr("src.processors.opportunity_engine.fetch_asset_history", lambda symbol, asset_type, config: sample_history)  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", lambda config, relevant_asset_types=None: {})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._collect_fund_profile", lambda *args, **kwargs: {})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._safe_history", lambda *args, **kwargs: None)  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._intraday_snapshot", lambda *args, **kwargs: {"enabled": False})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._correlation_to_watchlist", lambda *args, **kwargs: None)  # noqa: ARG005

    def _unexpected(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("cn_stock should not request index_topic_bundle")

    monkeypatch.setattr("src.processors.opportunity_engine._context_index_topic_bundle", _unexpected)

    analysis = analyze_opportunity("300750", "cn_stock", {})

    assert analysis["index_topic_bundle"] == {}


def test_analyze_opportunity_skips_index_topic_bundle_for_active_cn_fund(monkeypatch) -> None:
    sample_history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=80, freq="B"),
            "open": np.linspace(1.0, 1.2, 80),
            "high": np.linspace(1.03, 1.24, 80),
            "low": np.linspace(0.98, 1.17, 80),
            "close": np.linspace(1.01, 1.22, 80),
            "volume": np.linspace(1_000_000, 1_500_000, 80),
            "amount": np.linspace(100_000_000.0, 150_000_000.0, 80),
        }
    )
    sample_history.attrs["history_source"] = "tushare"
    sample_history.attrs["history_source_label"] = "Tushare 日线"

    monkeypatch.setattr("src.processors.opportunity_engine.fetch_asset_history", lambda symbol, asset_type, config: sample_history)  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", lambda config, relevant_asset_types=None: {})  # noqa: ARG005
    monkeypatch.setattr(
        "src.processors.opportunity_engine._collect_fund_profile",
        lambda *args, **kwargs: {
            "overview": {"基金简称": "永赢科技智选混合发起C", "基金类型": "混合型-偏股", "业绩比较基准": "中国战略新兴产业成份指数收益率"},
            "style": {"tags": ["科技主题"], "summary": "这只基金更像在买科技方向的主动选股框架。"},
        },
    )
    monkeypatch.setattr("src.processors.opportunity_engine._safe_history", lambda *args, **kwargs: None)  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._intraday_snapshot", lambda *args, **kwargs: {"enabled": False})  # noqa: ARG005
    monkeypatch.setattr("src.processors.opportunity_engine._correlation_to_watchlist", lambda *args, **kwargs: None)  # noqa: ARG005

    def _unexpected(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("active cn_fund should not request index_topic_bundle")

    monkeypatch.setattr("src.processors.opportunity_engine._context_index_topic_bundle", _unexpected)

    analysis = analyze_opportunity("022365", "cn_fund", {})

    assert analysis["index_topic_bundle"] == {}


def test_hard_checks_can_skip_unlock_and_pledge_runtime(monkeypatch) -> None:
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
    context = {
        "config": {
            "opportunity": {},
            "skip_cn_stock_unlock_pressure_runtime": True,
            "skip_cn_stock_pledge_risk_runtime": True,
        }
    }
    fundamental_dimension = {"valuation_snapshot": {"index_name": "新易盛", "pe_ttm": 52.0}, "valuation_extreme": False}

    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_unlock_pressure",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unlock pressure should be skipped")),
    )
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_pledge_stat",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pledge stat should be skipped")),
    )
    monkeypatch.setattr(
        ValuationCollector,
        "get_cn_stock_pledge_detail",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pledge detail should be skipped")),
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "ℹ️",
            "detail": "当前未拿到交易所风险专题",
            "active_st": False,
            "components": {},
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
    assert "预筛阶段已跳过" in check_map["解禁压力"]["detail"]
    assert "预筛阶段已跳过" in check_map["质押风险"]["detail"]
    assert exclusion_reasons == []
    assert warnings == []


def test_catalyst_dimension_can_skip_direct_cn_stock_news_runtime(monkeypatch):
    def _fail_stock_news(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("_context_stock_news should be skipped")

    monkeypatch.setattr("src.processors.opportunity_engine._context_stock_news", _fail_stock_news)

    dimension = _catalyst_dimension(
        {"symbol": "300750", "asset_type": "cn_stock", "name": "宁德时代", "sector": "新能源"},
        {
            "config": {"skip_cn_stock_direct_news_runtime": True, "news_topic_search_enabled": False},
            "news_report": {"mode": "live", "items": [], "all_items": []},
            "events": [],
        },
    )

    assert isinstance(dimension, dict)
    assert "score" in dimension


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

    assert fundamental_led["family_code"] == "swing"
    assert momentum_led["family_code"] == "swing"
    assert repair_led["family_code"] == "swing"
    assert fundamental_led["code"] == "swing_fundamental_resilience_with_volatility"
    assert momentum_led["code"] == "swing_mainline_rotation_followthrough"
    assert repair_led["code"] == "swing_repair_after_drawdown"
    assert "基本面和主线没有坏" in fundamental_led["fit_reason"]
    assert "催化和相对强弱都在线" in momentum_led["fit_reason"]
    assert "风险收益比相对不差" in repair_led["fit_reason"]


def test_build_analysis_horizon_profile_marks_crowded_mainline_observation_without_calling_it_repair():
    crowded_watch = build_analysis_horizon_profile(
        rating=1,
        asset_type="cn_etf",
        technical_score=32,
        fundamental_score=55,
        catalyst_score=12,
        relative_score=45,
        risk_score=58,
        macro_reverse=False,
        trade_state="观察为主",
        direction="观望",
        position="先按观察仓理解，不预设正式建仓",
        price_percentile_1y=1.0,
        rsi=76.4,
        sentiment_index=97.5,
        false_break_kind="bullish_false_break",
        divergence_signal="bearish",
        near_pressure=True,
        phase_label="强势整理",
    )

    assert crowded_watch["family_code"] == "watch"
    assert crowded_watch["code"] == "watch_crowded_mainline_consolidation"
    assert crowded_watch["setup_code"] == "crowded_mainline_consolidation"
    assert crowded_watch["setup_label"] == "高位拥挤主线分歧"
    assert "高位拥挤主线" in crowded_watch["style"]
    assert crowded_watch["expression_contract"] == "horizon_expression.v1"
    assert "高位拥挤主线分歧" in crowded_watch["expression_prompt_hint"]
    assert "修复早期" in crowded_watch["expression_forbidden_terms"]
    assert "修复早期" not in crowded_watch["fit_reason"]


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


def test_action_guidance_hint_softens_watch_language_for_missing_catalyst_signals():
    analysis = {
        "dimensions": {
            "catalyst": {
                "score": 0,
                "factors": [
                    {"name": "政策催化", "signal": "近 7 日直接政策情报偏弱", "display_score": "0/30"},
                ],
            }
        }
    }

    hint = _action_guidance_hint(analysis)

    assert hint["fit"] == "眼下更卡在催化面还缺新增直接情报确认。"
    assert "还停在" not in hint["fit"]


def test_build_stock_pool_warns_when_cn_snapshot_missing_required_columns(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_stock_realtime",
        lambda self: pd.DataFrame([{"代码": "000001", "最新价": 10.0}]),  # noqa: ARG005
    )
    pool, warnings = build_stock_pool({}, market="cn")
    assert pool == []
    assert any("A 股实时快照缺少必要列" in warning for warning in warnings)


def test_build_stock_pool_prefers_cached_cn_snapshot_when_runtime_flag_enabled(monkeypatch):
    cached_snapshot = pd.DataFrame(
        [
            {
                "代码": "300274",
                "名称": "阳光电源",
                "成交额": 180_000_000.0,
                "总市值": 20_000_000_000.0,
                "行业": "电气设备",
            }
        ]
    )

    monkeypatch.setattr(ChinaMarketCollector, "get_cached_stock_realtime_snapshot", lambda self: cached_snapshot.copy())  # noqa: ARG005
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_stock_realtime",
        lambda self: (_ for _ in ()).throw(AssertionError("live stock realtime should be skipped")),
    )
    monkeypatch.setattr("src.processors.opportunity_engine.detect_asset_type", lambda symbol, config: "cn_stock")

    pool, warnings = build_stock_pool({"stock_pool_prefer_cached_realtime_runtime": True}, market="cn", max_candidates=5)

    assert warnings == []
    assert [item.symbol for item in pool] == ["300274"]


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


def test_build_stock_pool_preserves_sector_breadth_before_hitting_candidate_cap(monkeypatch):
    monkeypatch.setattr(
        ChinaMarketCollector,
        "get_stock_realtime",
        lambda self: pd.DataFrame(  # noqa: ARG005
            [
                {"代码": "300001", "名称": "半导体一号", "成交额": 350_000_000.0, "总市值": 18_000_000_000.0, "行业": "半导体"},
                {"代码": "300002", "名称": "通信二号", "成交额": 320_000_000.0, "总市值": 17_000_000_000.0, "行业": "通信设备"},
                {"代码": "300003", "名称": "软件三号", "成交额": 300_000_000.0, "总市值": 16_000_000_000.0, "行业": "软件服务"},
                {"代码": "300004", "名称": "电子四号", "成交额": 280_000_000.0, "总市值": 15_000_000_000.0, "行业": "元器件"},
                {"代码": "600001", "名称": "创新药一号", "成交额": 180_000_000.0, "总市值": 12_000_000_000.0, "行业": "化学制药"},
                {"代码": "600002", "名称": "食品一号", "成交额": 170_000_000.0, "总市值": 11_000_000_000.0, "行业": "食品饮料"},
            ]
        ),
    )
    monkeypatch.setattr("src.processors.opportunity_engine.detect_asset_type", lambda symbol, config: "cn_stock")

    pool, warnings = build_stock_pool({}, market="cn", max_candidates=4)

    assert warnings == []
    assert len(pool) == 4
    sectors = [item.sector for item in pool]
    assert sectors.count("科技") == 2
    assert "医药" in sectors
    assert "消费" in sectors


def test_discover_stock_opportunities_includes_watch_positive(monkeypatch):
    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", lambda config, relevant_asset_types=None: {})  # noqa: ARG005
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_stock_pool",
        lambda config, market="all", sector_filter="", max_candidates=60: (  # noqa: ARG005
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


def test_discover_stock_opportunities_keeps_excluded_observe_candidates_for_fallback(monkeypatch):
    monkeypatch.setattr("src.processors.opportunity_engine.build_market_context", lambda config, relevant_asset_types=None: {})  # noqa: ARG005
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_stock_pool",
        lambda config, market="all", sector_filter="", max_candidates=60: (  # noqa: ARG005
            [
                type(
                    "PoolItemStub",
                    (),
                    {
                        "symbol": "300274",
                        "asset_type": "cn_stock",
                        "name": "阳光电源",
                        "sector": "新能源",
                        "chain_nodes": ["光伏主链"],
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
            "name": "阳光电源",
            "asset_type": asset_type,
            "rating": {"rank": 0, "label": "无信号", "stars": "—"},
            "dimensions": {
                "technical": {"score": 28},
                "fundamental": {"score": 62},
                "catalyst": {"score": 18},
                "relative_strength": {"score": 56},
                "chips": {"score": 0},
                "risk": {"score": 72},
                "seasonality": {"score": 0},
                "macro": {"score": 25},
            },
            "excluded": True,
        },
    )

    payload = discover_stock_opportunities({}, top_n=5, market="cn")

    assert payload["passed_pool"] == 0
    assert len(payload["coverage_analyses"]) == 1
    assert payload["top"][0]["symbol"] == "300274"
    assert payload["watch_positive"][0]["symbol"] == "300274"


def test_discover_stock_opportunities_keeps_specific_day_theme_ahead_of_off_theme_formal_pick(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_market_context",
        lambda config, relevant_asset_types=None: {"day_theme": {"label": "硬科技 / AI硬件链"}},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine.build_stock_pool",
        lambda config, market="all", sector_filter="", max_candidates=60: (  # noqa: ARG005
            [
                type(
                    "PoolItemStub",
                    (),
                    {
                        "symbol": "600989",
                        "asset_type": "cn_stock",
                        "name": "宝丰能源",
                        "sector": "能源",
                        "chain_nodes": ["煤炭"],
                        "region": "CN",
                        "in_watchlist": False,
                        "metadata": {"industry": "煤炭"},
                    },
                )(),
                type(
                    "PoolItemStub",
                    (),
                    {
                        "symbol": "300308",
                        "asset_type": "cn_stock",
                        "name": "中际旭创",
                        "sector": "科技",
                        "chain_nodes": ["光模块", "CPO"],
                        "region": "CN",
                        "in_watchlist": False,
                        "metadata": {"industry": "通信设备"},
                    },
                )(),
            ],
            [],
        ),
    )

    def _analysis(symbol, asset_type, config, context=None, metadata_override=None):  # noqa: ARG001
        if symbol == "600989":
            return {
                "symbol": symbol,
                "name": "宝丰能源",
                "asset_type": asset_type,
                "metadata": {"sector": "能源", "industry": "煤炭"},
                "rating": {"rank": 3, "label": "较强机会", "stars": "⭐⭐⭐"},
                "dimensions": {
                    "technical": {"score": 40},
                    "fundamental": {"score": 72},
                    "catalyst": {"score": 51},
                    "relative_strength": {"score": 50},
                    "chips": {"score": 20},
                    "risk": {"score": 32},
                    "seasonality": {"score": 10},
                    "macro": {"score": 24},
                },
                "excluded": False,
            }
        return {
            "symbol": symbol,
            "name": "中际旭创",
            "asset_type": asset_type,
            "metadata": {"sector": "科技", "industry": "通信设备", "chain_nodes": ["光模块", "CPO"]},
            "rating": {"rank": 1, "label": "有信号但不充分", "stars": "⭐"},
            "dimensions": {
                "technical": {"score": 38},
                "fundamental": {"score": 60},
                "catalyst": {"score": 16},
                "relative_strength": {"score": 64},
                "chips": {"score": 70},
                "risk": {"score": 15},
                "seasonality": {"score": 10},
                "macro": {"score": 24},
            },
            "excluded": True,
            "exclusion_reasons": ["个股估值处于极高区间"],
        }

    monkeypatch.setattr("src.processors.opportunity_engine.analyze_opportunity", _analysis)

    payload = discover_stock_opportunities({}, top_n=5, market="cn")

    assert payload["passed_pool"] == 1
    assert payload["top"][0]["symbol"] == "300308"
    assert payload["top"][0]["excluded"] is True
    assert payload["selection_context"]["theme_gate_applied"] is True
    assert "不退到非主线" in payload["selection_context"]["theme_gate_reason"]


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
    assert [item["symbol"] for item in payload["top"]] == ["518880"]
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


def test_j5_fund_factor_pro_snapshot_scores_supportively():
    """fund_factor_pro 技术快照可用时，应生成场内基金技术状态因子并给正分。"""
    from src.processors.opportunity_engine import _j5_etf_fund_factors
    fp = _make_etf_fund_profile(benchmark="沪深300", sector="宽基")
    fp["fund_factor_snapshot"] = {
        "trend_label": "趋势偏强",
        "momentum_label": "动能改善",
        "latest_date": "2026-04-01",
        "detail": "收盘 4.05 / MA20 4.01 / MA60 3.95",
    }
    factors, raw, available = _j5_etf_fund_factors("cn_etf", {}, fp)
    fund_factor = next((f for f in factors if f["name"] == "场内基金技术状态"), None)
    assert fund_factor is not None
    assert fund_factor["factor_id"] == "j5_fund_factor_pro"
    assert fund_factor["awarded"] == 10
    assert "趋势偏强" in fund_factor["signal"]


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

    first = _chips_dimension("300308", "cn_stock", metadata, context, {})
    second = _chips_dimension("300308", "cn_stock", metadata, context, {})

    assert len(snapshot_calls) == 0
    assert len(proxy_calls) == 0
    for dimension in (first, second):
        factor_map = {factor["name"]: factor for factor in dimension["factors"]}
        assert factor_map["机构集中度代理"]["display_score"] == "不适用"
        assert factor_map["机构集中度代理"]["signal"] == "个股主链不适用"


def test_valuation_keywords_drop_generic_ai_terms_for_semiconductor_etf() -> None:
    metadata = {"name": "半导体ETF", "sector": "科技", "chain_nodes": ["AI算力", "半导体"]}
    fund_profile = {
        "overview": {"业绩比较基准": "中证全指半导体产品与设备指数收益率"},
        "industry_allocation": [{"行业类别": "半导体"}],
        "top_holdings": [
            {"股票名称": "寒武纪"},
            {"股票名称": "中芯国际"},
            {"股票名称": "北方华创"},
        ],
    }

    keywords = _valuation_keywords(metadata, "cn_etf", fund_profile)

    assert "半导体" in keywords
    assert "芯片" in keywords
    assert "人工智能" not in keywords


def test_build_narrative_uses_playbook_label_for_semiconductor_when_day_theme_is_generic() -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=80, freq="B"),
            "open": np.linspace(1.2, 1.6, 80),
            "high": np.linspace(1.22, 1.62, 80),
            "low": np.linspace(1.18, 1.58, 80),
            "close": np.linspace(1.2, 1.6, 80),
            "volume": [10_000_000] * 80,
            "amount": [100_000_000.0] * 80,
        }
    )
    dimensions = {
        "technical": {"score": 28, "factors": [], "summary": "技术结构偏弱。"},
        "fundamental": {
            "score": 61,
            "valuation_snapshot": {"pe_ttm": 92.4},
            "valuation_note": "当前使用最接近的主题指数代理。",
            "factors": [],
        },
        "catalyst": {"score": 0, "factors": []},
        "relative_strength": {"score": 0, "factors": []},
        "risk": {"score": 35, "factors": []},
        "macro": {"score": 3, "macro_reverse": True, "factors": []},
    }
    technical = {"rsi": {"RSI": 47.0}, "ma_system": {"mas": {"MA20": 1.55, "MA60": 1.48}}, "fibonacci": {"levels": {}}}
    action = {
        "direction": "回避",
        "position": "暂不出手",
        "entry": "技术结构偏弱，等 MA20 / MA60 方向向上拐头后再考虑介入时机",
        "stop": "跌破 1.394 或主线/催化失效时重新评估",
        "target": "先看前高/近 60 日高点 1.787 附近的承压与突破情况",
        "timeframe": "等待更好窗口",
        "horizon": {"label": "观察期", "fit_reason": "技术结构还在修复早期，先等趋势确认比仓促出手更重要。"},
    }
    context = {"day_theme": {"label": "背景宏观主导"}, "regime": {"current_regime": "deflation"}}
    metadata = {"symbol": "512480", "name": "半导体ETF", "sector": "科技", "chain_nodes": ["半导体"]}
    fund_profile = {"overview": {"业绩比较基准": "中证全指半导体产品与设备指数收益率"}}

    narrative = _build_narrative(
        {"symbol": "512480"},
        metadata,
        "cn_etf",
        history,
        {"price_percentile_1y": 0.68},
        dimensions,
        technical,
        action,
        context,
        fund_profile=fund_profile,
    )

    assert "半导体" in narrative["summary_lines"][0]
    assert "背景宏观主导" not in narrative["summary_lines"][0]
    assert "半导体" in narrative["watch_points"][-1]
    assert "背景宏观主导" not in narrative["watch_points"][-1]
    assert "半导体" in narrative["risk_points"]["fundamental"]
    assert "半导体" in narrative["validation_points"][-1]["bull"]


def test_discover_helpers_prefer_subject_theme_over_market_day_theme() -> None:
    analysis = {
        "symbol": "300274",
        "name": "阳光电源",
        "day_theme": {"label": "背景宏观主导"},
        "metadata": {
            "sector": "电力设备",
            "industry_framework_label": "光伏主链",
            "chain_nodes": ["光伏主链", "储能", "电网设备"],
        },
        "theme_playbook": {
            "key": "solar_mainchain",
            "label": "光伏主链",
            "playbook_level": "theme",
            "hard_sector_label": "电力设备 / 新能源设备",
        },
        "dimensions": {
            "technical": {"score": 46, "summary": "技术结构仍在修复。"},
            "catalyst": {"score": 52, "summary": "产业链情报开始改善。"},
            "relative_strength": {"score": 48, "summary": "相对强弱仍需继续确认。"},
            "risk": {"score": 58, "summary": "风险暂时可控。"},
            "macro": {"score": 26},
        },
        "rating": {"rank": 1, "label": "1星"},
        "action": {"direction": "观察为主"},
        "narrative": {"phase": {"label": "下行修复"}},
    }

    lines = _discover_today_reason_lines(
        analysis,
        driver_type="主线驱动",
        driver_reason="它的直接映射更靠近 `光伏主链`，也更符合 `背景宏观主导` 这层盘面背景，因此优先进入预筛。",
    )
    next_steps = _discover_next_step_commands(analysis)

    assert "今天把它捞出来，首先是因为 `光伏主链` 这条线在 `背景宏观主导` 背景下仍有观察价值。" in lines
    assert not any("`电力设备` 在 `背景宏观主导` 背景下仍有观察价值" in item for item in lines)
    assert any(step["command"] == "python -m src.commands.scan 300274" for step in next_steps)


def test_build_narrative_for_passive_cn_fund_deduplicates_passive_exposure_summary() -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=80, freq="B"),
            "open": np.linspace(1.16, 1.26, 80),
            "high": np.linspace(1.17, 1.27, 80),
            "low": np.linspace(1.15, 1.25, 80),
            "close": np.linspace(1.16, 1.26, 80),
            "volume": [10_000_000] * 80,
            "amount": [100_000_000.0] * 80,
        }
    )
    dimensions = {
        "technical": {"score": 26, "factors": [], "summary": "技术结构仍偏弱。"},
        "fundamental": {"score": 68, "factors": [], "valuation_snapshot": {"pe_ttm": 20.9}},
        "catalyst": {"score": 0, "factors": []},
        "relative_strength": {"score": 34, "factors": []},
        "risk": {"score": 75, "factors": []},
        "macro": {"score": 10, "macro_reverse": True, "factors": []},
    }
    technical = {
        "rsi": {"RSI": 39.8},
        "ma_system": {"mas": {"MA20": 1.188, "MA60": 1.203}},
        "fibonacci": {"levels": {}},
    }
    action = {
        "direction": "观察为主",
        "position": "暂不出手",
        "entry": "短线动能重启：MACD 重新金叉，且收盘站回 MA20 1.188 上方",
        "stop": "跌破关键支撑重新评估",
        "target": "先看 MA20 1.188 一带的承压",
        "timeframe": "观察期",
        "horizon": {"label": "观察期", "fit_reason": "技术结构还在修复早期，先等趋势确认比仓促出手更重要。"},
    }
    context = {"day_theme": {"label": "背景宏观主导"}, "regime": {"current_regime": "recovery"}}
    metadata = {"symbol": "022456", "name": "招商中证A500ETF联接C", "sector": "宽基", "is_passive_fund": True}
    fund_profile = {
        "overview": {"业绩比较基准": "中证A500指数收益率*95%+中国人民银行人民币活期存款利率(税后)*5%"},
        "style": {"summary": "这只基金更像在买`宽基`方向的被动暴露，当前标签是 `宽基主题 / 被动跟踪`。"},
    }

    narrative = _build_narrative(
        {"symbol": "022456"},
        metadata,
        "cn_fund",
        history,
        {"price_percentile_1y": 0.68},
        dimensions,
        technical,
        action,
        context,
        fund_profile=fund_profile,
    )

    assert narrative["drivers"]["macro"].count("被动暴露") == 1
    assert "当前标签是 `宽基主题 / 被动跟踪`" in narrative["drivers"]["macro"]


def test_build_narrative_prefers_standard_industry_framework_label_for_focus_exposure() -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=80, freq="B"),
            "open": np.linspace(45.0, 52.0, 80),
            "high": np.linspace(46.0, 53.0, 80),
            "low": np.linspace(44.0, 51.0, 80),
            "close": np.linspace(45.0, 52.0, 80),
            "volume": [8_000_000] * 80,
            "amount": [260_000_000.0] * 80,
        }
    )
    dimensions = {
        "technical": {"score": 48, "factors": [], "summary": "技术结构中性。"},
        "fundamental": {"score": 62, "factors": [], "valuation_snapshot": {"pe_ttm": 24.0}},
        "catalyst": {"score": 28, "factors": []},
        "relative_strength": {"score": 54, "factors": []},
        "chips": {"score": 20, "factors": []},
        "risk": {"score": 58, "factors": []},
        "macro": {"score": 16, "macro_reverse": False, "factors": []},
    }
    technical = {"rsi": {"RSI": 51.0}, "ma_system": {"mas": {"MA20": 50.0, "MA60": 47.0}}, "fibonacci": {"levels": {}}}
    action = {
        "direction": "观察为主",
        "position": "小仓观察",
        "entry": "等右侧确认",
        "stop": "跌破关键支撑重新评估",
        "target": "先看前高",
        "timeframe": "观察期",
        "horizon": {"label": "观察期", "fit_reason": "先等确认。"},
    }
    context = {"day_theme": {"label": "背景宏观主导"}, "regime": {"current_regime": "recovery"}}
    metadata = {
        "symbol": "300308",
        "name": "中际旭创",
        "sector": "科技",
        "chain_nodes": ["光模块"],
        "industry_framework_label": "通信设备",
    }

    narrative = _build_narrative(
        {"symbol": "300308"},
        metadata,
        "cn_stock",
        history,
        {"price_percentile_1y": 0.55},
        dimensions,
        technical,
        action,
        context,
    )

    assert "通信设备" in narrative["summary_lines"][0]
    assert "科技" not in narrative["summary_lines"][0]
    assert "通信设备" in narrative["risk_points"]["fundamental"]


def test_analysis_theme_playbook_context_prefers_structured_agriculture_over_market_theme_noise() -> None:
    metadata = {
        "symbol": "000792",
        "name": "盐湖股份",
        "sector": "农业",
        "industry": "农药化肥",
        "industry_framework_label": "粮食安全",
        "business_scope": "旅游业务 住宿服务 餐饮服务 再生资源回收",
    }
    playbook = _analysis_theme_playbook_context(
        metadata,
        {"day_theme": {"label": "硬科技 / AI硬件链"}},
        narrative={
            "summary_lines": [
                "总体来看，盐湖股份 的核心逻辑在于 硬科技 / AI硬件链 主线下的 粮食安全 暴露仍有跟踪价值；"
            ]
        },
        notes=["旅游业务 住宿服务 餐饮服务"],
    )

    assert playbook["key"] == "sector::agriculture"
    assert playbook["hard_sector_label"] == "农业 / 种植链"


def test_build_narrative_does_not_write_market_day_theme_as_core_logic_for_misaligned_stock() -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=80, freq="B"),
            "open": np.linspace(8.0, 9.0, 80),
            "high": np.linspace(8.1, 9.1, 80),
            "low": np.linspace(7.9, 8.9, 80),
            "close": np.linspace(8.0, 9.0, 80),
            "volume": [8_000_000] * 80,
            "amount": [120_000_000.0] * 80,
        }
    )
    dimensions = {
        "technical": {"score": 38, "factors": [], "summary": "技术结构偏弱。"},
        "fundamental": {"score": 58, "factors": [], "valuation_snapshot": {"pe_ttm": 18.0}},
        "catalyst": {"score": 12, "factors": []},
        "relative_strength": {"score": 36, "factors": []},
        "risk": {"score": 55, "factors": []},
        "macro": {"score": 18, "macro_reverse": False, "factors": []},
    }
    technical = {"rsi": {"RSI": 48.0}, "ma_system": {"mas": {"MA20": 8.8, "MA60": 8.4}}, "fibonacci": {"levels": {}}}
    action = {
        "direction": "观察为主",
        "position": "小仓观察",
        "entry": "等右侧确认",
        "stop": "跌破关键支撑重新评估",
        "target": "先看前高",
        "timeframe": "观察期",
        "horizon": {"label": "观察期", "fit_reason": "先等确认。"},
    }
    context = {"day_theme": {"label": "硬科技 / AI硬件链"}, "regime": {"current_regime": "recovery"}}
    metadata = {
        "symbol": "000792",
        "name": "盐湖股份",
        "sector": "农业",
        "industry": "农药化肥",
        "industry_framework_label": "粮食安全",
    }

    narrative = _build_narrative(
        {"symbol": "000792"},
        metadata,
        "cn_stock",
        history,
        {"price_percentile_1y": 0.46},
        dimensions,
        technical,
        action,
        context,
    )

    assert "粮食安全" in narrative["summary_lines"][0]
    assert "硬科技 / AI硬件链" not in narrative["summary_lines"][0]
    assert "硬科技 / AI硬件链" not in narrative["drivers"]["macro"]


def test_build_narrative_surfaces_real_chip_signals_for_cn_stock() -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=80, freq="B"),
            "open": np.linspace(95.0, 108.0, 80),
            "high": np.linspace(96.0, 109.0, 80),
            "low": np.linspace(94.0, 107.0, 80),
            "close": np.linspace(95.0, 108.0, 80),
            "volume": [10_000_000] * 80,
            "amount": [500_000_000.0] * 80,
        }
    )
    dimensions = {
        "technical": {"score": 55, "factors": [], "summary": "技术结构尚可。"},
        "fundamental": {"score": 62, "factors": [], "valuation_snapshot": {"pe_ttm": 28.0}},
        "catalyst": {"score": 30, "factors": []},
        "relative_strength": {"score": 58, "factors": []},
        "chips": {
            "score": 68,
            "factors": [
                {"name": "筹码胜率", "signal": "盈利筹码约 71.5%", "awarded": 12, "display_score": "12/20"},
                {"name": "平均成本位置", "signal": "现价相对加权平均成本 +5.4%（均价约 103.20 元）", "awarded": 15, "display_score": "15/15"},
                {"name": "套牢盘压力", "signal": "现价上方筹码约 21.0%", "awarded": 15, "display_score": "15/15"},
                {"name": "筹码密集区", "signal": "主筹码密集区约 104.00 元 / 单价位占比 18.5%", "awarded": 5, "display_score": "5/10"},
            ],
        },
        "risk": {"score": 66, "factors": []},
        "macro": {"score": 18, "macro_reverse": False, "factors": []},
    }
    technical = {"rsi": {"RSI": 58.0}, "ma_system": {"mas": {"MA20": 106.0, "MA60": 101.0}}, "fibonacci": {"levels": {}}}
    action = {
        "direction": "观察为主",
        "position": "小仓跟踪",
        "entry": "等价格继续确认",
        "stop": "跌破关键支撑重新评估",
        "target": "先看前高突破",
        "timeframe": "观察期",
        "horizon": {"label": "观察期", "fit_reason": "先等确认。"},
    }
    context = {"day_theme": {"label": "AI算力"}, "regime": {"current_regime": "recovery"}}
    metadata = {"symbol": "300308", "name": "中际旭创", "sector": "科技", "chain_nodes": ["光模块"]}

    narrative = _build_narrative(
        {"symbol": "300308"},
        metadata,
        "cn_stock",
        history,
        {"price_percentile_1y": 0.62},
        dimensions,
        technical,
        action,
        context,
    )

    assert "真实筹码分布已经开始配合价格" in narrative["drivers"]["flow"]
    assert any("真实筹码分布没有显示明显套牢盘压制" in item for item in narrative["positives"])
    assert "真实筹码结构是否继续改善" in narrative["watch_points"][3]


def test_build_narrative_surfaces_p1_stock_flow_and_crowding_signals() -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=80, freq="B"),
            "open": np.linspace(95.0, 108.0, 80),
            "high": np.linspace(96.0, 109.0, 80),
            "low": np.linspace(94.0, 107.0, 80),
            "close": np.linspace(95.0, 108.0, 80),
            "volume": [10_000_000] * 80,
            "amount": [500_000_000.0] * 80,
        }
    )
    dimensions = {
        "technical": {"score": 58, "factors": []},
        "fundamental": {"score": 62, "factors": [], "valuation_snapshot": {"pe_ttm": 28.0}},
        "catalyst": {
            "score": 44,
            "factors": [
                {"name": "龙虎榜/打板确认", "signal": "龙虎榜净买入 / 竞价高开", "awarded": 12, "display_score": "12/12"},
            ],
        },
        "relative_strength": {"score": 58, "factors": []},
        "chips": {
            "score": 64,
            "factors": [
                {"name": "机构资金承接", "signal": "个股主力净流入 1.60亿 / 近 5 日累计 1.90亿", "awarded": 15, "display_score": "15/15"},
                {"name": "两融拥挤度", "signal": "融资盘仍在升温，需防一致性交易", "awarded": None, "display_score": "观察提示"},
            ],
        },
        "risk": {
            "score": 45,
            "factors": [
                {"name": "两融拥挤", "signal": "融资盘升温明显，短线拥挤度偏高", "awarded": 0, "display_score": "-12"},
                {"name": "打板情绪风险", "signal": "情绪交易升温，需防打板过热", "awarded": 0, "display_score": "-5"},
            ],
        },
        "macro": {"score": 18, "macro_reverse": False, "factors": []},
    }
    technical = {"rsi": {"RSI": 58.0}, "ma_system": {"mas": {"MA20": 106.0, "MA60": 101.0}}, "fibonacci": {"levels": {}}}
    action = {
        "direction": "观察为主",
        "position": "小仓跟踪",
        "entry": "等价格继续确认",
        "stop": "跌破关键支撑重新评估",
        "target": "先看前高突破",
        "timeframe": "观察期",
        "horizon": {"label": "观察期", "fit_reason": "先等确认。"},
    }
    context = {"day_theme": {"label": "AI算力"}, "regime": {"current_regime": "recovery"}}
    metadata = {"symbol": "300308", "name": "中际旭创", "sector": "科技", "chain_nodes": ["光模块"]}

    narrative = _build_narrative(
        {"symbol": "300308"},
        metadata,
        "cn_stock",
        history,
        {"price_percentile_1y": 0.62},
        dimensions,
        technical,
        action,
        context,
    )

    assert "个股级资金流已经开始给出直接承接" in narrative["drivers"]["flow"]
    assert "龙虎榜净买入 / 竞价高开" in narrative["drivers"]["flow"]
    assert "两融/打板情绪已经开始升温" in narrative["contradiction"]
    assert any("个股/主题资金流开始给出承接" in item for item in narrative["positives"])
    assert any("两融资金正在升温" in item for item in narrative["cautions"])
    assert any("龙虎榜/打板结构提示情绪交易风险抬升" in item for item in narrative["cautions"])
    assert "两融拥挤度是否降温" in narrative["watch_points"][3]
    assert "打板/龙虎榜结构提示短线情绪交易偏热" in narrative["risk_points"]["crowding"]


def test_build_narrative_cn_stock_softens_thesis_when_direct_catalyst_coverage_is_degraded() -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=80, freq="B"),
            "open": np.linspace(7.8, 8.5, 80),
            "high": np.linspace(7.9, 8.6, 80),
            "low": np.linspace(7.7, 8.4, 80),
            "close": np.linspace(7.8, 8.5, 80),
            "volume": [8_000_000] * 80,
            "amount": [180_000_000.0] * 80,
        }
    )
    dimensions = {
        "technical": {"score": 33, "factors": [], "summary": "技术结构仍偏弱。"},
        "fundamental": {"score": 12, "factors": [], "valuation_snapshot": {"pe_ttm": 68.0}},
        "catalyst": {
            "score": 0,
            "factors": [],
            "coverage": {
                "diagnosis": "proxy_degraded",
                "high_confidence_company_news": False,
                "effective_structured_event": False,
            },
        },
        "relative_strength": {"score": 18, "factors": []},
        "risk": {"score": 36, "factors": []},
        "macro": {"score": 9, "macro_reverse": True, "factors": []},
    }
    technical = {"rsi": {"RSI": 42.0}, "ma_system": {"mas": {"MA20": 8.2, "MA60": 7.9}}, "fibonacci": {"levels": {}}}
    action = {
        "direction": "观察为主",
        "position": "暂不出手",
        "entry": "等确认",
        "stop": "跌破关键支撑重新评估",
        "target": "先看近端压力",
        "timeframe": "观察期",
        "horizon": {"label": "观察期", "fit_reason": "先等确认。"},
    }
    context = {"day_theme": {"label": "粮食安全"}, "regime": {"current_regime": "recovery"}}
    metadata = {"symbol": "600313", "name": "农发种业", "sector": "农业", "chain_nodes": ["种业"]}

    narrative = _build_narrative(
        {"symbol": "600313"},
        metadata,
        "cn_stock",
        history,
        {"price_percentile_1y": 0.84},
        dimensions,
        technical,
        action,
        context,
    )

    assert "个股级新增证据还不够" in narrative["headline"]
    assert "个股级新增证据还不够" in narrative["contradiction"]
    assert "个股主力净流入/行业主力净流入" in narrative["validation_points"][3]["judge"]


def test_market_event_rows_from_context_include_broker_recommend_signal(monkeypatch):
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_drivers",
        lambda context, config: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_industry_index_snapshot",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_index_topic_bundle",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._matched_sector_spot_row",
        lambda metadata, drivers: (None, pd.DataFrame(), ""),  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._hot_rank_snapshot",
        lambda metadata, drivers: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_chip_snapshot",
        lambda metadata, context, history=None: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_theme_membership_snapshot",
        lambda metadata, context: {"items": []},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {"components": {}, "active_st": False, "active_alert_count": 0, "high_shock_count": 0},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_capital_flow_snapshot",
        lambda metadata, context: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_broker_recommend_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "matched",
            "is_fresh": True,
            "latest_broker_count": 4,
            "broker_delta": 2,
            "crowding_level": "medium",
            "detail": "2026-04 命中 4 家券商月度金股推荐，较上月增加 2 家。",
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_margin_snapshot",
        lambda metadata, context: {"is_fresh": False, "crowding_level": ""},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_board_action_snapshot",
        lambda metadata, context: {"is_fresh": False},  # noqa: ARG005
    )

    rows = _market_event_rows_from_context(
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技"},
        {"config": {}, "as_of": "2026-04-01"},
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "卖方共识升温：中际旭创 本月获 4 家券商金股推荐" in joined
    assert "卖方共识专题" in joined


def test_market_event_rows_from_context_include_tdx_structure_and_auxiliary_layers(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_drivers",
        lambda context, config: {  # noqa: ARG005
            "tdx_board": pd.DataFrame(
                [
                    {"名称": "中际旭创", "板块名称": "通信设备", "涨跌幅": 3.6},
                ]
            ),
            "tdx_style": pd.DataFrame(
                [
                    {"名称": "中际旭创", "风格": "进攻"},
                ]
            ),
            "tdx_region": pd.DataFrame(
                [
                    {"名称": "中际旭创", "地区": "CN"},
                ]
            ),
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_industry_index_snapshot",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_index_topic_bundle",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._matched_sector_spot_row",
        lambda metadata, drivers: (None, pd.DataFrame(), ""),  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._hot_rank_snapshot",
        lambda metadata, drivers: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_chip_snapshot",
        lambda metadata, context, history=None: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_theme_membership_snapshot",
        lambda metadata, context: {"items": []},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {"components": {}, "active_st": False, "active_alert_count": 0, "high_shock_count": 0},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_capital_flow_snapshot",
        lambda metadata, context: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_broker_recommend_snapshot",
        lambda metadata, context: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_margin_snapshot",
        lambda metadata, context: {"is_fresh": False, "crowding_level": ""},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_board_action_snapshot",
        lambda metadata, context: {"is_fresh": False},  # noqa: ARG005
    )

    rows = _market_event_rows_from_context(
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技"},
        {"config": {}, "as_of": "2026-04-01"},
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "TDX 结构框架：中际旭创 通信设备 / 进攻 / CN（+3.60%）" in joined
    assert "标准结构归因" in joined
    assert "TDX结构专题" in joined


def test_market_event_rows_from_context_include_dc_structure_layer(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_drivers",
        lambda context, config: {  # noqa: ARG005
            "dc_board": pd.DataFrame(
                [
                    {"名称": "中际旭创", "板块名称": "光通信", "涨跌幅": 2.8},
                ]
            ),
            "dc_style": pd.DataFrame(
                [
                    {"名称": "中际旭创", "风格": "进攻"},
                ]
            ),
            "dc_region": pd.DataFrame(
                [
                    {"名称": "中际旭创", "地区": "CN"},
                ]
            ),
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_industry_index_snapshot",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_index_topic_bundle",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._matched_sector_spot_row",
        lambda metadata, drivers: (None, pd.DataFrame(), ""),  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._hot_rank_snapshot",
        lambda metadata, drivers: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_chip_snapshot",
        lambda metadata, context, history=None: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_theme_membership_snapshot",
        lambda metadata, context: {"items": []},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {"components": {}, "active_st": False, "active_alert_count": 0, "high_shock_count": 0},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_capital_flow_snapshot",
        lambda metadata, context: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_broker_recommend_snapshot",
        lambda metadata, context: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_margin_snapshot",
        lambda metadata, context: {"is_fresh": False, "crowding_level": ""},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_board_action_snapshot",
        lambda metadata, context: {"is_fresh": False},  # noqa: ARG005
    )

    rows = _market_event_rows_from_context(
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技"},
        {"config": {}, "as_of": "2026-04-01"},
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "DC 结构框架：中际旭创 光通信 / 进攻 / CN（+2.80%）" in joined
    assert "DC结构专题" in joined
    assert "标准结构归因" in joined


def test_market_event_rows_from_context_include_ccass_hold_auxiliary_layer(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_drivers",
        lambda context, config: {  # noqa: ARG005
            "ccass_hold": pd.DataFrame(
                [
                    {"名称": "小米集团-W", "持股市值": 12_300_000.0},
                ]
            ),
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_industry_index_snapshot",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_index_topic_bundle",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._matched_sector_spot_row",
        lambda metadata, drivers: (None, pd.DataFrame(), ""),  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._hot_rank_snapshot",
        lambda metadata, drivers: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_chip_snapshot",
        lambda metadata, context, history=None: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_theme_membership_snapshot",
        lambda metadata, context: {"items": []},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {"components": {}, "active_st": False, "active_alert_count": 0, "high_shock_count": 0},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_capital_flow_snapshot",
        lambda metadata, context: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_broker_recommend_snapshot",
        lambda metadata, context: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_margin_snapshot",
        lambda metadata, context: {"is_fresh": False, "crowding_level": ""},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_board_action_snapshot",
        lambda metadata, context: {"is_fresh": False},  # noqa: ARG005
    )

    rows = _market_event_rows_from_context(
        {"symbol": "00700", "name": "小米集团-W", "asset_type": "hk", "sector": "科技"},
        {"config": {}, "as_of": "2026-04-01"},
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "港股辅助层：小米集团-W" in joined
    assert "港股/短线辅助" in joined
    assert "CCASS持股统计" in joined
    assert "可转债辅助层" not in joined


def test_market_event_rows_from_context_include_report_rc_layer(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_drivers",
        lambda context, config: {  # noqa: ARG005
            "report_rc": pd.DataFrame(
                [
                    {"名称": "中际旭创", "最新评级": "买入", "机构": "中信证券", "报告标题": "光模块景气延续"},
                ]
            ),
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_industry_index_snapshot",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_index_topic_bundle",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._matched_sector_spot_row",
        lambda metadata, drivers: (None, pd.DataFrame(), ""),  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._hot_rank_snapshot",
        lambda metadata, drivers: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_chip_snapshot",
        lambda metadata, context, history=None: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_theme_membership_snapshot",
        lambda metadata, context: {"items": []},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {"components": {}, "active_st": False, "active_alert_count": 0, "high_shock_count": 0},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_capital_flow_snapshot",
        lambda metadata, context: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_broker_recommend_snapshot",
        lambda metadata, context: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_margin_snapshot",
        lambda metadata, context: {"is_fresh": False, "crowding_level": ""},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_board_action_snapshot",
        lambda metadata, context: {"is_fresh": False},  # noqa: ARG005
    )

    rows = _market_event_rows_from_context(
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技"},
        {"config": {}, "as_of": "2026-04-01"},
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "研报辅助：中际旭创 买入" in joined
    assert "研报辅助层" in joined
    assert "研报评级/研究报告" in joined


def test_market_event_rows_from_context_include_convertible_auxiliary_layer(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_drivers",
        lambda context, config: {  # noqa: ARG005
            "cb_issue": pd.DataFrame(
                [
                    {"名称": "宁德时代", "可转债简称": "宁德转债", "余额": 1_230_000_000.0},
                ]
            ),
            "cb_share": pd.DataFrame(
                [
                    {"名称": "宁德时代", "转债简称": "宁德转债", "剩余规模": 1_020_000_000.0},
                ]
            ),
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_industry_index_snapshot",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._context_index_topic_bundle",
        lambda metadata, context, fund_profile=None: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._matched_sector_spot_row",
        lambda metadata, drivers: (None, pd.DataFrame(), ""),  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._hot_rank_snapshot",
        lambda metadata, drivers: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_chip_snapshot",
        lambda metadata, context, history=None: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_theme_membership_snapshot",
        lambda metadata, context: {"items": []},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {"components": {}, "active_st": False, "active_alert_count": 0, "high_shock_count": 0},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_capital_flow_snapshot",
        lambda metadata, context: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_broker_recommend_snapshot",
        lambda metadata, context: {"status": "empty", "is_fresh": False},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_margin_snapshot",
        lambda metadata, context: {"is_fresh": False, "crowding_level": ""},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_board_action_snapshot",
        lambda metadata, context: {"is_fresh": False},  # noqa: ARG005
    )

    rows = _market_event_rows_from_context(
        {"symbol": "300750", "name": "宁德时代", "asset_type": "cn_stock", "sector": "电池"},
        {"config": {}, "as_of": "2026-04-01"},
    )

    joined = "\n".join(" | ".join(str(part) for part in row) for row in rows)
    assert "可转债辅助层：宁德时代" in joined
    assert "转债辅助层" in joined
    assert "港股辅助层：小米集团-W" not in joined


def test_trim_market_event_rows_keeps_broker_and_irm_rows_ahead_of_generic_frameworks() -> None:
    rows = [
        ["2026-04-03", "标准行业框架：医药", "申万行业框架", "低", "医疗保健", "", "标准行业归因", "行业当前承压。"],
        ["2026-04-03", "相关指数框架：创新药", "相关指数/框架", "低", "创新药", "", "行业/指数映射", "先看指数主链。"],
        ["2026-04-03", "指数技术面：创新药 趋势偏弱", "指数技术面", "低", "创新药", "", "技术确认", "趋势仍弱。"],
        ["2026-04-03", "A股主题成员：药明康德 属于 创新药", "同花顺主题成分", "低", "创新药", "", "主线归因", "先按主题成员解释。"],
        ["2026-04-03", "卖方共识非当期：药明康德 最新券商金股仍停在 2026-02", "卖方共识专题", "低", "药明康德", "", "卖方共识观察", "最近一次命中 3 家券商推荐。"],
        ["2026-04-03", "互动易确认：公司回应海外订单进展", "互动易/投资者关系", "中", "药明康德", "", "管理层口径确认", "先按补充证据处理，不替代正式公告。"],
    ]

    trimmed = _trim_market_event_rows(rows, limit=5)
    joined = "\n".join(" | ".join(str(part) for part in row) for row in trimmed)

    assert "卖方共识非当期：药明康德 最新券商金股仍停在 2026-02" in joined
    assert "互动易确认：公司回应海外订单进展" in joined
    assert not (
        "相关指数框架：创新药" in joined and "指数技术面：创新药 趋势偏弱" in joined
    )


def test_catalyst_dimension_surfaces_broker_recommend_factor(monkeypatch):
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_repurchase", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_dividend", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_disclosure_dates", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(ValuationCollector, "get_cn_stock_holder_trades", lambda self, symbol: [])  # noqa: ARG005
    monkeypatch.setattr(NewsCollector, "get_stock_news", lambda self, symbol, limit=10: [])  # noqa: ARG005
    monkeypatch.setattr(NewsCollector, "search_by_keyword_groups", lambda self, groups, preferred_sources=None, limit=6, recent_days=7: [])  # noqa: ARG005
    monkeypatch.setattr(NewsCollector, "search_by_keywords", lambda self, keywords, preferred_sources=None, limit=6, recent_days=7: [])  # noqa: ARG005
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_broker_recommend_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "matched",
            "is_fresh": True,
            "latest_broker_count": 5,
            "broker_delta": 2,
            "detail": "2026-04 命中 5 家券商月度金股推荐，较上月增加 2 家。",
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_board_action_snapshot",
        lambda metadata, context: {},  # noqa: ARG005
    )

    dimension = _catalyst_dimension(
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock", "sector": "科技", "chain_nodes": []},
        {"config": {}, "news_report": {"mode": "live", "all_items": []}, "events": []},
    )

    factor = next(item for item in dimension["factors"] if item["name"] == "卖方覆盖/一致预期")
    assert factor["awarded"] == 8
    assert "5 家券商金股推荐" in factor["signal"]


def test_risk_dimension_penalizes_broker_recommend_crowding(monkeypatch):
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=80, freq="B"),
            "open": np.linspace(95.0, 108.0, 80),
            "high": np.linspace(96.0, 109.0, 80),
            "low": np.linspace(94.0, 107.0, 80),
            "close": np.linspace(95.0, 108.0, 80),
            "volume": [10_000_000] * 80,
            "amount": [500_000_000.0] * 80,
        }
    )
    asset_returns = history["close"].astype(float).pct_change().dropna()
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_regulatory_risk_snapshot",
        lambda metadata, context: {},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_margin_snapshot",
        lambda metadata, context: {"is_fresh": False, "crowding_level": ""},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_broker_recommend_snapshot",
        lambda metadata, context: {  # noqa: ARG005
            "status": "matched",
            "is_fresh": True,
            "latest_broker_count": 6,
            "consecutive_months": 4,
            "crowding_level": "high",
            "detail": "卖方月度金股覆盖已经连续 4 个月偏密。",
        },
    )
    monkeypatch.setattr(
        "src.processors.opportunity_engine._cn_stock_board_action_snapshot",
        lambda metadata, context, history=None: {},  # noqa: ARG005
    )

    dimension = _risk_dimension(
        "300308",
        "cn_stock",
        {"symbol": "300308", "name": "中际旭创", "asset_type": "cn_stock"},
        history,
        asset_returns,
        {"benchmark_returns": {"cn_stock": asset_returns}, "news_report": {"items": []}, "config": {}},
        None,
    )

    factors = {item["name"]: item for item in dimension["factors"]}
    assert factors["卖方一致预期过热"]["display_score"] == "-8"
    assert "一致预期过热" in factors["卖方一致预期过热"]["signal"]


def test_build_narrative_surfaces_broker_recommend_signals() -> None:
    history = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=80, freq="B"),
            "open": np.linspace(95.0, 108.0, 80),
            "high": np.linspace(96.0, 109.0, 80),
            "low": np.linspace(94.0, 107.0, 80),
            "close": np.linspace(95.0, 108.0, 80),
            "volume": [10_000_000] * 80,
            "amount": [500_000_000.0] * 80,
        }
    )
    dimensions = {
        "technical": {"score": 58, "factors": []},
        "fundamental": {"score": 62, "factors": [], "valuation_snapshot": {"pe_ttm": 28.0}},
        "catalyst": {
            "score": 46,
            "factors": [
                {"name": "卖方覆盖/一致预期", "signal": "本月 4 家券商金股推荐，较上月增加 2 家", "awarded": 8, "display_score": "8/8"},
                {"name": "龙虎榜/打板确认", "signal": "未命中明确龙虎榜/打板确认", "awarded": 0, "display_score": "信息项"},
            ],
        },
        "relative_strength": {"score": 58, "factors": []},
        "chips": {
            "score": 64,
            "factors": [
                {"name": "机构资金承接", "signal": "个股主力净流入 1.60亿 / 近 5 日累计 1.90亿", "awarded": 15, "display_score": "15/15"},
            ],
        },
        "risk": {
            "score": 42,
            "factors": [
                {"name": "卖方一致预期过热", "signal": "券商月度金股覆盖偏密，需防一致预期过热", "awarded": 0, "display_score": "-8"},
            ],
        },
        "macro": {"score": 18, "macro_reverse": False, "factors": []},
    }
    technical = {"rsi": {"RSI": 58.0}, "ma_system": {"mas": {"MA20": 106.0, "MA60": 101.0}}, "fibonacci": {"levels": {}}}
    action = {
        "direction": "观察为主",
        "position": "小仓跟踪",
        "entry": "等价格继续确认",
        "stop": "跌破关键支撑重新评估",
        "target": "先看前高突破",
        "timeframe": "观察期",
        "horizon": {"label": "观察期", "fit_reason": "先等确认。"},
    }
    context = {"day_theme": {"label": "AI算力"}, "regime": {"current_regime": "recovery"}}
    metadata = {"symbol": "300308", "name": "中际旭创", "sector": "科技", "chain_nodes": ["光模块"]}

    narrative = _build_narrative(
        {"symbol": "300308"},
        metadata,
        "cn_stock",
        history,
        {"price_percentile_1y": 0.62},
        dimensions,
        technical,
        action,
        context,
    )

    assert "卖方侧当前还能看到" in narrative["drivers"]["flow"]
    assert any("卖方覆盖没有掉线" in item for item in narrative["positives"])
    assert any("券商月度金股覆盖已经偏密" in item for item in narrative["cautions"])
    assert any("卖方覆盖是否继续扩散" in item for item in narrative["watch_points"])
    assert "券商月度金股覆盖已经偏密" in narrative["risk_points"]["crowding"]


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


def test_summarize_proxy_contracts_from_analyses_keeps_confidence_and_downgrade() -> None:
    analyses = [
        {
            "proxy_signals": {
                "social_sentiment": {
                    "aggregate": {
                        "interpretation": "情绪指数 62.0，讨论热度偏高，需防拥挤交易。",
                        "confidence_label": "中",
                        "limitations": ["这是价格和量能行为推导出的情绪代理，不是真实社媒抓取。"],
                        "downgrade_impact": "更适合提示拥挤线索，不适合单独作为买卖信号。",
                    }
                }
            }
        },
        {
            "proxy_signals": {
                "social_sentiment": {
                    "aggregate": {
                        "interpretation": "情绪指数 48.0，当前未出现极端一致预期。",
                        "confidence_label": "高",
                        "limitations": ["这是价格和量能行为推导出的情绪代理，不是真实社媒抓取。"],
                        "downgrade_impact": "更适合提示拥挤线索，不适合单独作为买卖信号。",
                    }
                }
            }
        },
    ]

    summary = summarize_proxy_contracts_from_analyses(
        analyses,
        market_proxy={
            "lines": ["成长相对黄金更强，资金风格偏 risk-on。"],
            "confidence_label": "中",
            "coverage_summary": "科技 / 黄金 / 国内 / 海外",
            "limitations": ["这是相对强弱代理，不是原始资金流向数据。"],
            "downgrade_impact": "可作为市场风格辅助证据，但不应单独决定交易动作。",
        },
    )

    assert summary["market_flow"]["confidence_label"] == "中"
    assert summary["market_flow"]["coverage_summary"] == "科技 / 黄金 / 国内 / 海外"
    assert summary["social_sentiment"]["covered"] == 2
    assert summary["social_sentiment"]["confidence_labels"] == {"中": 1, "高": 1}
    assert "市场风格代理" in summary["lines"][0]


def test_collect_fund_profile_times_out_to_disclosed_empty_snapshot(monkeypatch) -> None:
    class _SlowProfileCollector:
        def __init__(self, config=None):  # noqa: ARG002
            pass

        def collect_profile(self, symbol, asset_type="cn_fund", profile_mode="full"):  # noqa: ARG002
            time.sleep(0.05)
            return {"overview": {"基金简称": "不会命中"}}

    monkeypatch.setattr("src.processors.opportunity_engine.FundProfileCollector", _SlowProfileCollector)

    snapshot = _collect_fund_profile("021740", "cn_fund", {"fund_profile_timeout_seconds": 0.01})

    assert snapshot["timeout"] is True
    assert snapshot["overview"] == {}
    assert any("基金画像拉取超时" in item for item in snapshot["notes"])


def test_context_index_topic_bundle_times_out_to_blocked_disclosure(monkeypatch) -> None:
    class _SlowIndexCollector:
        def __init__(self, config=None):  # noqa: ARG002
            pass

        def get_index_bundle(self, **kwargs):  # noqa: ARG002
            time.sleep(0.05)
            return {"index_snapshot": {"index_name": "不会命中"}}

    monkeypatch.setattr("src.processors.opportunity_engine.IndexTopicCollector", _SlowIndexCollector)

    bundle = _context_index_topic_bundle(
        {"asset_type": "cn_fund", "symbol": "021740", "name": "前海开源黄金ETF联接C", "benchmark": "中证A500"},
        {"config": {"index_topic_bundle_timeout_seconds": 0.01}},
        fund_profile={},
    )

    assert bundle["fallback"] == "timeout"
    assert bundle["technical_snapshot"]["diagnosis"] == "timeout"
    assert "指数专题主链拉取超时" in bundle["technical_snapshot"]["disclosure"]


def test_context_index_topic_bundle_skips_unanchored_cn_stock(monkeypatch) -> None:
    class _FailCollector:
        def __init__(self, config=None):  # noqa: ARG002
            raise AssertionError("unanchored cn_stock should not instantiate IndexTopicCollector")

    monkeypatch.setattr("src.processors.opportunity_engine.IndexTopicCollector", _FailCollector)

    bundle = _context_index_topic_bundle(
        {
            "asset_type": "cn_stock",
            "symbol": "300308",
            "name": "中际旭创",
            "sector": "科技",
            "chain_nodes": ["AI算力", "半导体"],
        },
        {"config": {}},
        fund_profile={},
    )

    assert bundle["fallback"] == "not_applicable"
    assert bundle["technical_snapshot"]["diagnosis"] == "not_applicable"
    assert bundle["technical_snapshot"]["status"] == "skipped"
    assert "个股主链默认按板块/主题/行业行情理解" in bundle["technical_snapshot"]["disclosure"]
