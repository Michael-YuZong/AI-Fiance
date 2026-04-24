"""Tests for market drivers collector Tushare-first aggregation."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.collectors.market_drivers import MarketDriversCollector


def test_market_drivers_market_flow_prefers_tushare_moneyflow(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260310")

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "moneyflow":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": "20260310",
                        "buy_sm_amount": 100.0,
                        "sell_sm_amount": 90.0,
                        "buy_md_amount": 110.0,
                        "sell_md_amount": 100.0,
                        "buy_lg_amount": 140.0,
                        "sell_lg_amount": 120.0,
                        "buy_elg_amount": 150.0,
                        "sell_elg_amount": 130.0,
                    }
                ]
            )
        if api_name == "daily":
            return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20260310", "amount": 1_000.0}])
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    report = collector._market_flow(datetime(2026, 3, 10))
    frame = report["frame"]
    assert not frame.empty
    assert report["latest_date"] == "2026-03-10"
    assert float(frame.iloc[0]["超大单净流入-净额"]) == 200_000.0
    assert float(frame.iloc[0]["大单净流入-净额"]) == 200_000.0
    assert float(frame.iloc[0]["主力净流入-净额"]) == 400_000.0


def test_market_drivers_top10_normalizes_tushare_fields(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "hsgt_top10"
        assert kwargs.get("trade_date") == "20260310"
        return pd.DataFrame(
            [
                {
                    "trade_date": "20260310",
                    "ts_code": "600519.SH",
                    "name": "贵州茅台",
                    "close": 1500.0,
                    "change": 2.5,
                    "rank": 1,
                    "market_type": 1,
                    "amount": 3_200_000_000.0,
                    "net_amount": 500_000_000.0,
                    "buy": 1_850_000_000.0,
                    "sell": 1_350_000_000.0,
                }
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    monkeypatch.setattr("src.collectors.market_drivers.datetime", type("FrozenDateTime", (), {
        "now": staticmethod(lambda: datetime(2026, 3, 10)),
        "strptime": staticmethod(datetime.strptime),
    }))
    frame = collector._ts_northbound_top10()
    assert not frame.empty
    assert frame.loc[0, "日期"] == "2026-03-10"
    assert frame.loc[0, "代码"] == "600519"
    assert frame.loc[0, "市场"] == "沪股通"
    assert float(frame.loc[0, "净买额"]) == 500_000_000.0


def test_market_drivers_pledge_stat_normalizes_tushare_fields(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "pledge_stat"
        assert kwargs.get("end_date") == "20260310"
        return pd.DataFrame(
            [
                {
                    "end_date": "20260310",
                    "ts_code": "300750.SZ",
                    "name": "宁德时代",
                    "pledge_count": 2,
                    "unrest_pledge": 150.0,
                    "rest_pledge": 30.0,
                    "total_share": 2_448_907.12,
                    "pledge_ratio": 1.2,
                }
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    monkeypatch.setattr("src.collectors.market_drivers.datetime", type("FrozenDateTime", (), {"now": staticmethod(lambda: datetime(2026, 3, 10))}))
    frame = collector._ts_pledge_stat()
    assert not frame.empty
    assert frame.loc[0, "截止日期"] == "2026-03-10"
    assert frame.loc[0, "代码"] == "300750"
    assert float(frame.loc[0, "质押比例"]) == 1.2


def test_market_drivers_board_spot_prefers_tushare_ths(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260310")

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "ths_index":
            assert kwargs.get("type") == "I"
            return pd.DataFrame(
                [
                    {"ts_code": "881001.TI", "name": "半导体", "type": "I"},
                    {"ts_code": "881002.TI", "name": "电力设备", "type": "I"},
                ]
            )
        if api_name == "ths_daily":
            return pd.DataFrame(
                [
                    {"ts_code": "881001.TI", "trade_date": "20260310", "pct_change": 2.5, "amount": 120_000.0},
                    {"ts_code": "881002.TI", "trade_date": "20260310", "pct_change": -0.5, "amount": 80_000.0},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector._ts_board_spot("industry")
    assert list(frame["名称"]) == ["半导体", "电力设备"]
    assert float(frame.loc[0, "涨跌幅"]) == 2.5
    assert float(frame.loc[0, "成交额"]) == 120_000.0 * 1000.0


def test_market_drivers_stock_theme_membership_joins_member_and_board_strength(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260310")

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "ths_member":
            assert kwargs.get("con_code") == "300308.SZ"
            return pd.DataFrame(
                [
                    {"ts_code": "885976.TI", "con_code": "300308.SZ", "con_name": "中际旭创", "is_new": "Y"},
                    {"ts_code": "881001.TI", "con_code": "300308.SZ", "con_name": "中际旭创", "is_new": "Y"},
                ]
            )
        if api_name == "ths_index":
            if kwargs.get("type") == "N":
                return pd.DataFrame([{"ts_code": "885976.TI", "name": "AI算力", "type": "N"}])
            if kwargs.get("type") == "I":
                return pd.DataFrame([{"ts_code": "881001.TI", "name": "通信设备", "type": "I"}])
            return pd.DataFrame()
        if api_name == "ths_daily":
            return pd.DataFrame(
                [
                    {"ts_code": "885976.TI", "trade_date": "20260310", "pct_change": 6.4, "amount": 220_000.0},
                    {"ts_code": "881001.TI", "trade_date": "20260310", "pct_change": 2.1, "amount": 120_000.0},
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    report = collector.get_stock_theme_membership("300308", reference_date=datetime(2026, 3, 10, 18, 0, 0))

    assert report["diagnosis"] == "live"
    assert report["is_fresh"] is True
    assert report["items"][0]["board_name"] == "AI算力"
    assert report["items"][0]["board_type"] == "concept"
    assert report["items"][0]["pct_change"] == 6.4


def test_market_drivers_stock_theme_membership_skips_unknown_board_names(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260310")

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "ths_member":
            return pd.DataFrame(
                [
                    {"ts_code": "700044.TI", "con_code": "300308.SZ", "con_name": "中际旭创"},
                    {"ts_code": "886033.TI", "con_code": "300308.SZ", "con_name": "中际旭创"},
                    {"ts_code": "884262.TI", "con_code": "300308.SZ", "con_name": "中际旭创"},
                ]
            )
        if api_name == "ths_index":
            if kwargs.get("type") == "N":
                return pd.DataFrame([{"ts_code": "886033.TI", "name": "共封装光学(CPO)", "type": "N"}])
            if kwargs.get("type") == "I":
                return pd.DataFrame([{"ts_code": "884262.TI", "name": "通信网络设备及器件", "type": "I"}])
            return pd.DataFrame()
        if api_name == "ths_daily":
            return pd.DataFrame(
                [
                    {"ts_code": "700044.TI", "trade_date": "20260310", "pct_change": 6.8, "amount": 250_000.0},
                    {"ts_code": "886033.TI", "trade_date": "20260310", "pct_change": 3.6, "amount": 160_000.0},
                    {"ts_code": "884262.TI", "trade_date": "20260310", "pct_change": 3.2, "amount": 120_000.0},
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    report = collector.get_stock_theme_membership("300308", reference_date=datetime(2026, 3, 10, 18, 0, 0))

    assert [item["board_name"] for item in report["items"]] == ["共封装光学(CPO)", "通信网络设备及器件"]
    assert all(item["board_name"].lower() != "nan" for item in report["items"])


def test_market_drivers_stock_theme_membership_skips_broad_index_member_labels(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260310")

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "ths_member":
            return pd.DataFrame(
                [
                    {"ts_code": "885001.TI", "con_code": "603259.SH", "con_name": "药明康德"},
                    {"ts_code": "885002.TI", "con_code": "603259.SH", "con_name": "药明康德"},
                ]
            )
        if api_name == "ths_index":
            if kwargs.get("type") == "N":
                return pd.DataFrame(
                    [
                        {"ts_code": "885001.TI", "name": "上证50样本股", "type": "N"},
                        {"ts_code": "885002.TI", "name": "创新药", "type": "N"},
                    ]
                )
            return pd.DataFrame()
        if api_name == "ths_daily":
            return pd.DataFrame(
                [
                    {"ts_code": "885001.TI", "trade_date": "20260310", "pct_change": -0.8, "amount": 180_000.0},
                    {"ts_code": "885002.TI", "trade_date": "20260310", "pct_change": 2.6, "amount": 220_000.0},
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    report = collector.get_stock_theme_membership("603259", reference_date=datetime(2026, 3, 10, 18, 0, 0))

    assert [item["board_name"] for item in report["items"]] == ["创新药"]


def test_market_drivers_stock_theme_membership_handles_empty_board_reference_frames(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260310")

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "ths_member":
            return pd.DataFrame([{"ts_code": "700044.TI", "con_code": "300308.SZ", "con_name": "中际旭创"}])
        if api_name in {"ths_index", "ths_daily"}:
            return pd.DataFrame()
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    report = collector.get_stock_theme_membership("300308", reference_date=datetime(2026, 3, 10, 18, 0, 0))

    assert report["diagnosis"] == "live"
    assert report["status"] == "empty"
    assert report["items"] == []


def test_market_drivers_same_day_board_spot_skips_intraday_cache(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 24}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260401")
    monkeypatch.setattr(
        collector,
        "_ts_board_index_rows",
        lambda _board_type: pd.DataFrame([{"ts_code": "881001.TI", "名称": "半导体", "板块类型": "I"}]),
    )
    monkeypatch.setattr(
        "src.collectors.market_drivers.datetime",
        type("FrozenDateTime", (), {"now": staticmethod(lambda: datetime(2026, 4, 1))}),
    )

    cache_calls = {"load": 0, "save": 0}
    monkeypatch.setattr(collector, "_load_cache", lambda *args, **kwargs: cache_calls.__setitem__("load", cache_calls["load"] + 1))
    monkeypatch.setattr(collector, "_save_cache", lambda *args, **kwargs: cache_calls.__setitem__("save", cache_calls["save"] + 1))
    monkeypatch.setattr(
        collector,
        "_ts_call",
        lambda api_name, **kwargs: pd.DataFrame(
            [{"ts_code": "881001.TI", "trade_date": "20260401", "pct_change": 1.8, "amount": 100_000.0}]
        ) if api_name == "ths_daily" else pd.DataFrame(),
    )

    frame = collector._ts_board_spot("industry")

    assert not frame.empty
    assert cache_calls == {"load": 0, "save": 0}


def test_market_drivers_hot_rank_live_fetch_disables_cache(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 24}})
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        collector,
        "cached_call",
        lambda *args, **kwargs: captured.update(kwargs) or pd.DataFrame([{"名称": "测试", "代码": "000001"}]),
    )
    monkeypatch.setattr("src.collectors.market_drivers.ak", type("FakeAK", (), {"stock_hot_rank_em": staticmethod(lambda: pd.DataFrame())}))

    collector._hot_rank()

    assert captured["use_cache"] is False


def test_market_drivers_ts_hot_rank_filters_to_latest_a_share_snapshot(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260401")

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "ths_hot":
            return pd.DataFrame(
                [
                    {
                        "trade_date": "20260331",
                        "data_type": "热股",
                        "ts_code": "300308.SZ",
                        "ts_name": "中际旭创",
                        "rank": 3,
                        "pct_change": 8.0,
                        "rank_time": "2026-03-31 14:20:00",
                    },
                    {
                        "trade_date": "20260401",
                        "data_type": "热股",
                        "ts_code": "300308.SZ",
                        "ts_name": "中际旭创",
                        "rank": 2,
                        "pct_change": 12.5,
                        "rank_time": "2026-04-01 10:01:00",
                    },
                    {
                        "trade_date": "20260401",
                        "data_type": "热股",
                        "ts_code": "300308.SZ",
                        "ts_name": "中际旭创",
                        "rank": 1,
                        "pct_change": 12.5,
                        "rank_time": "2026-04-01 14:59:59",
                    },
                    {
                        "trade_date": "20260401",
                        "data_type": "美股",
                        "ts_code": None,
                        "ts_name": "英伟达",
                        "rank": 1,
                        "pct_change": 2.1,
                        "rank_time": "2026-04-01 09:00:00",
                    },
                ]
            )
        if api_name == "stock_basic":
            return pd.DataFrame([{"ts_code": "300308.SZ", "name": "中际旭创"}])
        if api_name == "daily":
            return pd.DataFrame([{"ts_code": "300308.SZ", "pct_chg": 12.5}])
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    frame = collector._ts_hot_rank()

    assert len(frame) == 1
    assert frame.loc[0, "股票名称"] == "中际旭创"
    assert frame.loc[0, "代码"] == "300308"
    assert frame.loc[0, "日期"] == "2026-04-01"
    assert float(frame.loc[0, "排名"]) == 1


def test_market_drivers_sector_flow_normalizes_tushare_moneyflow(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260310"])

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "moneyflow_ind_ths"
        return pd.DataFrame(
            [
                {
                    "trade_date": "20260310",
                    "name": "军工",
                    "pct_change": 1.8,
                    "net_amount": 25_000.0,
                    "net_amount_rate": 3.2,
                    "elg_net_amount": 8_000.0,
                    "lg_net_amount": 5_000.0,
                }
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector._ts_sector_fund_flow("industry")
    assert not frame.empty
    assert frame.loc[0, "名称"] == "军工"
    assert float(frame.loc[0, "今日主力净流入-净额"]) == 25_000.0 * 10_000.0
    assert float(frame.loc[0, "今日超大单净流入-净额"]) == 8_000.0 * 10_000.0


def test_market_drivers_northbound_industry_aggregates_tushare_hk_hold(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260309", "20260310"])

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "hk_hold" and kwargs.get("trade_date") == "20260310":
            return pd.DataFrame(
                [
                    {"ts_code": "600519.SH", "vol": 1200},
                    {"ts_code": "300750.SZ", "vol": 500},
                ]
            )
        if api_name == "hk_hold" and kwargs.get("trade_date") == "20260309":
            return pd.DataFrame(
                [
                    {"ts_code": "600519.SH", "vol": 1000},
                    {"ts_code": "300750.SZ", "vol": 450},
                ]
            )
        if api_name == "daily":
            return pd.DataFrame(
                [
                    {"ts_code": "600519.SH", "close": 1500.0},
                    {"ts_code": "300750.SZ", "close": 220.0},
                ]
            )
        if api_name == "stock_basic":
            return pd.DataFrame(
                [
                    {"ts_code": "600519.SH", "industry": "白酒"},
                    {"ts_code": "300750.SZ", "industry": "新能源设备"},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    report = collector._ts_northbound_industry(datetime(2026, 3, 10))
    frame = report["frame"]
    assert not frame.empty
    assert report["latest_date"] == "2026-03-10"
    assert frame.loc[0, "名称"] == "白酒"
    assert float(frame.loc[0, "北向资金今日增持估计-市值"]) == 200 * 1500.0


def test_market_drivers_collect_emits_freshness_reports_for_volatile_frames(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    monkeypatch.setattr(
        "src.collectors.market_drivers.ChinaMarketCollector",
        lambda _config: type(
            "StubCN",
            (),
            {
                "get_north_south_flow": staticmethod(lambda: pd.DataFrame()),
                "get_margin_trading": staticmethod(lambda: pd.DataFrame()),
            },
        )(),
    )
    monkeypatch.setattr(collector, "_market_flow", lambda as_of: {"frame": pd.DataFrame(), "latest_date": "2026-03-10", "is_fresh": True})
    monkeypatch.setattr(collector, "_ts_northbound_top10", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_pledge_stat", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_sector_fund_flow", lambda _kind: pd.DataFrame())
    monkeypatch.setattr(collector, "_sector_fund_flow", lambda _kind: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_northbound_industry", lambda as_of: collector._empty_rank_report())
    monkeypatch.setattr(collector, "_northbound_rank", lambda symbol, as_of: collector._empty_rank_report())
    monkeypatch.setattr(
        collector,
        "_ts_board_spot",
        lambda kind: pd.DataFrame([{"名称": "创新药", "涨跌幅": 7.8, "日期": "2026-03-10"}]) if kind == "concept" else pd.DataFrame([{"名称": "通信", "涨跌幅": 3.6, "日期": "2026-03-10"}]),
    )
    monkeypatch.setattr(collector, "_board_spot", lambda _name: (_ for _ in ()).throw(AssertionError("AKShare board spot should not be used when Tushare board spot is available")))
    monkeypatch.setattr(collector, "_ts_hot_rank", lambda: pd.DataFrame([{"股票名称": "新易盛", "涨跌幅": 12.5, "日期": "2026-03-10"}]))
    monkeypatch.setattr(collector, "_hot_rank", lambda: (_ for _ in ()).throw(AssertionError("AKShare hot rank should not be used when Tushare hot rank is available")))

    result = collector.collect(datetime(2026, 3, 10, 18, 0, 0))

    assert result["as_of"] == "2026-03-10 18:00:00"
    assert result["industry_spot_report"]["is_fresh"] is True
    assert result["concept_spot_report"]["is_fresh"] is True
    assert result["concept_spot_report"]["fallback"] == "none"
    assert result["hot_rank_report"]["is_fresh"] is True
    assert result["hot_rank_report"]["fallback"] == "none"
    assert result["hot_rank_report"]["latest_date"] == "2026-03-10"


def test_market_drivers_collect_prefers_standard_sw_industry_snapshot(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    monkeypatch.setattr(
        "src.collectors.market_drivers.ChinaMarketCollector",
        lambda _config: type(
            "StubCN",
            (),
            {
                "get_north_south_flow": staticmethod(lambda: pd.DataFrame()),
                "get_margin_trading": staticmethod(lambda: pd.DataFrame()),
            },
        )(),
    )
    monkeypatch.setattr(
        "src.collectors.market_drivers.IndustryIndexCollector",
        lambda _config: type(
            "StubIndustry",
            (),
            {
                "collect_market_snapshot": staticmethod(
                    lambda _as_of: {
                        "sw_industry_spot": pd.DataFrame([{"名称": "通信设备", "涨跌幅": 3.6, "框架来源": "申万二级行业", "日期": "2026-04-01"}]),
                        "sw_industry_report": {
                            "frame": pd.DataFrame([{"名称": "通信设备", "涨跌幅": 3.6, "框架来源": "申万二级行业", "日期": "2026-04-01"}]),
                            "latest_date": "2026-04-01",
                            "is_fresh": True,
                            "source": "tushare.sw_daily+tushare.index_classify",
                            "fallback": "none",
                            "diagnosis": "live",
                            "disclosure": "申万行业主链可用。",
                        },
                        "ci_industry_spot": pd.DataFrame(),
                        "ci_industry_report": {
                            "frame": pd.DataFrame(),
                            "latest_date": "",
                            "is_fresh": False,
                            "source": "tushare.ci_daily",
                            "fallback": "none",
                            "diagnosis": "empty",
                            "disclosure": "中信行业暂缺。",
                        },
                    }
                )
            },
        )(),
    )
    monkeypatch.setattr(collector, "_market_flow", lambda as_of: {"frame": pd.DataFrame(), "latest_date": "2026-04-01", "is_fresh": True})
    monkeypatch.setattr(collector, "_ts_northbound_top10", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_pledge_stat", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_sector_fund_flow", lambda _kind: pd.DataFrame())
    monkeypatch.setattr(collector, "_sector_fund_flow", lambda _kind: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_northbound_industry", lambda as_of: collector._empty_rank_report())
    monkeypatch.setattr(collector, "_northbound_rank", lambda symbol, as_of: collector._empty_rank_report())
    monkeypatch.setattr(collector, "_ts_board_spot", lambda _kind: pd.DataFrame())
    monkeypatch.setattr(collector, "_board_spot", lambda _name: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_hot_rank", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "_hot_rank", lambda: pd.DataFrame())

    result = collector.collect(datetime(2026, 4, 1, 18, 0, 0))

    assert result["industry_spot"].iloc[0]["名称"] == "通信设备"
    assert result["industry_spot_report"]["source"] == "tushare.sw_daily+tushare.index_classify"
    assert result["industry_spot_report"]["fallback"] == "none"
    assert result["industry_spot_report"]["is_fresh"] is True


def test_market_drivers_tdx_index_normalizes_board_universe(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "tdx_index"
        assert kwargs == {}
        return pd.DataFrame(
            [
                {
                    "board_code": "881001.TI",
                    "board_name": "半导体",
                    "board_type": "industry",
                    "trade_date": "20260310",
                    "pct_change": 2.4,
                    "amount": 123.0,
                },
                {
                    "board_code": "885008.TI",
                    "board_name": "AI应用",
                    "board_type": "concept",
                    "trade_date": "20260310",
                    "pct_change": 4.2,
                    "amount": 456.0,
                },
                {
                    "board_code": "999001.TI",
                    "board_name": "北京板块",
                    "board_type": "region",
                    "trade_date": "20260309",
                    "pct_change": -0.7,
                    "amount": 78.0,
                },
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    report = collector.get_tdx_index(reference_date=datetime(2026, 3, 10, 18, 0, 0), board_type="industry")

    frame = report["frame"]
    assert report["source"] == "tushare.tdx_index"
    assert report["fallback"] == "none"
    assert report["board_type"] == "industry"
    assert report["latest_date"] == "2026-03-10"
    assert report["is_fresh"] is True
    assert report["status"] == "matched"
    assert list(frame["板块名称"]) == ["半导体"]
    assert float(frame.iloc[0]["涨跌幅"]) == 2.4
    assert float(frame.iloc[0]["成交额"]) == 123.0


def test_market_drivers_tdx_member_returns_constituent_snapshot(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "tdx_member"
        assert kwargs == {}
        return pd.DataFrame(
            [
                {
                    "board_code": "885008.TI",
                    "board_name": "AI应用",
                    "board_type": "concept",
                    "member_code": "300308.SZ",
                    "member_name": "中际旭创",
                    "weight": 8.5,
                    "trade_date": "20260310",
                },
                {
                    "board_code": "881001.TI",
                    "board_name": "半导体",
                    "board_type": "industry",
                    "member_code": "688981.SH",
                    "member_name": "中芯国际",
                    "weight": 6.2,
                    "trade_date": "20260310",
                },
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    report = collector.get_tdx_member(reference_date=datetime(2026, 3, 10, 18, 0, 0), board_type="concept", board_name="AI应用")

    frame = report["frame"]
    assert report["source"] == "tushare.tdx_member"
    assert report["fallback"] == "none"
    assert report["board_type"] == "concept"
    assert report["board_name"] == "AI应用"
    assert report["latest_date"] == "2026-03-10"
    assert report["is_fresh"] is True
    assert report["status"] == "matched"
    assert list(frame["板块名称"]) == ["AI应用"]
    assert list(frame["成分名称"]) == ["中际旭创"]
    assert float(frame.iloc[0]["权重"]) == 8.5


def test_market_drivers_tdx_daily_reports_board_trend_snapshot(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "tdx_daily"
        assert kwargs == {}
        return pd.DataFrame(
            [
                {
                    "board_code": "999001.TI",
                    "board_name": "北京板块",
                    "board_type": "region",
                    "trade_date": "20260310",
                    "open": 10.0,
                    "close": 10.8,
                    "high": 11.0,
                    "low": 9.9,
                    "pct_change": 3.1,
                    "amount": 88.0,
                    "vol": 1200.0,
                },
                {
                    "board_code": "885008.TI",
                    "board_name": "AI应用",
                    "board_type": "concept",
                    "trade_date": "20260310",
                    "open": 20.0,
                    "close": 21.2,
                    "high": 21.6,
                    "low": 19.8,
                    "pct_change": 5.2,
                    "amount": 188.0,
                    "vol": 2400.0,
                },
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    report = collector.get_tdx_daily(reference_date=datetime(2026, 3, 10, 18, 0, 0), board_type="region")

    frame = report["frame"]
    assert report["source"] == "tushare.tdx_daily"
    assert report["fallback"] == "none"
    assert report["board_type"] == "region"
    assert report["latest_date"] == "2026-03-10"
    assert report["is_fresh"] is True
    assert report["status"] == "matched"
    assert list(frame["板块名称"]) == ["北京板块"]
    assert float(frame.iloc[0]["开盘"]) == 10.0
    assert float(frame.iloc[0]["收盘"]) == 10.8
    assert float(frame.iloc[0]["涨跌幅"]) == 3.1
    assert float(frame.iloc[0]["成交额"]) == 88.0


def test_market_drivers_dc_index_normalizes_concept_snapshot(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "dc_index"
        assert kwargs.get("trade_date") == "20260310"
        return pd.DataFrame(
            [
                {
                    "ts_code": "BK1063.DC",
                    "trade_date": "20260310",
                    "name": "半导体",
                    "leading": "中芯国际",
                    "leading_code": "688981.SH",
                    "pct_change": 2.4,
                    "leading_pct": 6.8,
                    "total_mv": 123456.0,
                    "turnover_rate": 3.2,
                    "up_num": 28,
                    "down_num": 4,
                },
                {
                    "ts_code": "BK1064.DC",
                    "trade_date": "20260310",
                    "name": "AI应用",
                    "leading": "中际旭创",
                    "leading_code": "300308.SZ",
                    "pct_change": 5.1,
                    "leading_pct": 9.4,
                    "total_mv": 223456.0,
                    "turnover_rate": 4.1,
                    "up_num": 31,
                    "down_num": 2,
                },
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    report = collector.get_dc_index(reference_date=datetime(2026, 3, 10, 18, 0, 0), trade_date="2026-03-10", name="半导体")

    frame = report["frame"]
    assert report["source"] == "tushare.dc_index"
    assert report["fallback"] == "none"
    assert report["latest_date"] == "2026-03-10"
    assert report["is_fresh"] is True
    assert report["status"] == "matched"
    assert report["board_type"] == "concept"
    assert list(frame["板块名称"]) == ["半导体"]
    assert list(frame["领涨股票"]) == ["中芯国际"]
    assert float(frame.iloc[0]["涨跌幅"]) == 2.4


def test_market_drivers_dc_daily_maps_idx_type_and_filters_snapshot(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "dc_daily"
        assert kwargs.get("trade_date") == "20260310"
        assert kwargs.get("idx_type") == "地域板块"
        return pd.DataFrame(
            [
                {
                    "ts_code": "BK9001.DC",
                    "trade_date": "20260310",
                    "name": "北京板块",
                    "idx_type": "地域板块",
                    "open": 10.0,
                    "close": 10.8,
                    "high": 11.0,
                    "low": 9.9,
                    "change": 0.8,
                    "pct_change": 3.1,
                    "vol": 1200.0,
                    "amount": 88.0,
                    "swing": 2.2,
                    "turnover_rate": 1.6,
                },
                {
                    "ts_code": "BK9002.DC",
                    "trade_date": "20260310",
                    "name": "上海板块",
                    "idx_type": "地域板块",
                    "open": 20.0,
                    "close": 21.2,
                    "high": 21.6,
                    "low": 19.8,
                    "change": 1.2,
                    "pct_change": 5.2,
                    "vol": 2400.0,
                    "amount": 188.0,
                    "swing": 3.0,
                    "turnover_rate": 2.4,
                },
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    report = collector.get_dc_daily(reference_date=datetime(2026, 3, 10, 18, 0, 0), idx_type="region", trade_date="2026-03-10", name="北京")

    frame = report["frame"]
    assert report["source"] == "tushare.dc_daily"
    assert report["fallback"] == "none"
    assert report["latest_date"] == "2026-03-10"
    assert report["is_fresh"] is True
    assert report["status"] == "matched"
    assert report["board_type"] == "region"
    assert list(frame["板块名称"]) == ["北京板块"]
    assert float(frame.iloc[0]["收盘"]) == 10.8
    assert float(frame.iloc[0]["涨跌幅"]) == 3.1
    assert float(frame.iloc[0]["成交额"]) == 88.0


def test_market_drivers_moneyflow_mkt_dc_returns_daily_market_flow_snapshot(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260310")

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "moneyflow_mkt_dc"
        assert kwargs.get("start_date") == "20260219"
        assert kwargs.get("end_date") == "20260310"
        return pd.DataFrame(
            [
                {
                    "trade_date": "20260309",
                    "close_sh": 3050.0,
                    "pct_change_sh": 0.8,
                    "close_sz": 9600.0,
                    "pct_change_sz": 1.1,
                    "net_amount": 12_000_000.0,
                    "net_amount_rate": 1.8,
                    "buy_elg_amount": 4_000_000.0,
                    "buy_elg_amount_rate": 0.6,
                    "buy_lg_amount": 3_000_000.0,
                    "buy_lg_amount_rate": 0.4,
                    "buy_md_amount": 2_000_000.0,
                    "buy_md_amount_rate": 0.3,
                    "buy_sm_amount": 3_000_000.0,
                    "buy_sm_amount_rate": 0.5,
                },
                {
                    "trade_date": "20260310",
                    "close_sh": 3070.0,
                    "pct_change_sh": 1.2,
                    "close_sz": 9680.0,
                    "pct_change_sz": 1.5,
                    "net_amount": 15_000_000.0,
                    "net_amount_rate": 2.0,
                    "buy_elg_amount": 5_000_000.0,
                    "buy_elg_amount_rate": 0.7,
                    "buy_lg_amount": 4_000_000.0,
                    "buy_lg_amount_rate": 0.5,
                    "buy_md_amount": 3_000_000.0,
                    "buy_md_amount_rate": 0.4,
                    "buy_sm_amount": 3_000_000.0,
                    "buy_sm_amount_rate": 0.4,
                },
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    report = collector.get_moneyflow_mkt_dc(reference_date=datetime(2026, 3, 10, 18, 0, 0), lookback_days=20)

    frame = report["frame"]
    assert report["source"] == "tushare.moneyflow_mkt_dc"
    assert report["fallback"] == "none"
    assert report["latest_date"] == "2026-03-10"
    assert report["is_fresh"] is True
    assert report["status"] == "matched"
    assert list(frame["日期"]) == ["2026-03-09", "2026-03-10"]
    assert float(frame.iloc[-1]["主力净流入-净额"]) == 15_000_000.0
    assert float(frame.iloc[-1]["上证涨跌幅"]) == 1.2


def test_market_drivers_report_rc_returns_sell_side_snapshot(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260309", "20260310"])

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "report_rc"
        if kwargs.get("report_date") == "20260310":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "300308.SZ",
                        "name": "中际旭创",
                        "report_date": "20260310",
                        "report_title": "AI光模块需求保持高景气",
                        "report_type": "一般报告",
                        "classify": "首次覆盖",
                        "org_name": "中信证券",
                        "author_name": "张三",
                        "quarter": "2024Q4",
                        "eps": 6.78,
                        "pe": 14.2,
                        "rating": "买入",
                        "max_price": 180.0,
                        "min_price": 150.0,
                        "imp_dg": "高",
                    },
                    {
                        "ts_code": "000001.SZ",
                        "name": "平安银行",
                        "report_date": "20260310",
                        "report_title": "银行板块稳健增长",
                        "report_type": "一般报告",
                        "classify": "跟踪",
                        "org_name": "华泰证券",
                        "author_name": "李四",
                        "quarter": "2024Q4",
                        "eps": 1.2,
                        "pe": 8.1,
                        "rating": "增持",
                        "max_price": 15.0,
                        "min_price": 12.0,
                        "imp_dg": "中",
                    },
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    report = collector.get_report_rc(reference_date=datetime(2026, 3, 10, 18, 0, 0), ts_code="300308", max_rows=50)

    frame = report["frame"]
    assert report["source"] == "tushare.report_rc"
    assert report["fallback"] == "none"
    assert report["latest_date"] == "2026-03-10"
    assert report["is_fresh"] is True
    assert report["status"] == "matched"
    assert list(frame["股票代码"]) == ["300308.SZ"]
    assert list(frame["机构名称"]) == ["中信证券"]
    assert float(frame.iloc[0]["预测每股收益"]) == 6.78


def test_market_drivers_collect_exposes_dc_snapshots(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    monkeypatch.setattr(
        "src.collectors.market_drivers.ChinaMarketCollector",
        lambda _config: type(
            "StubCN",
            (),
            {
                "get_north_south_flow": staticmethod(lambda: pd.DataFrame()),
                "get_margin_trading": staticmethod(lambda: pd.DataFrame()),
            },
        )(),
    )
    monkeypatch.setattr(
        "src.collectors.market_drivers.IndustryIndexCollector",
        lambda _config: type(
            "StubIndustry",
            (),
            {
                "collect_market_snapshot": staticmethod(
                    lambda _as_of: {
                        "sw_industry_spot": pd.DataFrame(),
                        "sw_industry_report": {"frame": pd.DataFrame(), "latest_date": "", "is_fresh": False, "source": "", "fallback": "none", "diagnosis": "empty", "disclosure": ""},
                        "ci_industry_spot": pd.DataFrame(),
                        "ci_industry_report": {"frame": pd.DataFrame(), "latest_date": "", "is_fresh": False, "source": "", "fallback": "none", "diagnosis": "empty", "disclosure": ""},
                    }
                )
            },
        )(),
    )
    monkeypatch.setattr(collector, "_market_flow", lambda as_of: {"frame": pd.DataFrame(), "latest_date": "", "is_fresh": False})
    monkeypatch.setattr(collector, "_ts_northbound_top10", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_pledge_stat", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_sector_fund_flow", lambda _kind: pd.DataFrame())
    monkeypatch.setattr(collector, "_sector_fund_flow", lambda _kind: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_northbound_industry", lambda as_of: collector._empty_rank_report())
    monkeypatch.setattr(collector, "_northbound_rank", lambda symbol, as_of: collector._empty_rank_report())
    monkeypatch.setattr(collector, "_ts_board_spot", lambda _kind: pd.DataFrame())
    monkeypatch.setattr(collector, "_board_spot", lambda _name: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_hot_rank", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "_hot_rank", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_tdx_index", lambda **kwargs: {"frame": pd.DataFrame(), "source": "tushare.tdx_index", "fallback": "none", "latest_date": "", "is_fresh": False})
    monkeypatch.setattr(collector, "get_tdx_member", lambda **kwargs: {"frame": pd.DataFrame(), "source": "tushare.tdx_member", "fallback": "none", "latest_date": "", "is_fresh": False})
    monkeypatch.setattr(collector, "get_tdx_daily", lambda **kwargs: {"frame": pd.DataFrame(), "source": "tushare.tdx_daily", "fallback": "none", "latest_date": "", "is_fresh": False})
    monkeypatch.setattr(collector, "get_dc_index", lambda **kwargs: {"frame": pd.DataFrame(), "source": "tushare.dc_index", "fallback": "none", "latest_date": "", "is_fresh": False})
    monkeypatch.setattr(collector, "get_dc_daily", lambda **kwargs: {"frame": pd.DataFrame(), "source": "tushare.dc_daily", "fallback": "none", "latest_date": "", "is_fresh": False})
    monkeypatch.setattr(collector, "get_moneyflow_mkt_dc", lambda **kwargs: {"frame": pd.DataFrame(), "source": "tushare.moneyflow_mkt_dc", "fallback": "none", "latest_date": "", "is_fresh": False})
    monkeypatch.setattr(collector, "get_report_rc", lambda **kwargs: {"frame": pd.DataFrame(), "source": "tushare.report_rc", "fallback": "none", "latest_date": "", "is_fresh": False})

    result = collector.collect(datetime(2026, 3, 10, 18, 0, 0))

    assert "dc_index" in result
    assert "dc_daily" in result
    assert "moneyflow_mkt_dc" in result
    assert "report_rc" in result
    assert result["dc_index"]["source"] == "tushare.dc_index"
    assert result["report_rc"]["source"] == "tushare.report_rc"


def test_market_drivers_volatile_frame_report_marks_empty_frames_not_fresh(tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    report = collector._volatile_frame_report(
        pd.DataFrame(),
        datetime(2026, 4, 1, 19, 0, 0),
        default_date="20260401",
        source="concept_spot",
    )

    assert report["frame"].empty is True
    assert report["latest_date"] == ""
    assert report["is_fresh"] is False
    assert report["diagnosis"] == "empty_or_blocked"


def test_market_drivers_stock_capital_flow_snapshot_prefers_direct_moneyflow(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260401")
    monkeypatch.setattr(
        collector,
        "_ts_stock_moneyflow_snapshot",
        lambda **kwargs: pd.DataFrame(
            [
                {
                    "ts_code": "300308.SZ",
                    "trade_date": "20260331",
                    "buy_lg_amount": 10_000.0,
                    "sell_lg_amount": 8_000.0,
                    "buy_elg_amount": 5_000.0,
                    "sell_elg_amount": 4_000.0,
                },
                {
                    "ts_code": "300308.SZ",
                    "trade_date": "20260401",
                    "buy_lg_amount": 20_000.0,
                    "sell_lg_amount": 9_000.0,
                    "buy_elg_amount": 12_000.0,
                    "sell_elg_amount": 7_000.0,
                },
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_stock_theme_membership",
        lambda symbol, reference_date=None, limit=3: {  # noqa: ARG005
            "source": "tushare.ths_member",
            "latest_date": "2026-04-01",
            "fallback": "none",
            "diagnosis": "live",
            "disclosure": "ths_member 命中主题成员。",
            "status": "matched",
            "items": [{"board_name": "AI算力", "board_type": "concept"}],
        },
    )
    monkeypatch.setattr(collector, "_ts_sector_fund_flow", lambda board_type: pd.DataFrame())

    snapshot = collector.get_stock_capital_flow_snapshot(
        "300308",
        reference_date=datetime(2026, 4, 1, 18, 0, 0),
        display_name="中际旭创",
        sector="科技",
        chain_nodes=["光模块"],
    )

    assert snapshot["status"] == "matched"
    assert snapshot["is_fresh"] is True
    assert snapshot["latest_date"] == "2026-04-01"
    assert snapshot["direct_main_flow"] == 160_000_000.0
    assert snapshot["direct_5d_main_flow"] == 190_000_000.0


def test_market_drivers_stock_capital_flow_snapshot_skips_proxy_fetch_when_direct_moneyflow_is_fresh(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260401")
    monkeypatch.setattr(
        collector,
        "_ts_stock_moneyflow_snapshot",
        lambda **kwargs: pd.DataFrame(
            [
                {
                    "ts_code": "300308.SZ",
                    "trade_date": "20260401",
                    "buy_lg_amount": 20_000.0,
                    "sell_lg_amount": 9_000.0,
                    "buy_elg_amount": 12_000.0,
                    "sell_elg_amount": 7_000.0,
                },
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_stock_theme_membership",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("fresh direct moneyflow should skip theme proxy fetch")),
    )
    monkeypatch.setattr(
        collector,
        "_ts_sector_fund_flow",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("fresh direct moneyflow should skip board proxy fetch")),
    )

    snapshot = collector.get_stock_capital_flow_snapshot(
        "300308",
        reference_date=datetime(2026, 4, 1, 18, 0, 0),
        display_name="中际旭创",
        sector="科技",
        chain_nodes=["光模块"],
    )

    assert snapshot["status"] == "matched"
    assert snapshot["components"]["theme_membership"]["status"] == "skipped"
    assert snapshot["components"]["board_flow"]["status"] == "skipped"


def test_market_drivers_stock_capital_flow_snapshot_falls_back_to_board_proxy(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260401")
    monkeypatch.setattr(collector, "_ts_stock_moneyflow_snapshot", lambda **kwargs: pd.DataFrame())
    monkeypatch.setattr(
        collector,
        "get_stock_theme_membership",
        lambda symbol, reference_date=None, limit=3: {  # noqa: ARG005
            "source": "tushare.ths_member",
            "latest_date": "2026-04-01",
            "fallback": "none",
            "diagnosis": "live",
            "disclosure": "ths_member 命中主题成员。",
            "status": "matched",
            "items": [{"board_name": "AI算力", "board_type": "concept"}],
        },
    )

    def fake_sector_flow(board_type: str) -> pd.DataFrame:
        if board_type == "concept":
            return pd.DataFrame(
                [
                    {
                        "名称": "AI算力",
                        "今日主力净流入-净额": 280_000_000.0,
                        "今日主力净流入-净占比": 0.12,
                        "日期": "2026-04-01",
                    }
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_sector_fund_flow", fake_sector_flow)

    snapshot = collector.get_stock_capital_flow_snapshot(
        "300308",
        reference_date=datetime(2026, 4, 1, 18, 0, 0),
        display_name="中际旭创",
        sector="科技",
        chain_nodes=["AI算力"],
    )

    assert snapshot["status"] == "proxy"
    assert snapshot["fallback"] == "sector_or_concept_flow"
    assert snapshot["is_fresh"] is True
    assert snapshot["board_name"] == "AI算力"
    assert snapshot["board_main_flow"] == 280_000_000.0


def test_market_drivers_stock_capital_flow_snapshot_keeps_t1_direct_detail_when_board_proxy_is_current(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260402")
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda lookback_days=14, exchange="SSE": ["20260401", "20260402"])  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "_ts_stock_moneyflow_snapshot",
        lambda **kwargs: pd.DataFrame(
            [
                {
                    "ts_code": "300308.SZ",
                    "trade_date": "20260401",
                    "buy_lg_amount": 20_000.0,
                    "sell_lg_amount": 9_000.0,
                    "buy_elg_amount": 12_000.0,
                    "sell_elg_amount": 7_000.0,
                },
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_stock_theme_membership",
        lambda symbol, reference_date=None, limit=3: {  # noqa: ARG005
            "source": "tushare.ths_member",
            "latest_date": "2026-04-02",
            "fallback": "none",
            "diagnosis": "live",
            "disclosure": "ths_member 命中主题成员。",
            "status": "matched",
            "items": [{"board_name": "AI算力", "board_type": "concept"}],
        },
    )
    monkeypatch.setattr(
        collector,
        "_ts_sector_fund_flow",
        lambda board_type: pd.DataFrame(
            [
                {
                    "名称": "AI算力",
                    "今日主力净流入-净额": 280_000_000.0,
                    "今日主力净流入-净占比": 0.12,
                    "日期": "2026-04-02",
                }
            ]
        ) if board_type == "concept" else pd.DataFrame(),
    )

    snapshot = collector.get_stock_capital_flow_snapshot(
        "300308",
        reference_date=datetime(2026, 4, 2, 18, 0, 0),
        display_name="中际旭创",
        sector="科技",
        chain_nodes=["AI算力"],
    )

    assert snapshot["status"] == "proxy"
    assert snapshot["is_fresh"] is True
    assert snapshot["direct_trade_gap_days"] == 1
    assert "T+1 直连" in snapshot["detail"]
    assert snapshot["direct_main_flow"] == 160_000_000.0


def test_market_drivers_stock_broker_recommend_snapshot_prefers_current_month(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "broker_recommend"
        month = str(kwargs.get("month") or "")
        if month == "202604":
            return pd.DataFrame(
                [
                    {"month": "202604", "broker": "中信证券", "ts_code": "300308.SZ", "name": "中际旭创"},
                    {"month": "202604", "broker": "国泰君安", "ts_code": "300308.SZ", "name": "中际旭创"},
                    {"month": "202604", "broker": "华泰证券", "ts_code": "300308.SZ", "name": "中际旭创"},
                    {"month": "202604", "broker": "招商证券", "ts_code": "300308.SZ", "name": "中际旭创"},
                ]
            )
        if month == "202603":
            return pd.DataFrame(
                [
                    {"month": "202603", "broker": "中信证券", "ts_code": "300308.SZ", "name": "中际旭创"},
                    {"month": "202603", "broker": "国泰君安", "ts_code": "300308.SZ", "name": "中际旭创"},
                ]
            )
        if month == "202602":
            return pd.DataFrame(
                [
                    {"month": "202602", "broker": "中信证券", "ts_code": "300308.SZ", "name": "中际旭创"},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    report = collector.get_stock_broker_recommend_snapshot(
        "300308",
        reference_date=datetime(2026, 4, 2, 18, 0, 0),
        display_name="中际旭创",
    )

    assert report["status"] == "matched"
    assert report["diagnosis"] == "live"
    assert report["is_fresh"] is True
    assert report["latest_date"] == "2026-04"
    assert report["latest_broker_count"] == 4
    assert report["broker_delta"] == 2
    assert report["consecutive_months"] == 3
    assert report["crowding_level"] == "medium"


def test_market_drivers_stock_broker_recommend_snapshot_marks_previous_month_stale(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "broker_recommend"
        month = str(kwargs.get("month") or "")
        if month == "202604":
            return pd.DataFrame(columns=["month", "broker", "ts_code", "name"])
        if month == "202603":
            return pd.DataFrame(
                [
                    {"month": "202603", "broker": "中信证券", "ts_code": "300308.SZ", "name": "中际旭创"},
                    {"month": "202603", "broker": "国泰君安", "ts_code": "300308.SZ", "name": "中际旭创"},
                ]
            )
        if month == "202602":
            return pd.DataFrame(
                [
                    {"month": "202602", "broker": "中信证券", "ts_code": "300308.SZ", "name": "中际旭创"},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    report = collector.get_stock_broker_recommend_snapshot(
        "300308",
        reference_date=datetime(2026, 4, 2, 18, 0, 0),
        display_name="中际旭创",
    )

    assert report["status"] == "stale"
    assert report["diagnosis"] == "stale"
    assert report["is_fresh"] is False
    assert report["fallback"] == "previous_month"
    assert report["latest_date"] == "2026-03"
    assert report["latest_broker_count"] == 2
    assert report["broker_delta"] == 1


def test_market_drivers_stock_broker_recommend_snapshot_does_not_mask_permission_block(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "broker_recommend"
        month = str(kwargs.get("month") or "")
        if month == "202604":
            raise RuntimeError("抱歉，积分不足，暂时没有权限")
        if month == "202603":
            return pd.DataFrame(
                [
                    {"month": "202603", "broker": "中信证券", "ts_code": "300308.SZ", "name": "中际旭创"},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    report = collector.get_stock_broker_recommend_snapshot(
        "300308",
        reference_date=datetime(2026, 4, 2, 18, 0, 0),
        display_name="中际旭创",
    )

    assert report["status"] == "blocked"
    assert report["diagnosis"] == "permission_blocked"
    assert report["is_fresh"] is False
    assert report["fallback"] == "none"


def test_market_drivers_stock_broker_recommend_snapshot_falls_back_when_current_month_unavailable(monkeypatch, tmp_path):
    collector = MarketDriversCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "broker_recommend"
        month = str(kwargs.get("month") or "")
        if month == "202604":
            return None
        if month == "202603":
            return pd.DataFrame(columns=["month", "broker", "ts_code", "name"])
        if month == "202602":
            return pd.DataFrame(
                [
                    {"month": "202602", "broker": "中信证券", "ts_code": "600276.SH", "name": "恒瑞医药"},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    report = collector.get_stock_broker_recommend_snapshot(
        "600276",
        reference_date=datetime(2026, 4, 3, 18, 0, 0),
        display_name="恒瑞医药",
    )

    assert report["status"] == "stale"
    assert report["diagnosis"] == "stale"
    assert report["fallback"] == "previous_month"
    assert report["latest_date"] == "2026-02"
    assert report["latest_broker_count"] == 1
