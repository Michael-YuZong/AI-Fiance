"""Tests for CN ETF market collector fallbacks."""

from __future__ import annotations

import pandas as pd
import pytest

from src.collectors.market_cn import ChinaMarketCollector


def test_market_cn_stock_daily_returns_empty_when_tushare_is_missing(monkeypatch):
    collector = ChinaMarketCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_ts_stock_daily", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("ts failed")))
    monkeypatch.setattr(collector, "_ak_function", lambda *_: (_ for _ in ()).throw(AssertionError("AKShare stock daily should not be used")))

    frame = collector.get_stock_daily("300750")
    assert frame.empty is True


def test_market_cn_stock_industry_prefers_tushare_stock_basic(monkeypatch):
    collector = ChinaMarketCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_ts_call",
        lambda api_name, **kwargs: pd.DataFrame([{"ts_code": kwargs.get("ts_code"), "name": "宁德时代", "industry": "新能源设备"}]) if api_name == "stock_basic" else (_ for _ in ()).throw(AssertionError(api_name)),
    )
    monkeypatch.setattr(collector, "_ak_function", lambda *_: (_ for _ in ()).throw(AssertionError("AKShare stock industry fallback should not be used")))

    assert collector.get_stock_industry("300750") == "新能源设备"


def test_market_cn_open_fund_daily_returns_empty_when_tushare_is_missing(monkeypatch):
    collector = ChinaMarketCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_ts_call", lambda api_name, **kwargs: pd.DataFrame() if api_name == "fund_nav" else (_ for _ in ()).throw(AssertionError(api_name)))
    monkeypatch.setattr(collector, "_ak_function", lambda *_: (_ for _ in ()).throw(AssertionError("AKShare open fund fallback should not be used")))

    frame = collector.get_open_fund_daily("022365")
    assert frame.empty is True


def test_market_cn_index_daily_falls_back_to_proxy_etf(monkeypatch):
    collector = ChinaMarketCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_topic_collector",
        lambda: type("FakeIndexTopic", (), {"get_index_history": staticmethod(lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("ts failed")))})(),
    )
    monkeypatch.setattr(collector, "_ak_function", lambda *_: (_ for _ in ()).throw(AssertionError("AKShare index fallback should not be used")))
    monkeypatch.setattr(collector, "get_etf_daily", lambda symbol, **_: pd.DataFrame({"date": ["2026-03-08"], "open": [1], "high": [1], "low": [1], "close": [1], "volume": [0], "amount": [0]}))
    frame = collector.get_index_daily("000300", proxy_symbol="510330")
    assert not frame.empty


def test_market_cn_index_daily_uses_index_code_candidates(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_topic_collector",
        lambda: type(
            "FakeIndexTopic",
            (),
            {
                "get_index_history": staticmethod(
                    lambda symbol, period="daily", start_date="", end_date="": pd.DataFrame(
                        [
                            {"日期": pd.Timestamp("2026-03-09"), "开盘": 9.8, "最高": 10.1, "最低": 9.7, "收盘": 10.0, "成交量": 900, "成交额": 9_000_000.0},
                            {"日期": pd.Timestamp("2026-03-10"), "开盘": 10.0, "最高": 10.5, "最低": 9.8, "收盘": 10.2, "成交量": 1000, "成交额": 10_000_000.0},
                        ]
                    )
                )
            },
        )(),
    )
    frame = collector.get_index_daily("000300")
    assert not frame.empty
    assert float(frame["收盘"].iloc[-1]) == 10.2


def test_market_cn_ts_stock_daily_scales_amount_to_yuan(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        if api_name == "daily":
            assert kwargs.get("ts_code") == "300750.SZ"
            return pd.DataFrame(
                [
                    {"trade_date": "20260310", "open": 250.0, "high": 255.0, "low": 248.0, "close": 252.0, "vol": 1000, "amount": 1234.5},
                    {"trade_date": "20260309", "open": 245.0, "high": 250.0, "low": 243.0, "close": 248.0, "vol": 900, "amount": 1000.0},
                ]
            )
        if api_name == "adj_factor":
            return pd.DataFrame(
                [
                    {"trade_date": "20260310", "adj_factor": 1.0},
                    {"trade_date": "20260309", "adj_factor": 1.0},
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_stock_daily("300750")
    assert not frame.empty
    assert float(frame["成交额"].iloc[-1]) == 1_234_500.0
    assert frame.attrs["history_source"] == "tushare"
    assert frame.attrs["history_source_label"] == "Tushare 日线"


def test_market_cn_ts_etf_daily_scales_amount_to_yuan(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        if api_name == "fund_daily":
            assert kwargs.get("ts_code") == "510300.SH"
            return pd.DataFrame(
                [
                    {"trade_date": "20260310", "open": 4.0, "high": 4.1, "low": 3.9, "close": 4.05, "vol": 2000, "amount": 567.8},
                    {"trade_date": "20260309", "open": 3.9, "high": 4.0, "low": 3.8, "close": 3.95, "vol": 1800, "amount": 500.0},
                ]
            )
        if api_name == "fund_adj":
            return pd.DataFrame(
                [
                    {"trade_date": "20260310", "adj_factor": 1.0},
                    {"trade_date": "20260309", "adj_factor": 1.0},
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_etf_daily("510300")
    assert not frame.empty
    assert float(frame["成交额"].iloc[-1]) == 567_800.0
    assert frame.attrs["history_source"] == "tushare"
    assert frame.attrs["history_source_label"] == "Tushare 日线"


def test_market_cn_ts_etf_daily_falls_back_to_adj_factor_when_fund_adj_missing(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        if api_name == "fund_daily":
            assert kwargs.get("ts_code") == "510300.SH"
            return pd.DataFrame(
                [
                    {"trade_date": "20260310", "open": 4.0, "high": 4.1, "low": 3.9, "close": 4.05, "vol": 2000, "amount": 567.8},
                ]
            )
        if api_name == "fund_adj":
            return pd.DataFrame()
        if api_name == "adj_factor":
            return pd.DataFrame(
                [
                    {"trade_date": "20260310", "adj_factor": 1.0},
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_etf_daily("510300")
    assert not frame.empty
    assert float(frame["成交额"].iloc[-1]) == 567_800.0


def test_market_cn_etf_daily_does_not_fallback_to_akshare_when_tushare_unavailable(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_etf_daily(symbol: str, start: str, end: str, adjust: str):  # noqa: ARG001
        raise RuntimeError("temporary ts failure")

    monkeypatch.setattr(collector, "_ts_etf_daily", fake_ts_etf_daily)
    monkeypatch.setattr(collector, "_ak_function", lambda *_: (_ for _ in ()).throw(AssertionError("AKShare ETF daily fallback should not run")))

    with pytest.raises(RuntimeError, match="temporary ts failure"):
        collector.get_etf_daily("510300")


def test_market_cn_retries_tushare_stock_daily_before_fallback(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    attempts = {"count": 0}

    def fake_ts_stock_daily(symbol: str, start: str, end: str, adjust: str):  # noqa: ARG001
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("temporary ts failure")
        return pd.DataFrame(
            [
                {"日期": "2026-03-10", "开盘": 10.0, "最高": 10.3, "最低": 9.9, "收盘": 10.2, "成交量": 1000, "成交额": 1_000_000.0},
                {"日期": "2026-03-11", "开盘": 10.2, "最高": 10.5, "最低": 10.1, "收盘": 10.4, "成交量": 1200, "成交额": 1_200_000.0},
            ]
        )

    monkeypatch.setattr(collector, "_ts_stock_daily", fake_ts_stock_daily)
    monkeypatch.setattr(collector, "_ak_function", lambda *_: (_ for _ in ()).throw(AssertionError("AK fallback should not run")))
    frame = collector.get_stock_daily("300750")

    assert attempts["count"] == 2
    assert not frame.empty
    assert frame.attrs["history_source"] == "tushare"


def test_market_cn_regulatory_risk_snapshot_aggregates_tushare_contracts(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260312"])

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        if api_name == "stock_st":
            assert kwargs.get("trade_date") == "20260312"
            return pd.DataFrame(
                [{"ts_code": "000711.SZ", "name": "ST京蓝", "trade_date": "20260312", "type": "ST", "type_name": "风险警示板"}]
            )
        if api_name == "st":
            assert kwargs.get("ts_code") == "000711.SZ"
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000711.SZ",
                        "name": "ST京蓝",
                        "pub_date": "20260303",
                        "imp_date": "20260304",
                        "st_tpye": "ST",
                        "st_reason": "其他风险警示",
                        "st_explain": "持续经营存在不确定性",
                    }
                ]
            )
        if api_name == "stk_high_shock":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000711.SZ",
                        "trade_date": "20260310",
                        "name": "ST京蓝",
                        "trade_market": "深市主板",
                        "reason": "连续10个交易日内4次出现同正向异常波动的证券",
                        "period": "2026-02-25-2026-03-11",
                    }
                ]
            )
        if api_name == "stk_alert":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000711.SZ",
                        "name": "ST京蓝",
                        "start_date": "20260304",
                        "end_date": "20260317",
                        "type": "交易所重点提示证券",
                    }
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    snapshot = collector.get_stock_regulatory_risk_snapshot(
        "000711",
        as_of="2026-03-12",
        display_name="ST京蓝",
    )

    assert snapshot["status"] == "❌"
    assert snapshot["active_st"] is True
    assert snapshot["high_shock_count"] == 1
    assert snapshot["active_alert_count"] == 1
    assert snapshot["components"]["stock_st"]["source"] == "tushare.stock_st"
    assert snapshot["components"]["stk_alert"]["status"] == "⚠️"


def test_market_cn_regulatory_risk_snapshot_does_not_fake_pass_on_blocked_st(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260312"])

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        if api_name == "stock_st":
            raise RuntimeError("抱歉，您没有访问该接口的权限")
        if api_name in {"st", "stk_high_shock", "stk_alert"}:
            return pd.DataFrame()
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    snapshot = collector.get_stock_regulatory_risk_snapshot("300308", as_of="2026-03-12")

    assert snapshot["status"] == "ℹ️"
    assert snapshot["components"]["stock_st"]["diagnosis"] == "permission_blocked"
    assert "不把缺口写成通过" in snapshot["detail"] or "不把缺口写成通过" in snapshot["disclosure"]


def test_market_cn_etf_universe_snapshot_merges_basic_and_daily(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260312"])
    monkeypatch.setattr(
        collector,
        "_ts_fund_basic_snapshot",
        lambda market="E": pd.DataFrame(
            [
                {
                    "ts_code": "510880.SH",
                    "name": "红利ETF",
                    "management": "华泰柏瑞基金",
                    "fund_type": "股票型",
                    "found_date": "20061117",
                    "list_date": "20070118",
                    "benchmark": "上证红利指数收益率",
                    "status": "L",
                    "invest_type": "被动指数型",
                    "delist_date": None,
                }
            ]
        ),
    )

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        if api_name == "etf_basic":
            return pd.DataFrame()
        if api_name == "etf_share_size":
            return pd.DataFrame()
        assert api_name == "fund_daily"
        assert kwargs.get("trade_date") == "20260312"
        return pd.DataFrame(
            [
                {
                    "ts_code": "510880.SH",
                    "trade_date": "20260312",
                    "pre_close": 3.25,
                    "open": 3.26,
                    "high": 3.29,
                    "low": 3.24,
                    "close": 3.28,
                    "change": 0.03,
                    "pct_chg": 0.92,
                    "vol": 756432.0,
                    "amount": 238000.0,
                }
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_etf_universe_snapshot()
    assert not frame.empty
    row = frame.iloc[0]
    assert row["symbol"] == "510880"
    assert row["trade_date"] == "2026-03-12"
    assert float(row["amount"]) == 238_000_000.0
    assert row["benchmark"] == "上证红利指数收益率"


def test_market_cn_etf_universe_snapshot_prefers_etf_basic_and_share_size(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260312"])
    monkeypatch.setattr(
        collector,
        "_ts_etf_basic_snapshot",
        lambda: pd.DataFrame(
            [
                {
                    "ts_code": "510300.SH",
                    "csname": "沪深300ETF",
                    "extname": "沪深300ETF华泰柏瑞",
                    "cname": "华泰柏瑞沪深300ETF",
                    "index_code": "000300.SH",
                    "index_name": "沪深300指数",
                    "setup_date": "20110718",
                    "list_date": "20110802",
                    "etf_type": "境内",
                    "mgr_name": "华泰柏瑞基金",
                    "custod_name": "中国银行",
                    "mgt_fee": 0.15,
                    "exchange": "SH",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "_ts_etf_share_size_snapshot",
        lambda **kwargs: pd.DataFrame(
            [
                {
                    "trade_date": "20260312",
                    "ts_code": "510300.SH",
                    "etf_name": "沪深300ETF",
                    "total_share": 4_741_854.98,
                    "total_size": 22_878_980.0,
                    "nav": 4.8332,
                    "close": 4.8500,
                    "exchange": "SH",
                }
            ]
        ),
    )

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        assert api_name == "fund_daily"
        assert kwargs.get("trade_date") == "20260312"
        return pd.DataFrame(
            [
                {
                    "ts_code": "510300.SH",
                    "trade_date": "20260312",
                    "pre_close": 4.80,
                    "open": 4.82,
                    "high": 4.87,
                    "low": 4.79,
                    "close": 4.85,
                    "change": 0.05,
                    "pct_chg": 1.04,
                    "vol": 756432.0,
                    "amount": 238000.0,
                }
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_etf_universe_snapshot()
    assert not frame.empty
    row = frame.iloc[0]
    assert row["name"] == "沪深300ETF"
    assert row["benchmark"] == "沪深300指数"
    assert row["management"] == "华泰柏瑞基金"
    assert row["fund_type"] == "境内"
    assert row["ETF总份额"] == 4_741_854.98
    assert row["ETF总规模"] == 22_878_980.0


def test_market_cn_etf_universe_snapshot_falls_back_to_previous_open_day_when_latest_empty(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260311", "20260312", "20260313"])
    monkeypatch.setattr(
        collector,
        "_ts_fund_basic_snapshot",
        lambda market="E": pd.DataFrame(
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
                }
            ]
        ),
    )

    seen_dates: list[str] = []

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        if api_name == "etf_basic":
            return pd.DataFrame()
        if api_name == "etf_share_size":
            return pd.DataFrame()
        assert api_name == "fund_daily"
        trade_date = str(kwargs.get("trade_date"))
        seen_dates.append(trade_date)
        if trade_date == "20260313":
            return pd.DataFrame()
        if trade_date == "20260312":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "513120.SH",
                        "trade_date": "20260312",
                        "pre_close": 1.22,
                        "open": 1.23,
                        "high": 1.25,
                        "low": 1.20,
                        "close": 1.21,
                        "change": -0.01,
                        "pct_chg": -0.82,
                        "vol": 123456.0,
                        "amount": 45678.0,
                    }
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_etf_universe_snapshot()

    assert seen_dates[:2] == ["20260313", "20260312"]
    assert not frame.empty
    assert frame.iloc[0]["trade_date"] == "2026-03-12"


def test_market_cn_unlock_pressure_flags_large_near_term_share_float(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        assert api_name == "share_float"
        assert kwargs.get("ts_code") == "300502.SZ"
        return pd.DataFrame(
            [
                {
                    "ts_code": "300502.SZ",
                    "ann_date": "20260301",
                    "float_date": "20260320",
                    "float_share": 52_000.0,
                    "float_ratio": 3.2,
                    "holder_name": "示例股东A",
                    "share_type": "定向增发机构配售股份",
                },
                {
                    "ts_code": "300502.SZ",
                    "ann_date": "20260301",
                    "float_date": "20260320",
                    "float_share": 40_000.0,
                    "float_ratio": 2.1,
                    "holder_name": "示例股东B",
                    "share_type": "定向增发机构配售股份",
                },
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    snapshot = collector.get_unlock_pressure("300502", as_of="2026-03-12")
    assert snapshot["status"] == "❌"
    assert snapshot["ratio_30d"] == 5.3
    assert snapshot["next_date"] == "2026-03-20"
    assert "未来 30 日预计解禁约 5.30%" in snapshot["detail"]


def test_market_cn_unlock_pressure_passes_when_no_upcoming_share_float(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_ts_call", lambda api_name, **kwargs: pd.DataFrame() if api_name == "share_float" else None)
    snapshot = collector.get_unlock_pressure("300308", as_of="2026-03-12")
    assert snapshot["status"] == "✅"
    assert snapshot["ratio_30d"] == 0.0
    assert "未来 90 日未见明确限售股解禁安排" in snapshot["detail"]


def test_market_cn_stock_auction_normalizes_tushare_fields(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        assert api_name == "stk_auction"
        assert kwargs.get("ts_code") == "300502.SZ"
        return pd.DataFrame(
            [
                {
                    "ts_code": "300502.SZ",
                    "trade_date": "20260312",
                    "vol": 285300,
                    "price": 400.01,
                    "amount": 114122853.0,
                    "pre_close": 393.23,
                    "turnover_rate": 0.032222,
                    "volume_ratio": 0.618598,
                    "float_share": 88541.9,
                }
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_stock_auction("300502", trade_date="20260312")
    assert not frame.empty
    assert frame.iloc[0]["trade_date"] == "2026-03-12"
    assert float(frame.iloc[0]["price"]) == 400.01


def test_market_cn_stock_limit_normalizes_tushare_fields(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        assert api_name == "stk_limit"
        assert kwargs.get("ts_code") == "300502.SZ"
        return pd.DataFrame(
            [
                {
                    "ts_code": "300502.SZ",
                    "trade_date": "20260312",
                    "up_limit": 432.0,
                    "down_limit": 353.45,
                }
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_stock_limit("300502", trade_date="20260312")
    assert not frame.empty
    assert frame.iloc[0]["trade_date"] == "2026-03-12"
    assert float(frame.iloc[0]["up_limit"]) == 432.0


def test_ts_daily_basic_snapshot_merges_name_industry_and_amount(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    daily_basic = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260310",
                "close": 10.81,
                "turnover_rate": 0.4647,
                "turnover_rate_f": 0.7,
                "volume_ratio": 1.1,
                "pe": 5.6,
                "pe_ttm": 5.4,
                "pb": 0.7,
                "ps": 1.0,
                "ps_ttm": 1.0,
                "dv_ratio": 4.2,
                "dv_ttm": 4.1,
                "total_share": 100.0,
                "float_share": 80.0,
                "free_share": 60.0,
                "total_mv": 120_000.0,
                "circ_mv": 96_000.0,
            }
        ]
    )
    stock_basic = pd.DataFrame([{"ts_code": "000001.SZ", "name": "平安银行", "industry": "银行"}])
    daily = pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20260310", "amount": 850_573.536}])

    def fake_ts_call(api_name: str, **_: object) -> pd.DataFrame | None:
        if api_name == "daily_basic":
            return daily_basic
        if api_name == "stock_basic":
            return stock_basic
        if api_name == "daily":
            return daily
        if api_name == "bak_daily":
            return pd.DataFrame()
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260310")
    frame = collector._ts_daily_basic_snapshot()
    assert frame is not None
    assert frame.loc[0, "代码"] == "000001"
    assert frame.loc[0, "名称"] == "平安银行"
    assert frame.loc[0, "行业"] == "银行"
    assert frame.loc[0, "成交额"] == 850_573_536.0
    assert frame.loc[0, "总市值"] == 1_200_000_000.0


def test_ts_daily_basic_snapshot_estimates_amount_when_daily_missing(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    daily_basic = pd.DataFrame(
        [
            {
                "ts_code": "600001.SH",
                "trade_date": "20260310",
                "close": 12.0,
                "turnover_rate": 2.5,
                "turnover_rate_f": 3.0,
                "volume_ratio": 1.2,
                "pe": 18.0,
                "pe_ttm": 17.5,
                "pb": 1.8,
                "ps": 2.0,
                "ps_ttm": 1.9,
                "dv_ratio": 2.0,
                "dv_ttm": 2.0,
                "total_share": 100.0,
                "float_share": 80.0,
                "free_share": 60.0,
                "total_mv": 120_000.0,
                "circ_mv": 50_000.0,
            }
        ]
    )
    stock_basic = pd.DataFrame([{"ts_code": "600001.SH", "name": "示例股份", "industry": "电力"}])

    def fake_ts_call(api_name: str, **_: object) -> pd.DataFrame | None:
        if api_name == "daily_basic":
            return daily_basic
        if api_name == "stock_basic":
            return stock_basic
        if api_name == "daily":
            return pd.DataFrame()
        if api_name == "bak_daily":
            return pd.DataFrame()
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260310")
    frame = collector._ts_daily_basic_snapshot()
    assert frame is not None
    assert frame.loc[0, "成交额"] == 12_500_000.0


def test_ts_daily_basic_snapshot_uses_bak_daily_for_enrichment_and_amount_fallback(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    daily_basic = pd.DataFrame(
        [
            {
                "ts_code": "300502.SZ",
                "trade_date": "20260311",
                "close": 393.23,
                "turnover_rate": 3.82,
                "turnover_rate_f": 3.93,
                "volume_ratio": 0.74,
                "pe": 137.7,
                "pe_ttm": 51.99,
                "pb": 26.91,
                "total_mv": 39087.4,
                "circ_mv": 34817.3,
            }
        ]
    )

    def fake_ts_call(api_name: str, **_: object) -> pd.DataFrame | None:
        if api_name == "daily_basic":
            return daily_basic
        if api_name == "stock_basic":
            return pd.DataFrame()
        if api_name == "daily":
            return pd.DataFrame()
        if api_name == "bak_daily":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "300502.SZ",
                        "trade_date": "20260311",
                        "name": "新易盛",
                        "industry": "通信设备",
                        "amount": 1669605.4478,
                        "swing": 4.79,
                        "avg_price": 393.0,
                        "strength": 2.31,
                        "activity": 3988.0,
                        "attack": 3.42,
                        "area": "四川",
                    }
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260311")
    frame = collector._ts_daily_basic_snapshot()
    assert frame is not None
    assert frame.loc[0, "名称"] == "新易盛"
    assert frame.loc[0, "行业"] == "通信设备"
    assert frame.loc[0, "成交额"] == 16_696_054_478.0
    assert frame.loc[0, "强弱度"] == 2.31
    assert frame.loc[0, "攻击度"] == 3.42


def test_market_cn_open_fund_daily_resolves_tushare_fund_code(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        if api_name == "fund_basic":
            return pd.DataFrame([{"ts_code": "022365.OF", "name": "永赢科技智选C"}])
        if api_name == "fund_nav":
            assert kwargs.get("ts_code") == "022365.OF"
            return pd.DataFrame([{"end_date": "20260310", "unit_nav": 1.234}])
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_open_fund_daily("022365")
    assert not frame.empty
    assert float(frame["close"].iloc[-1]) == 1.234


def test_market_cn_tushare_north_south_flow_normalizes_to_yuan(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **_: object) -> pd.DataFrame | None:
        if api_name == "moneyflow_hsgt":
            return pd.DataFrame(
                [
                    {
                        "trade_date": "20260310",
                        "hgt": 800.0,
                        "sgt": 700.0,
                        "north_money": 1500.0,
                        "ggt_ss": 200.0,
                        "ggt_sz": 100.0,
                        "south_money": 300.0,
                    }
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_north_south_flow()
    assert list(frame.columns) == [
        "日期",
        "沪股通净流入",
        "深股通净流入",
        "北向资金净流入",
        "港股通(沪)净流入",
        "港股通(深)净流入",
        "南向资金净流入",
    ]
    assert frame.loc[0, "日期"] == "2026-03-10"
    assert float(frame.loc[0, "北向资金净流入"]) == 1_500_000_000.0
    assert float(frame.loc[0, "南向资金净流入"]) == 300_000_000.0


def test_market_cn_north_south_flow_returns_empty_without_akshare_fallback(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_ts_north_south_flow", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "_ak_function", lambda *_: (_ for _ in ()).throw(AssertionError("AKShare north/south fallback should not be used")))

    frame = collector.get_north_south_flow()

    assert frame.empty is True


def test_market_cn_margin_trading_returns_empty_without_akshare_fallback(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    monkeypatch.setattr(collector, "_ts_margin", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "_ak_function", lambda *_: (_ for _ in ()).throw(AssertionError("AKShare margin fallback should not be used")))
    frame = collector.get_margin_trading()

    assert frame.empty is True


def test_market_cn_stock_margin_snapshot_surfaces_crowding_risk(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260328", "20260331", "20260401"])
    monkeypatch.setattr(
        collector,
        "_ts_margin_detail_snapshot",
        lambda **kwargs: pd.DataFrame(
            [
                {"ts_code": "300308.SZ", "trade_date": "20260328", "rzye": 1_000_000_000.0, "rzmre": 120_000_000.0, "rzche": 100_000_000.0, "rqye": 10_000_000.0, "rzrqye": 1_010_000_000.0},
                {"ts_code": "300308.SZ", "trade_date": "20260331", "rzye": 1_080_000_000.0, "rzmre": 180_000_000.0, "rzche": 120_000_000.0, "rqye": 11_000_000.0, "rzrqye": 1_091_000_000.0},
                {"ts_code": "300308.SZ", "trade_date": "20260401", "rzye": 1_180_000_000.0, "rzmre": 240_000_000.0, "rzche": 150_000_000.0, "rqye": 12_000_000.0, "rzrqye": 1_192_000_000.0},
            ]
        ),
    )

    snapshot = collector.get_stock_margin_snapshot("300308", as_of="2026-04-01", display_name="中际旭创")

    assert snapshot["status"] == "⚠️"
    assert snapshot["is_fresh"] is True
    assert snapshot["crowding_level"] == "high"
    assert round(float(snapshot["buy_repay_ratio"]), 2) == 1.60
    assert round(float(snapshot["five_day_change_pct"]), 2) == 0.18


def test_market_cn_stock_margin_snapshot_permission_block_does_not_fake_fresh(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260401"])

    def fake_margin_detail(**kwargs):  # noqa: ANN003
        raise RuntimeError("not enough points")

    monkeypatch.setattr(collector, "_ts_margin_detail_snapshot", fake_margin_detail)

    snapshot = collector.get_stock_margin_snapshot("300308", as_of="2026-04-01", display_name="中际旭创")

    assert snapshot["diagnosis"] == "permission_blocked"
    assert snapshot["is_fresh"] is False
    assert snapshot["status"] == "ℹ️"
