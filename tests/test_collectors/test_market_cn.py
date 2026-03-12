"""Tests for CN ETF market collector fallbacks."""

from __future__ import annotations

import pandas as pd

from src.collectors.market_cn import ChinaMarketCollector


class _FakeTicker:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol

    def history(self, **_: object) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "Open": [1.0, 1.1],
                "High": [1.1, 1.2],
                "Low": [0.9, 1.0],
                "Close": [1.05, 1.15],
                "Volume": [1000, 1200],
            }
        )


class _FakeYFinance:
    def Ticker(self, symbol: str) -> _FakeTicker:  # noqa: N802
        return _FakeTicker(symbol)


def test_market_cn_falls_back_to_yahoo_when_ak_unavailable(monkeypatch):
    collector = ChinaMarketCollector(
        {
            "storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0},
            "market": {"enable_yahoo_fallback_for_cn_etf": True},
        }
    )
    monkeypatch.setattr(collector, "_ak_function", lambda *_: (_ for _ in ()).throw(RuntimeError("ak failed")))
    monkeypatch.setattr("src.collectors.market_cn.yf", _FakeYFinance())
    frame = collector.get_etf_daily("512400")
    assert not frame.empty


def test_market_cn_maps_exchange_suffix():
    collector = ChinaMarketCollector({})
    assert collector._yahoo_symbol("512400") == "512400.SS"
    assert collector._yahoo_symbol("159980") == "159980.SZ"


def test_market_cn_normalizes_open_fund_nav(monkeypatch):
    collector = ChinaMarketCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})

    def fake_cached_call(_cache_key, fetcher, *args, **kwargs):  # noqa: ANN001
        return fetcher(*args, **kwargs)

    monkeypatch.setattr(collector, "cached_call", fake_cached_call)
    monkeypatch.setattr(
        collector,
        "_ak_function",
        lambda name: (
            (lambda **_: pd.DataFrame({"净值日期": ["2026-03-07", "2026-03-08"], "单位净值": ["1.01", "1.02"]}))
            if name == "fund_open_fund_info_em"
            else (_ for _ in ()).throw(RuntimeError("unexpected"))
        ),
    )
    frame = collector.get_open_fund_daily("022365")
    assert list(frame.columns) == ["date", "open", "high", "low", "close", "volume", "amount"]
    assert float(frame["close"].iloc[-1]) == 1.02


def test_market_cn_index_daily_falls_back_to_proxy_etf(monkeypatch):
    collector = ChinaMarketCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_ak_function", lambda *_: (_ for _ in ()).throw(RuntimeError("ak failed")))
    monkeypatch.setattr(collector, "get_etf_daily", lambda symbol, **_: pd.DataFrame({"date": ["2026-03-08"], "open": [1], "high": [1], "low": [1], "close": [1], "volume": [0], "amount": [0]}))
    frame = collector.get_index_daily("000300", proxy_symbol="510330")
    assert not frame.empty


def test_market_cn_index_daily_uses_index_code_candidates(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame | None:
        assert api_name == "index_daily"
        if kwargs.get("ts_code") == "000300.SH":
            return pd.DataFrame(
                [
                    {"trade_date": "20260310", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "vol": 1000, "amount": 10000},
                    {"trade_date": "20260309", "open": 9.8, "high": 10.1, "low": 9.7, "close": 10.0, "vol": 900, "amount": 9000},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
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
        if api_name == "adj_factor":
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


def test_market_cn_etf_universe_snapshot_merges_basic_and_daily(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260312")
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


def test_market_cn_margin_fallback_combines_ak_summaries(monkeypatch, tmp_path):
    collector = ChinaMarketCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    monkeypatch.setattr(collector, "_ts_margin", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "cached_call", lambda _cache_key, fetcher, *args, **kwargs: fetcher(*args, **kwargs))

    def fake_ak_function(name: str):
        if name == "stock_margin_sse":
            return lambda **_: pd.DataFrame(
                [
                    {
                        "信用交易日期": "20260309",
                        "融资余额": 1_333_276_798_688.0,
                        "融资买入额": 120_266_698_867.0,
                        "融券余量金额": 12_372_384_331.0,
                        "融资融券余额": 1_345_649_183_019.0,
                    }
                ]
            )
        if name == "stock_margin_szse":
            return lambda **_: pd.DataFrame(
                [
                    {
                        "融资买入额": 1178.67,
                        "融资余额": 12857.72,
                        "融券余额": 57.47,
                        "融资融券余额": 12915.20,
                    }
                ]
            )
        raise AssertionError(name)

    monkeypatch.setattr(collector, "_ak_function", fake_ak_function)
    frame = collector.get_margin_trading()
    assert set(frame["交易所"]) == {"上交所", "深交所"}
    sse_row = frame[frame["交易所"] == "上交所"].iloc[0]
    szse_row = frame[frame["交易所"] == "深交所"].iloc[0]
    assert sse_row["日期"] == "2026-03-09"
    assert float(sse_row["融资余额"]) == 1_333_276_798_688.0
    assert float(szse_row["融资余额"]) == 1_285_772_000_000.0
    assert float(szse_row["融资买入额"]) == 117_867_000_000.0
