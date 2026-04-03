"""Tests for market pulse collector Tushare-first pools."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.collectors.market_pulse import MarketPulseCollector


def test_market_pulse_limit_pool_prefers_tushare(monkeypatch, tmp_path):
    collector = MarketPulseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260310"])

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "limit_list_d":
            assert kwargs.get("limit_type") == "U"
            return pd.DataFrame(
                [
                    {"ts_code": "300750.SZ", "name": "宁德时代", "industry": "新能源设备", "pct_chg": 9.99, "up_stat": "2/2"},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    info = collector._ts_limit_pool("U", datetime(2026, 3, 10))
    frame = info["frame"]
    assert info["date"] == "2026-03-10"
    assert frame.loc[0, "名称"] == "宁德时代"
    assert frame.loc[0, "所属行业"] == "新能源设备"
    assert float(frame.loc[0, "连板数"]) == 2.0


def test_market_pulse_same_day_limit_pool_skips_intraday_cache(monkeypatch, tmp_path):
    collector = MarketPulseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 24}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260401"])

    cache_calls = {"load": 0, "save": 0}
    monkeypatch.setattr(collector, "_load_cache", lambda *args, **kwargs: cache_calls.__setitem__("load", cache_calls["load"] + 1))
    monkeypatch.setattr(collector, "_save_cache", lambda *args, **kwargs: cache_calls.__setitem__("save", cache_calls["save"] + 1))
    monkeypatch.setattr(
        collector,
        "_ts_call",
        lambda api_name, **kwargs: pd.DataFrame(
            [{"ts_code": "300750.SZ", "name": "宁德时代", "industry": "新能源设备", "pct_chg": 9.99, "up_stat": "2/2"}]
        ) if api_name == "limit_list_d" else pd.DataFrame(),
    )

    info = collector._ts_limit_pool("U", datetime(2026, 4, 1, 18, 0, 0))

    assert not info["frame"].empty
    assert cache_calls == {"load": 0, "save": 0}


def test_market_pulse_latest_pool_live_fetch_disables_cache_for_same_day(monkeypatch, tmp_path):
    collector = MarketPulseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 24}})
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        collector,
        "cached_call",
        lambda *args, **kwargs: captured.update(kwargs) or pd.DataFrame([{"代码": "300750"}]),
    )
    monkeypatch.setattr("src.collectors.market_pulse.ak", type("FakeAK", (), {"stock_zt_pool_em": staticmethod(lambda date=None: pd.DataFrame())}))

    collector._latest_pool("stock_zt_pool_em", datetime(2026, 4, 1, 18, 0, 0))

    assert captured["use_cache"] is False


def test_market_pulse_prev_zt_pool_uses_previous_limit_and_current_daily(monkeypatch, tmp_path):
    collector = MarketPulseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda *args, **kwargs: ["20260309", "20260310"])

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "limit_list_d":
            return pd.DataFrame(
                [
                    {"ts_code": "300750.SZ", "name": "宁德时代", "industry": "新能源设备", "pct_chg": 9.99, "up_stat": "1/1"},
                ]
            )
        if api_name == "daily":
            return pd.DataFrame([{"ts_code": "300750.SZ", "pct_chg": 3.5}])
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    info = collector._derive_prev_zt_pool("2026-03-10")
    frame = info["frame"]
    assert info["date"] == "2026-03-10"
    assert float(frame.loc[0, "涨跌幅"]) == 3.5


def test_market_pulse_lhb_stats_aggregates_tushare_top_list(monkeypatch, tmp_path):
    collector = MarketPulseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_recent_open_trade_dates",
        lambda *args, **kwargs: ["20260307", "20260308", "20260309", "20260310"],
    )

    def fake_top_list(trade_date: str) -> pd.DataFrame:
        if trade_date in {"2026-03-09", "2026-03-10"}:
            return pd.DataFrame(
                [
                    {"ts_code": "300750.SZ", "name": "宁德时代"},
                    {"ts_code": "600519.SH", "name": "贵州茅台"},
                ]
            )
        return pd.DataFrame([{"ts_code": "300750.SZ", "name": "宁德时代"}])

    monkeypatch.setattr(collector, "_ts_top_list", fake_top_list)
    frame = collector._ts_lhb_stats("2026-03-10")
    assert not frame.empty
    assert frame.loc[0, "名称"] == "宁德时代"
    assert int(frame.loc[0, "上榜次数"]) == 4


def test_market_pulse_collect_emits_freshness_reports_for_limit_pools(monkeypatch, tmp_path):
    collector = MarketPulseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_ts_limit_pool",
        lambda limit_type, as_of: {"date": "2026-03-10", "frame": pd.DataFrame([{"名称": "宁德时代"}]) if limit_type == "U" else pd.DataFrame()},
    )
    monkeypatch.setattr(collector, "_derive_strong_pool", lambda zt_info: {"date": zt_info["date"], "frame": pd.DataFrame([{"名称": "宁德时代"}])})
    monkeypatch.setattr(collector, "_derive_prev_zt_pool", lambda trade_date: {"date": trade_date, "frame": pd.DataFrame([{"名称": "宁德时代"}])})
    monkeypatch.setattr(collector, "_ts_top_list", lambda trade_date: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_top_inst", lambda trade_date: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_lhb_stats", lambda trade_date: pd.DataFrame())
    monkeypatch.setattr(collector, "_ts_lhb_active_desks", lambda trade_date: pd.DataFrame())

    result = collector.collect(datetime(2026, 3, 10, 18, 0, 0))

    assert result["as_of"] == "2026-03-10 18:00:00"
    assert result["zt_pool_report"]["is_fresh"] is True
    assert result["strong_pool_report"]["is_fresh"] is True
    assert result["prev_zt_pool_report"]["is_fresh"] is True
    assert result["market_date"] == "2026-03-10"


def test_market_pulse_stock_board_action_snapshot_aggregates_lhb_auction_and_limit_pools(monkeypatch, tmp_path):
    collector = MarketPulseCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_latest_open_trade_date", lambda *args, **kwargs: "20260401")
    monkeypatch.setattr(
        collector,
        "_ts_top_list",
        lambda trade_date: pd.DataFrame(
            [{"ts_code": "300308.SZ", "reason": "日涨幅偏离值达到7%", "net_amount": 200_000_000.0, "amount": 1_800_000_000.0}]
        ),
    )
    monkeypatch.setattr(
        collector,
        "_ts_top_inst",
        lambda trade_date: pd.DataFrame(
            [{"ts_code": "300308.SZ", "net_buy": 80_000_000.0}]
        ),
    )
    monkeypatch.setattr(
        collector,
        "_ts_limit_pool",
        lambda limit_type, as_of: {
            "date": "2026-04-01",
            "frame": pd.DataFrame([{"代码": "300308", "连板数": 2.0}]) if limit_type == "U" else pd.DataFrame(),
        },
    )
    monkeypatch.setattr(
        collector,
        "_derive_strong_pool",
        lambda zt_info: {"date": zt_info["date"], "frame": pd.DataFrame([{"代码": "300308", "连板数": 2.0}])},
    )
    monkeypatch.setattr(
        "src.collectors.market_cn.ChinaMarketCollector.get_stock_auction",
        lambda self, symbol, trade_date="": pd.DataFrame(  # noqa: ARG005
            [{"ts_code": "300308.SZ", "trade_date": "2026-04-01", "price": 131.0, "pre_close": 127.0, "volume_ratio": 1.8, "turnover_rate": 0.6}]
        ),
    )
    monkeypatch.setattr(
        "src.collectors.market_cn.ChinaMarketCollector.get_stock_limit",
        lambda self, symbol, trade_date="": pd.DataFrame(  # noqa: ARG005
            [{"ts_code": "300308.SZ", "trade_date": "2026-04-01", "up_limit": 131.5, "down_limit": 114.0}]
        ),
    )

    snapshot = collector.get_stock_board_action_snapshot(
        "300308",
        reference_date=datetime(2026, 4, 1, 18, 0, 0),
        display_name="中际旭创",
        current_price=131.0,
    )

    assert snapshot["status"] == "✅"
    assert snapshot["is_fresh"] is True
    assert snapshot["has_positive_signal"] is True
    assert snapshot["lhb_net_amount"] == 200_000_000.0
    assert snapshot["inst_net_amount"] == 80_000_000.0
    assert snapshot["in_strong_pool"] is True
    assert round(float(snapshot["auction_gap_pct"]), 4) == round(131.0 / 127.0 - 1.0, 4)
