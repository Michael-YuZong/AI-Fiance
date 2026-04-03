"""Tests for shared Tushare index-topic collector."""

from __future__ import annotations

import pandas as pd

from src.collectors.index_topic import INDEX_BASIC_MARKETS, IndexTopicCollector


def test_index_topic_index_weight_uses_index_code_candidates(monkeypatch, tmp_path):
    collector = IndexTopicCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "index_weight"
        if kwargs.get("index_code") == "000300.SH":
            return pd.DataFrame(
                [
                    {"trade_date": "20260310", "con_code": "600519.SH", "weight": 5.0, "con_name": "贵州茅台"},
                    {"trade_date": "20260310", "con_code": "300750.SZ", "weight": 4.0, "con_name": "宁德时代"},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_cn_index_constituent_weights("000300", top_n=2)
    assert list(frame["symbol"]) == ["600519", "300750"]


def test_index_topic_snapshot_prefers_specific_theme_proxy_over_generic_keyword(monkeypatch, tmp_path):
    collector = IndexTopicCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_basic_frame",
        lambda: pd.DataFrame(
            [
                {"ts_code": "399276.SZ", "name": "创科技", "fullname": "创科技指数", "market": "SZSE"},
                {"ts_code": "980087.CSI", "name": "人工智能精选", "fullname": "中证人工智能精选指数", "market": "CSI"},
                {"ts_code": "970070.CSI", "name": "创业板人工智能", "fullname": "创业板人工智能指数", "market": "CSI"},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "_latest_index_dailybasic_snapshot",
        lambda code, as_of: (
            (
                {"pe_ttm": 35.5 if code == "399276.SZ" else 71.6 if code == "980087.CSI" else 69.1, "trade_date": "2026-03-10", "is_fresh": True},
                "live",
            )
        ),
    )

    snapshot = collector.get_cn_index_snapshot(["中证人工智能主题指数", "人工智能", "科技"])
    assert snapshot is not None
    assert snapshot["index_name"] == "人工智能精选"
    assert snapshot["display_label"] == "指数估值代理"
    assert "代理" in snapshot["match_note"]


def test_index_topic_snapshot_limits_dailybasic_lookups_to_top_ranked_candidates(monkeypatch, tmp_path):
    collector = IndexTopicCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_basic_frame",
        lambda: pd.DataFrame(
            [
                {"ts_code": "980087.CSI", "name": "人工智能精选", "fullname": "中证人工智能精选指数", "market": "CSI"},
                {"ts_code": "970070.CSI", "name": "创业板人工智能", "fullname": "创业板人工智能指数", "market": "CSI"},
                {"ts_code": "399276.SZ", "name": "创科技", "fullname": "创科技指数", "market": "SZSE"},
                {"ts_code": "930599.CSI", "name": "软件服务", "fullname": "中证软件服务指数", "market": "CSI"},
            ]
        ),
    )
    calls: list[str] = []

    def fake_dailybasic(code: str, as_of):  # noqa: ANN001
        calls.append(code)
        return ({"pe_ttm": 71.6, "trade_date": "2026-03-10", "is_fresh": True}, "live")

    monkeypatch.setattr(collector, "_latest_index_dailybasic_snapshot", fake_dailybasic)

    snapshot = collector.get_cn_index_snapshot(["中证人工智能主题指数", "人工智能", "科技"])

    assert snapshot is not None
    assert snapshot["index_name"] == "人工智能精选"
    assert calls == ["980087.CSI"]


def test_index_topic_snapshot_keeps_exact_benchmark_without_pe_instead_of_wrong_theme_proxy(monkeypatch, tmp_path):
    collector = IndexTopicCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_basic_frame",
        lambda: pd.DataFrame(
            [
                {"ts_code": "987008.CSI", "name": "港股通科技", "fullname": "恒生港股通科技主题指数", "market": "CSI"},
                {"ts_code": "980087.CSI", "name": "人工智能精选", "fullname": "中证人工智能精选指数", "market": "CSI"},
            ]
        ),
    )

    def fake_dailybasic(code: str, as_of):  # noqa: ANN001
        if code == "987008.CSI":
            return ({}, "empty")
        return ({"pe_ttm": 71.6, "trade_date": "2026-03-10", "is_fresh": True}, "live")

    monkeypatch.setattr(collector, "_latest_index_dailybasic_snapshot", fake_dailybasic)

    snapshot = collector.get_cn_index_snapshot(["恒生港股通科技主题指数", "港股通科技", "科技"])
    assert snapshot is not None
    assert snapshot["index_name"] == "港股通科技"
    assert snapshot["match_quality"] == "exact_no_pe"
    assert snapshot["display_label"] == "真实指数估值"
    assert "缺少可用滚动PE" in snapshot["match_note"]


def test_index_topic_snapshot_blocks_wrong_theme_proxy_for_broad_benchmark(monkeypatch, tmp_path):
    collector = IndexTopicCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_basic_frame",
        lambda: pd.DataFrame(
            [
                {"ts_code": "930301.CSI", "name": "绿色电力", "fullname": "中证绿色电力指数", "market": "CSI"},
                {"ts_code": "930599.CSI", "name": "创业软件", "fullname": "中证创业软件指数", "market": "CSI"},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "_latest_index_dailybasic_snapshot",
        lambda code, as_of: (
            (
                {"pe_ttm": 20.9 if code == "930301.CSI" else 60.4, "trade_date": "2026-03-10", "is_fresh": True},
                "live",
            )
        ),
    )

    snapshot = collector.get_cn_index_snapshot(["中证A500指数", "中证A500", "宽基"])

    assert snapshot is not None
    assert snapshot["index_name"] == "中证A500指数"
    assert snapshot["match_quality"] == "benchmark_no_proxy"
    assert snapshot["pe_ttm"] is None
    assert "不再回退到其他主题指数代理" in snapshot["match_note"]


def test_index_topic_snapshot_prefers_explicit_index_code_over_generic_proxy(monkeypatch, tmp_path):
    collector = IndexTopicCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_basic_frame",
        lambda: pd.DataFrame(
            [
                {"ts_code": "000001.SH", "name": "上证指数", "fullname": "上证综合指数", "market": "SSE"},
                {"ts_code": "000685.SH", "name": "科创芯片", "fullname": "上证科创板芯片指数", "market": "SSE"},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "_latest_index_dailybasic_snapshot",
        lambda code, as_of: ({"pe_ttm": 48.3, "trade_date": "2026-04-01", "is_fresh": True}, "live"),
    )

    snapshot = collector.get_cn_index_snapshot(["科技", "上证科创板芯片指数", "000685.SH"])

    assert snapshot is not None
    assert snapshot["index_name"] == "科创芯片"
    assert snapshot["index_code"] == "000685.SH"
    assert snapshot["match_quality"] == "exact_code"


def test_index_topic_snapshot_keeps_explicit_code_anchor_when_index_basic_missing(monkeypatch, tmp_path):
    collector = IndexTopicCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_basic_frame",
        lambda: pd.DataFrame(
            [
                {"ts_code": "932092.CSI", "name": "数据中心", "fullname": "中证数据中心指数", "market": "CSI"},
            ]
        ),
    )

    snapshot = collector.get_cn_index_snapshot(["科技", "中证全指半导体产品与设备指数", "H30184.CSI"])

    assert snapshot is not None
    assert snapshot["index_name"] == "中证全指半导体产品与设备指数"
    assert snapshot["index_code"] == "H30184.CSI"
    assert snapshot["match_quality"] == "explicit_code_unmatched"
    assert snapshot["pe_ttm"] is None
    assert "不再回退到其他主题指数代理" in snapshot["match_note"]


def test_index_topic_reuses_process_index_basic_cache(monkeypatch, tmp_path):
    collector = IndexTopicCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    IndexTopicCollector._INDEX_BASIC_FRAME = None
    calls = {"count": 0}

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "index_basic"
        calls["count"] += 1
        if kwargs.get("market") == "CSI":
            return pd.DataFrame([{"ts_code": "931160.CSI", "name": "中证光模块", "fullname": "中证光模块指数", "market": "CSI"}])
        return pd.DataFrame()

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    first = collector._index_basic_frame()
    second = collector._index_basic_frame()

    assert not first.empty
    assert not second.empty
    assert calls["count"] == len(INDEX_BASIC_MARKETS)
    IndexTopicCollector._INDEX_BASIC_FRAME = None


def test_index_topic_domestic_overview_supports_weekly_and_monthly_periods(monkeypatch, tmp_path):
    collector = IndexTopicCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    calls: list[tuple[str, str]] = []

    def fake_history(symbol: str, period: str = "daily", start_date: str = "", end_date: str = "") -> pd.DataFrame:  # noqa: ARG001
        calls.append((symbol, period))
        return pd.DataFrame(
            [
                {"日期": pd.Timestamp("2026-03-01"), "开盘": 9.8, "最高": 10.1, "最低": 9.7, "收盘": 10.0, "成交量": 900, "成交额": 9_000_000.0},
                {"日期": pd.Timestamp("2026-03-08"), "开盘": 10.0, "最高": 10.5, "最低": 9.8, "收盘": 10.2, "成交量": 1000, "成交额": 10_000_000.0},
            ]
        )

    monkeypatch.setattr(collector, "get_index_history", fake_history)

    weekly_rows = collector.get_domestic_overview_rows([{"symbol": "000300", "name": "沪深300"}], period="weekly")
    monthly_rows = collector.get_domestic_overview_rows([{"symbol": "000300", "name": "沪深300"}], period="monthly")

    assert weekly_rows and monthly_rows
    assert weekly_rows[0]["source"] == "tushare.index_weekly"
    assert monthly_rows[0]["source"] == "tushare.index_monthly"
    assert calls[0][1] == "weekly"
    assert calls[1][1] == "monthly"


def test_index_topic_history_snapshot_summarizes_weekly_and_monthly_periods(monkeypatch, tmp_path):
    collector = IndexTopicCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    frame = pd.DataFrame(
        [
            {"日期": pd.Timestamp("2026-03-01"), "开盘": 9.8, "最高": 10.1, "最低": 9.7, "收盘": 10.0, "成交量": 900, "成交额": 9_000_000.0},
            {"日期": pd.Timestamp("2026-03-08"), "开盘": 10.0, "最高": 10.5, "最低": 9.8, "收盘": 10.8, "成交量": 1000, "成交额": 10_000_000.0},
        ]
    )
    monkeypatch.setattr(collector, "get_index_history", lambda *args, **kwargs: frame.copy())  # noqa: ARG005

    weekly = collector.get_index_history_snapshot("000300", period="weekly", reference_date=pd.Timestamp("2026-04-02").to_pydatetime())
    monthly = collector.get_index_history_snapshot("000300", period="monthly", reference_date=pd.Timestamp("2026-04-02").to_pydatetime())

    assert weekly["source_label"] == "Tushare 指数周线"
    assert monthly["source_label"] == "Tushare 指数月线"
    assert weekly["trend_label"] == "趋势偏强"
    assert monthly["trend_label"] == "趋势偏强"
    assert "最近一周" in weekly["summary"]
    assert "最近一月" in monthly["summary"] or "最近一" in monthly["summary"]
    assert weekly["signal_strength"] == "高"


def test_index_topic_domestic_overview_includes_weekly_and_monthly_summary_fields(monkeypatch, tmp_path):
    collector = IndexTopicCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    frame = pd.DataFrame(
        [
            {"日期": pd.Timestamp("2026-03-01"), "开盘": 9.8, "最高": 10.1, "最低": 9.7, "收盘": 10.0, "成交量": 900, "成交额": 9_000_000.0},
            {"日期": pd.Timestamp("2026-03-08"), "开盘": 10.0, "最高": 10.5, "最低": 9.8, "收盘": 10.8, "成交量": 1000, "成交额": 10_000_000.0},
        ]
    )
    monkeypatch.setattr(collector, "get_index_history", lambda *args, **kwargs: frame.copy())  # noqa: ARG005

    rows = collector.get_domestic_overview_rows([{"symbol": "000300", "name": "沪深300"}])

    assert rows
    assert rows[0]["weekly_summary"]
    assert rows[0]["monthly_summary"]
    assert rows[0]["weekly_source_label"] == "Tushare 指数周线"
    assert rows[0]["monthly_source_label"] == "Tushare 指数月线"
