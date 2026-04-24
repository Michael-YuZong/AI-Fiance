"""Tests for valuation collector Tushare-first helpers."""

from __future__ import annotations

import pandas as pd

from src.collectors.valuation import ValuationCollector


def test_valuation_index_weight_uses_index_code_candidates(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_topic_collector",
        lambda: type(
            "FakeIndexTopic",
            (),
            {
                "get_cn_index_constituent_weights": staticmethod(
                    lambda index_code, top_n=10: pd.DataFrame(
                        [
                            {"symbol": "600519", "name": "贵州茅台", "weight": 5.0},
                            {"symbol": "300750", "name": "宁德时代", "weight": 4.0},
                        ]
                    )
                )
            },
        )(),
    )
    frame = collector.get_cn_index_constituent_weights("000300", top_n=2)
    assert list(frame["symbol"]) == ["600519", "300750"]


def test_valuation_index_snapshot_prefers_specific_theme_proxy_over_generic_keyword(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_topic_collector",
        lambda: type(
            "FakeIndexTopic",
            (),
            {
                "get_cn_index_snapshot": staticmethod(
                    lambda keywords: {
                        "index_name": "人工智能精选",
                        "display_label": "指数估值代理",
                        "match_note": "指数主链未直接命中精确基准，当前使用最接近的主题指数代理。",
                    }
                )
            },
        )(),
    )
    snapshot = collector.get_cn_index_snapshot(["中证人工智能主题指数", "人工智能", "科技"])
    assert snapshot is not None
    assert snapshot["index_name"] == "人工智能精选"
    assert snapshot["display_label"] == "指数估值代理"
    assert "代理" in snapshot["match_note"]


def test_valuation_index_snapshot_keeps_exact_benchmark_without_pe_instead_of_wrong_theme_proxy(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_topic_collector",
        lambda: type(
            "FakeIndexTopic",
            (),
            {
                "get_cn_index_snapshot": staticmethod(
                    lambda keywords: {
                        "index_name": "港股通科技",
                        "match_quality": "exact_no_pe",
                        "display_label": "真实指数估值",
                        "match_note": "指数主链已命中基准指数，但当前缺少可用滚动PE。",
                    }
                )
            },
        )(),
    )
    snapshot = collector.get_cn_index_snapshot(["恒生港股通科技主题指数", "港股通科技", "科技"])
    assert snapshot is not None
    assert snapshot["index_name"] == "港股通科技"
    assert snapshot["match_quality"] == "exact_no_pe"
    assert snapshot["display_label"] == "真实指数估值"
    assert "缺少可用滚动PE" in snapshot["match_note"]


def test_valuation_index_snapshot_blocks_wrong_theme_proxy_for_broad_benchmark(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_index_topic_collector",
        lambda: type(
            "FakeIndexTopic",
            (),
            {
                "get_cn_index_snapshot": staticmethod(
                    lambda keywords: {
                        "index_name": "中证A500指数",
                        "match_quality": "benchmark_no_proxy",
                        "display_label": "真实指数估值",
                        "pe_ttm": None,
                        "match_note": "指数主链未直接命中 `中证A500指数`；为避免错配，不再回退到其他主题指数代理。",
                    }
                )
            },
        )(),
    )
    snapshot = collector.get_cn_index_snapshot(["中证A500指数", "中证A500", "宽基"])

    assert snapshot is not None
    assert snapshot["index_name"] == "中证A500指数"
    assert snapshot["match_quality"] == "benchmark_no_proxy"
    assert snapshot["pe_ttm"] is None
    assert "不再回退到其他主题指数代理" in snapshot["match_note"]


def test_weighted_market_financial_proxies_uses_harmonic_pe(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "get_yf_fundamental",
        lambda symbol, asset_type: {  # noqa: ARG005
            "0700": {"pe_ttm": 20.0, "roe": 20.0},
            "9988": {"pe_ttm": 40.0, "roe": 10.0},
        }.get(symbol, {}),
    )

    proxies = collector.get_weighted_market_financial_proxies(
        [
            {"symbol": "0700", "name": "腾讯", "weight": 15.0},
            {"symbol": "9988", "name": "阿里", "weight": 15.0},
        ],
        asset_type="hk",
        top_n=2,
    )

    assert proxies["coverage_weight"] == 30.0
    assert proxies["coverage_count"] == 2
    assert proxies["pe_ttm"] == 26.666666666666668


def test_weighted_stock_financial_proxies_include_harmonic_pe(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "get_cn_stock_financial_proxy",
        lambda symbol: {  # noqa: ARG005
            "000998": {"pe_ttm": 10.0, "roe": 8.0, "gross_margin": 20.0},
            "600598": {"pe_ttm": 30.0, "roe": 12.0, "gross_margin": 30.0},
        }.get(symbol, {}),
    )

    proxies = collector.get_weighted_stock_financial_proxies(
        [
            {"symbol": "000998", "name": "隆平高科", "weight": 15.0},
            {"symbol": "600598", "name": "北大荒", "weight": 15.0},
        ],
        top_n=2,
    )

    assert proxies["coverage_weight"] == 30.0
    assert proxies["coverage_count"] == 2
    assert proxies["pe_ttm"] == 15.0


def test_valuation_index_snapshot_reuses_process_index_master_frame(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    calls = {"count": 0}
    fake_snapshot = {
        "index_name": "中证光模块",
        "pe_ttm": 42.5,
    }

    def fake_get_cn_index_snapshot(keywords):  # noqa: ANN001
        calls["count"] += 1
        return dict(fake_snapshot)

    monkeypatch.setattr(
        collector,
        "_index_topic_collector",
        lambda: type("FakeIndexTopic", (), {"get_cn_index_snapshot": staticmethod(fake_get_cn_index_snapshot)})(),
    )

    first = collector.get_cn_index_snapshot(["光模块"])
    second = collector.get_cn_index_snapshot(["光模块"])

    assert first is not None
    assert second is not None
    assert calls["count"] == 2


def test_valuation_etf_nav_resolves_tushare_fund_code(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "fund_basic":
            return pd.DataFrame([{"ts_code": "510300.SH", "name": "沪深300ETF华泰柏瑞"}])
        if api_name == "fund_nav":
            assert kwargs.get("ts_code") == "510300.SH"
            return pd.DataFrame([{"end_date": "20260310", "unit_nav": 3.0}])
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    frame = collector.get_cn_etf_nav_history("510300", days=30)
    assert not frame.empty
    assert str(frame.iloc[-1]["end_date"].date()) == "2026-03-10"


def test_valuation_etf_nav_returns_empty_without_akshare_fallback(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "fund_basic":
            return pd.DataFrame([{"ts_code": "510300.SH", "name": "沪深300ETF华泰柏瑞"}])
        if api_name == "fund_nav":
            assert kwargs.get("ts_code") == "510300.SH"
            return pd.DataFrame()
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    frame = collector.get_cn_etf_nav_history("510300", days=30)
    assert frame.empty is True


def test_valuation_etf_scale_uses_tushare_share_size_without_akshare_fallback(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda lookback_days=21, exchange="SSE": ["20260328", "20260331"])  # noqa: ARG005

    def fake_snapshot(**kwargs: object) -> pd.DataFrame:
        assert kwargs.get("ts_code") == "510300.SH"
        assert kwargs.get("start_date") == "20260328"
        assert kwargs.get("end_date") == "20260331"
        return pd.DataFrame(
            [
                {"ts_code": "510300.SH", "trade_date": "20260331", "total_share": 3_091_998.74, "total_size": 3_818_927.64, "exchange": "SSE"},
                {"ts_code": "510300.SH", "trade_date": "20260328", "total_share": 3_060_100.00, "total_size": 3_800_000.00, "exchange": "SSE"},
            ]
        )

    monkeypatch.setattr(collector, "_ts_etf_share_size_snapshot", fake_snapshot)

    snapshot = collector.get_cn_etf_scale("510300")
    assert snapshot is not None
    assert snapshot["trade_date"] == "2026-03-31"
    assert snapshot["total_share"] == 3_091_998.74
    assert snapshot["total_size"] == 3_818_927.64


def test_cn_stock_financial_proxy_merges_daily_basic_into_fina_indicator(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_tushare_stock_financial",
        lambda symbol: {  # noqa: ARG005
            "report_date": "2025-12-31",
            "roe": 15.0,
            "gross_margin": 28.0,
        },
    )
    monkeypatch.setattr(
        collector,
        "_ts_daily_basic_for_stock",
        lambda symbol: {  # noqa: ARG005
            "pe_ttm": 24.5,
            "pb": 3.1,
            "ps_ttm": 2.8,
        },
    )
    result = collector.get_cn_stock_financial_proxy("300750")
    assert result["report_date"] == "2025-12-31"
    assert result["roe"] == 15.0
    assert result["pe_ttm"] == 24.5
    assert result["pb"] == 3.1


def test_cn_stock_financial_proxy_returns_daily_basic_without_akshare_fallback(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_tushare_stock_financial", lambda symbol: {})  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "_ts_daily_basic_for_stock",
        lambda symbol: {  # noqa: ARG005
            "pe_ttm": 24.5,
            "pb": 3.1,
            "ps_ttm": 2.8,
        },
    )

    result = collector.get_cn_stock_financial_proxy("300750")
    assert result["pe_ttm"] == 24.5
    assert result["pb"] == 3.1
    assert result["ps_ttm"] == 2.8


def test_valuation_disclosure_date_normalizes_dates(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        assert api_name == "disclosure_date"
        assert kwargs.get("ts_code") == "300502.SZ"
        return pd.DataFrame(
            [
                {"ts_code": "300502.SZ", "ann_date": "20260101", "end_date": "20251231", "pre_date": "20260424", "actual_date": None},
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    rows = collector.get_cn_stock_disclosure_dates("300502")
    assert rows[0]["pre_date"] == "2026-04-24"
    assert rows[0]["end_date"] == "2025-12-31"


def test_valuation_holdertrade_and_capital_return_normalize_fields(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "stk_holdertrade":
            return pd.DataFrame(
                [
                    {"ts_code": "300502.SZ", "ann_date": "20260305", "holder_name": "张三", "holder_type": "G", "in_de": "IN", "change_vol": "100000", "change_ratio": "0.12"},
                ]
            )
        if api_name == "repurchase":
            return pd.DataFrame(
                [
                    {"ts_code": "300502.SZ", "ann_date": "20260304", "proc": "实施", "amount": "1000000000"},
                ]
            )
        if api_name == "dividend":
            return pd.DataFrame(
                [
                    {"ts_code": "300502.SZ", "ann_date": "20260311", "end_date": "20251231", "div_proc": "预案", "cash_div_tax": "0.65"},
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    holder_rows = collector.get_cn_stock_holder_trades("300502")
    repurchase_rows = collector.get_cn_stock_repurchase("300502")
    dividend_rows = collector.get_cn_stock_dividend("300502")
    assert holder_rows[0]["ann_date"] == "2026-03-05"
    assert holder_rows[0]["change_ratio"] == 0.12
    assert repurchase_rows[0]["proc"] == "实施"
    assert dividend_rows[0]["div_proc"] == "预案"


def test_valuation_top_holders_and_pledge_normalize_fields(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "top10_holders":
            return pd.DataFrame(
                [
                    {"ts_code": "300502.SZ", "ann_date": "20251030", "end_date": "20250930", "holder_name": "张三", "hold_amount": "1000000", "hold_ratio": "6.5", "hold_float_ratio": "1.8", "hold_change": "0"},
                ]
            )
        if api_name == "top10_floatholders":
            return pd.DataFrame(
                [
                    {"ts_code": "300502.SZ", "ann_date": "20251030", "end_date": "20250930", "holder_name": "李四", "hold_amount": "500000", "hold_ratio": "2.5", "hold_float_ratio": "2.3", "hold_change": "0"},
                ]
            )
        if api_name == "pledge_stat":
            return pd.DataFrame(
                [
                    {"ts_code": "300502.SZ", "end_date": "20260306", "pledge_count": 2, "unrest_pledge": 1000, "rest_pledge": 500, "total_share": 99400.9312, "pledge_ratio": 6.2},
                ]
            )
        if api_name == "pledge_detail":
            return pd.DataFrame(
                [
                    {"ts_code": "300502.SZ", "ann_date": "20260301", "holder_name": "张三", "pledge_amount": 500, "start_date": "20260201", "end_date": "20270301", "is_release": "0", "release_date": None, "pledgor": "某券商", "holding_amount": 1000, "pledged_amount": 500, "p_total_ratio": 0.5, "h_total_ratio": 50.0, "is_buyback": "1"},
                ]
            )
        raise AssertionError(api_name)

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)
    holder_rows = collector.get_cn_stock_top10_holders("300502")
    float_holder_rows = collector.get_cn_stock_top10_floatholders("300502")
    pledge_stat_rows = collector.get_cn_stock_pledge_stat("300502")
    pledge_detail_rows = collector.get_cn_stock_pledge_detail("300502")
    assert holder_rows[0]["hold_ratio"] == 6.5
    assert float_holder_rows[0]["hold_float_ratio"] == 2.3
    assert pledge_stat_rows[0]["pledge_ratio"] == 6.2
    assert pledge_detail_rows[0]["h_total_ratio"] == 50.0


def test_cn_stock_chip_snapshot_aggregates_cyq_perf_and_cyq_chips(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda lookback_days=14, exchange="SSE": ["20260331", "20260401"])  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "_ts_cyq_perf_snapshot",
        lambda ts_code="", start_date="", end_date="": pd.DataFrame(  # noqa: ARG005
            [
                {
                    "ts_code": "300308.SZ",
                    "trade_date": "20260401",
                    "cost_15pct": 92.0,
                    "cost_50pct": 100.0,
                    "cost_85pct": 109.0,
                    "weight_avg": 101.0,
                    "winner_rate": 0.72,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "_ts_cyq_chips_snapshot",
        lambda ts_code="", trade_date="": pd.DataFrame(  # noqa: ARG005
            [
                {"ts_code": "300308.SZ", "trade_date": "20260401", "price": 99.0, "percent": 18.0},
                {"ts_code": "300308.SZ", "trade_date": "20260401", "price": 104.0, "percent": 22.0},
                {"ts_code": "300308.SZ", "trade_date": "20260401", "price": 111.0, "percent": 31.0},
            ]
        ),
    )

    snapshot = collector.get_cn_stock_chip_snapshot("300308", as_of="2026-04-01", current_price=106.0)

    assert snapshot["status"] == "matched"
    assert snapshot["diagnosis"] == "live"
    assert snapshot["is_fresh"] is True
    assert snapshot["source"] == "tushare.cyq_perf+tushare.cyq_chips"
    assert snapshot["fallback"] == "none"
    assert snapshot["winner_rate_pct"] == 72.0
    assert snapshot["weight_avg"] == 101.0
    assert snapshot["cost_50pct"] == 100.0
    assert snapshot["peak_price"] == 111.0
    assert snapshot["components"]["cyq_perf"]["status"] == "matched"
    assert snapshot["components"]["cyq_chips"]["status"] == "matched"


def test_cn_stock_chip_snapshot_prefers_near_window_before_broad_fallback(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda lookback_days=14, exchange="SSE": ["20260331", "20260401"])  # noqa: ARG005
    calls: list[tuple[str, str]] = []

    def _perf(ts_code="", start_date="", end_date=""):  # noqa: ARG001
        calls.append((start_date, end_date))
        if len(calls) == 1:
            return pd.DataFrame()
        return pd.DataFrame(
            [
                {
                    "ts_code": "300308.SZ",
                    "trade_date": "20260401",
                    "cost_15pct": 92.0,
                    "cost_50pct": 100.0,
                    "cost_85pct": 109.0,
                    "weight_avg": 101.0,
                    "winner_rate": 0.72,
                }
            ]
        )

    monkeypatch.setattr(collector, "_ts_cyq_perf_snapshot", _perf)
    monkeypatch.setattr(
        collector,
        "_ts_cyq_chips_snapshot",
        lambda ts_code="", trade_date="": pd.DataFrame(  # noqa: ARG005
            [
                {"ts_code": "300308.SZ", "trade_date": "20260401", "price": 99.0, "percent": 18.0},
                {"ts_code": "300308.SZ", "trade_date": "20260401", "price": 104.0, "percent": 22.0},
            ]
        ),
    )

    snapshot = collector.get_cn_stock_chip_snapshot("300308", as_of="2026-04-01", current_price=106.0)

    assert snapshot["status"] == "matched"
    assert len(calls) == 2
    assert calls[0] == ("20260322", "20260401")
    assert calls[1] == ("20260101", "20260401")


def test_cn_stock_chip_snapshot_permission_block_does_not_fake_fresh_hit(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda lookback_days=14, exchange="SSE": ["20260401"])  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "_ts_cyq_perf_snapshot",
        lambda ts_code="", start_date="", end_date="": (_ for _ in ()).throw(RuntimeError("抱歉，积分不足，无权限访问")),  # noqa: ARG005
    )
    monkeypatch.setattr(
        collector,
        "_ts_cyq_chips_snapshot",
        lambda ts_code="", trade_date="": pd.DataFrame(),  # noqa: ARG005
    )

    snapshot = collector.get_cn_stock_chip_snapshot("300308", as_of="2026-04-01", current_price=106.0)

    assert snapshot["status"] == "blocked"
    assert snapshot["diagnosis"] == "permission_blocked"
    assert snapshot["is_fresh"] is False
    assert "未授权或积分不足" in snapshot["detail"]
    assert snapshot["components"]["cyq_perf"]["status"] == "blocked"
    assert snapshot["components"]["cyq_chips"]["status"] == "empty"


def test_cn_stock_chip_snapshot_stale_match_is_not_fresh(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "_recent_open_trade_dates",
        lambda lookback_days=14, exchange="SSE": [item.strftime("%Y%m%d") for item in pd.bdate_range("2026-03-20", "2026-04-01")],  # noqa: ARG005
    )
    monkeypatch.setattr(
        collector,
        "_ts_cyq_perf_snapshot",
        lambda ts_code="", start_date="", end_date="": pd.DataFrame(  # noqa: ARG005
            [
                {
                    "ts_code": "300308.SZ",
                    "trade_date": "20260320",
                    "cost_15pct": 92.0,
                    "cost_50pct": 100.0,
                    "cost_85pct": 109.0,
                    "weight_avg": 101.0,
                    "winner_rate": 0.72,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "_ts_cyq_chips_snapshot",
        lambda ts_code="", trade_date="": pd.DataFrame(  # noqa: ARG005
            [
                {"ts_code": "300308.SZ", "trade_date": "20260320", "price": 99.0, "percent": 18.0},
                {"ts_code": "300308.SZ", "trade_date": "20260320", "price": 104.0, "percent": 22.0},
            ]
        ),
    )

    snapshot = collector.get_cn_stock_chip_snapshot("300308", as_of="2026-04-01", current_price=106.0)

    assert snapshot["status"] == "matched"
    assert snapshot["diagnosis"] == "stale"
    assert snapshot["latest_date"] == "2026-03-20"
    assert snapshot["is_fresh"] is False
    assert "不按 fresh 命中处理" in snapshot["detail"]


def test_cn_stock_chip_snapshot_marks_previous_trade_day_as_t1_direct(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda lookback_days=14, exchange="SSE": ["20260401", "20260402"])  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "_ts_cyq_perf_snapshot",
        lambda ts_code="", start_date="", end_date="": pd.DataFrame(  # noqa: ARG005
            [
                {
                    "ts_code": "300308.SZ",
                    "trade_date": "20260401",
                    "cost_15pct": 92.0,
                    "cost_50pct": 100.0,
                    "cost_85pct": 109.0,
                    "weight_avg": 101.0,
                    "winner_rate": 0.72,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "_ts_cyq_chips_snapshot",
        lambda ts_code="", trade_date="": pd.DataFrame(  # noqa: ARG005
            [
                {"ts_code": "300308.SZ", "trade_date": "20260401", "price": 99.0, "percent": 18.0},
                {"ts_code": "300308.SZ", "trade_date": "20260401", "price": 104.0, "percent": 22.0},
            ]
        ),
    )

    snapshot = collector.get_cn_stock_chip_snapshot("300308", as_of="2026-04-02", current_price=106.0)

    assert snapshot["status"] == "matched"
    assert snapshot["diagnosis"] == "stale"
    assert snapshot["trade_gap_days"] == 1
    assert "T+1 直连" in snapshot["detail"]


def test_cn_stock_factor_snapshot_marks_stale_rows_without_faking_fresh(monkeypatch, tmp_path):
    collector = ValuationCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "_recent_open_trade_dates", lambda lookback_days=30, exchange="SSE": ["20260401", "20260403"])  # noqa: ARG005
    calls: list[tuple[str, str, str]] = []

    def fake_ts_call(api_name: str, **kwargs: object) -> pd.DataFrame:
        calls.append((api_name, str(kwargs.get("ts_code", "")), str(kwargs.get("start_date", "")) + ":" + str(kwargs.get("end_date", ""))))
        assert api_name == "stk_factor_pro"
        assert kwargs.get("ts_code") == "300750.SZ"
        assert kwargs.get("start_date") == "20260304"
        assert kwargs.get("end_date") == "20260403"
        return pd.DataFrame(
            [
                {
                    "ts_code": "300750.SZ",
                    "trade_date": "20260401",
                    "close_qfq": 100.0,
                    "bbi_qfq": 98.0,
                    "bias1_qfq": 2.1,
                    "dmi_pdi_qfq": 28.0,
                    "dmi_mdi_qfq": 18.0,
                    "dmi_adx_qfq": 31.0,
                    "pct_chg": 2.5,
                    "volume_ratio": 1.4,
                    "turnover_rate": 3.1,
                    "turnover_rate_f": 2.7,
                    "atr_qfq": 3.2,
                    "obv_qfq": 1234567.0,
                    "rsi_qfq": 58.0,
                }
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    snapshot = collector.get_cn_stock_factor_snapshot("300750", as_of="2026-04-03", lookback_days=30)

    assert len(calls) == 1
    assert snapshot["status"] == "matched"
    assert snapshot["diagnosis"] == "stale"
    assert snapshot["latest_date"] == "2026-04-01"
    assert snapshot["is_fresh"] is False
    assert snapshot["source"] == "tushare.stk_factor_pro"
    assert snapshot["trend_label"] == "趋势偏强"
    assert snapshot["momentum_label"] == "动能改善"
    assert snapshot["components"]["stk_factor_pro"]["status"] == "matched"
    assert "不按 fresh 命中处理" in snapshot["detail"]
