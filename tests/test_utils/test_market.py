"""Tests for shared market helpers."""

from __future__ import annotations

import pandas as pd
import pytest

from src.utils import market


def test_fetch_intraday_history_routes_cn_stock_to_a_share_collector(monkeypatch):
    calls: list[tuple[str, str]] = []
    expected = pd.DataFrame(
        {
            "时间": pd.to_datetime(["2026-04-16 09:31:00"]),
            "开盘": [43.7],
            "收盘": [43.8],
            "最高": [43.9],
            "最低": [43.6],
            "成交量": [1000],
        }
    )

    class _Collector:
        def __init__(self, config):
            self.config = config

        def get_cn_stock_intraday_chart(self, symbol: str, period: str = "1") -> pd.DataFrame:
            calls.append(("stock", symbol))
            return expected

        def get_intraday_chart(self, symbol: str, period: str = "1") -> pd.DataFrame:
            calls.append(("etf", symbol))
            return expected

    import src.collectors.intraday as intraday_module

    monkeypatch.setattr(
        market,
        "get_asset_context",
        lambda *args, **kwargs: market.AssetContext("600584", "长电科技", "cn_stock", "600584", {}),
    )
    monkeypatch.setattr(intraday_module, "IntradayCollector", _Collector)

    frame = market.fetch_intraday_history("600584", "cn_stock", {})

    assert frame is expected
    assert calls == [("stock", "600584")]


def test_fetch_intraday_history_keeps_cn_etf_on_etf_collector(monkeypatch):
    calls: list[tuple[str, str]] = []
    expected = pd.DataFrame(
        {
            "时间": pd.to_datetime(["2026-04-16 09:31:00"]),
            "开盘": [0.9],
            "收盘": [0.91],
            "最高": [0.92],
            "最低": [0.9],
            "成交量": [1000],
        }
    )

    class _Collector:
        def __init__(self, config):
            self.config = config

        def get_cn_stock_intraday_chart(self, symbol: str, period: str = "1") -> pd.DataFrame:
            calls.append(("stock", symbol))
            return expected

        def get_intraday_chart(self, symbol: str, period: str = "1") -> pd.DataFrame:
            calls.append(("etf", symbol))
            return expected

    import src.collectors.intraday as intraday_module

    monkeypatch.setattr(
        market,
        "get_asset_context",
        lambda *args, **kwargs: market.AssetContext("159516", "半导体材料设备ETF", "cn_etf", "159516", {}),
    )
    monkeypatch.setattr(intraday_module, "IntradayCollector", _Collector)

    frame = market.fetch_intraday_history("159516", "cn_etf", {})

    assert frame is expected
    assert calls == [("etf", "159516")]


def test_infer_previous_close_uses_latest_daily_close_for_new_intraday_day():
    history = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-03-09", "2026-03-10"]),
            "open": [1.523, 1.569],
            "high": [1.550, 1.582],
            "low": [1.483, 1.548],
            "close": [1.537, 1.571],
            "volume": [4_239_746.0, 3_488_434.0],
            "amount": [640_243_930.0, 546_530_217.0],
        }
    )
    prev_close = market.infer_previous_close(history, pd.Timestamp("2026-03-11 14:59:00"))
    assert prev_close == pytest.approx(1.571)


def test_build_intraday_snapshot_prefers_realtime_prev_close_for_cn_etf(monkeypatch):
    history = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-03-09", "2026-03-10"]),
            "open": [1.523, 1.569],
            "high": [1.550, 1.582],
            "low": [1.483, 1.548],
            "close": [1.537, 1.571],
            "volume": [4_239_746.0, 3_488_434.0],
            "amount": [640_243_930.0, 546_530_217.0],
        }
    )
    intraday = pd.DataFrame(
        {
            "时间": pd.to_datetime(["2026-03-11 09:30:00", "2026-03-11 15:00:00"]),
            "开盘": [1.572, 1.555],
            "收盘": [1.572, 1.555],
            "最高": [1.587, 1.555],
            "最低": [1.553, 1.555],
            "成交量": [100_000, 2_213_436],
            "成交额": [156_700_000.0, 205_938_350.748],
            "均价": [1.568, 1.5675],
        }
    )

    monkeypatch.setattr(market, "fetch_intraday_history", lambda *args, **kwargs: intraday)
    monkeypatch.setattr(
        market,
        "fetch_cn_etf_realtime_row",
        lambda *args, **kwargs: {
            "current": 1.555,
            "open": 1.572,
            "high": 1.587,
            "low": 1.553,
            "prev_close": 1.571,
            "volume": 2_313_436.0,
            "updated_at": pd.Timestamp("2026-03-11 15:00:00"),
        },
    )

    snapshot = market.build_intraday_snapshot("159819", "cn_etf", {}, history)
    assert snapshot["change_vs_prev_close"] == pytest.approx(1.555 / 1.571 - 1)
    assert snapshot["change_vs_open"] == pytest.approx(1.555 / 1.572 - 1)
    assert snapshot["opening_gap"] == pytest.approx(1.572 / 1.571 - 1)
    assert snapshot["first_30m_change"] == pytest.approx(0.0)
    assert snapshot["first_30m_volume_share"] > 0
    assert snapshot["range_position"] == pytest.approx((1.555 - 1.553) / (1.587 - 1.553))


def test_build_snapshot_fallback_history_uses_realtime_row_for_cn_etf(monkeypatch):
    monkeypatch.setattr(
        market,
        "fetch_cn_etf_realtime_row",
        lambda *args, **kwargs: {
            "current": 3.28,
            "open": 3.263,
            "high": 3.288,
            "low": 3.24,
            "prev_close": 3.27,
            "volume": 730_543.0,
            "amount": 238_082_767.0,
            "updated_at": pd.Timestamp("2026-03-11 11:52:50"),
        },
    )

    frame = market.build_snapshot_fallback_history("510880", "cn_etf", {}, periods=60)
    assert len(frame) == 60
    assert frame["date"].is_monotonic_increasing
    assert frame.iloc[-1]["close"] == pytest.approx(3.28)
    assert frame.iloc[-1]["amount"] == pytest.approx(238_082_767.0)
    assert frame.iloc[0]["close"] == pytest.approx(3.27)


def test_build_snapshot_fallback_history_uses_realtime_row_for_cn_stock(monkeypatch):
    monkeypatch.setattr(
        market,
        "fetch_cn_stock_realtime_row",
        lambda *args, **kwargs: {
            "current": 376.3,
            "open": 375.0,
            "high": 379.77,
            "low": 366.5,
            "prev_close": 357.5,
            "volume": 507_417.0,
            "amount": 19_049_080_512.74,
            "updated_at": pd.Timestamp("2026-03-11 15:00:00"),
        },
    )

    frame = market.build_snapshot_fallback_history("300750", "cn_stock", {}, periods=40)
    assert len(frame) == 40
    assert frame["date"].is_monotonic_increasing
    assert frame.iloc[-1]["close"] == pytest.approx(376.3)
    assert frame.iloc[-1]["amount"] == pytest.approx(19_049_080_512.74)
    assert frame.iloc[0]["close"] == pytest.approx(357.5)


def test_build_intraday_snapshot_for_cn_stock_includes_auction_metrics(monkeypatch):
    history = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-03-10", "2026-03-11"]),
            "open": [393.23, 401.0],
            "high": [410.0, 405.0],
            "low": [390.0, 398.0],
            "close": [393.23, 400.5],
            "volume": [1_000_000, 900_000],
            "amount": [3_000_000_000.0, 2_800_000_000.0],
        }
    )
    intraday = pd.DataFrame(
        {
            "时间": pd.to_datetime(["2026-03-12 09:30:00", "2026-03-12 10:00:00"]),
            "开盘": [400.01, 402.0],
            "收盘": [400.01, 403.0],
            "最高": [401.0, 404.0],
            "最低": [399.0, 400.0],
            "成交量": [285300, 500000],
            "成交额": [114122853.0, 200000000.0],
            "均价": [400.0, 401.5],
        }
    )

    monkeypatch.setattr(market, "fetch_intraday_history", lambda *args, **kwargs: intraday)
    monkeypatch.setattr(
        market,
        "fetch_cn_stock_realtime_row",
        lambda *args, **kwargs: {
            "current": 403.0,
            "open": 400.01,
            "high": 404.0,
            "low": 399.0,
            "prev_close": 393.23,
            "volume": 785300.0,
            "updated_at": pd.Timestamp("2026-03-12 10:00:00"),
        },
    )
    monkeypatch.setattr(
        market,
        "fetch_cn_stock_auction_row",
        lambda *args, **kwargs: {
            "auction_price": 400.01,
            "auction_amount": 114122853.0,
            "auction_volume_ratio": 1.35,
            "auction_turnover_rate": 0.0322,
            "auction_gap": 400.01 / 393.23 - 1,
        },
    )
    monkeypatch.setattr(
        market,
        "fetch_cn_stock_limit_row",
        lambda *args, **kwargs: {
            "up_limit": 432.0,
            "down_limit": 353.45,
        },
    )

    snapshot = market.build_intraday_snapshot("300502", "cn_stock", {}, history)
    assert snapshot["snapshot_time"] == pd.Timestamp("2026-03-12 10:00:00")
    assert snapshot["updated_at"] == pd.Timestamp("2026-03-12 10:00:00")
    assert snapshot["auction_price"] == pytest.approx(400.01)
    assert snapshot["auction_volume_ratio"] == pytest.approx(1.35)
    assert snapshot["auction_gap"] == pytest.approx(400.01 / 393.23 - 1)
    assert "抢筹" in snapshot["auction_commentary"]
    assert snapshot["up_limit"] == pytest.approx(432.0)
    assert snapshot["down_limit"] == pytest.approx(353.45)
    assert snapshot["limit_distance_up"] == pytest.approx(432.0 / 403.0 - 1)
    assert "涨跌停边界" in snapshot["limit_commentary"] or "涨跌停" in snapshot["limit_commentary"]


def test_close_yfinance_runtime_caches_closes_known_peewee_managers(monkeypatch):
    closed: list[str] = []

    class _Manager:
        def __init__(self, label: str) -> None:
            self.label = label
            self._db = object()

        def close_db(self) -> None:
            closed.append(self.label)

    class _Dummy:
        pass

    import yfinance.cache as yf_cache

    monkeypatch.setattr(yf_cache, "_TzDBManager", _Manager("tz"))
    monkeypatch.setattr(yf_cache, "_CookieDBManager", _Manager("cookie"))
    monkeypatch.setattr(yf_cache, "_ISINDBManager", _Manager("isin"))
    monkeypatch.setattr(yf_cache, "_TzCacheDummy", _Dummy)
    monkeypatch.setattr(yf_cache, "_CookieCacheDummy", _Dummy)
    monkeypatch.setattr(yf_cache, "_ISINCacheDummy", _Dummy)
    monkeypatch.setattr(yf_cache, "_TzCacheManager", type("_TzCacheManager", (), {"_tz_cache": None}))
    monkeypatch.setattr(yf_cache, "_CookieCacheManager", type("_CookieCacheManager", (), {"_Cookie_cache": None}))
    monkeypatch.setattr(yf_cache, "_ISINCacheManager", type("_ISINCacheManager", (), {"_isin_cache": None}))

    market.close_yfinance_runtime_caches()

    assert closed.count("tz") >= 1
    assert closed.count("cookie") >= 1
    assert closed.count("isin") >= 1
    assert yf_cache._TzDBManager._db is None
    assert yf_cache._CookieDBManager._db is None
    assert yf_cache._ISINDBManager._db is None
    assert isinstance(yf_cache._TzCacheManager._tz_cache, _Dummy)
    assert isinstance(yf_cache._CookieCacheManager._Cookie_cache, _Dummy)
    assert isinstance(yf_cache._ISINCacheManager._isin_cache, _Dummy)


def test_get_asset_context_resolves_name_input_via_lookup(monkeypatch):
    class _FakeLookup:
        def __init__(self, _config):  # noqa: ANN001
            pass

        def resolve_best(self, query: str):  # noqa: ANN001
            if query == "农发种业":
                return {
                    "symbol": "600313",
                    "name": "农发种业",
                    "asset_type": "cn_stock",
                    "sector": "农业",
                    "chain_nodes": ["粮食安全", "种业"],
                }
            return None

    monkeypatch.setattr(market, "AssetLookupCollector", _FakeLookup)
    monkeypatch.setattr(market, "load_asset_aliases", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(market, "load_watchlist", lambda *_args, **_kwargs: [])

    context = market.resolve_asset_context("农发种业", {})

    assert context.symbol == "600313"
    assert context.asset_type == "cn_stock"
    assert context.name == "农发种业"
    assert context.metadata["sector"] == "农业"
    assert context.metadata["chain_nodes"] == ["粮食安全", "种业"]


def test_get_asset_context_fast_paths_direct_cn_symbol_without_lookup(monkeypatch):
    class _FailLookup:
        def __init__(self, _config):  # noqa: ANN001
            raise AssertionError("AssetLookupCollector should not be created for direct CN symbols")

    monkeypatch.setattr(market, "AssetLookupCollector", _FailLookup)
    monkeypatch.setattr(
        market,
        "load_asset_aliases",
        lambda *_args, **_kwargs: [{"symbol": "600313", "name": "农发种业", "asset_type": "cn_stock", "sector": "农业"}],
    )
    monkeypatch.setattr(market, "load_watchlist", lambda *_args, **_kwargs: [])

    context = market.get_asset_context("600313", "cn_stock", {})

    assert context.symbol == "600313"
    assert context.asset_type == "cn_stock"
    assert context.name == "农发种业"
    assert context.metadata["sector"] == "农业"


def test_get_asset_context_fast_paths_direct_cn_symbol_backfills_name_from_cached_stock_basic(monkeypatch):
    class _FailLookup:
        def __init__(self, _config):  # noqa: ANN001
            raise AssertionError("AssetLookupCollector should not be created when stock_basic cache already has the name")

    class _FakeChinaCollector:
        def __init__(self, _config):  # noqa: ANN001
            pass

        def _load_cache(self, key: str, ttl_hours=None, allow_stale=False):  # noqa: ANN001
            assert allow_stale is True
            if key == "cn_market:ts_stock_basic_snapshot:v1":
                return pd.DataFrame(
                    [
                        {"ts_code": "300274.SZ", "symbol": "300274", "name": "阳光电源", "industry": "电网设备"},
                    ]
                )
            if key == "cn_market:ts_stock_company:300274.SZ:v1":
                return None
            raise AssertionError(f"unexpected cache key: {key}")

        def _ts_call(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            raise AssertionError("live stock_basic should not be fetched when cache already exists")

        def _save_cache(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            raise AssertionError("cache should not be rewritten in this path")

    monkeypatch.setattr(market, "AssetLookupCollector", _FailLookup)
    monkeypatch.setattr(market, "ChinaMarketCollector", _FakeChinaCollector)
    monkeypatch.setattr(market, "load_asset_aliases", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(market, "load_watchlist", lambda *_args, **_kwargs: [])

    context = market.get_asset_context("300274", "cn_stock", {})

    assert context.symbol == "300274"
    assert context.asset_type == "cn_stock"
    assert context.name == "阳光电源"
    assert context.metadata["sector"] == "电网设备"


def test_get_asset_context_fast_paths_direct_cn_symbol_backfills_company_profile_from_tushare(monkeypatch):
    class _FakeChinaCollector:
        def __init__(self, _config):  # noqa: ANN001
            pass

        def _load_cache(self, key: str, ttl_hours=None, allow_stale=False):  # noqa: ANN001
            if key == "cn_market:ts_stock_basic_snapshot:v1":
                return pd.DataFrame(
                    [
                        {"ts_code": "300274.SZ", "symbol": "300274", "name": "阳光电源", "industry": "电气设备"},
                    ]
                )
            if key == "cn_market:ts_stock_company:300274.SZ:v1":
                return pd.DataFrame(
                    [
                        {
                            "ts_code": "300274.SZ",
                            "main_business": "主营业务是太阳能光伏逆变器、储能系统和风能变流器。",
                            "business_scope": "新能源发电设备、储能电源及相关电力电子设备。",
                            "introduction": "专注于太阳能、风能、储能等新能源电源设备。",
                            "province": "安徽",
                            "city": "合肥市",
                        }
                    ]
                )
            raise AssertionError(f"unexpected cache key: {key}")

        def _ts_call(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            raise AssertionError("live tushare should not be fetched when cache already exists")

        def _save_cache(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
            raise AssertionError("cache should not be rewritten in this path")

        def _to_ts_code(self, symbol: str) -> str:
            return f"{symbol}.SZ"

    monkeypatch.setattr(market, "ChinaMarketCollector", _FakeChinaCollector)
    monkeypatch.setattr(market, "load_asset_aliases", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(market, "load_watchlist", lambda *_args, **_kwargs: [])

    context = market.get_asset_context("300274", "cn_stock", {})

    assert context.metadata["main_business"].startswith("主营业务是太阳能光伏逆变器")
    assert context.metadata["business_scope"].startswith("新能源发电设备")
    assert context.metadata["company_profile_source"] == "tushare_stock_company_cache"


def test_market_regime_proxy_skips_timeout_ticker(monkeypatch):
    calls: list[str] = []

    def fake_history(symbol: str, **_kwargs):  # noqa: ANN001
        calls.append(symbol)
        if symbol == "^VIX":
            raise TimeoutError("timeout")
        return pd.DataFrame({"Close": [100.0, 102.0, 103.0]})

    monkeypatch.setattr(market, "_ticker_history_with_timeout", fake_history)

    result = market.market_regime_proxy()

    assert "^VIX" in calls
    assert "vix" not in result
    assert result["dxy"] == pytest.approx(103.0)
    assert "copper_gold_ratio" in result


def test_load_global_proxy_snapshot_disabled_by_default() -> None:
    from src.processors.context import load_global_proxy_snapshot

    assert load_global_proxy_snapshot({}) == {}


def test_ticker_history_with_timeout_raises_timeout(monkeypatch):
    def _fake_timeout(_loader, **kwargs):  # noqa: ANN001, ANN003
        raise kwargs["timeout_exc"]

    monkeypatch.setattr(market, "run_with_timeout", _fake_timeout)

    with pytest.raises(TimeoutError, match="market_regime_proxy timeout for \\^VIX"):
        market._ticker_history_with_timeout("^VIX")
