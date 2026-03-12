"""Tests for shared market helpers."""

from __future__ import annotations

import pandas as pd
import pytest

from src.utils import market


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
    assert snapshot["auction_price"] == pytest.approx(400.01)
    assert snapshot["auction_volume_ratio"] == pytest.approx(1.35)
    assert snapshot["auction_gap"] == pytest.approx(400.01 / 393.23 - 1)
    assert "抢筹" in snapshot["auction_commentary"]
    assert snapshot["up_limit"] == pytest.approx(432.0)
    assert snapshot["down_limit"] == pytest.approx(353.45)
    assert snapshot["limit_distance_up"] == pytest.approx(432.0 / 403.0 - 1)
    assert "涨跌停边界" in snapshot["limit_commentary"] or "涨跌停" in snapshot["limit_commentary"]
