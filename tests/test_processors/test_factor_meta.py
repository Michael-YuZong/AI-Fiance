"""Tests for factor metadata registry and payloads."""

from __future__ import annotations

from src.processors.factor_meta import FACTOR_REGISTRY, factor_meta_payload, get_factor_meta


def test_stk_factor_pro_is_registered_as_a_stock_price_volume_factor():
    meta = get_factor_meta("j1_stk_factor_pro")
    assert meta is not None
    assert meta.factor_id == "j1_stk_factor_pro"
    assert meta in FACTOR_REGISTRY.values()
    assert meta.family == "J-1"
    assert meta.source_type == "price_volume"
    assert meta.visibility_class == "daily_close"
    assert meta.proxy_level == "direct"
    assert meta.state == "production_factor"
    assert meta.supports_scoring is True
    assert meta.supports_strategy_candidate is False


def test_stk_factor_pro_payload_keeps_runtime_source_as_of_override():
    payload = factor_meta_payload("j1_stk_factor_pro", overrides={"source_as_of": "2026-04-03"})
    assert payload["source_as_of"] == "2026-04-03"
    assert payload["point_in_time_ready"] is True
    assert payload["supports_scoring"] is True
    assert payload["supports_strategy_candidate"] is False


def test_fund_sales_ratio_and_gold_spot_anchor_are_registered():
    fund_sales = get_factor_meta("j5_fund_sales_ratio")
    gold_anchor = get_factor_meta("j5_gold_spot_anchor")

    assert fund_sales is not None
    assert fund_sales.factor_id == "j5_fund_sales_ratio"
    assert fund_sales.family == "J-5"
    assert fund_sales.source_type == "fund_specific"

    assert gold_anchor is not None
    assert gold_anchor.factor_id == "j5_gold_spot_anchor"
    assert gold_anchor.family == "J-5"
    assert gold_anchor.source_type == "etf_specific"


def test_ah_comparison_and_convertible_bond_factors_are_registered():
    ah = get_factor_meta("j3_ah_comparison")
    cb = get_factor_meta("j4_convertible_bond_proxy")

    assert ah is not None
    assert ah.factor_id == "j3_ah_comparison"
    assert ah.family == "J-3"
    assert ah.source_type == "proxy"
    assert ah.proxy_level == "market_proxy"
    assert ah.visibility_class == "daily_close"

    assert cb is not None
    assert cb.factor_id == "j4_convertible_bond_proxy"
    assert cb.family == "J-4"
    assert cb.source_type == "proxy"
    assert cb.proxy_level == "direct"
    assert cb.visibility_class == "daily_close"
