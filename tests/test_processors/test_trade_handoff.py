from src.processors.trade_handoff import portfolio_whatif_handoff, recommendation_timing_context


def test_cn_equity_short_term_after_close_rolls_to_next_trade_day() -> None:
    payload = portfolio_whatif_handoff(
        symbol="300502",
        horizon={"code": "short_term", "label": "短线交易（3-10日）"},
        direction="小仓试仓",
        asset_type="cn_stock",
        reference_price=128.88,
        generated_at="2026-03-16 15:10:00",
    )

    assert payload["decision_scope"] == "下一个交易日的计划"
    assert "下一个交易日" in payload["timing_summary"]
    assert "当天成交" in payload["timing_summary"]


def test_cn_fund_before_cutoff_uses_same_day_subscription_scope() -> None:
    payload = portfolio_whatif_handoff(
        symbol="021740",
        horizon={"code": "position_trade", "label": "中线配置（1-3月）"},
        direction="继续持有",
        asset_type="cn_fund",
        reference_price=1.238,
        generated_at="2026-03-16 10:35:00",
    )

    assert payload["decision_scope"] == "今天的申赎决策"
    assert "申赎决策" in payload["timing_summary"]
    assert "收盘后确认净值" in payload["timing_summary"]


def test_cn_etf_morning_scope_is_today_remaining_session() -> None:
    timing = recommendation_timing_context(
        asset_type="cn_etf",
        horizon={"code": "short_term", "label": "短线交易（3-10日）"},
        generated_at="2026-03-16 10:05:00",
    )

    assert timing["headline_scope"] == "今天"
    assert timing["decision_scope"] == "今天剩余交易时段的计划"
    assert "今天剩余交易时段" in timing["summary"]
