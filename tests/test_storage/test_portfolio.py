"""Tests for portfolio repository."""

from __future__ import annotations

from pathlib import Path

from src.storage.portfolio import PortfolioRepository


def test_portfolio_log_trade_and_rebalance(tmp_path: Path):
    repo = PortfolioRepository(
        portfolio_path=tmp_path / "portfolio.json",
        trade_log_path=tmp_path / "trade_log.json",
    )
    repo.log_trade(
        action="buy",
        symbol="561380",
        name="电网 ETF",
        asset_type="cn_etf",
        price=2.0,
        amount=1000.0,
        region="CN",
        sector="电网",
    )
    repo.set_target_weight("561380", 0.3)
    status = repo.build_status({"561380": 2.2})
    assert len(status["holdings"]) == 1
    assert status["holdings"][0]["weight"] == 1.0
    suggestions = repo.rebalance_suggestions({"561380": 2.2}, threshold=0.05)
    assert suggestions[0]["action"] == "reduce"


def test_portfolio_log_trade_persists_thesis_snapshot(tmp_path: Path):
    repo = PortfolioRepository(
        portfolio_path=tmp_path / "portfolio.json",
        trade_log_path=tmp_path / "trade_log.json",
    )
    repo.log_trade(
        action="buy",
        symbol="510880",
        name="红利ETF",
        asset_type="cn_etf",
        price=3.2,
        amount=3200.0,
        thesis_snapshot={"core_assumption": "高股息底仓", "holding_period": "1-3月"},
    )
    trade = repo.list_trades()[0]
    assert trade["thesis_snapshot"]["core_assumption"] == "高股息底仓"


def test_portfolio_log_trade_persists_decision_and_execution_snapshots(tmp_path: Path):
    repo = PortfolioRepository(
        portfolio_path=tmp_path / "portfolio.json",
        trade_log_path=tmp_path / "trade_log.json",
    )
    repo.log_trade(
        action="buy",
        symbol="561380",
        name="电网ETF",
        asset_type="cn_etf",
        price=2.1,
        amount=2100.0,
        decision_snapshot={"market_data_as_of": "2026-03-13", "market_data_source": "561380"},
        execution_snapshot={"tradability_label": "可成交", "estimated_total_cost": 6.5},
    )
    trade = repo.list_trades()[0]
    assert trade["decision_snapshot"]["market_data_as_of"] == "2026-03-13"
    assert trade["execution_snapshot"]["tradability_label"] == "可成交"


def test_portfolio_repository_defaults_do_not_leak_between_repos(tmp_path: Path):
    first = PortfolioRepository(
        portfolio_path=tmp_path / "first_portfolio.json",
        trade_log_path=tmp_path / "first_trade_log.json",
    )
    second = PortfolioRepository(
        portfolio_path=tmp_path / "second_portfolio.json",
        trade_log_path=tmp_path / "second_trade_log.json",
    )

    first.log_trade(
        action="buy",
        symbol="561380",
        name="电网 ETF",
        asset_type="cn_etf",
        price=2.0,
        amount=1000.0,
    )

    assert len(first.list_holdings()) == 1
    assert second.list_holdings() == []
