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
