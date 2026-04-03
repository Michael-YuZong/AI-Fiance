"""Tests for portfolio repository."""

from __future__ import annotations

from pathlib import Path

from src.storage.portfolio import (
    PortfolioRepository,
    build_candidate_set_linkage_summary,
    build_portfolio_overlap_summary,
)


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


def test_build_portfolio_overlap_summary_highlights_repeat_theme():
    status = {
        "base_currency": "CNY",
        "total_value": 100.0,
        "holdings": [
            {"symbol": "561380", "name": "电网ETF", "sector": "电网", "region": "CN", "weight": 0.28},
            {"symbol": "510300", "name": "沪深300ETF", "sector": "宽基", "region": "CN", "weight": 0.42},
            {"symbol": "512480", "name": "创新药ETF", "sector": "医药", "region": "CN", "weight": 0.30},
        ],
        "sector_exposure": {"电网": 0.28, "宽基": 0.42, "医药": 0.30},
        "region_exposure": {"CN": 1.0},
    }

    summary = build_portfolio_overlap_summary(
        status,
        candidate_symbol="563360",
        candidate_name="电网ETF华泰柏瑞",
        candidate_sector="电网",
        candidate_region="CN",
        projected_weight=0.34,
        projected_sector_weight=0.40,
        projected_region_weight=1.0,
        suggested_max_weight=0.32,
        sector_limit=0.35,
        region_limit=0.95,
    )

    assert summary["overlap_label"] in {"同一行业主线加码", "主题/行业重复较高"}
    assert "重复度" in summary["summary_line"] or "加码" in summary["summary_line"]
    assert summary["conflict_label"]
    assert any("同主题/行业持仓" in item for item in summary["detail_lines"])


def test_build_portfolio_overlap_summary_surfaces_style_and_priority_hint():
    status = {
        "base_currency": "CNY",
        "total_value": 100.0,
        "holdings": [
            {"symbol": "510300", "name": "沪深300ETF", "sector": "宽基", "region": "CN", "asset_type": "cn_etf", "weight": 0.55},
            {"symbol": "510880", "name": "红利ETF", "sector": "红利", "region": "CN", "asset_type": "cn_etf", "weight": 0.25},
            {"symbol": "512000", "name": "科技ETF", "sector": "科技", "region": "CN", "asset_type": "cn_etf", "weight": 0.20},
        ],
        "sector_exposure": {"宽基": 0.55, "红利": 0.25, "科技": 0.20},
        "region_exposure": {"CN": 1.0},
    }

    summary = build_portfolio_overlap_summary(
        status,
        candidate_symbol="563360",
        candidate_name="创新药ETF",
        candidate_sector="创新药",
        candidate_region="CN",
        candidate_asset_type="cn_etf",
    )

    assert "风格" in summary["style_summary_line"]
    assert summary["style_direction_label"] in {"防守偏重", "进攻偏重", "均衡"}
    assert summary["candidate_style_bucket"] in {"防守", "进攻", "顺周期", "均衡", "中性"}
    assert summary["style_priority_hint"]
    assert any("风格暴露" in item for item in summary["detail_lines"])


def test_build_candidate_set_linkage_summary_highlights_same_theme_internal_compare():
    summary = build_candidate_set_linkage_summary(
        [
            {"symbol": "561380", "name": "电网ETF", "asset_type": "cn_etf", "metadata": {"sector": "电网"}},
            {"symbol": "159611", "name": "电力ETF", "asset_type": "cn_etf", "metadata": {"sector": "电网"}},
            {"symbol": "512400", "name": "有色ETF", "asset_type": "cn_etf", "metadata": {"sector": "有色"}},
        ]
    )

    assert summary["overlap_label"] in {"同一行业主线对比", "部分同主线重合"}
    assert "主线" in summary["summary_line"]
    assert "风格" in summary["style_summary_line"]
    assert any("主线分布" in item for item in summary["detail_lines"])
