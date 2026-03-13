from __future__ import annotations

import sys

from src.commands import portfolio as portfolio_module


def test_portfolio_whatif_main_renders_trade_preview(monkeypatch, capsys) -> None:
    monkeypatch.setattr(portfolio_module, "load_config", lambda _path=None: {})
    monkeypatch.setattr(portfolio_module, "PortfolioRepository", lambda: object())
    monkeypatch.setattr(portfolio_module, "ThesisRepository", lambda: object())
    monkeypatch.setattr(
        portfolio_module,
        "build_trade_plan",
        lambda **kwargs: {
            "action": "buy",
            "symbol": "561380",
            "name": "电网ETF",
            "amount": 20_000.0,
            "price": 2.1,
            "headline": "561380 买入后仓位约 `12.0%`，仍在组合和执行约束内。",
            "current_weight": 0.0,
            "projected_weight": 0.12,
            "suggested_max_weight": 0.18,
            "current_sector": "电网",
            "projected_sector_weight": 0.18,
            "current_region": "CN",
            "projected_region_weight": 0.52,
            "current_risk": {"annual_vol": 0.12, "beta": 0.92},
            "projected_risk": {"annual_vol": 0.14, "beta": 0.98},
            "execution": {
                "execution_mode": "场内成交",
                "tradability_label": "可成交",
                "avg_turnover_20d": 220_000_000.0,
                "participation_rate": 0.02,
                "slippage_bps": 9.0,
                "estimated_slippage_cost": 18.0,
                "fee_rate": 0.0003,
                "estimated_fee_cost": 6.0,
                "estimated_total_cost": 24.0,
                "liquidity_note": "参与率可控。",
                "execution_note": "未含极端行情冲击。",
            },
            "decision_snapshot": {
                "recorded_at": "2026-03-13T10:00:00",
                "market_data_as_of": "2026-03-13",
                "market_data_source": "561380",
                "history_window": "3y",
                "thesis_snapshot_at": "2026-03-12 20:00:00",
                "notes": ["只使用当时可见日线。"],
            },
            "alerts": [],
        },
    )
    monkeypatch.setattr(sys, "argv", ["portfolio", "whatif", "buy", "561380", "2.1", "20000"])

    portfolio_module.main()

    captured = capsys.readouterr()
    assert "# 交易预演" in captured.out
    assert "## 执行成本与可成交性" in captured.out
    assert "## 时点与证据快照" in captured.out
