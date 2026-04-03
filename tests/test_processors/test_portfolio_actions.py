from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from src.processors.portfolio_actions import build_candidate_portfolio_overlap_summary, build_trade_plan
from src.storage.portfolio import PortfolioRepository
from src.storage.thesis import ThesisRepository
from src.utils.market import AssetContext


def _history(level: float, drift: float = 0.2, amount: float = 300_000_000.0) -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=260)
    close = np.linspace(level, level * (1.0 + drift), len(dates))
    return pd.DataFrame(
        {
            "date": dates,
            "open": close * 0.995,
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
            "volume": [1_000_000.0] * len(dates),
            "amount": [amount] * len(dates),
        }
    )


def test_build_trade_plan_estimates_budget_execution_and_provenance(monkeypatch, tmp_path: Path) -> None:
    repo = PortfolioRepository(
        portfolio_path=tmp_path / "portfolio.json",
        trade_log_path=tmp_path / "trade_log.json",
    )
    thesis_repo = ThesisRepository(thesis_path=tmp_path / "thesis.json")
    repo.log_trade(
        action="buy",
        symbol="510300",
        name="沪深300ETF",
        asset_type="cn_etf",
        price=4.0,
        amount=40_000.0,
        region="CN",
        sector="宽基",
    )
    thesis_repo.upsert(
        "561380",
        core_assumption="电网投资强度上行",
        validation_metric="招标和设备订单",
        stop_condition="主线失速",
        holding_period="1-3月",
    )

    histories = {
        "510300": _history(4.0, drift=0.08, amount=800_000_000.0),
        "561380": _history(2.0, drift=0.15, amount=250_000_000.0),
    }

    def fake_history(symbol, asset_type, config, period="3y"):  # noqa: ARG001
        return histories.get(symbol, histories["510300"])

    monkeypatch.setattr("src.processors.portfolio_actions.fetch_asset_history", fake_history)
    monkeypatch.setattr("src.processors.risk_support.fetch_asset_history", fake_history)
    monkeypatch.setattr(
        "src.processors.portfolio_actions.get_asset_context",
        lambda symbol, asset_type, config: AssetContext(
            symbol=symbol,
            name="电网ETF" if symbol == "561380" else "沪深300ETF",
            asset_type=asset_type,
            source_symbol=symbol,
            metadata={"region": "CN", "sector": "电网" if symbol == "561380" else "宽基"},
        ),
    )

    payload = build_trade_plan(
        action="buy",
        symbol="561380",
        price=2.1,
        amount=20_000.0,
        config={
            "risk_limits": {
                "single_position_max": 0.30,
                "single_sector_max": 0.40,
                "single_region_max": 1.00,
                "position_risk_budget": 0.04,
                "max_trade_participation": 0.05,
            }
        },
        asset_type="cn_etf",
        repo=repo,
        thesis_repo=thesis_repo,
        analysis={
            "symbol": "561380",
            "name": "电网ETF",
            "proxy_signals": {
                "market_flow": {
                    "lines": ["黄金相对成长更抗跌，市场风格偏防守。"],
                    "confidence_label": "中",
                    "coverage_summary": "科技/黄金/国内/海外代理样本",
                    "limitations": ["这是相对强弱代理，不是原始资金流。"],
                    "downgrade_impact": "更适合辅助判断风格切换，不适合单独下交易结论。",
                },
                "social_sentiment": {
                    "aggregate": {
                        "confidence_label": "高",
                        "limitations": ["这是价格和量能推导出的情绪代理，不是真实社媒抓取。"],
                        "downgrade_impact": "更适合提示拥挤线索，不适合单独作为买卖信号。",
                    }
                },
            },
            "dimensions": {
                "technical": {
                    "factors": [
                        {
                            "name": "量价结构",
                            "factor_id": "j1_volume_structure",
                            "factor_meta": {
                                "factor_id": "j1_volume_structure",
                                "family": "J-1",
                                "state": "strategy_challenger",
                                "visibility_class": "daily_close",
                                "proxy_level": "direct",
                                "supports_strategy_candidate": True,
                                "point_in_time_ready": True,
                            },
                        }
                    ]
                }
            },
        },
        period="12m",
    )

    assert payload["projected_weight"] > 0
    assert payload["suggested_max_weight"] > 0
    assert payload["execution"]["estimated_total_cost"] > 0
    assert payload["execution"]["tradability_label"] in {"顺畅", "可成交", "谨慎", "冲击偏高", "数据不足"}
    assert payload["decision_snapshot"]["market_data_as_of"] == "2025-12-31"
    assert payload["decision_snapshot"]["factor_contract"]["families"]["J-1"] == 1
    assert payload["decision_snapshot"]["proxy_contract"]["market_flow"]["confidence_label"] == "中"
    assert payload["decision_snapshot"]["proxy_contract"]["social_sentiment"]["covered"] == 1
    assert payload["portfolio_overlap"]["overlap_label"] in {"同一行业主线加码", "主题/行业重复较高", "重复度较低"}
    assert payload["portfolio_overlap"]["style_direction_label"] in {"防守偏重", "进攻偏重", "均衡"}
    assert payload["portfolio_overlap"]["style_priority_hint"]
    assert payload["portfolio_overlap"]["detail_lines"]
    assert payload["horizon"]["code"] == "position_trade"
    assert payload["decision_snapshot"]["horizon"]["code"] == "position_trade"
    assert payload["current_risk"]["annual_vol"] >= 0
    assert payload["projected_risk"]["annual_vol"] >= 0
    assert "仓位" in payload["headline"]


def test_build_candidate_portfolio_overlap_summary_highlights_same_sector_overlap(monkeypatch, tmp_path: Path) -> None:
    repo = PortfolioRepository(
        portfolio_path=tmp_path / "portfolio.json",
        trade_log_path=tmp_path / "trade_log.json",
    )
    repo.log_trade(
        action="buy",
        symbol="510300",
        name="沪深300ETF",
        asset_type="cn_etf",
        price=4.0,
        amount=40_000.0,
        region="CN",
        sector="宽基",
    )
    repo.log_trade(
        action="buy",
        symbol="561380",
        name="电网ETF",
        asset_type="cn_etf",
        price=2.0,
        amount=25_000.0,
        region="CN",
        sector="电网",
    )

    histories = {
        "510300": _history(4.0, drift=0.08, amount=800_000_000.0),
        "561380": _history(2.0, drift=0.15, amount=250_000_000.0),
        "600406": _history(6.5, drift=0.10, amount=400_000_000.0),
    }

    def fake_history(symbol, asset_type, config, period="3y"):  # noqa: ARG001
        return histories[symbol]

    monkeypatch.setattr("src.processors.portfolio_actions.fetch_asset_history", fake_history)
    monkeypatch.setattr(
        "src.processors.portfolio_actions.get_asset_context",
        lambda symbol, asset_type, config: AssetContext(
            symbol=symbol,
            name="国电南瑞" if symbol == "600406" else symbol,
            asset_type=asset_type,
            source_symbol=symbol,
            metadata={"region": "CN", "sector": "电网" if symbol in {"561380", "600406"} else "宽基"},
        ),
    )

    summary = build_candidate_portfolio_overlap_summary(
        {
            "symbol": "600406",
            "name": "国电南瑞",
            "asset_type": "cn_stock",
            "metadata": {"sector": "电网", "region": "CN"},
        },
        {},
        repo=repo,
    )

    assert summary["overlap_label"] in {"同一行业主线加码", "主题/行业重复较高"}
    assert "电网" in summary["summary_line"]
    assert summary["style_summary_line"]
