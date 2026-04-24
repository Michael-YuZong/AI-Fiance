from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.processors.decision_review import build_monthly_decision_review
from src.storage.portfolio import PortfolioRepository
from src.storage.thesis import ThesisRepository


def _sample_history() -> pd.DataFrame:
    dates = pd.bdate_range("2026-03-02", periods=30)
    close = [
        10.0,
        10.1,
        10.4,
        10.8,
        11.1,
        11.4,
        11.7,
        11.5,
        11.3,
        11.0,
    ] + [11.0] * 20
    frame = pd.DataFrame(
        {
            "date": dates,
            "open": close,
            "high": [value * 1.02 for value in close],
            "low": [value * 0.99 for value in close],
            "close": close,
            "volume": [1_000_000] * len(close),
            "amount": [value * 1_000_000 for value in close],
        }
    )
    return frame


def test_build_monthly_decision_review_computes_path_and_verdict(monkeypatch, tmp_path: Path):
    repo = PortfolioRepository(
        portfolio_path=tmp_path / "portfolio.json",
        trade_log_path=tmp_path / "trade_log.json",
    )
    thesis_repo = ThesisRepository(thesis_path=tmp_path / "thesis.json")
    thesis_repo.upsert(
        "300750",
        core_assumption="产业趋势还在",
        validation_metric="订单和盈利增速",
        stop_condition="跌破关键支撑",
        holding_period="1-3月",
    )
    repo.log_trade(
        action="buy",
        symbol="300750",
        name="宁德时代",
        asset_type="cn_stock",
        price=10.0,
        amount=10000.0,
        basis="rule",
        note="右侧确认后试单",
        signal_snapshot={
            "ma_signal": "bullish",
            "macd_signal": "bullish",
            "return_20d": 0.08,
            "volume_structure": "放量上攻",
            "rsi": 58.0,
        },
        thesis_snapshot={"core_assumption": "产业趋势还在", "holding_period": "1-3月"},
        decision_snapshot={"recorded_at": "2026-03-02T10:00:00", "market_data_as_of": "2026-03-02", "market_data_source": "300750"},
        execution_snapshot={"execution_mode": "场内成交", "tradability_label": "可成交", "estimated_total_cost": 18.0},
    )
    trades = repo.list_trades()
    trades[0]["timestamp"] = "2026-03-02T10:00:00"
    repo.trade_log_path.write_text(json.dumps(trades, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(
        "src.processors.decision_review.fetch_asset_history",
        lambda symbol, asset_type, config, period="3y": _sample_history(),
    )

    payload = build_monthly_decision_review(
        "2026-03",
        config={},
        repo=repo,
        thesis_repo=thesis_repo,
        lookahead=20,
        stop_pct=0.08,
        target_pct=0.10,
    )

    assert payload["basis_rows"][0][0] == "rule"
    assert payload["basis_rows"][0][3].startswith("+")
    assert payload["setup_rows"][0][0] in {"高把握", "中等把握", "低把握"}
    assert payload["attribution_rows"][0][0] in {"alpha兑现", "更多来自贝塔顺风", "方向没错但执行/标的拖累", "方向与执行都偏弱"}
    item = payload["items"][0]
    assert item["signal_alignment"] == "顺势买入"
    assert item["target_hit_day"] == 3
    assert item["verdict"]["outcome"] in {"结果兑现", "结果偏正"}
    assert item["forward_returns"]["5d"].startswith("+")
    assert item["benchmark_symbol"] == "510300"
    assert item["attribution"]["label"] in {"alpha兑现", "更多来自贝塔顺风", "方向没错但执行/标的拖累", "方向与执行都偏弱"}
    assert item["setup_profile"]["bucket"] in {"高把握", "中等把握", "低把握"}
    assert item["horizon"]["family_code"] == "position_trade"
    assert item["horizon"]["code"] == "position_trade_thesis_stated"
    assert payload["horizon_rows"][0][0] == "中线配置（1-3月）"
