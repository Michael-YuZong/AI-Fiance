"""Tests for research command helpers."""

from __future__ import annotations

from src.commands.research import ResearchIntent, _classify_question, _render_research_markdown, _snapshot_bias


def test_classify_question_prefers_portfolio_risk_when_holdings_exist() -> None:
    intent = _classify_question("如果美股跌20%我的组合会怎样", ["QQQM"], has_holdings=True)

    assert intent.kind == "portfolio_risk"
    assert intent.needs_risk is True
    assert intent.needs_regime is True


def test_classify_question_marks_market_diagnosis_without_symbol() -> None:
    intent = _classify_question("为什么最近市场有点别扭", [], has_holdings=False)

    assert intent.kind == "market_diagnosis"
    assert intent.needs_flow is True
    assert intent.needs_regime is True


def test_snapshot_bias_identifies_strong_trend() -> None:
    payload = _snapshot_bias(
        {"return_20d": 0.12},
        {
            "ma_system": {"signal": "bullish"},
            "macd": {"signal": "bullish"},
            "rsi": {"RSI": 66.0},
            "volume": {"vol_ratio": 1.2},
        },
    )

    assert payload["bias"] == "偏强"
    assert "顺势跟踪" in payload["answer"]


def test_render_research_markdown_has_structured_sections() -> None:
    markdown = _render_research_markdown(
        question="561380 现在还能不能买",
        intent=ResearchIntent("asset_thesis", "标的研究 / 交易问题", True, False, True),
        symbols=["561380"],
        direct_answer_lines=["561380 当前更像回踩确认，而不是无条件追高。"],
        evidence_lines=["[宏观] 当前 macro regime 为 recovery。", "[行情/技术] 561380 近20日 +8.0%。"],
        risk_lines=["RSI 已偏热。"],
        action_lines=["先跑 561380 对应的 scan。"],
    )

    assert "## 一句话回答" in markdown
    assert "## 证据" in markdown
    assert "## 风险与不确定性" in markdown
    assert "## 下一步" in markdown
    assert "类型: 标的研究 / 交易问题" in markdown
