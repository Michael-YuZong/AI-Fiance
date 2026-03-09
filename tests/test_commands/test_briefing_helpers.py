"""Tests for briefing helper logic."""

from __future__ import annotations

from src.commands.briefing import _important_event_lines


def test_important_event_lines_extracts_specific_drivers() -> None:
    report = {
        "items": [
            {"category": "fed", "title": "Reuters: Fed rate-cut odds rise after CPI cools", "source": "Reuters"},
            {"category": "ai", "title": "OpenAI prepares GPT-5 launch event", "source": "Reuters"},
            {"category": "semiconductor", "title": "TSMC expands chip capacity in Arizona", "source": "Bloomberg"},
        ]
    }

    lines = _important_event_lines(report)

    assert any("美联储与利率预期" in line for line in lines)
    assert any("AI 产品与模型" in line for line in lines)
    assert any("半导体产能与资本开支" in line for line in lines)


def test_important_event_lines_does_not_misclassify_generic_macro_as_ai() -> None:
    report = {
        "items": [
            {"category": "china_macro", "title": "China to boost spending to meet growth target", "source": "Reuters"},
        ]
    }

    lines = _important_event_lines(report)

    assert not any("AI 产品与模型" in line for line in lines)
