"""Tests for compare command."""

from __future__ import annotations

import sys

from src.commands import compare as compare_module


def test_compare_main_passes_all_symbols(monkeypatch, capsys) -> None:
    seen: dict[str, object] = {}

    def fake_compare(symbols, config):
        seen["symbols"] = list(symbols)
        return {"generated_at": "2026-03-13 08:00:00", "analyses": [], "best_symbol": "561380"}

    monkeypatch.setattr(compare_module, "compare_opportunities", fake_compare)
    monkeypatch.setattr(compare_module, "load_config", lambda _path=None: {})
    monkeypatch.setattr(compare_module, "setup_logger", lambda *_args, **_kwargs: None)

    class _FakeRenderer:
        def render_compare(self, payload):
            seen["payload"] = payload
            return "ok"

    monkeypatch.setattr(compare_module, "OpportunityReportRenderer", _FakeRenderer)
    monkeypatch.setattr(sys, "argv", ["compare", "561380", "GLD", "QQQM"])

    compare_module.main()

    captured = capsys.readouterr()
    assert seen["symbols"] == ["561380", "GLD", "QQQM"]
    assert captured.out.strip() == "ok"
