"""Tests for policy engine helpers."""

from __future__ import annotations

from src.processors.policy_engine import PolicyEngine


def test_policy_engine_matches_keyword():
    engine = PolicyEngine()
    match = engine.best_match("电网和特高压投资")
    assert match is not None
    assert match["id"] == "power-grid"


def test_policy_engine_extracts_numbers():
    engine = PolicyEngine()
    numbers = engine.extract_numbers("计划投资 2.5万亿，目标增速 10%，周期 5年。")
    assert "2.5万亿" in numbers
    assert "10%" in numbers
