"""Tests for policy engine helpers."""

from __future__ import annotations

from src.processors.policy_engine import PolicyEngine
from src.utils.data import load_watchlist


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


def test_policy_engine_match_policy_reports_aliases_and_confidence():
    engine = PolicyEngine()
    matched = engine.match_policy("关于加快新型电力系统建设并推进特高压投资的通知")

    assert matched is not None
    assert matched.template["id"] == "power-grid"
    assert "特高压" in matched.matched_aliases
    assert matched.confidence_label in {"高", "中"}


def test_policy_engine_classifies_direction_and_stage():
    engine = PolicyEngine()
    text = "关于推动人工智能产业发展的行动计划，提出加快算力基础设施建设。"

    assert engine.classify_policy_direction(text) == "偏支持"
    assert engine.infer_policy_stage("行动计划", text) == "顶层规划/行动方案"


def test_policy_engine_extracts_timeline_points():
    engine = PolicyEngine()
    text = "要求在2026年6月30日前完成首批申报，年内形成重点项目清单。"
    points = engine.extract_timeline_points(text)

    assert any("2026年6月30日" in item for item in points)
    assert any("年内" in item for item in points)


def test_policy_engine_watchlist_impact_matches_asset_name_alias():
    engine = PolicyEngine()
    policy = {
        "beneficiary_nodes": ["半导体自主可控"],
        "risk_nodes": [],
        "mapped_assets": ["芯片ETF"],
    }
    impacts = engine.watchlist_impact(policy, [])

    watchlist_names = [item["name"] for item in load_watchlist()]
    assert any("芯片ETF" in name for name in watchlist_names)
    assert any("芯片" in item for item in impacts)
