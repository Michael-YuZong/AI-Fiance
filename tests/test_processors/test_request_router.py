"""Tests for natural-language routing."""

from __future__ import annotations

from src.processors.request_router import route_request


def test_router_maps_briefing_request():
    routed = route_request("帮我写今天的财经晨报", candidate_symbols=["561380", "QQQM"])
    assert routed.module == "briefing"
    assert routed.args == ["daily"]


def test_router_maps_compare_request():
    routed = route_request("帮我对比 561380 和 QQQM", candidate_symbols=["561380", "QQQM"])
    assert routed.module == "compare"
    assert routed.args == ["561380", "QQQM"]


def test_router_falls_back_to_research_for_open_question():
    routed = route_request("为什么最近市场有点别扭", candidate_symbols=["561380", "QQQM"])
    assert routed.module == "research"
