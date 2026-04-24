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


def test_router_uses_resolved_symbol_for_scan():
    routed = route_request("分析一下有色金属ETF", resolved_symbols=["512400"])
    assert routed.module == "scan"
    assert routed.args == ["512400"]


def test_router_maps_code_lookup_request():
    routed = route_request("有色金属ETF代码是多少", resolved_symbols=["512400"])
    assert routed.module == "lookup"
    assert routed.args == ["有色金属ETF代码是多少"]


def test_router_maps_intel_request_to_free_intel_command():
    routed = route_request("收集有色金属相关的情报")
    assert routed.module == "intel"
    assert routed.args == ["收集有色金属相关的情报"]


def test_router_maps_compare_request_with_resolved_symbols():
    routed = route_request("对比有色金属ETF和黄金ETF", resolved_symbols=["512400", "GLD"])
    assert routed.module == "compare"
    assert routed.args == ["512400", "GLD"]


def test_router_maps_buy_question_to_scan():
    routed = route_request("561380 能不能买", candidate_symbols=["561380", "QQQM"])
    assert routed.module == "scan"
    assert routed.args == ["561380"]


def test_router_maps_today_buy_question_to_scan_today():
    routed = route_request("159819 今天适合买吗")
    assert routed.module == "scan"
    assert routed.args == ["159819", "--today"]


def test_router_detects_numeric_code_inside_chinese_text():
    routed = route_request("分析一下基金022365")
    assert routed.module == "scan"
    assert routed.args == ["022365"]


def test_router_maps_vs_phrase_to_compare():
    routed = route_request("561380 vs QQQM", candidate_symbols=["561380", "QQQM"])
    assert routed.module == "compare"
    assert routed.args == ["561380", "QQQM"]


def test_router_falls_back_to_research_for_open_question():
    routed = route_request("为什么最近市场有点别扭", candidate_symbols=["561380", "QQQM"])
    assert routed.module == "research"
