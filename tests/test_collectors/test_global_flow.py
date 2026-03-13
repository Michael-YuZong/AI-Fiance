"""Tests for global flow proxy collection."""

from __future__ import annotations

from src.collectors.global_flow import GlobalFlowCollector


def test_global_flow_detects_defensive_bias():
    collector = GlobalFlowCollector()
    payload = collector.collect(
        [
            {"symbol": "HSTECH", "region": "HK", "sector": "科技", "return_5d": -0.04, "return_20d": -0.08},
            {"symbol": "QQQM", "region": "US", "sector": "科技", "return_5d": -0.02, "return_20d": 0.01},
            {"symbol": "GLD", "region": "US", "sector": "黄金", "return_5d": 0.03, "return_20d": 0.08},
            {"symbol": "561380", "region": "CN", "sector": "电网", "return_5d": 0.02, "return_20d": 0.10},
        ]
    )
    assert payload["risk_bias"] == "risk_off"
    assert payload["method"] == "proxy"
    assert payload["lines"]
    assert payload["confidence_label"] in {"中", "高"}
    assert payload["limitations"]
    assert "原始流向数据" in payload["limitations"][0]


def test_global_flow_empty_rows_has_low_confidence() -> None:
    payload = GlobalFlowCollector().collect([])

    assert payload["confidence_label"] == "低"
    assert payload["coverage_summary"] == "无有效代理样本"
    assert "无法形成稳定的风格代理判断" in payload["limitations"][0]
