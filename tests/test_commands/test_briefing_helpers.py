"""Tests for briefing helper logic."""

from __future__ import annotations

import pandas as pd
import src.commands.briefing as briefing_module

from src.commands.briefing import (
    BriefingSnapshot,
    _anomaly_report,
    _important_event_lines,
    _narrative_validation_lines,
    _primary_narrative,
    _source_quality_lines,
    _yesterday_review_lines,
)


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


def test_primary_narrative_prefers_energy_shock_over_background_regime() -> None:
    report = {
        "items": [
            {"category": "geopolitics", "title": "Oil surges as war disrupts supply fears", "source": "Reuters"},
            {"category": "energy", "title": "Crude jumps again on Strait shipping risk", "source": "Reuters"},
        ]
    }
    monitor_rows = [
        {"name": "布伦特原油", "return_1d": 0.12, "return_5d": 0.28, "latest": 118.0},
        {"name": "美元指数", "return_5d": 0.01, "latest": 100.0},
        {"name": "VIX波动率", "latest": 30.0},
    ]
    pulse = {
        "zt_pool": pd.DataFrame({"所属行业": ["电力", "电网设备", "石油"]}),
        "strong_pool": pd.DataFrame({"所属行业": ["电力", "电网设备"]}),
    }
    snapshots = [
        BriefingSnapshot("QQQM", "QQQM", "us", "US", "科技", 1, -0.02, -0.01, 0.01, 1.0, "空头", -1, "", ""),
        BriefingSnapshot("HSTECH", "HSTECH", "hk", "HK", "科技", 1, -0.03, -0.02, -0.05, 1.0, "空头", -2, "", ""),
        BriefingSnapshot("GLD", "GLD", "us", "US", "黄金", 1, 0.01, 0.02, 0.05, 1.0, "震荡", 0, "", ""),
        BriefingSnapshot("561380", "电网ETF", "cn_etf", "CN", "电网", 1, 0.02, 0.03, 0.10, 1.0, "多头", 2, "", ""),
    ]

    narrative = _primary_narrative(
        news_report=report,
        monitor_rows=monitor_rows,
        pulse=pulse,
        snapshots=snapshots,
        drivers={},
        regime_result={"current_regime": "recovery", "preferred_assets": ["成长股", "顺周期"]},
    )

    assert narrative["theme"] == "energy_shock"
    assert "能源冲击" in narrative["summary"]


def test_narrative_validation_reports_energy_checks() -> None:
    narrative = {
        "theme": "energy_shock",
        "label": "能源冲击",
        "background_regime": "滞涨",
    }
    report = {"items": [{"category": "energy", "title": "Crude jumps", "source": "Reuters"}]}
    monitor_rows = [
        {"name": "布伦特原油", "return_1d": 0.09, "return_5d": 0.20, "latest": 115.0},
        {"name": "美元指数", "return_5d": 0.01, "latest": 100.0},
        {"name": "VIX波动率", "latest": 29.0},
    ]
    pulse = {
        "zt_pool": pd.DataFrame({"所属行业": ["电力", "电网设备"]}),
        "strong_pool": pd.DataFrame({"所属行业": ["电力"]}),
    }
    snapshots = [
        BriefingSnapshot("QQQM", "QQQM", "us", "US", "科技", 1, -0.02, -0.01, 0.01, 1.0, "空头", -1, "", ""),
        BriefingSnapshot("GLD", "GLD", "us", "US", "黄金", 1, 0.01, 0.02, 0.05, 1.0, "震荡", 0, "", ""),
    ]

    lines = _narrative_validation_lines(narrative, report, monitor_rows, pulse, snapshots)

    assert any("价格校验通过" in line for line in lines)
    assert any("盘面校验通过" in line for line in lines)
    assert any("结论: 当前主线校验通过" in line for line in lines)


def test_anomaly_report_flags_extreme_oil_and_etf_moves() -> None:
    snapshots = [
        BriefingSnapshot("561380", "电网ETF", "cn_etf", "CN", "电网", 2.234, -0.003, 0.023, 0.187, 0.74, "多头", 2, "", ""),
    ]
    monitor_rows = [
        {"name": "布伦特原油", "return_1d": 0.16, "return_5d": 0.39, "latest": 108.0},
        {"name": "WTI原油", "return_1d": 0.15, "return_5d": 0.48, "latest": 105.0},
    ]

    report = _anomaly_report(snapshots, monitor_rows)

    assert any("布伦特原油" in line for line in report["lines"])
    assert any("561380" in line for line in report["lines"])
    assert "561380" in report["flags"]


def test_source_quality_warns_on_single_source() -> None:
    report = {
        "items": [
            {"category": "energy", "title": "Oil surges", "source": "Reuters"},
            {"category": "fed", "title": "Fed waits", "source": "Reuters"},
        ]
    }

    lines = _source_quality_lines(report)

    assert any("当前新闻源不足 2 类" in line for line in lines)


def test_yesterday_review_falls_back_without_archive(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(briefing_module, "resolve_project_path", lambda _: tmp_path)
    lines = _yesterday_review_lines([], [])

    assert any("暂无" in line for line in lines)
