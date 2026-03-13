"""Tests for policy report rendering."""

from __future__ import annotations

from src.output.policy_report import PolicyReportRenderer


def test_policy_report_renderer_includes_match_and_timeline_sections():
    payload = {
        "title": "关于加快新型电力系统建设的行动计划",
        "source": "keyword",
        "theme": "电网投资与新型电力系统",
        "summary": "该主题的核心在于加快新型电力系统建设。",
        "match_confidence": "高",
        "matched_aliases": ["电网", "特高压", "新型电力系统"],
        "policy_direction": "偏支持",
        "policy_stage": "顶层规划/行动方案",
        "policy_goal": "提升电网承载和调峰能力。",
        "timeline": "需要结合后续细则和项目进度跟踪。",
        "timeline_points": ["要求在2026年6月30日前完成首批申报", "年内形成重点项目清单"],
        "support_points": ["特高压", "配电网改造"],
        "benefit_risk_lines": ["受益方向：电力需求, 电网设备", "风险点：落地节奏低于预期"],
        "headline_numbers": ["2.5万亿", "10%"],
        "watchlist_impact": ["561380 (电网 ETF) 命中受益方向 `模板显式映射`，适合进入重点跟踪。"],
        "raw_excerpt": "这是摘取内容。",
    }

    rendered = PolicyReportRenderer().render(payload)

    assert "模板置信度" in rendered
    assert "## 模板命中" in rendered
    assert "## 时间线 / 执行抓手" in rendered
    assert "## 对 watchlist / 持仓的影响" in rendered
    assert "这是摘取内容。" in rendered
