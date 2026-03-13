"""Tests for policy report rendering."""

from __future__ import annotations

from src.output.policy_report import PolicyReportRenderer


def test_policy_report_renderer_includes_match_and_timeline_sections():
    payload = {
        "title": "关于加快新型电力系统建设的行动计划",
        "source": "keyword",
        "input_type": "官方页面 / URL",
        "source_authority": "官方政府站点，且页面含发文机关元信息",
        "extraction_status": "正文抽取部分成功",
        "coverage_scope": ["标题", "元信息（3项）", "页面正文", "附件标题（1个）"],
        "theme": "电网投资与新型电力系统",
        "summary": "原文明确推进新型电力系统建设，当前判断偏支持。",
        "match_confidence": "高",
        "matched_aliases": ["电网", "特高压", "新型电力系统"],
        "policy_direction": "偏支持",
        "policy_stage": "顶层规划/行动方案",
        "policy_goal": "提升电网承载和调峰能力。",
        "timeline": "标题给出 2024—2027 年规划区间，正文还有申报节点。",
        "timeline_points": ["要求在2026年6月30日前完成首批申报", "年内形成重点项目清单"],
        "support_points": ["特高压", "配电网改造"],
        "policy_taxonomy": {
            "policy_family": "能源基础设施 / 新型电力系统",
            "driver_type": "CapEx 扩张 / 电网投资",
            "implementation_path": "规划立项 -> 项目审批 -> 招标开工 -> 投资兑现",
            "market_style": "顺周期设备 + 电力基础设施链",
            "base_horizon": "中线到长线",
            "source_level": "中央/官方原发",
            "evidence_mode": "已覆盖正文",
            "policy_tone": "偏支持",
            "policy_stage": "顶层规划/行动方案",
        },
        "body_facts": ["原文标题：关于加快新型电力系统建设的行动计划", "原文明确动作：推进特高压和配电网改造"],
        "inference_lines": ["受益链条映射：电力需求 -> 电网设备 -> 铜铝。", "风险映射：落地节奏低于预期。"],
        "headline_numbers": ["2.5万亿", "10%"],
        "attachment_titles": ["《加快构建新型电力系统行动方案（2024—2027年）》.pdf"],
        "watchlist_impact": ["561380 (电网 ETF) 命中受益方向 `模板显式映射`，适合进入重点跟踪。"],
        "unconfirmed_lines": ["检测到 PDF/OFD 附件，当前只抽取了公告页正文，未展开附件原文。"],
        "raw_excerpt": "这是摘取内容。",
    }

    rendered = PolicyReportRenderer().render(payload)

    assert "输入类型" in rendered
    assert "来源判断" in rendered
    assert "抽取状态" in rendered
    assert "抽取覆盖" in rendered
    assert "模板置信度" in rendered
    assert "## 模板命中" in rendered
    assert "## 原文覆盖与附件" in rendered
    assert "## 已抽取的正文事实" in rendered
    assert "## 基于模板 / 规则的推断" in rendered
    assert "## 政策分类法" in rendered
    assert "## 待确认 / 降级说明" in rendered
    assert "## 对 watchlist / 持仓的影响" in rendered
    assert "政策族群: 能源基础设施 / 新型电力系统" in rendered
    assert "检测到附件: 《加快构建新型电力系统行动方案（2024—2027年）》.pdf" in rendered
    assert "原文明确动作：推进特高压和配电网改造" in rendered
    assert "检测到 PDF/OFD 附件" in rendered
    assert "这是摘取内容。" in rendered
