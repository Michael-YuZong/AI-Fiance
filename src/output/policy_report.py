"""Policy report renderer."""

from __future__ import annotations

from typing import Any, Dict


class PolicyReportRenderer:
    """Render structured policy analysis into markdown."""

    def render(self, payload: Dict[str, Any]) -> str:
        lines = [
            f"# 政策解读: {payload['title']}",
            "",
            f"- 来源: `{payload['source']}`",
            f"- 输入类型: `{payload.get('input_type', '关键词')}`",
            f"- 来源判断: `{payload.get('source_authority', '待确认')}`",
            f"- 抽取状态: `{payload.get('extraction_status', '待确认')}`",
            f"- 抽取覆盖: `{', '.join(payload.get('coverage_scope', []) or ['待确认'])}`",
            f"- 主题: `{payload['theme']}`",
            f"- 模板置信度: `{payload.get('match_confidence', '低')}`",
            f"- 政策方向: `{payload.get('policy_direction', '中性/待原文确认')}`",
            f"- 所处阶段: `{payload.get('policy_stage', '阶段待原文确认')}`",
            "",
            "## 核心结论",
            f"- {payload['summary']}",
            "",
        ]
        matched_aliases = payload.get("matched_aliases", [])
        if matched_aliases:
            lines.extend(
                [
                    "## 模板命中",
                    f"- 命中别名: {', '.join(matched_aliases)}",
                    "",
                ]
            )

        coverage_scope = payload.get("coverage_scope", [])
        attachment_titles = payload.get("attachment_titles", [])
        if coverage_scope or attachment_titles:
            lines.extend(["## 原文覆盖与附件"])
            if coverage_scope:
                lines.append(f"- 当前已覆盖: {', '.join(coverage_scope)}")
            if attachment_titles:
                lines.append(f"- 检测到附件: {'；'.join(attachment_titles)}")
            lines.append("")

        lines.extend(["## 已抽取的正文事实"])
        body_facts = payload.get("body_facts", [])
        if body_facts:
            for item in body_facts:
                lines.append(f"- {item}")
        else:
            lines.append("- 当前没有抽到足够正文事实。")

        timeline_points = payload.get("timeline_points", [])
        if timeline_points:
            for item in timeline_points:
                lines.append(f"- 原文时间线: {item}")

        lines.extend(["", "## 基于模板 / 规则的推断"])
        if payload.get("policy_goal"):
            lines.append(f"- 模板政策目标: {payload['policy_goal']}")
        if payload.get("timeline"):
            lines.append(f"- 节奏判断: {payload['timeline']}")
        support_points = payload.get("support_points", [])
        if support_points:
            lines.append(f"- 重点支持方向: {', '.join(support_points)}")
        for item in payload.get("inference_lines", []):
            lines.append(f"- {item}")

        numbers = payload.get("headline_numbers", [])
        if numbers:
            lines.extend(["", "## 关注数字 / 观察点"])
            for item in numbers:
                lines.append(f"- {item}")

        watchlist_impact = payload.get("watchlist_impact", [])
        if watchlist_impact:
            lines.extend(["", "## 对 watchlist / 持仓的影响"])
            for item in watchlist_impact:
                lines.append(f"- {item}")

        unconfirmed_lines = payload.get("unconfirmed_lines", [])
        if unconfirmed_lines:
            lines.extend(["", "## 待确认 / 降级说明"])
            for item in unconfirmed_lines:
                lines.append(f"- {item}")

        if payload.get("raw_excerpt"):
            lines.extend(["", "## 文本摘取", payload["raw_excerpt"]])
        return "\n".join(lines)
