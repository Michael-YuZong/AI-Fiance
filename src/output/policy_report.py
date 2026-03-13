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
        lines.extend(
            [
            "## 目标与节奏",
            f"- 政策目标: {payload['policy_goal']}",
            f"- 落地节奏: {payload['timeline']}",
            "",
            "## 重点支持方向",
            ]
        )
        for item in payload.get("support_points", []):
            lines.append(f"- {item}")

        lines.extend(["", "## 受益 / 风险映射"])
        for item in payload.get("benefit_risk_lines", []):
            lines.append(f"- {item}")

        timeline_points = payload.get("timeline_points", [])
        if timeline_points:
            lines.extend(["", "## 时间线 / 执行抓手"])
            for item in timeline_points:
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

        if payload.get("raw_excerpt"):
            lines.extend(["", "## 文本摘取", payload["raw_excerpt"]])
        return "\n".join(lines)
