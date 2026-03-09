"""Alert renderer."""

from __future__ import annotations

from typing import Any, Dict, List


class AlertRenderer:
    """Render discovery alerts into markdown."""

    def render(self, payload: Dict[str, Any]) -> str:
        lines = [
            f"# {payload['title']}",
            "",
            f"- 生成时间: `{payload['generated_at']}`",
            "",
        ]
        if payload.get("regime_line"):
            lines.append(f"- {payload['regime_line']}")
            lines.append("")

        lines.append("## 候选标的")
        if payload.get("candidates"):
            for item in payload["candidates"]:
                lines.append(
                    f"- {item['symbol']} ({item['name']}): {item['signal']}，理由：{item['reason']}。"
                )
        else:
            lines.append("- 当前没有达到阈值的候选。")

        lines.append("")
        lines.append("## 提醒")
        if payload.get("alerts"):
            for alert in payload["alerts"]:
                lines.append(f"- {alert}")
        else:
            lines.append("- 当前没有额外提醒。")
        return "\n".join(lines)
