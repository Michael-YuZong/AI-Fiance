"""Markdown renderer for asset scan output."""

from __future__ import annotations

from typing import Dict


class ScannerReportRenderer:
    """Render a scorecard into markdown."""

    def render(self, scorecard: Dict[str, object]) -> str:
        lines = [
            f"# 标的扫描: {scorecard['symbol']}",
            "",
            f"- 资产类型: `{scorecard['asset_type']}`",
            f"- 生成时间: `{scorecard['generated_at']}`",
            "",
        ]

        for section in scorecard["sections"]:
            lines.append(f"## {section['overall']} {section['title']}")
            lines.append(section["summary"])
            lines.append("")
            lines.append("| 维度 | 结论 | 说明 |")
            lines.append("| --- | --- | --- |")
            for item in section["items"]:
                lines.append(f"| {item['name']} | {item['icon']} | {item['reason']} |")
            lines.append("")

        lines.append("## 备注")
        for note in scorecard.get("notes", []):
            lines.append(f"- {note}")
        return "\n".join(lines)
