"""Briefing markdown renderer."""

from __future__ import annotations

from typing import Any, Dict, List


def _table(headers: List[str], rows: List[List[str]]) -> List[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _append_block(lines: List[str], title: str, summary: str = "") -> None:
    lines.extend(["", f"## {title}"])
    if summary:
        lines.extend(["", summary])


def _append_subsection(lines: List[str], title: str, items: List[str]) -> None:
    lines.extend(["", f"### {title}"])
    if not items:
        lines.append("")
        lines.append("暂无。")
        return
    lines.append("")
    lines.append(items[0])
    for item in items[1:]:
        lines.append(f"- {item}")


def _first_item(payload: Dict[str, Any], key: str) -> str:
    items = payload.get(key, []) or []
    return str(items[0]) if items else ""


class BriefingRenderer:
    """Render daily/weekly briefing payloads to markdown."""

    def render(self, payload: Dict[str, Any]) -> str:
        lines = [
            f"# {payload['title']}",
            "",
            f"- 生成时间: `{payload['generated_at']}`",
        ]

        _append_block(lines, "主线判断", _first_item(payload, "headline_lines"))
        for item in (payload.get("headline_lines", []) or [])[1:]:
            lines.append(f"- {item}")
        _append_subsection(lines, "今天怎么做", payload.get("action_lines", []))
        _append_subsection(lines, "昨日验证点回顾", payload.get("yesterday_review_lines", []))
        _append_subsection(lines, "主线校验", payload.get("narrative_validation_lines", []))
        _append_subsection(lines, "重要催化", payload.get("important_event_lines", []))
        _append_subsection(
            lines,
            "新闻覆盖与异常",
            (payload.get("source_quality_lines", []) or []) + (payload.get("anomaly_lines", []) or []),
        )

        _append_block(
            lines,
            "资产仪表盘",
            _first_item(payload, "story_lines") or _first_item(payload, "macro_items"),
        )
        lines.extend(["", "### 资产仪表盘", ""])
        lines.extend(
            _table(
                ["对象", "类型", "最新", "区间表现", "状态", "备注"],
                payload.get("asset_dashboard_rows", []),
            )
        )
        _append_subsection(lines, "市场主线解读", payload.get("story_lines", []))
        _append_subsection(
            lines,
            "盘面与资金",
            (payload.get("rotation_driver_lines", []) or [])
            + (payload.get("main_flow_driver_lines", []) or [])
            + (payload.get("liquidity_lines", []) or []),
        )
        _append_subsection(lines, "宏观与流动性", payload.get("macro_items", []))
        _append_subsection(lines, "龙虎榜与活跃资金", payload.get("lhb_lines", []))

        _append_block(
            lines,
            "验证与行动",
            _first_item(payload, "verification_lines") or _first_item(payload, "event_lines"),
        )
        _append_subsection(lines, "今日验证点", payload.get("verification_lines", []))
        _append_subsection(lines, "今日已知事件", payload.get("event_lines", []))
        _append_subsection(lines, "跟踪清单", payload.get("calendar_lines", []))
        _append_subsection(lines, "关注提醒", payload.get("alerts", []))

        _append_block(
            lines,
            "Watchlist 与组合",
            _first_item(payload, "portfolio_lines") or _first_item(payload, "watchlist_technical_lines"),
        )
        lines.extend(["", "### Watchlist 雷达", ""])
        lines.extend(
            _table(
                ["标的", "价格", "1日", "5日", "20日", "趋势", "量比", "技术"],
                payload.get("watchlist_rows", []),
            )
        )
        _append_subsection(lines, "Watchlist 技术指标", payload.get("watchlist_technical_lines", []))
        _append_subsection(lines, "组合与 Thesis", payload.get("portfolio_lines", []))

        return "\n".join(lines)
