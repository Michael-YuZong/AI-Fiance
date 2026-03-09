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
    lines.extend(["", f"### {title}", ""])
    if not items:
        lines.append("暂无。")
        return
    lines.append(items[0])
    for item in items[1:]:
        lines.append(f"- {item}")


def _append_table_subsection(lines: List[str], title: str, headers: List[str], rows: List[List[str]]) -> None:
    lines.extend(["", f"### {title}", ""])
    if not rows:
        lines.append("暂无。")
        return
    lines.extend(_table(headers, rows))


def _append_details(lines: List[str], title: str, items: List[str]) -> None:
    if not items:
        return
    lines.extend(["", f"<details>", f"<summary>{title}</summary>", ""])
    for index, item in enumerate(items):
        prefix = "- " if index else ""
        lines.append(prefix + item)
    lines.extend(["", "</details>"])


def _first_item(payload: Dict[str, Any], key: str) -> str:
    items = payload.get(key, []) or []
    return str(items[0]) if items else ""


class BriefingRenderer:
    """Render daily/weekly briefing payloads to markdown."""

    def render(self, payload: Dict[str, Any]) -> str:
        lines = [
            f"# {payload['title']}",
            "",
            f"> **生成时间**: `{payload['generated_at']}`",
            f"> **数据覆盖**: {payload.get('data_coverage', '未标注')} | 缺失项: {payload.get('missing_sources', '无')}",
        ]

        _append_block(lines, "0. 昨日验证回顾", _first_item(payload, "yesterday_review_lines"))
        for item in (payload.get("yesterday_review_lines", []) or [])[1:]:
            lines.append(f"- {item}")

        _append_block(lines, "1. 主线判断与行动")
        _append_subsection(
            lines,
            "1.1 今日主线",
            (payload.get("headline_lines", []) or [])
            + (payload.get("regime_reason_lines", []) or [])
            + (payload.get("narrative_validation_lines", []) or []),
        )
        _append_subsection(lines, "1.2 今天怎么做", payload.get("action_lines", []))

        _append_block(lines, "2. 资产仪表盘")
        _append_table_subsection(
            lines,
            "2.1 宏观资产",
            ["资产", "最新价", "1日", "5日", "20日", "状态", "异常"],
            payload.get("asset_dashboard_rows", []),
        )
        _append_table_subsection(
            lines,
            "2.2 Watchlist",
            ["标的", "最新价", "1日", "5日", "20日", "趋势", "关键指标", "信号"],
            payload.get("watchlist_rows", []),
        )
        lines.extend(
            [
                "",
                "> **关键指标说明**: 只保留 RSI（强弱）+ ADX（趋势强度）两个指标。RSI>70 超买，<30 超卖；ADX>25 趋势确立，<20 无趋势。",
            ]
        )

        _append_block(lines, "3. 驱动与催化")
        _append_table_subsection(
            lines,
            "3.1 核心事件",
            ["驱动", "证据", "传导", "交易含义"],
            payload.get("catalyst_rows", []),
        )
        _append_table_subsection(
            lines,
            "3.2 今日日历",
            ["时间", "级别", "事件", "含义"],
            payload.get("event_rows", []),
        )
        _append_subsection(
            lines,
            "3.3 盘面与资金",
            (payload.get("market_pulse_lines", []) or [])[:4]
            + (payload.get("main_flow_driver_lines", []) or [])
            + (payload.get("liquidity_lines", []) or [])
            + (payload.get("rotation_driver_lines", []) or [])[:2],
        )
        _append_subsection(
            lines,
            "3.4 新闻覆盖与异常",
            (payload.get("source_quality_lines", []) or []) + (payload.get("anomaly_lines", []) or []),
        )

        _append_block(lines, "4. 今日验证点")
        _append_table_subsection(
            lines,
            "4.1 验证点表",
            ["观察什么", "怎么判断", "如果成立→", "如果不成立→"],
            payload.get("verification_rows", []),
        )
        if payload.get("alerts"):
            _append_subsection(lines, "4.2 关注提醒", payload.get("alerts", []))

        _append_block(lines, "5. 组合与持仓", _first_item(payload, "portfolio_lines"))
        for item in (payload.get("portfolio_lines", []) or [])[1:]:
            lines.append(f"- {item}")

        lines.extend(["", "## 附录"])
        _append_details(lines, "A. 完整技术指标", payload.get("watchlist_technical_lines", []))
        _append_details(lines, "B. 龙虎榜明细", payload.get("lhb_lines", []))
        _append_details(
            lines,
            "C. 全球资金流代理与情绪",
            (payload.get("flow_lines", []) or []) + (payload.get("sentiment_lines", []) or []),
        )

        return "\n".join(lines)
