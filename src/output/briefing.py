"""Briefing markdown renderer."""

from __future__ import annotations

from typing import Any, Dict, List


def _table(headers: List[str], rows: List[List[str]]) -> List[str]:
    def _escape(value: str) -> str:
        return value.replace("|", "\\|").replace("\n", "<br>")

    lines = [
        "| " + " | ".join(_escape(str(header)) for header in headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_escape(str(cell)) for cell in row) + " |")
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
    lines.extend(["", "<details>", f"<summary>{title}</summary>", ""])
    if not items:
        lines.append("暂无。")
    else:
        lines.append(items[0])
        for item in items[1:]:
            lines.append(f"- {item}")
    lines.extend(["", "</details>"])


def _append_table_details(lines: List[str], title: str, headers: List[str], rows: List[List[str]], fallback: str = "暂无。") -> None:
    lines.extend(["", "<details>", f"<summary>{title}</summary>", ""])
    if not rows:
        lines.append(fallback)
    else:
        lines.extend(_table(headers, rows))
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

        _append_block(lines, "0. 昨日验证回顾")
        if payload.get("yesterday_review_rows"):
            lines.extend(
                _table(
                    ["昨日验证点", "判断标准", "实际结果", "判定"],
                    payload.get("yesterday_review_rows", []),
                )
            )
        else:
            lines.extend(["", _first_item(payload, "yesterday_review_lines") or "暂无昨日晨报归档。"])
        for item in (payload.get("yesterday_review_lines", []) or [])[1:]:
            lines.append(f"- {item}")

        _append_block(lines, "1. 主线判断与行动")
        _append_subsection(lines, "1.1 今日主线", payload.get("headline_lines", []))
        _append_subsection(lines, "1.2 今天怎么做", payload.get("action_lines", []))

        _append_block(lines, "2. 市场全景")
        _append_table_subsection(
            lines,
            "2.1 国内市场概览",
            ["指数", "收盘", "涨跌幅", "成交额(亿)", "较前日", "简评"],
            payload.get("domestic_index_rows", []),
        )
        for item in payload.get("domestic_market_lines", []) or []:
            lines.append(f"- {item}")

        _append_table_subsection(
            lines,
            "2.2 风格与行业",
            ["维度", "今日强势", "今日弱势", "信号"],
            payload.get("style_rows", []),
        )
        if payload.get("industry_rows"):
            lines.extend(["", "**行业涨跌幅 TOP/BOTTOM 5**", ""])
            lines.extend(_table(["排名", "行业", "涨跌幅", "主要催化"], payload.get("industry_rows", [])))

        _append_table_subsection(
            lines,
            "2.3 宏观资产",
            ["资产", "最新价", "1日", "5日", "20日", "状态", "异常"],
            payload.get("macro_asset_rows", []),
        )
        _append_table_subsection(
            lines,
            "2.4 隔夜外盘",
            ["市场", "指数", "收盘", "涨跌幅", "简评"],
            payload.get("overnight_rows", []),
        )
        _append_table_subsection(
            lines,
            "2.5 Watchlist",
            ["标的", "最新价", "1日", "5日", "20日", "趋势", "RSI / ADX", "信号"],
            payload.get("watchlist_rows", []),
        )
        lines.extend(
            [
                "",
                "> RSI>70 超买，<30 超卖；ADX>25 趋势确立，<20 无趋势。",
                "> 异常值校验: ETF 单日>5% 或 20日>15% 标记 ⚠️；指数价格偏离量级>10x 标记 ⚠️。",
            ]
        )

        _append_block(lines, "3. 驱动与催化")
        _append_subsection(lines, "3.1 核心事件（限3-5条）", payload.get("core_event_lines", []))
        _append_table_subsection(
            lines,
            "3.2 今日日历 - 市场事件",
            ["时间", "事件", "预期/前值", "重要性", "影响标的"],
            payload.get("market_event_rows", []),
        )
        _append_table_subsection(
            lines,
            "3.2 今日日历 - 操作提醒",
            ["时间", "动作", "说明"],
            payload.get("workflow_event_rows", []),
        )
        _append_subsection(lines, "3.3 盘面与资金", payload.get("capital_flow_lines", []))
        _append_subsection(lines, "3.4 新闻覆盖与数据质量", payload.get("quality_lines", []))

        _append_block(lines, "4. 今日验证点")
        _append_table_subsection(
            lines,
            "4.1 验证点表",
            ["#", "观察什么", "怎么判断（量化标准）", "如果成立→", "如果不成立→"],
            payload.get("verification_rows", []),
        )

        _append_block(lines, "5. 组合与持仓", _first_item(payload, "portfolio_lines"))
        if payload.get("portfolio_table_rows"):
            lines.extend(
                _table(
                    ["标的", "方向", "成本", "现价", "盈亏", "Thesis", "状态"],
                    payload.get("portfolio_table_rows", []),
                )
            )
        for item in (payload.get("portfolio_lines", []) or [])[1:]:
            lines.append(f"- {item}")

        lines.extend(["", "## 附录（折叠，按需展开）"])
        _append_table_details(
            lines,
            "A. 完整技术指标",
            ["标的", "MACD", "KDJ", "RSI", "BOLL", "OBV", "ADX", "斐波那契"],
            payload.get("appendix_technical_rows", []),
        )
        _append_details(lines, "B. 龙虎榜明细", payload.get("appendix_lhb_lines", []))
        _append_details(lines, "C. 资金流代理与情绪", payload.get("appendix_flow_lines", []))
        _append_details(lines, "D. 股指期货与期权", payload.get("appendix_derivative_lines", []))
        _append_table_details(
            lines,
            "E. 重点公司财报点评（财报季启用）",
            ["公司", "报告期", "收入(YoY)", "利润(YoY)", "vs 一致预期", "核心变化", "交易含义"],
            payload.get("appendix_earnings_rows", []),
            fallback="当前未启用财报季正文解析，或今日无相关财报催化。",
        )
        _append_table_details(
            lines,
            "F. 分层配置建议（如需服务不同风险偏好）",
            ["类型", "仓位", "底仓方向", "弹性方向", "当日调整"],
            payload.get("appendix_allocation_rows", []),
            fallback="当前未生成分层配置建议。",
        )
        return "\n".join(lines)
