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


def _append_details_raw(lines: List[str], title: str, content_lines: List[str]) -> None:
    lines.extend(["", "<details>", f"<summary>{title}</summary>", ""])
    if not content_lines:
        lines.append("暂无。")
    else:
        lines.extend(content_lines)
    lines.extend(["", "</details>"])


def _chart_lines(charts: Dict[str, Dict[str, str]]) -> List[str]:
    result: List[str] = []
    for symbol, paths in charts.items():
        result.append(f"**{symbol}**")
        result.append("")
        if "windows" in paths:
            result.append(f"![{symbol} 阶段走势]({paths['windows']})")
            result.append("")
        if "indicators" in paths:
            result.append(f"![{symbol} 技术指标]({paths['indicators']})")
            result.append("")
    return result


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
        _append_block(lines, "3. 驱动与催化")
        _append_subsection(lines, "3.1 核心事件（限3-5条）", payload.get("core_event_lines", []))
        _append_table_subsection(
            lines,
            "3.2 行业与主题跟踪（限2-4个方向）",
            ["方向", "催化剂", "逻辑", "时间维度", "风险点"],
            payload.get("theme_tracking_rows", []),
        )
        for item in payload.get("theme_tracking_lines", []) or []:
            lines.append(f"- {item}")
        lines.extend(["", "### 3.3 今日日历", "", "**市场事件**", ""])
        if payload.get("market_event_rows"):
            lines.extend(
                _table(
                    ["时间", "事件", "预期/前值", "重要性", "影响标的"],
                    payload.get("market_event_rows", []),
                )
            )
        else:
            lines.append("暂无。")
        lines.extend(["", "**操作提醒**", ""])
        if payload.get("workflow_event_rows"):
            lines.extend(
                _table(
                    ["时间", "动作", "说明"],
                    payload.get("workflow_event_rows", []),
                )
            )
        else:
            lines.append("暂无。")
        _append_subsection(lines, "3.4 盘面与资金", payload.get("capital_flow_lines", []))
        _append_subsection(lines, "3.5 新闻覆盖与数据质量", payload.get("quality_lines", []))

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

        charts = payload.get("charts", {})
        if charts:
            _append_details_raw(
                lines,
                "A. 图表速览",
                _chart_lines(charts),
            )
        _append_table_details(
            lines,
            "B. 完整技术指标",
            ["标的", "MACD", "KDJ", "RSI", "BOLL", "OBV", "ADX", "斐波那契"],
            payload.get("appendix_technical_rows", []),
        )
        _append_details(lines, "C. 龙虎榜明细", payload.get("appendix_lhb_lines", []))
        _append_details(lines, "D. 资金流代理与情绪", payload.get("appendix_flow_lines", []))
        _append_details(lines, "E. 股指期货与期权", payload.get("appendix_derivative_lines", []))
        _append_table_details(
            lines,
            "F. 重点公司财报点评（财报季启用）",
            ["公司", "报告期", "收入(YoY)", "利润(YoY)", "vs 一致预期", "核心变化", "交易含义"],
            payload.get("appendix_earnings_rows", []),
            fallback="当前未启用财报季正文解析，或今日无相关财报催化。",
        )
        _append_table_details(
            lines,
            "G. 分层配置建议（如需服务不同风险偏好）",
            ["类型", "仓位", "底仓方向", "弹性方向", "当日调整"],
            payload.get("appendix_allocation_rows", []),
            fallback="当前未生成分层配置建议。",
        )
        return "\n".join(lines)

    def render_noon(self, payload: Dict[str, Any]) -> str:
        """Render noon briefing payload to markdown."""
        lines = [
            f"# {payload['title']}",
            "",
            f"> **生成时间**: `{payload['generated_at']}`",
        ]
        for item in payload.get("watchlist_change_lines", []) or []:
            lines.append(f"> {item}")

        _append_block(lines, "0. 晨报策略验证")
        if payload.get("morning_eval_rows"):
            lines.extend(
                _table(
                    ["验证点", "判断标准", "上午实际", "判定"],
                    payload.get("morning_eval_rows", []),
                )
            )
        else:
            lines.extend(["", payload.get("morning_eval_fallback", "暂无今日晨报，跳过策略验证。")])

        _append_block(lines, "1. 上午盘面回顾")
        _append_table_subsection(
            lines,
            "1.1 指数与成交",
            ["指数", "收盘", "涨跌幅", "成交额(亿)", "较前日", "简评"],
            payload.get("domestic_index_rows", []),
        )
        for item in payload.get("domestic_market_lines", []) or []:
            lines.append(f"- {item}")
        _append_table_subsection(
            lines,
            "1.2 风格与行业",
            ["维度", "今日强势", "今日弱势", "信号"],
            payload.get("style_rows", []),
        )
        if payload.get("industry_rows"):
            lines.extend(["", "**行业涨跌幅 TOP/BOTTOM 5**", ""])
            lines.extend(_table(["排名", "行业", "涨跌幅", "主要催化"], payload.get("industry_rows", [])))
        _append_table_subsection(
            lines,
            "1.3 Watchlist 表现",
            ["标的", "最新价", "1日", "5日", "20日", "趋势", "信号"],
            payload.get("watchlist_rows", []),
        )

        _append_block(lines, "2. 策略修正")
        _append_subsection(lines, "2.1 主线修正", payload.get("strategy_adjustment_lines", []))
        _append_subsection(lines, "2.2 下午观察", payload.get("afternoon_action_lines", []))

        _append_block(lines, "3. 下午看点")
        _append_table_subsection(
            lines,
            "3.1 下午验证点",
            ["#", "观察什么", "怎么判断（量化标准）", "如果成立→", "如果不成立→"],
            payload.get("afternoon_verification_rows", []),
        )
        lines.extend(["", "### 3.2 操作提醒", ""])
        if payload.get("afternoon_event_rows"):
            lines.extend(
                _table(
                    ["时间", "动作", "说明"],
                    payload.get("afternoon_event_rows", []),
                )
            )
        else:
            lines.append("暂无。")

        _append_block(lines, "4. 组合与持仓", _first_item(payload, "portfolio_lines"))
        if payload.get("portfolio_table_rows"):
            lines.extend(
                _table(
                    ["标的", "方向", "成本", "现价", "盈亏", "Thesis", "状态"],
                    payload.get("portfolio_table_rows", []),
                )
            )
        for item in (payload.get("portfolio_lines", []) or [])[1:]:
            lines.append(f"- {item}")

        return "\n".join(lines)

    def render_evening(self, payload: Dict[str, Any]) -> str:
        """Render evening briefing payload to markdown."""
        lines = [
            f"# {payload['title']}",
            "",
            f"> **生成时间**: `{payload['generated_at']}`",
        ]
        for item in payload.get("watchlist_change_lines", []) or []:
            lines.append(f"> {item}")

        _append_block(lines, "0. 全日验证回顾")
        if payload.get("full_day_eval_rows"):
            lines.extend(
                _table(
                    ["验证点", "判断标准", "收盘结果", "判定"],
                    payload.get("full_day_eval_rows", []),
                )
            )
        else:
            lines.extend(["", payload.get("full_day_eval_fallback", "暂无今日晨报，跳过全日验证。")])
        for item in payload.get("hit_rate_lines", []) or []:
            lines.append(f"- {item}")

        _append_block(lines, "1. 全日市场总结")
        _append_table_subsection(
            lines,
            "1.1 指数与成交",
            ["指数", "收盘", "涨跌幅", "成交额(亿)", "较前日", "简评"],
            payload.get("domestic_index_rows", []),
        )
        for item in payload.get("domestic_market_lines", []) or []:
            lines.append(f"- {item}")
        _append_table_subsection(
            lines,
            "1.2 风格与行业",
            ["维度", "今日强势", "今日弱势", "信号"],
            payload.get("style_rows", []),
        )
        if payload.get("industry_rows"):
            lines.extend(["", "**行业涨跌幅 TOP/BOTTOM 5**", ""])
            lines.extend(_table(["排名", "行业", "涨跌幅", "主要催化"], payload.get("industry_rows", [])))
        _append_table_subsection(
            lines,
            "1.3 宏观资产",
            ["资产", "最新价", "1日", "5日", "20日", "状态", "异常"],
            payload.get("macro_asset_rows", []),
        )
        _append_table_subsection(
            lines,
            "1.4 Watchlist 表现",
            ["标的", "最新价", "1日", "5日", "20日", "趋势", "信号"],
            payload.get("watchlist_rows", []),
        )

        _append_block(lines, "2. 主线复盘")
        _append_subsection(lines, "2.1 今日主线回顾", payload.get("narrative_review_lines", []))
        _append_subsection(lines, "2.2 核心事件复盘", payload.get("core_event_lines", []))
        _append_subsection(lines, "2.3 盘面与资金", payload.get("capital_flow_lines", []))

        _append_block(lines, "3. 明日展望")
        _append_table_subsection(
            lines,
            "3.1 隔夜外盘",
            ["市场", "指数", "收盘", "涨跌幅", "简评"],
            payload.get("overnight_rows", []),
        )
        _append_subsection(lines, "3.2 明日主线预判", payload.get("tomorrow_outlook_lines", []))
        _append_table_subsection(
            lines,
            "3.3 明日验证点（预设）",
            ["#", "观察什么", "怎么判断（量化标准）", "如果成立→", "如果不成立→"],
            payload.get("tomorrow_verification_rows", []),
        )
        _append_subsection(lines, "3.4 明日操作建议", payload.get("tomorrow_action_lines", []))

        _append_block(lines, "4. 组合与持仓", _first_item(payload, "portfolio_lines"))
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

        charts = payload.get("charts", {})
        if charts:
            _append_details_raw(
                lines,
                "A. 图表速览",
                _chart_lines(charts),
            )
        _append_table_details(
            lines,
            "B. 完整技术指标",
            ["标的", "MACD", "KDJ", "RSI", "BOLL", "OBV", "ADX", "斐波那契"],
            payload.get("appendix_technical_rows", []),
        )
        _append_details(lines, "C. 龙虎榜明细", payload.get("appendix_lhb_lines", []))
        _append_details(lines, "D. 资金流代理与情绪", payload.get("appendix_flow_lines", []))

        return "\n".join(lines)
