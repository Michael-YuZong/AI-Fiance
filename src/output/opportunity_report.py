"""Opportunity discovery, analysis, and comparison renderers."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence


def _table(headers: Sequence[str], rows: Iterable[Sequence[str]]) -> List[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        escaped = [str(cell).replace("|", "\\|").replace("\n", "<br>") for cell in row]
        lines.append("| " + " | ".join(escaped) + " |")
    return lines


def _dimension_rows(analysis: Dict[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    ordered = [
        ("technical", "技术面"),
        ("fundamental", "基本面"),
        ("catalyst", "催化面"),
        ("relative_strength", "相对强弱"),
        ("chips", "筹码结构"),
        ("risk", "风险特征"),
        ("seasonality", "季节/日历"),
        ("macro", "宏观敏感度"),
    ]
    for key, label in ordered:
        dimension = analysis["dimensions"][key]
        score = "缺失" if dimension.get("score") is None else f"{dimension['score']}/{dimension['max_score']}"
        rows.append([label, score, dimension.get("core_signal", "—")])
    return rows


class OpportunityReportRenderer:
    """Render opportunity scan, analysis, and compare payloads."""

    def render_scan(self, analysis: Dict[str, Any]) -> str:
        name = analysis["name"]
        symbol = analysis["symbol"]
        lines = [
            f"# {name} ({symbol}) 全景分析 | {analysis['generated_at'][:10]}",
            "",
            "## 一句话结论",
            f"{analysis['rating']['stars']} {analysis['rating']['label']} — {analysis['conclusion']}",
            "",
            "## 硬性检查",
        ]
        lines.extend(_table(["检查项", "状态", "说明"], [[item["name"], item["status"], item["detail"]] for item in analysis["hard_checks"]]))
        lines.extend(["", "## 八维详细分析"])
        ordered = [
            ("technical", "技术面"),
            ("fundamental", "基本面"),
            ("catalyst", "催化面"),
            ("relative_strength", "相对强弱"),
            ("chips", "筹码结构"),
            ("risk", "风险特征"),
            ("seasonality", "季节/日历"),
            ("macro", "宏观敏感度"),
        ]
        for key, label in ordered:
            dimension = analysis["dimensions"][key]
            score = "缺失" if dimension.get("score") is None else f"{dimension['score']}/{dimension['max_score']}"
            lines.extend(["", f"### {label} {score}", dimension["summary"]])
            for factor in dimension["factors"]:
                lines.append(f"- `{factor['name']}` {factor['display_score']}：{factor['signal']}。{factor['detail']}")

        lines.extend(
            [
                "",
                "## 综合评级",
                f"{analysis['rating']['stars']} {analysis['rating']['label']} — {analysis['rating']['meaning']}",
                "",
                "## 操作建议",
                f"- **方向**: {analysis['action']['direction']}",
                f"- **介入条件**: {analysis['action']['entry']}",
                f"- **建议仓位**: {analysis['action']['position']}",
                f"- **止损参考**: {analysis['action']['stop']}",
                f"- **目标参考**: {analysis['action']['target']}",
                f"- **时间框架**: {analysis['action']['timeframe']}",
                "",
                "## 风险提示",
            ]
        )
        for item in analysis["risks"]:
            lines.append(f"- {item}")
        if analysis.get("notes"):
            lines.extend(["", "## 备注"])
            for note in analysis["notes"]:
                lines.append(f"- {note}")
        return "\n".join(lines)

    def render_discovery(self, payload: Dict[str, Any]) -> str:
        regime = payload.get("regime", {})
        day_theme = payload.get("day_theme", {})
        lines = [
            f"# 每日机会发现 | {payload['generated_at'][:10]}",
            "",
            f"> 扫描池: {payload.get('scan_pool', 0)}只标的 | 过门槛: {payload.get('passed_pool', 0)}只 | 当前 Regime: {regime.get('current_regime', 'unknown')} | 今日主线: {day_theme.get('label', '未识别')}",
            "",
            f"## TOP {len(payload.get('top', []))} 机会",
        ]
        if not payload.get("top"):
            lines.append("")
            lines.append("当前没有达到输出阈值的标的。")
        for index, analysis in enumerate(payload.get("top", []), start=1):
            lines.extend(
                [
                    "",
                    f"### {index}. {analysis['name']} ({analysis['symbol']})  {analysis['rating']['stars']} {analysis['rating']['label']}",
                    "",
                    "**八维雷达：**",
                ]
            )
            lines.extend(_table(["维度", "得分", "核心信号"], _dimension_rows(analysis)))
            lines.extend(
                [
                    "",
                    f"**结论：** {analysis['conclusion']}",
                ]
            )
            for risk in analysis["rating"]["warnings"][:2]:
                lines.append(risk)
            lines.extend(
                [
                    "",
                    "**建议操作：**",
                    f"- 加入 watchlist / 继续跟踪，介入条件：{analysis['action']['entry']}",
                    f"- 建议仓位：{analysis['action']['position']}",
                    f"- 止损参考：{analysis['action']['stop']}",
                    "",
                    "---",
                ]
            )
        if payload.get("blind_spots"):
            lines.extend(["", "## 数据盲区与降级说明"])
            for item in payload["blind_spots"]:
                lines.append(f"- {item}")
        return "\n".join(lines).rstrip()

    def render_compare(self, payload: Dict[str, Any]) -> str:
        analyses = payload["analyses"]
        best_symbol = payload["best_symbol"]
        best = next(item for item in analyses if item["symbol"] == best_symbol)
        other = next(item for item in analyses if item["symbol"] != best_symbol)
        best_total = sum((dimension.get("score") or 0) for dimension in best["dimensions"].values())
        other_total = sum((dimension.get("score") or 0) for dimension in other["dimensions"].values())
        if best["rating"]["rank"] > other["rating"]["rank"]:
            reason = f"{best['rating']['label']} 评级更高，且多维得分更均衡。"
        else:
            reason = f"评级相同，但综合八维总分 {best_total} 高于 {other_total}。"
        lines = [
            f"# {analyses[0]['symbol']} vs {analyses[1]['symbol']} 对比分析 | {payload['generated_at'][:10]}",
            "",
            "## 结论",
            f"**推荐 {best_symbol}**，理由：{reason}",
            "",
            "## 八维对比",
        ]
        rows: List[List[str]] = []
        ordered = [
            ("technical", "技术面"),
            ("fundamental", "基本面"),
            ("catalyst", "催化面"),
            ("relative_strength", "相对强弱"),
            ("chips", "筹码结构"),
            ("risk", "风险特征"),
            ("seasonality", "季节/日历"),
            ("macro", "宏观敏感度"),
        ]
        for key, label in ordered:
            a_score = analyses[0]["dimensions"][key]["score"]
            b_score = analyses[1]["dimensions"][key]["score"]
            a_display = "缺失" if a_score is None else str(a_score)
            b_display = "缺失" if b_score is None else str(b_score)
            if a_score is None and b_score is None:
                winner = "—"
            elif b_score is None or (a_score is not None and a_score > (b_score or -1)):
                winner = analyses[0]["symbol"]
            elif a_score is None or (b_score is not None and b_score > (a_score or -1)):
                winner = analyses[1]["symbol"]
            else:
                winner = "平"
            rows.append([label, a_display, b_display, winner])
        lines.extend(_table(["维度", analyses[0]["symbol"], analyses[1]["symbol"], "优势方"], rows))

        diffs = []
        for key, label in ordered:
            a_score = analyses[0]["dimensions"][key]["score"] or 0
            b_score = analyses[1]["dimensions"][key]["score"] or 0
            if a_score != b_score:
                winner = analyses[0]["symbol"] if a_score > b_score else analyses[1]["symbol"]
                diffs.append((abs(a_score - b_score), label, winner, a_score, b_score))
        diffs.sort(reverse=True)
        lines.extend(["", "## 核心差异"])
        for index, (_, label, winner, a_score, b_score) in enumerate(diffs[:3], start=1):
            lines.append(f"{index}. `{label}` 差异最大：{analyses[0]['symbol']}={a_score}，{analyses[1]['symbol']}={b_score}，当前更强的是 {winner}。")

        lines.extend(
            [
                "",
                "## 场景化建议",
                f"- 如果你追求短线确认和趋势跟随：选 {analyses[0]['symbol'] if (analyses[0]['dimensions']['technical']['score'] or 0) >= (analyses[1]['dimensions']['technical']['score'] or 0) else analyses[1]['symbol']}",
                f"- 如果你更在意风险控制和分散：选 {analyses[0]['symbol'] if (analyses[0]['dimensions']['risk']['score'] or 0) >= (analyses[1]['dimensions']['risk']['score'] or 0) else analyses[1]['symbol']}",
                f"- 如果你只想选一个综合更均衡的：当前优先 {best_symbol}",
            ]
        )
        return "\n".join(lines)
