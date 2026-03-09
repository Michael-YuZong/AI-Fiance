"""Opportunity discovery, analysis, and comparison renderers."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


DIMENSION_ORDER = [
    ("technical", "技术面"),
    ("fundamental", "基本面"),
    ("catalyst", "催化面"),
    ("relative_strength", "相对强弱"),
    ("chips", "筹码结构"),
    ("risk", "风险特征"),
    ("seasonality", "季节/日历"),
    ("macro", "宏观敏感度"),
]


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
    for key, label in DIMENSION_ORDER:
        dimension = analysis["dimensions"][key]
        score = "缺失" if dimension.get("score") is None else f"{dimension['score']}/{dimension['max_score']}"
        rows.append([label, score, dimension.get("core_signal", "—")])
    return rows


def _dimension_summary_rows(analysis: Dict[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    for key, label in DIMENSION_ORDER:
        dimension = analysis["dimensions"][key]
        score = "—/100" if dimension.get("score") is None else f"{dimension['score']}/{dimension['max_score']}"
        rows.append([label, score, dimension.get("summary", "—"), dimension.get("core_signal", "—")])
    return rows


def _win_rate_label(analysis: Dict[str, Any]) -> tuple[str, str]:
    dimension_scores = [dimension.get("score") for dimension in analysis["dimensions"].values()]
    strong = sum(1 for score in dimension_scores if score is not None and score >= 60)
    medium = sum(1 for score in dimension_scores if score is not None and score >= 40)
    rank = int(analysis.get("rating", {}).get("rank", 0))
    if rank >= 4 or strong >= 4:
        return "高", f"当前有 {strong} 个维度达到 60 分以上，共振较完整。"
    if rank >= 2 or strong >= 2 or medium >= 4:
        return "中", f"当前有 {strong} 个强维度、{medium} 个中等维度，胜率取决于确认是否补齐。"
    return "低", "当前共振维度不多，更多是方向观察而不是高胜率出手点。"


def _rating_header(analysis: Dict[str, Any]) -> str:
    rating = analysis.get("rating", {})
    stars = rating.get("stars", "—")
    label = rating.get("label", "未评级")
    return f"**{stars} {label}**"


def _hard_check_rows(analysis: Dict[str, Any]) -> List[List[str]]:
    return [[item["name"], item["status"], item["detail"]] for item in analysis["hard_checks"]]


def _factor_rows(dimension: Dict[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    for factor in dimension.get("factors", []):
        rows.append(
            [
                factor.get("name", "—"),
                factor.get("signal", "—"),
                factor.get("detail", "—"),
                factor.get("display_score", "—"),
            ]
        )
    return rows


def _data_sources(analysis: Dict[str, Any]) -> str:
    sources: List[str] = ["行情/技术: AKShare + Yahoo 回退"]
    if analysis["dimensions"]["fundamental"].get("valuation_snapshot"):
        sources.append("估值: 指数估值快照")
    else:
        sources.append("估值: 价格位置代理")
    sources.append("催化: RSS + 动态关键词检索")
    sources.append("事件: 本地事件日历")
    if analysis.get("day_theme", {}).get("label"):
        sources.append("上下文: 晨报主线/Regime")
    return "；".join(sources)


def _missing_data_notes(analysis: Dict[str, Any]) -> str:
    items: List[str] = []
    for _, label in DIMENSION_ORDER:
        pass
    for key, label in DIMENSION_ORDER:
        dimension = analysis["dimensions"][key]
        missing_factors = [
            factor.get("name", "—")
            for factor in dimension.get("factors", [])
            if factor.get("display_score") == "缺失" or factor.get("signal") == "缺失"
        ]
        if dimension.get("score") is None and not missing_factors:
            items.append(f"{label}: 维度数据缺失")
        elif missing_factors:
            preview = "、".join(missing_factors[:3])
            suffix = "等" if len(missing_factors) > 3 else ""
            items.append(f"{label}: {preview}{suffix}")
    return "；".join(items) if items else "—"


def _estimated_notes(analysis: Dict[str, Any]) -> str:
    notes: List[str] = []
    fundamental = analysis["dimensions"]["fundamental"]
    if not fundamental.get("valuation_snapshot"):
        notes.append("基本面估值未接入真实指数估值时，使用价格位置代理")
    for note in analysis.get("notes", []):
        if "代理" in note or "估算" in note or "回退" in note:
            notes.append(note)
    proxy_symbol = analysis.get("metadata", {}).get("proxy_symbol")
    if proxy_symbol:
        notes.append(f"行情代理使用 {proxy_symbol}")
    return "；".join(dict.fromkeys(notes)) if notes else "—"


def _linked_briefing(analysis: Dict[str, Any]) -> str:
    day_theme = analysis.get("day_theme", {}).get("label")
    regime = analysis.get("regime", {}).get("current_regime")
    if day_theme and regime:
        return f"当日主线 `{day_theme}`；背景 Regime `{regime}`"
    if day_theme:
        return f"当日主线 `{day_theme}`"
    if regime:
        return f"背景 Regime `{regime}`"
    return "—"


def _visual_lines(visuals: Optional[Mapping[str, str]]) -> List[str]:
    if not visuals:
        return []
    dashboard = str(visuals.get("dashboard", "")).strip()
    windows = str(visuals.get("windows", "")).strip()
    indicators = str(visuals.get("indicators", "")).strip()
    if not any([dashboard, windows, indicators]):
        return []
    lines = [
        "## 图表速览",
        "> 下图由本地 Python 数据管线自动生成，和正文分析使用的是同一份行情与评分结果。",
        "",
    ]
    if dashboard:
        lines.extend(["### 总览看板", f"![分析看板]({dashboard})", ""])
    if windows:
        lines.extend(["### 阶段走势", f"![阶段走势]({windows})", ""])
    if indicators:
        lines.extend(["### 技术指标总览", f"![技术指标]({indicators})", ""])
    return lines


def _fallback_narrative(analysis: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "headline": analysis.get("conclusion", ""),
        "judgment": {
            "direction": analysis.get("action", {}).get("direction", "观察"),
            "cycle": analysis.get("action", {}).get("timeframe", "中期"),
            "odds": "中",
            "state": analysis.get("action", {}).get("direction", "观察"),
        },
        "drivers": {
            "macro": analysis["dimensions"]["macro"]["summary"],
            "flow": analysis["dimensions"]["chips"]["summary"],
            "relative": analysis["dimensions"]["relative_strength"]["summary"],
            "technical": analysis["dimensions"]["technical"]["summary"],
        },
        "contradiction": analysis.get("conclusion", ""),
        "positives": analysis.get("risks", [])[:0],
        "cautions": analysis.get("risks", [])[:3],
        "phase": {"label": "震荡整理", "body": "当前仍以观察为主。"},
        "risk_points": {
            "fundamental": "基本面仍需继续验证。",
            "valuation": "当前位置需要继续评估估值消化情况。",
            "crowding": "仍需留意交易拥挤与资金撤退风险。",
            "external": "外部变量变化可能会先于基本面影响价格。",
        },
        "watch_points": [analysis.get("action", {}).get("entry", "等待更多确认")],
        "scenarios": {
            "base": "基准情景仍以震荡消化为主。",
            "bull": "若价格与资金重新共振，判断会升级。",
            "bear": "若支撑失效，风险会先释放。",
        },
        "playbook": {
            "trend": "等待趋势信号更完整后再参与。",
            "allocation": "以分批和控制仓位为主。",
            "defensive": "更适合继续观察。",
        },
        "summary_lines": [analysis.get("conclusion", "")],
    }


class OpportunityReportRenderer:
    """Render opportunity scan, analysis, and compare payloads."""

    def render_scan(self, analysis: Dict[str, Any], visuals: Optional[Mapping[str, str]] = None) -> str:
        name = analysis["name"]
        symbol = analysis["symbol"]
        narrative = analysis.get("narrative") or _fallback_narrative(analysis)
        win_rate, win_rate_note = _win_rate_label(analysis)
        warnings = analysis.get("rating", {}).get("warnings", [])
        action = analysis["action"]
        lines = [
            f"# {name} ({symbol}) 全景分析 | {analysis['generated_at'][:10]}",
            "",
            "## 一句话结论",
            _rating_header(analysis),
            "",
            narrative["headline"],
        ]
        if warnings:
            lines.extend(["", *warnings[:2]])
        lines.extend(
            [
                "",
                "## 当前判断",
            ]
        )
        lines.extend(
            _table(
                ["维度", "判断", "说明"],
                [
                    ["方向", narrative["judgment"]["direction"], "回答大方向是否成立。"],
                    ["所处阶段", narrative["phase"]["label"], narrative["phase"]["body"]],
                    ["时间框架", narrative["judgment"]["cycle"], "当前判断更偏短线、中线还是长线。"],
                    ["赔率", narrative["judgment"]["odds"], "基于位置、支撑和目标空间的综合判断。"],
                    ["胜率", win_rate, win_rate_note],
                    ["交易状态", narrative["judgment"]["state"], "这是当前更合理的参与方式，而不是口号式买卖建议。"],
                ],
            )
        )
        visual_lines = _visual_lines(visuals)
        if visual_lines:
            lines.extend(["", *visual_lines])
        lines.extend(["", "## 硬性检查"])
        lines.extend(_table(["检查项", "状态", "说明"], _hard_check_rows(analysis)))
        lines.extend(
            [
                "",
                "## 核心矛盾",
                narrative["contradiction"],
                "",
                "## 八维评分",
            ]
        )
        lines.extend(_table(["维度", "得分", "一句话判断", "详情"], _dimension_summary_rows(analysis)))
        lines.extend(
            [
                "",
                "## 核心驱动",
                f"1. **基本面 / 宏观**: {narrative['drivers']['macro']}",
                f"2. **资金面 / 配置面**: {narrative['drivers']['flow']}",
                f"3. **相对强弱**: {narrative['drivers']['relative']}",
                f"4. **技术结构**: {narrative['drivers']['technical']}",
                "",
                "## 值得继续看的理由",
            ]
        )
        for item in narrative["positives"]:
            lines.append(f"- {item}")
        lines.extend(["", "## 现在不适合激进的理由"])
        for item in narrative["cautions"]:
            lines.append(f"- {item}")
        lines.extend(
            [
                "",
                "## 所处阶段",
                f"当前更像处于 **{narrative['phase']['label']}** 阶段。",
                narrative["phase"]["body"],
                "",
                "## 分维度详解",
            ]
        )
        for key, label in DIMENSION_ORDER:
            dimension = analysis["dimensions"][key]
            score = "—/100" if dimension.get("score") is None else f"{dimension['score']}/{dimension['max_score']}"
            lines.extend(
                [
                    "",
                    f"### {label} {score}",
                ]
            )
            lines.extend(_table(["因子", "当前值/信号", "说明", "得分"], _factor_rows(dimension)))
            lines.append("")
            lines.append(f"**小结：** {dimension['summary']}")
        lines.extend(
            [
                "",
                "## 后续观察重点",
            ]
        )
        for item in narrative["watch_points"]:
            lines.append(f"- {item}")
        validation_points = narrative.get("validation_points", [])
        if validation_points:
            lines.extend(["", "## 后续验证点"])
            lines.extend(
                _table(
                    ["#", "观察什么", "量化标准", "如果成立", "如果不成立"],
                    [
                        [str(index), item["watch"], item["judge"], item["bull"], item["bear"]]
                        for index, item in enumerate(validation_points, start=1)
                    ],
                )
            )
        lines.extend(
            [
                "",
                "## 情景分析",
                f"- **基准情景**: {narrative['scenarios']['base']}",
                f"- **乐观情景**: {narrative['scenarios']['bull']}",
                f"- **风险情景**: {narrative['scenarios']['bear']}",
                "",
                "## 操作建议",
            ]
        )
        lines.extend(
            _table(
                ["项目", "建议"],
                [
                    ["当前动作", f"{action['direction']} / {narrative['judgment']['state']}"],
                    ["适合谁", f"左侧型：{narrative['playbook']['allocation']} / 右侧型：{narrative['playbook']['trend']}"],
                    ["介入条件", action["entry"]],
                    ["仓位", action["position"]],
                    ["止损", action["stop"]],
                    ["目标", action["target"]],
                    ["时间框架", action["timeframe"]],
                ],
            )
        )
        lines.extend(
            [
                "",
                "## 风险提示",
            ]
        )
        lines.extend(
            _table(
                ["风险类型", "说明"],
                [
                    ["基本面风险", narrative["risk_points"]["fundamental"]],
                    ["估值风险", narrative["risk_points"]["valuation"]],
                    ["交易拥挤风险", narrative["risk_points"]["crowding"]],
                    ["外部变量风险", narrative["risk_points"]["external"]],
                ],
            )
        )
        extra_risks = [item for item in analysis["risks"] if item not in warnings]
        if extra_risks:
            lines.append("")
        for item in extra_risks:
            lines.append(f"- {item}")
        lines.extend(
            [
                "",
                "## 分析元数据",
            ]
        )
        lines.extend(
            _table(
                ["项目", "内容"],
                [
                    ["分析时间", analysis["generated_at"]],
                    ["当前 Regime", analysis.get("regime", {}).get("current_regime", "—")],
                    ["数据源", _data_sources(analysis)],
                    ["数据缺失", _missing_data_notes(analysis)],
                    ["估算标注", _estimated_notes(analysis)],
                    ["关联晨报", _linked_briefing(analysis)],
                ],
            )
        )
        if analysis.get("notes"):
            lines.extend(["", "## 备注"])
            for note in analysis["notes"]:
                lines.append(f"- {note}")
        lines.extend(
            [
                "",
                "## 总结",
            ]
        )
        for item in narrative["summary_lines"]:
            lines.append(f"- {item}")
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
