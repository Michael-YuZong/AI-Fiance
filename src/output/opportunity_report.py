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


def _hard_check_inline(analysis: Dict[str, Any]) -> str:
    items = []
    for item in analysis.get("hard_checks", []) or []:
        name = str(item.get("name", "—"))
        status = str(item.get("status", "—"))
        items.append(f"`{name} {status}`")
    return " · ".join(items) if items else "—"


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


def _catalyst_layer(name: str) -> str:
    return {
        "政策催化": "市场/政策",
        "龙头公告/业绩": "个股/产业链",
        "个股公告/事件": "个股直连",
        "负面事件": "个股风险",
        "海外映射": "海外映射",
        "研报/新闻密度": "个股热度",
        "新闻热度": "传播热度",
        "前瞻催化": "事件日历",
    }.get(name, "其他")


def _catalyst_factor_rows(dimension: Dict[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    for factor in dimension.get("factors", []):
        rows.append(
            [
                factor.get("name", "—"),
                _catalyst_layer(str(factor.get("name", ""))),
                factor.get("signal", "—"),
                factor.get("display_score", "—"),
            ]
        )
    return rows


def _watch_positive_reason_rows(analyses: Sequence[Dict[str, Any]]) -> List[List[str]]:
    rows: List[List[str]] = []
    for analysis in analyses:
        dims = analysis.get("dimensions", {})
        tech = dims.get("technical", {}).get("score") or 0
        fund = dims.get("fundamental", {}).get("score") or 0
        catalyst = dims.get("catalyst", {}).get("score") or 0
        relative = dims.get("relative_strength", {}).get("score") or 0
        risk = dims.get("risk", {}).get("score") or 0

        positives: List[str] = []
        if fund >= 60:
            positives.append(f"基本面 `{fund}`")
        if catalyst >= 50:
            positives.append(f"催化 `{catalyst}`")
        if relative >= 70:
            positives.append(f"相对强弱 `{relative}`")
        if risk >= 70:
            positives.append(f"风险收益比 `{risk}`")
        if not positives:
            positives.append("至少有一项维度明显不差")

        blockers: List[str] = []
        if fund < 60:
            blockers.append("基本面未过线")
        if tech < 40:
            blockers.append("技术确认不足")
        if catalyst < 50 and relative < 60:
            blockers.append("催化和相对强弱都不够")
        if not blockers:
            blockers.append("综合共振还差一脚")

        rows.append(
            [
                f"{analysis.get('name', '—')} ({analysis.get('symbol', '—')})",
                " / ".join(positives[:2]),
                " / ".join(blockers[:2]),
            ]
        )
    return rows


def _score_change_rows(analysis: Dict[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    for item in analysis.get("score_changes", []) or []:
        delta = int(item.get("delta", 0))
        delta_text = f"{delta:+d}"
        rows.append(
            [
                str(item.get("label", "—")),
                f"{item.get('previous', '—')} -> {item.get('current', '—')} ({delta_text})",
                str(item.get("reason", "主因是子项重算。")),
            ]
        )
    return rows


def _data_sources(analysis: Dict[str, Any]) -> str:
    sources: List[str] = ["行情/技术: Tushare 优先，AKShare / Yahoo 回退"]
    if dict(analysis.get("intraday") or {}).get("enabled"):
        sources.append("盘中快照: AKShare 分钟线 / 实时行情")
    if analysis["dimensions"]["fundamental"].get("valuation_snapshot"):
        sources.append("估值/基本面: Tushare + 指数估值快照/财务代理")
    else:
        sources.append("估值: 价格位置代理")
    sources.append("催化: RSS + 动态关键词检索")
    sources.append("事件: 本地事件日历")
    if analysis.get("asset_type") in {"cn_fund", "cn_etf"}:
        sources.append("基金画像: Tushare + 天天基金/雪球")
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
    if analysis.get("asset_type") in {"cn_fund", "cn_etf"}:
        for note in (analysis.get("fund_profile") or {}).get("notes", []):
            if "缺失" in str(note):
                items.append(str(note))
    return "；".join(items) if items else "—"


def _estimated_notes(analysis: Dict[str, Any]) -> str:
    notes: List[str] = []
    fundamental = analysis["dimensions"]["fundamental"]
    if not fundamental.get("valuation_snapshot"):
        notes.append("基本面估值未接入可用指数估值时，使用价格位置代理")
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


def _fmt_number(value: Any, digits: int = 2, suffix: str = "") -> str:
    if value is None or value == "":
        return "—"
    try:
        return f"{float(value):.{digits}f}{suffix}"
    except (TypeError, ValueError):
        return str(value)


def _fmt_pct_point(value: Any) -> str:
    if value is None or value == "":
        return "—"
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return str(value)


def _fund_profile_lines(analysis: Dict[str, Any]) -> List[str]:
    if analysis.get("asset_type") not in {"cn_fund", "cn_etf"}:
        return []
    profile = dict(analysis.get("fund_profile") or {})
    if not profile:
        return []
    overview = dict(profile.get("overview") or {})
    manager = dict(profile.get("manager") or {})
    style = dict(profile.get("style") or {})
    rating = dict(profile.get("rating") or {})
    top_holdings = list(profile.get("top_holdings") or [])
    asset_mix = list(profile.get("asset_allocation") or [])
    industries = list(profile.get("industry_allocation") or [])
    achievement = dict(profile.get("achievement") or {})

    lines = [
        "",
        "## 基金画像",
    ]
    lines.extend(
        _table(
            ["项目", "内容"],
            [
                ["基金类型", overview.get("基金类型", "—")],
                ["基金公司", overview.get("基金管理人", "—")],
                ["基金经理", overview.get("基金经理人", "—")],
                ["成立日期", overview.get("成立日期/规模", "—")],
                ["净资产规模", overview.get("净资产规模", "—")],
                ["业绩比较基准", overview.get("业绩比较基准", "—")],
                [
                    "基金评级",
                    " / ".join(
                        part
                        for part in [
                            f"5星家数 {_fmt_number(rating.get('five_star_count'), 0)}" if rating else "",
                            f"晨星 {_fmt_number(rating.get('morningstar'), 0)}" if rating.get("morningstar") is not None else "",
                            f"上海证券 {_fmt_number(rating.get('shanghai'), 0)}" if rating.get("shanghai") is not None else "",
                            f"招商证券 {_fmt_number(rating.get('zhaoshang'), 0)}" if rating.get("zhaoshang") is not None else "",
                            f"济安金信 {_fmt_number(rating.get('jiaan'), 0)}" if rating.get("jiaan") is not None else "",
                        ]
                        if part
                    )
                    or "—",
                ],
            ],
        )
    )

    if achievement:
        perf_rows = []
        for period in ("近1月", "近3月", "近6月", "今年以来", "成立以来"):
            item = achievement.get(period)
            if not item:
                continue
            perf_rows.append(
                [
                    period,
                    _fmt_pct_point(item.get("return_pct")),
                    _fmt_pct_point(item.get("max_drawdown_pct")),
                    item.get("peer_rank", "—"),
                ]
            )
        if perf_rows:
            lines.extend(["", "### 业绩快照"])
            lines.extend(_table(["区间", "收益", "最大回撤", "同类排名"], perf_rows))

    lines.extend(["", "## 基金成分分析"])
    lines.append(style.get("summary", "这只基金本质上是在买基金经理的主动组合框架。"))

    if asset_mix:
        lines.extend(["", "### 资产配置"])
        lines.extend(
            _table(
                ["资产类型", "仓位占比"],
                [[item.get("资产类型", "—"), _fmt_pct_point(item.get("仓位占比"))] for item in asset_mix],
            )
        )

    if top_holdings:
        lines.extend(["", "### 前十大持仓"])
        lines.extend(
            _table(
                ["股票代码", "股票名称", "占净值比例", "持仓市值", "季度"],
                [
                    [
                        item.get("股票代码", "—"),
                        item.get("股票名称", "—"),
                        _fmt_pct_point(item.get("占净值比例")),
                        _fmt_number(item.get("持仓市值")),
                        item.get("季度", "—"),
                    ]
                    for item in top_holdings[:10]
                ],
            )
        )

    if industries:
        lines.extend(["", "### 行业暴露"])
        lines.extend(
            _table(
                ["行业", "占净值比例", "截止时间"],
                [
                    [
                        item.get("行业类别", "—"),
                        _fmt_pct_point(item.get("占净值比例")),
                        item.get("截止时间", "—"),
                    ]
                    for item in industries[:8]
                ],
            )
        )

    lines.extend(
        [
            "",
            "## 基金经理风格分析",
        ]
    )
    lines.extend(
        _table(
            ["维度", "说明"],
            [
                ["风格标签", " / ".join(style.get("tags", [])) or "—"],
                ["仓位画像", style.get("positioning", "—")],
                ["选股方式", style.get("selection", "—")],
                ["风格一致性", style.get("consistency", "—")],
                ["基准映射", style.get("benchmark_note", "—")],
                [
                    "经理画像",
                    (
                        f"{manager.get('name', '—')} | 从业 {_fmt_number(manager.get('tenure_days'), 0)} 天"
                        f" | 在管规模 {_fmt_number(manager.get('aum_billion'))} 亿"
                        f" | 最佳回报 {_fmt_pct_point(manager.get('best_return_pct'))}"
                    )
                    if manager
                    else "—"
                ],
            ],
        )
    )
    if profile.get("notes"):
        lines.extend(["", "### 数据说明"])
        for item in profile["notes"]:
            lines.append(f"- {item}")
    return lines


def _intraday_lines(analysis: Dict[str, Any]) -> List[str]:
    intraday = dict(analysis.get("intraday") or {})
    if not intraday.get("enabled"):
        return []
    lines = [
        "## 今日盘中视角",
        "这部分只回答“今天/现在的执行节奏”，不替代默认的日线评分框架。",
        "",
    ]
    lines.extend(
        _table(
            ["项目", "内容"],
            [
                ["当前价", _fmt_number(intraday.get("current"), 3)],
                ["相对昨收", _fmt_pct_point(float(intraday.get("change_vs_prev_close", 0.0)) * 100)],
                ["相对今开", _fmt_pct_point(float(intraday.get("change_vs_open", 0.0)) * 100)],
                ["日内高低", f"{_fmt_number(intraday.get('low'), 3)} / {_fmt_number(intraday.get('high'), 3)}"],
                ["VWAP", _fmt_number(intraday.get("vwap"), 3)],
                ["日内位置", f"{float(intraday.get('range_position', 0.0)):.0%}"],
                ["盘中状态", intraday.get("trend", "—")],
            ],
        )
    )
    lines.extend(["", f"- {intraday.get('commentary', '当前没有额外盘中结论。')}"])
    if intraday.get("fallback_mode"):
        lines.append("- 分钟线不可用，盘中视角已退化为最近一根日K快照。")
    return lines


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
        intraday_lines = _intraday_lines(analysis)
        if intraday_lines:
            lines.extend(["", *intraday_lines])
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
            ]
        )
        fund_profile_lines = _fund_profile_lines(analysis)
        if fund_profile_lines:
            lines.extend(fund_profile_lines)
        lines.extend(["", "## 值得继续看的理由"])
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

    def render_stock_picks(self, payload: Dict[str, Any]) -> str:
        """Render stock recommendation report."""
        regime = payload.get("regime", {})
        day_theme = payload.get("day_theme", {})
        top = payload.get("top", [])
        market_label = payload.get("market_label", "全市场")
        sector_filter = payload.get("sector_filter", "")

        # Count by market
        cn_count = sum(1 for a in top if a.get("asset_type") == "cn_stock")
        hk_count = sum(1 for a in top if a.get("asset_type") == "hk")
        us_count = sum(1 for a in top if a.get("asset_type") == "us")

        lines = [
            f"# 个股精选 TOP {len(top)} | {payload['generated_at'][:10]}",
            "",
            f"> 范围: {market_label}" + (f" / {sector_filter}" if sector_filter else ""),
            f"> 扫描池: {payload.get('scan_pool', 0)}只 | 过门槛: {payload.get('passed_pool', 0)}只 | Regime: {regime.get('current_regime', 'unknown')} | 主线: {day_theme.get('label', '未识别')}",
        ]

        market_parts = []
        if cn_count:
            market_parts.append(f"A 股 {cn_count} 只")
        if hk_count:
            market_parts.append(f"港股 {hk_count} 只")
        if us_count:
            market_parts.append(f"美股 {us_count} 只")
        if market_parts:
            lines.append(f"> 入选分布: {' / '.join(market_parts)}")
        if payload.get("model_version"):
            lines.append(f"> 模型版本: `{payload['model_version']}`")
        if payload.get("baseline_snapshot_at"):
            lines.append(f"> 当日基准版: `{payload['baseline_snapshot_at']}`")
        if payload.get("is_daily_baseline"):
            lines.append("> 当前输出角色: 当日基准版")
        elif payload.get("comparison_basis_at"):
            basis_label = payload.get("comparison_basis_label", "对比基准")
            lines.append(f"> 当前输出角色: 当日修正版（相对{basis_label}显示差异）")
        if payload.get("comparison_basis_at"):
            basis_label = payload.get("comparison_basis_label", "对比基准")
            lines.append(f"> 分数变动对比基准: {basis_label} `{payload['comparison_basis_at']}`")
        if payload.get("model_version_warning"):
            lines.append(f"> 口径变更提示: {payload['model_version_warning']}")

        lines.append("")

        if payload.get("model_changelog"):
            lines.append("## 本版口径变更")
            for item in payload["model_changelog"]:
                lines.append(f"- {item}")
            lines.append("")

        if not top:
            lines.append("当前没有达到输出阈值的个股。")
            return "\n".join(lines)

        for index, analysis in enumerate(top, start=1):
            asset_type = analysis.get("asset_type", "")
            market_tag = {"cn_stock": "A", "hk": "HK", "us": "US"}.get(asset_type, "")
            lines.extend(
                [
                    f"### {index}. [{market_tag}] {analysis['name']} ({analysis['symbol']})  {analysis['rating']['stars']} {analysis['rating']['label']}",
                    "",
                    "**八维雷达：**",
                ]
            )
            lines.extend(_table(["维度", "得分", "核心信号"], _dimension_rows(analysis)))
            if analysis.get("score_changes"):
                lines.extend(
                    [
                        "",
                        f"**分数变化：** 相比 {analysis.get('comparison_basis_label', payload.get('comparison_basis_label', '对比基准'))} `{analysis.get('comparison_snapshot_at', payload.get('comparison_basis_at', '上次快照'))}`，以下维度变化超过 10 分。",
                    ]
                )
                lines.extend(_table(["维度", "分数变化", "主要原因"], _score_change_rows(analysis)))
            catalyst_dimension = analysis["dimensions"].get("catalyst", {})
            lines.extend(
                [
                    "",
                    f"**催化拆解：** 当前催化分 `{catalyst_dimension.get('score', '缺失')}/{catalyst_dimension.get('max_score', 100)}`。",
                ]
            )
            lines.extend(_table(["催化子项", "层级", "当前信号", "得分"], _catalyst_factor_rows(catalyst_dimension)))
            lines.extend(
                [
                    "",
                    f"**硬排除检查：** {_hard_check_inline(analysis)}",
                ]
            )
            lines.extend(_table(["检查项", "状态", "说明"], _hard_check_rows(analysis)))
            risk_dimension = analysis["dimensions"].get("risk", {})
            lines.extend(
                [
                    "",
                    f"**风险拆解：** 当前风险分 `{risk_dimension.get('score', '缺失')}/{risk_dimension.get('max_score', 100)}`。分数越低，说明波动/窗口/相关性压力越大。",
                ]
            )
            lines.extend(_table(["风险子项", "当前信号", "说明", "得分"], _factor_rows(risk_dimension)))
            lines.extend(
                [
                    "",
                    f"**结论：** {analysis['conclusion']}",
                ]
            )
            for risk in analysis["rating"]["warnings"][:2]:
                lines.append(risk)
            action = analysis["action"]
            lines.extend(
                [
                    "",
                    "**建议操作：**",
                    f"- 介入条件：{action['entry']}",
                    f"- 建议仓位：{action['position']}",
                    f"- 单标的上限：{action.get('max_portfolio_exposure', '—')}",
                    f"- 加仓节奏：{action.get('scaling_plan', '—')}",
                    f"- 建议止损：{action.get('stop_loss_pct', '—')}",
                    f"- 止损参考：{action['stop']}",
                    f"- 目标参考：{action.get('target', '待定')}",
                ]
            )
            if action.get("correlated_warning"):
                lines.append(f"- ⚠️ 相关性：{action['correlated_warning']}")
            lines.extend(["", "---"])

        if payload.get("watch_positive"):
            lines.extend(
                [
                    "",
                    "## 看好但暂不推荐",
                    "这些标的当前没有进入正式推荐，但至少有一块是明显成立的，适合作为下一轮观察池。",
                ]
            )
            lines.extend(_table(["标的", "看好的地方", "暂不推荐原因"], _watch_positive_reason_rows(payload.get("watch_positive", []))))

        if payload.get("blind_spots"):
            lines.extend(["", "## 数据盲区与降级说明"])
            for item in payload["blind_spots"]:
                lines.append(f"- {item}")

        lines.extend(
            [
                "",
                "> 免责声明：以上为量化多因子筛选结果，不构成投资建议。个股风险显著高于 ETF，请结合自身风险承受能力审慎决策。",
            ]
        )
        return "\n".join(lines).rstrip()
