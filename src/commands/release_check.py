"""Release consistency checks for client-facing Markdown reports."""

from __future__ import annotations

import argparse
from collections import Counter
import re
from pathlib import Path
from typing import Dict, List, Tuple

from src.reporting.review_lessons import format_lesson_finding
from src.utils.config import resolve_project_path


BANNED_CLIENT_PHRASES = [
    "项目内初筛",
    "外部复核",
    "这轮修掉了什么",
    "模型版本",
    "评审闭环",
    "当日基准版",
    "本版口径变更",
    "当前输出角色",
]

RAW_EXCEPTION_PATTERNS = (
    "Too Many Requests",
    "Traceback",
    "ProxyError",
    "ConnectionError",
    "RemoteDisconnected",
    "SSLError",
    "ReadTimeout",
    "HTTPError",
)

INTRADAY_CLAIM_TERMS = ("盘中", "首30分钟", "集合竞价", "竞价", "VWAP", "开盘缺口", "相对今开", "相对昨收", "日内位置")
INTRADAY_EVIDENCE_TERMS = (
    "VWAP",
    "相对昨收",
    "相对今开",
    "日内位置",
    "盘中状态",
    "开盘缺口",
    "首30分钟",
    "分钟线",
)
AUCTION_EVIDENCE_TERMS = ("竞价成交", "未匹配量", "封单", "开盘缺口", "竞价量能")

GENERIC_OPERATION_PREFIXES = (
    "介入条件：",
    "首次仓位：",
    "加仓节奏：",
    "止损参考：",
    "建议仓位：",
    "单标的上限：",
    "建议止损：",
    "目标参考：",
    "当前动作：",
    "单票上限",
    "执行原则",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run pre-release consistency checks for client Markdown reports.")
    parser.add_argument("report_type", choices=["stock_pick", "stock_analysis", "briefing", "fund_pick", "etf_pick", "scan", "retrospect"], help="Report type to validate")
    parser.add_argument("--client", required=True, help="Path to client-facing Markdown")
    parser.add_argument("--source", default="", help="Path to source/detail Markdown")
    return parser


def _read(path: str) -> str:
    return Path(resolve_project_path(path)).read_text(encoding="utf-8")


def _parse_markdown_table(lines: List[str], start_index: int) -> Tuple[List[str], List[List[str]]]:
    table_lines: List[str] = []
    started = False
    for line in lines[start_index:]:
        stripped = line.strip()
        if not stripped and not started:
            continue
        if not stripped.startswith("|"):
            break
        started = True
        table_lines.append(line.rstrip("\n"))
    if len(table_lines) < 2:
        return [], []
    header = [cell.strip() for cell in table_lines[0].strip("|").split("|")]
    rows: List[List[str]] = []
    for line in table_lines[2:]:
        rows.append([cell.strip() for cell in line.strip("|").split("|")])
    return header, rows


def _bullets_in_section(text: str, heading: str) -> List[str]:
    lines = text.splitlines()
    collecting = False
    bullets: List[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped == heading:
            collecting = True
            continue
        if collecting and stripped.startswith("## "):
            break
        if collecting and stripped.startswith("- "):
            bullets.append(stripped[2:].strip())
    return bullets


def _section_exists(text: str, heading: str) -> bool:
    return any(line.strip() == heading for line in text.splitlines())


def _pick_reason_heading(report_type: str, text: str) -> str:
    options = {
        "fund_pick": ("## 为什么先看它", "## 为什么推荐它"),
        "etf_pick": ("## 为什么先看它", "## 为什么推荐它"),
    }.get(report_type, ())
    for heading in options:
        if _section_exists(text, heading):
            return heading
    return options[-1] if options else ""


def _section_items(text: str, heading: str) -> List[str]:
    lines = text.splitlines()
    collecting = False
    items: List[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped == heading:
            collecting = True
            continue
        if collecting and (stripped.startswith("# ") or stripped.startswith("## ")):
            break
        if not collecting or not stripped:
            continue
        if stripped.startswith("|"):
            break
        if stripped.startswith("- "):
            items.append(stripped[2:].strip())
            continue
        if stripped.startswith(">"):
            items.append(stripped[1:].strip())
            continue
        items.append(stripped)
    return items


def _explanation_bullets(text: str) -> List[str]:
    bullets: List[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("- "):
            continue
        body = stripped[2:].strip()
        if not body or body.startswith(GENERIC_OPERATION_PREFIXES):
            continue
        bullets.append(body)
    return bullets


def _duplicate_explanation_findings(text: str, *, max_repeat: int = 2) -> List[str]:
    findings: List[str] = []
    duplicates = Counter(_explanation_bullets(text))
    for item, count in duplicates.items():
        if count > max_repeat:
            findings.append(format_lesson_finding("L003", f"[P1] 解释文案重复过多（{count} 次），像模板而不像成稿: {item}"))
    return findings


def _fund_profile_findings(text: str) -> List[str]:
    findings: List[str] = []
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if line.strip() != "## 基金画像":
            continue
        header, rows = _parse_markdown_table(lines, idx + 1)
        if header[:2] != ["项目", "内容"]:
            findings.append(format_lesson_finding("L005", "[P1] 基金画像章节存在，但未找到标准画像表"))
            return findings
        payload = {row[0]: row[1] for row in rows if len(row) >= 2}
        required = ("基金类型", "基金公司", "基金经理", "成立日期", "业绩比较基准")
        for key in required:
            value = str(payload.get(key, "")).strip()
            if value in ("", "—", "nan", "None"):
                findings.append(format_lesson_finding("L005", f"[P1] 基金画像基础字段缺失: {key}"))
        return findings
    return findings


def _standard_taxonomy_findings(text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if line.strip() != "## 标准化分类":
            continue
        table_start = None
        for probe in range(idx + 1, len(lines)):
            stripped = lines[probe].strip()
            if stripped.startswith("|"):
                table_start = probe
                break
            if stripped.startswith("## "):
                break
        if table_start is None:
            findings.append(format_lesson_finding("L005", f"[P1] {report_type} 缺少标准化分类表"))
            return findings
        header, rows = _parse_markdown_table(lines, table_start)
        if header[:2] != ["维度", "结果"]:
            findings.append(format_lesson_finding("L005", f"[P1] {report_type} 标准化分类章节存在，但未找到标准分类表"))
            return findings
        payload = {row[0]: row[1] for row in rows if len(row) >= 2}
        required = ("产品形态", "载体角色", "管理方式", "暴露类型", "主方向")
        for key in required:
            value = str(payload.get(key, "")).strip()
            if value in ("", "—", "nan", "None"):
                findings.append(format_lesson_finding("L005", f"[P1] {report_type} 标准化分类缺失关键字段: {key}"))
        return findings
    return findings


def _extract_delivery_tier_label(text: str) -> str:
    if not text:
        return ""
    candidates = [*_section_items(text, "## 交付等级"), *text.splitlines()]
    for item in candidates:
        match = re.search(r"交付等级\s*[：:]\s*`?([^`\n]+?)`?(?:。|$)", str(item).strip())
        if match:
            return match.group(1).strip()
    return ""


def _extract_pick_passed_pool(text: str) -> int | None:
    patterns = (
        r"完整分析:\s*`?(\d+)`?",
        r"再对其中\s*`?(\d+)`?\s*只做完整分析",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return int(match.group(1))
    return None


def _extract_pick_coverage_total(text: str) -> int | None:
    match = re.search(r"覆盖率的分母是今天进入完整分析的\s*`?(\d+)`?\s*只", text)
    if match:
        return int(match.group(1))
    return None


def _delivery_tier_findings(client_text: str, source_text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    items = _section_items(client_text, "## 交付等级")
    if len(items) < 2:
        findings.append(format_lesson_finding("L002", f"[P2] {report_type} 客户稿解释性不足：'交付等级' 至少需要等级和适用口径两条说明"))
    if not any("初筛" in item and "完整分析" in item for item in items):
        findings.append(format_lesson_finding("L002", f"[P2] {report_type} 交付等级没有说明“初筛 -> 少量完整分析”的真实流程"))

    client_label = _extract_delivery_tier_label(client_text)
    source_label = _extract_delivery_tier_label(source_text)
    if source_label and client_label and client_label != source_label:
        findings.append(format_lesson_finding("L005", f"[P1] {report_type} 客户稿与详细稿交付等级不一致: client={client_label} source={source_label}"))

    effective_label = source_label or client_label
    if effective_label and effective_label != "标准推荐稿":
        observe_markers = (
            "观察优先",
            "不按正式推荐稿理解",
            "不是正式买入稿",
            "不代表完整全市场优选结论",
            "只适合当作兜底观察名单",
            "只适合按观察优先处理",
        )
        if not any(marker in client_text for marker in observe_markers):
            findings.append(format_lesson_finding("L002", f"[P1] {report_type} 当前是 `{effective_label}`，但客户稿没有明确按观察优先/非正式推荐处理"))
        first_heading = next((line.strip() for line in client_text.splitlines() if line.strip().startswith("# ")), "")
        if "推荐" in first_heading:
            findings.append(format_lesson_finding("L018", f"[P2] {report_type} 当前是 `{effective_label}`，标题仍写成“推荐”，容易高估这份稿件的可执行性"))
        if "## 为什么推荐它" in client_text:
            findings.append(format_lesson_finding("L018", f"[P2] {report_type} 当前是 `{effective_label}`，观察稿章节仍写成“为什么推荐它”，建议改成“为什么先看它”"))
    return findings


def _pick_delivery_consistency_findings(client_text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    passed_pool = _extract_pick_passed_pool(client_text)
    coverage_total = _extract_pick_coverage_total(client_text)
    if passed_pool is not None and coverage_total is not None and passed_pool != coverage_total:
        findings.append(
            format_lesson_finding(
                "L031",
                f"[P1] {report_type} 覆盖率分母与完整分析样本不一致: coverage_total={coverage_total} passed_pool={passed_pool}",
            )
        )

    delivery_label = _extract_delivery_tier_label(client_text)
    if delivery_label == "标准推荐稿":
        conflict_markers = (
            "只能按观察优先或降级稿处理",
            "只能按观察优先处理",
            "不按正式推荐稿理解",
        )
        alternative_items = _section_items(client_text, "## 为什么不是另外几只")
        if any(marker in item for marker in conflict_markers for item in alternative_items):
            findings.append(
                format_lesson_finding(
                    "L032",
                    f"[P1] {report_type} 当前仍是 `标准推荐稿`，但单候选说明把它改写成了观察/降级稿口径",
                )
            )
    return findings


def _client_stock_table(text: str, market_heading: str = "## A股") -> Dict[str, Dict[str, str]]:
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if line.strip() == market_heading:
            table_start = None
            for probe in range(idx + 1, len(lines)):
                if lines[probe].strip().startswith("|"):
                    table_start = probe
                    break
            if table_start is None:
                return {}
            header, rows = _parse_markdown_table(lines, table_start)
            if header[:7] != ["标的", "技术", "基本面", "催化", "相对强弱", "风险", "结论"]:
                return {}
            payload: Dict[str, Dict[str, str]] = {}
            for row in rows:
                if len(row) < 7:
                    continue
                payload[row[0]] = {
                    "technical": row[1],
                    "fundamental": row[2],
                    "catalyst": row[3],
                    "relative_strength": row[4],
                    "risk": row[5],
                    "conclusion": row[6],
                }
            return payload
    return {}


def _analysis_client_dimension_map(text: str) -> Dict[str, str]:
    return _pick_client_dimension_map(text, "## 为什么这么判断")


def _pick_client_dimension_map(text: str, heading: str) -> Dict[str, str]:
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if line.strip() != heading:
            continue
        table_start = None
        for probe in range(idx + 1, len(lines)):
            if lines[probe].strip().startswith("|"):
                table_start = probe
                break
            if lines[probe].strip().startswith("## "):
                break
        if table_start is None:
            return {}
        header, rows = _parse_markdown_table(lines, table_start)
        if header[:3] != ["维度", "分数", "为什么是这个分"]:
            return {}
        payload: Dict[str, str] = {}
        for row in rows:
            if len(row) < 2:
                continue
            payload[str(row[0]).strip()] = str(row[1]).strip()
        return payload
    return {}


def _analysis_source_dimension_map(text: str) -> Dict[str, str]:
    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if line.strip() != "## 八维评分":
            continue
        table_start = None
        for probe in range(idx + 1, len(lines)):
            if lines[probe].strip().startswith("|"):
                table_start = probe
                break
            if lines[probe].strip().startswith("## "):
                break
        if table_start is None:
            return {}
        header, rows = _parse_markdown_table(lines, table_start)
        if header[:4] != ["维度", "得分", "一句话判断", "详情"]:
            return {}
        payload: Dict[str, str] = {}
        for row in rows:
            if len(row) < 2:
                continue
            payload[str(row[0]).strip()] = str(row[1]).strip()
        return payload
    return {}


def _analysis_source_consistency_findings(client_text: str, source_text: str, report_type: str) -> List[str]:
    findings: List[str] = []
    client_map = _analysis_client_dimension_map(client_text)
    if not client_map:
        findings.append(f"[P1] {report_type} 客户稿未解析出“为什么这么判断”维度表，无法做源稿一致性校验")
        return findings
    source_map = _analysis_source_dimension_map(source_text)
    if not source_map:
        findings.append(f"[P1] {report_type} 详细稿未解析出“八维评分”维度表，无法做源稿一致性校验")
        return findings
    for label, client_score in client_map.items():
        source_score = source_map.get(label)
        if source_score is None:
            findings.append(f"[P1] {report_type} 客户稿维度在详细稿里不存在: {label}")
            continue
        if _normalized_score_token(client_score) != _normalized_score_token(source_score):
            findings.append(
                f"[P1] {report_type} 客户稿与详细稿分数不一致: {label} client={client_score} source={source_score}"
            )
    return findings


def _normalized_score_token(value: str) -> str:
    text = str(value).strip()
    if text in {"", "—", "—/100", "缺失", "信息项", "不适用"}:
        return "MISSING"
    return text


def _pick_source_consistency_findings(client_text: str, source_text: str, report_type: str, heading: str) -> List[str]:
    findings: List[str] = []
    client_map = _pick_client_dimension_map(client_text, heading)
    if not client_map:
        findings.append(f"[P1] {report_type} 客户稿未解析出维度评分表，无法做源稿一致性校验")
        return findings
    source_map = _analysis_source_dimension_map(source_text)
    if not source_map:
        findings.append(f"[P1] {report_type} 详细稿未解析出“八维评分”维度表，无法做源稿一致性校验")
        return findings
    for label, client_score in client_map.items():
        source_score = source_map.get(label)
        if source_score is None:
            findings.append(f"[P1] {report_type} 客户稿维度在详细稿里不存在: {label}")
            continue
        if _normalized_score_token(client_score) != _normalized_score_token(source_score):
            findings.append(
                f"[P1] {report_type} 客户稿与详细稿分数不一致: {label} client={client_score} source={source_score}"
            )
    return findings


def _briefing_source_consistency_findings(client_text: str, source_text: str) -> List[str]:
    findings: List[str] = []
    client_why = _bullets_in_section(client_text, "## 为什么今天这么判断")
    client_actions = _bullets_in_section(client_text, "## 今天怎么做")
    source_headlines = _section_items(source_text, "### 1.1 今日主线")
    source_actions = _section_items(source_text, "### 1.2 今天怎么做")
    if not source_headlines:
        findings.append("[P1] briefing 详细稿缺少“1.1 今日主线”内容，无法做源稿一致性校验")
        return findings
    if not source_actions:
        findings.append("[P1] briefing 详细稿缺少“1.2 今天怎么做”内容，无法做源稿一致性校验")
        return findings
    normalized_headlines = {_normalize_briefing_consistency_line(item) for item in source_headlines}
    normalized_actions = {_normalize_briefing_consistency_line(item) for item in source_actions}
    for item in client_why:
        if _normalize_briefing_consistency_line(item) not in normalized_headlines:
            findings.append(f"[P1] briefing 客户稿理由在详细稿主线章节中不存在: {item}")
    for item in client_actions:
        if _normalize_briefing_consistency_line(item) not in normalized_actions:
            findings.append(f"[P1] briefing 客户稿动作在详细稿行动章节中不存在: {item}")
    return findings


def _normalize_briefing_consistency_line(text: str) -> str:
    line = str(text).strip()
    replacements = (
        (r"开盘\s*30\s*分钟", "早段"),
        (r"开盘后先观察\s*\d+\s*分钟", "先观察早段延续性"),
        (r"明天开盘前", "明早"),
        (r"盘中", "交易时段"),
        (r"日内", "当天"),
    )
    for pattern, repl in replacements:
        line = re.sub(pattern, repl, line)
    return line


def _normalize_markdown(text: str) -> str:
    return "\n".join(line.rstrip() for line in text.splitlines()).strip()


def _retrospect_source_consistency_findings(client_text: str, source_text: str) -> List[str]:
    if _normalize_markdown(client_text) == _normalize_markdown(source_text):
        return []
    return ["[P1] retrospect 客户稿与内部详细稿不一致：当前流程要求复盘成稿与内部详细稿保持同稿发布"]


def _source_stock_dimensions(text: str) -> Dict[str, Dict[str, str]]:
    pattern = re.compile(
        r"^###\s+\d+\.\s+\[A\]\s+(?P<name>.+?)\s+\((?P<symbol>\d{6})\)\s+(?P<label>.+?)\n(?P<body>.*?)(?=^---\n|^###\s+\d+\.|\Z)",
        re.M | re.S,
    )
    payload: Dict[str, Dict[str, str]] = {}
    for match in pattern.finditer(text):
        body = match.group("body")
        dim_match = re.search(
            r"\| 技术面 \| (?P<technical>\d+)/100 \|.*?\n"
            r"\| 基本面 \| (?P<fundamental>\d+)/100 \|.*?\n"
            r"\| 催化面 \| (?P<catalyst>\d+)/100 \|.*?\n"
            r"\| 相对强弱 \| (?P<relative_strength>\d+)/100 \|.*?\n"
            r"\| 筹码结构 \| .*?\n"
            r"\| 风险特征 \| (?P<risk>\d+)/100 \|",
            body,
            re.S,
        )
        if not dim_match:
            continue
        payload[match.group("name")] = {
            "technical": dim_match.group("technical"),
            "fundamental": dim_match.group("fundamental"),
            "catalyst": dim_match.group("catalyst"),
            "relative_strength": dim_match.group("relative_strength"),
            "risk": dim_match.group("risk"),
            "label": match.group("label").strip(),
        }
    return payload


def check_stock_pick_client_report(client_text: str, source_text: str) -> List[str]:
    findings: List[str] = []

    for phrase in BANNED_CLIENT_PHRASES:
        if phrase in client_text:
            findings.append(format_lesson_finding("L001", f"[P1] 客户稿出现内部过程词: {phrase}"))

    if client_text.count("为什么") < 3:
        findings.append(format_lesson_finding("L002", "[P2] 客户稿解释性不足：'为什么' 类型说明明显不够"))
    if "数据完整度" not in client_text:
        findings.append(format_lesson_finding("L013", "[P1] 个股成稿缺少数据完整度/覆盖率说明"))
    if "当前置信度" in client_text:
        findings.append(format_lesson_finding("L023", "[P1] 个股成稿仍把样本置信度写成“当前置信度”，容易被误读成总推荐置信度"))
    if "估值偏高或财务安全边际不足" in client_text:
        findings.append(format_lesson_finding("L025", "[P2] 个股成稿仍使用“估值偏高或财务安全边际不足”模板句，未拆开真实原因"))
    if "结构化事件覆盖" in client_text and "分母" not in client_text:
        findings.append(format_lesson_finding("L024", "[P2] 个股成稿披露了覆盖率，但没有说明分母定义"))
    for line in client_text.splitlines():
        if "北向增持估计" in line and all(token not in line for token in ("行业", "板块", "代理")):
            findings.append(format_lesson_finding("L012", "[P1] 个股成稿把板块/行业北向代理写成了像个股专属信号"))
            break
    if "催化证据来源" not in client_text:
        findings.append(format_lesson_finding("L014", "[P1] 个股成稿缺少可直接复核的催化证据来源"))
    if "历史相似样本" not in client_text:
        findings.append(format_lesson_finding("L017", "[P1] 个股成稿缺少历史相似样本/置信度章节"))
    else:
        if "非重叠样本" not in client_text:
            findings.append(format_lesson_finding("L030", "[P1] 个股成稿引用了历史相似样本，但没有说明严格去重后的非重叠样本数"))
        if "95%区间" not in client_text:
            findings.append(format_lesson_finding("L030", "[P2] 个股成稿引用了历史相似样本，但没有展示胜率置信区间"))
        if "样本质量" not in client_text:
            findings.append(format_lesson_finding("L030", "[P2] 个股成稿引用了历史相似样本，但没有展示样本质量判断"))
    findings.extend(_duplicate_explanation_findings(client_text, max_repeat=2))
    if len(_explanation_bullets(client_text)) < 8:
        findings.append(format_lesson_finding("L002", "[P2] 客户稿解释性不足：实质性解释条目太少"))
    findings.extend(_intraday_claim_findings(client_text))

    source_map = _source_stock_dimensions(source_text)
    if not source_map:
        findings.append("[P1] 详细稿未解析出 A股 八维表，无法做发布前一致性校验")
        return findings

    client_table = _client_stock_table(client_text, "## A股")
    if client_table:
        for name, client_row in client_table.items():
            if name not in source_map:
                findings.append(f"[P1] 客户稿中的标的在详细稿里不存在: {name}")
                continue
            source_row = source_map[name]
            for key, label in (
                ("technical", "技术"),
                ("fundamental", "基本面"),
                ("catalyst", "催化"),
                ("relative_strength", "相对强弱"),
                ("risk", "风险"),
            ):
                if str(client_row[key]) != str(source_row[key]):
                    findings.append(
                        f"[P1] 客户稿与详细稿分数不一致: {name} {label} client={client_row[key]} source={source_row[key]}"
                    )
        return findings

    client_detail_map = _source_stock_dimensions(client_text)
    if not client_detail_map:
        findings.append("[P1] 客户稿既没有 A股 汇总表，也没有可解析的详细八维表，无法做发布前一致性校验")
        return findings

    for name, client_row in client_detail_map.items():
        if name not in source_map:
            findings.append(f"[P1] 客户详细稿中的标的在内部详细稿里不存在: {name}")
            continue
        source_row = source_map[name]
        for key, label in (
            ("technical", "技术"),
            ("fundamental", "基本面"),
            ("catalyst", "催化"),
            ("relative_strength", "相对强弱"),
            ("risk", "风险"),
        ):
            if str(client_row[key]) != str(source_row[key]):
                findings.append(
                    f"[P1] 客户详细稿与内部详细稿分数不一致: {name} {label} client={client_row[key]} source={source_row[key]}"
                )
    return findings


def check_generic_client_report(client_text: str, report_type: str, source_text: str = "") -> List[str]:
    findings: List[str] = []
    for phrase in BANNED_CLIENT_PHRASES:
        if phrase in client_text:
            findings.append(format_lesson_finding("L001", f"[P1] 客户稿出现内部过程词: {phrase}"))
    for token in RAW_EXCEPTION_PATTERNS:
        if token in client_text:
            findings.append(format_lesson_finding("L029", f"[P1] 客户稿暴露了原始异常/系统报错信息: {token}"))

    minimum_why = {
        "briefing": 1,
        "fund_pick": 2,
        "etf_pick": 2,
        "scan": 1,
        "stock_analysis": 1,
        "retrospect": 1,
    }.get(report_type, 1)
    if client_text.count("为什么") < minimum_why:
        findings.append(format_lesson_finding("L002", f"[P2] {report_type} 客户稿解释性不足：缺少足够的“为什么”说明"))
    findings.extend(_duplicate_explanation_findings(client_text, max_repeat=2))
    findings.extend(_intraday_claim_findings(client_text))

    required_headings = {
        "briefing": ["## 为什么今天这么判断", "## 宏观领先指标", "## 数据完整度", "## 今天怎么做", "## 重点观察", "## 今日A股观察池"],
        "fund_pick": ["## 数据完整度", "## 交付等级", ("## 为什么推荐它", "## 为什么先看它"), "## 这只基金为什么是这个分", "## 标准化分类"],
        "etf_pick": ["## 数据完整度", "## 交付等级", ("## 为什么推荐它", "## 为什么先看它"), "## 这只ETF为什么是这个分", "## 标准化分类", "## 关键证据"],
        "scan": ["## 为什么这么判断", "## 当前更合适的动作"],
        "stock_analysis": ["## 为什么这么判断", "## 当前更合适的动作"],
        "retrospect": ["## 原始决策", "## 为什么当时会做这个决定", "## 后验路径", "## 复盘结论"],
    }.get(report_type, [])
    for heading in required_headings:
        if isinstance(heading, tuple):
            if not any(option in client_text for option in heading):
                findings.append(format_lesson_finding("L002", f"[P2] {report_type} 客户稿缺少解释性章节: {' / '.join(heading)}"))
            continue
        if heading not in client_text:
            findings.append(format_lesson_finding("L002", f"[P2] {report_type} 客户稿缺少解释性章节: {heading}"))

    if report_type == "briefing":
        if len(_bullets_in_section(client_text, "## 为什么今天这么判断")) < 3:
            findings.append(format_lesson_finding("L002", "[P2] briefing 客户稿解释性不足：'为什么今天这么判断' 至少需要 3 条理由"))
        if len(_bullets_in_section(client_text, "## 数据完整度")) < 2:
            findings.append(format_lesson_finding("L013", "[P2] briefing 客户稿缺少“数据完整度”说明：至少要交代覆盖和缺失/代理口径。"))
        if len(_bullets_in_section(client_text, "## 重点观察")) < 2:
            findings.append(format_lesson_finding("L002", "[P2] briefing 客户稿解释性不足：'重点观察' 至少需要 2 条可执行观察点"))
        a_share_items = _section_items(client_text, "## 今日A股观察池")
        if len(a_share_items) < 2:
            findings.append(format_lesson_finding("L013", "[P2] briefing 客户稿缺少“A股全市场观察池”说明：至少要交代全市场/初筛池与完整分析口径。"))
        elif not any(token in " ".join(a_share_items) for token in ("全市场", "初筛池", "完整分析", "Tushare")):
            findings.append(format_lesson_finding("L013", "[P2] briefing A股观察池章节没有讲清全市场初筛口径。"))
        if "## 宏观领先指标" not in client_text:
            findings.append(format_lesson_finding("L027", "[P2] briefing 客户稿缺少“宏观领先指标”章节，未来 3-6 个月判断不够透明"))
        elif len(_bullets_in_section(client_text, "## 宏观领先指标")) < 3:
            findings.append(format_lesson_finding("L027", "[P2] briefing 宏观领先指标解释不足：至少要讲清景气、价格链条和信用脉冲中的 3 条。"))
        completeness_items = _section_items(client_text, "## 数据完整度")
        if completeness_items and not any(token in " ".join(completeness_items) for token in ("覆盖", "缺失", "代理")):
            findings.append(format_lesson_finding("L013", "[P2] briefing 数据完整度章节没有讲清覆盖、缺失或代理口径。"))
    elif report_type == "fund_pick":
        why_heading = _pick_reason_heading(report_type, client_text)
        if len(_bullets_in_section(client_text, why_heading)) < 3:
            findings.append(format_lesson_finding("L002", f"[P2] fund_pick 客户稿解释性不足：'{why_heading.replace('## ', '')}' 至少需要 3 条理由"))
        if len(_section_items(client_text, "## 为什么不是另外几只")) < 2:
            findings.append(format_lesson_finding("L002", "[P2] fund_pick 客户稿解释性不足：'为什么不是另外几只' 需要至少给出备选原因或候选不足说明"))
        if "覆盖率" in client_text and "分母" not in client_text:
            findings.append(format_lesson_finding("L024", "[P2] fund_pick 披露了覆盖率，但没有说明分母定义"))
        findings.extend(_delivery_tier_findings(client_text, source_text, report_type))
        findings.extend(_pick_delivery_consistency_findings(client_text, report_type))
        findings.extend(_standard_taxonomy_findings(client_text, report_type))
    elif report_type == "etf_pick":
        why_heading = _pick_reason_heading(report_type, client_text)
        if len(_bullets_in_section(client_text, why_heading)) < 3:
            findings.append(format_lesson_finding("L002", f"[P2] etf_pick 客户稿解释性不足：'{why_heading.replace('## ', '')}' 至少需要 3 条理由"))
        if len(_section_items(client_text, "## 为什么不是另外几只")) < 2:
            findings.append(format_lesson_finding("L002", "[P2] etf_pick 客户稿解释性不足：'为什么不是另外几只' 需要至少给出备选原因或候选不足说明"))
        if "覆盖率" in client_text and "分母" not in client_text:
            findings.append(format_lesson_finding("L024", "[P2] etf_pick 披露了覆盖率，但没有说明分母定义"))
        findings.extend(_delivery_tier_findings(client_text, source_text, report_type))
        findings.extend(_pick_delivery_consistency_findings(client_text, report_type))
        findings.extend(_standard_taxonomy_findings(client_text, report_type))
        findings.extend(_fund_profile_findings(client_text))
    elif report_type == "scan":
        if len(_bullets_in_section(client_text, "## 值得继续看的地方")) < 1:
            findings.append(format_lesson_finding("L002", "[P2] scan 客户稿缺少正向理由：'值得继续看的地方' 至少要有 1 条"))
        if len(_bullets_in_section(client_text, "## 现在不适合激进的地方")) < 2:
            findings.append(format_lesson_finding("L002", "[P2] scan 客户稿缺少反向理由：'现在不适合激进的地方' 至少要有 2 条"))
        findings.extend(_fund_profile_findings(client_text))
    elif report_type == "stock_analysis":
        if len(_bullets_in_section(client_text, "## 值得继续看的地方")) < 1:
            findings.append(format_lesson_finding("L002", "[P2] stock_analysis 客户稿缺少正向理由：'值得继续看的地方' 至少要有 1 条"))
        if len(_bullets_in_section(client_text, "## 现在不适合激进的地方")) < 2:
            findings.append(format_lesson_finding("L002", "[P2] stock_analysis 客户稿缺少反向理由：'现在不适合激进的地方' 至少要有 2 条"))
        if "## 历史相似样本验证" in client_text:
            if "非重叠样本" not in client_text:
                findings.append(format_lesson_finding("L030", "[P1] stock_analysis 引用了历史相似样本，但没有说明非重叠样本数"))
            if "95%区间" not in client_text:
                findings.append(format_lesson_finding("L030", "[P2] stock_analysis 引用了历史相似样本，但没有展示胜率置信区间"))
            if "样本质量" not in client_text:
                findings.append(format_lesson_finding("L030", "[P2] stock_analysis 引用了历史相似样本，但没有展示样本质量"))
    elif report_type == "retrospect":
        if client_text.count("### ") < 1:
            findings.append(format_lesson_finding("L002", "[P2] retrospect 客户稿至少要展开 1 笔具体决策。"))
        if len(_explanation_bullets(client_text)) < 6:
            findings.append(format_lesson_finding("L002", "[P2] retrospect 客户稿解释性不足：复盘理由和结论太少。"))

    if any(token in client_text for token in ("3-6个月", "未来3-6个月", "未来 3-6 个月", "中期判断")):
        macro_tokens = ("PMI", "PPI", "CPI", "社融", "M1-M2", "剪刀差")
        hits = sum(1 for token in macro_tokens if token in client_text)
        if hits < 3:
            findings.append(format_lesson_finding("L027", "[P2] 报告使用了中期宏观判断语气，但没有把 PMI/PPI/CPI/信用脉冲 的角色讲清楚。"))
    if source_text:
        if report_type in {"scan", "stock_analysis"}:
            findings.extend(_analysis_source_consistency_findings(client_text, source_text, report_type))
        elif report_type == "fund_pick":
            findings.extend(_pick_source_consistency_findings(client_text, source_text, report_type, "## 这只基金为什么是这个分"))
        elif report_type == "etf_pick":
            findings.extend(_pick_source_consistency_findings(client_text, source_text, report_type, "## 这只ETF为什么是这个分"))
        elif report_type == "briefing":
            findings.extend(_briefing_source_consistency_findings(client_text, source_text))
        elif report_type == "retrospect":
            findings.extend(_retrospect_source_consistency_findings(client_text, source_text))
    return findings


def _intraday_claim_findings(text: str) -> List[str]:
    findings: List[str] = []
    normalized = re.sub(r"^\|\s*盘中快照 as_of\s*\|.*$", "", text, flags=re.M)
    risky_opening_patterns = (
        r"开盘.{0,8}(做|买|追|加仓|执行|跟随|介入)",
        r"明天开盘.{0,8}(做|买|追|加仓|执行|跟随|介入)",
    )
    has_intraday_claim = any(term in normalized for term in INTRADAY_CLAIM_TERMS) or any(
        re.search(pattern, normalized) for pattern in risky_opening_patterns
    )
    if has_intraday_claim and not any(term in normalized for term in INTRADAY_EVIDENCE_TERMS):
        findings.append(format_lesson_finding("L004", "[P1] 报告使用盘中/开盘执行语言，但没有展示对应盘中因子或数据依据（如 VWAP、相对今开、日内位置、开盘缺口、首30分钟）。"))
    if "集合竞价" in normalized and not any(term in normalized for term in AUCTION_EVIDENCE_TERMS):
        findings.append(format_lesson_finding("L004", "[P2] 报告提到集合竞价，但没有对应竞价因子依据；当前不应把普通日线结论写成竞价判断。"))
    return findings


def main() -> None:
    args = build_parser().parse_args()
    client_text = _read(args.client)
    source_text = _read(args.source) if args.source else ""
    if args.report_type == "stock_pick":
        if not args.source:
            raise SystemExit("stock_pick 一致性校验必须提供 --source")
        findings = check_stock_pick_client_report(client_text, source_text)
    else:
        findings = check_generic_client_report(client_text, args.report_type, source_text=source_text)
    if findings:
        print("发布前一致性校验未通过：")
        for item in findings:
            print(f"- {item}")
        raise SystemExit(1)
    print("发布前一致性校验通过。")


if __name__ == "__main__":
    main()
