"""Natural-language routing for command selection."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Sequence


RULE_ALIASES = {
    "macd_golden_cross": ["macd", "金叉", "macd金叉"],
    "oversold_rebound": ["超卖", "反弹", "oversold"],
}

SCENARIO_ALIASES = {
    "美股崩盘": ["美股跌20", "标普跌20", "纳指大跌", "崩盘", "美股崩盘"],
    "人民币急贬": ["人民币贬值", "汇率压力", "人民币急贬", "汇率波动"],
    "原油飙升": ["原油飙升", "油价大涨", "布伦特", "油价冲击"],
}

SYMBOL_STOPWORDS = {"VS"}


@dataclass
class RoutedCommand:
    """Routing result for a natural-language request."""

    module: str
    args: List[str]
    reason: str

    @property
    def display(self) -> str:
        return "python -m src.commands." + self.module + (" " + " ".join(self.args) if self.args else "")


def route_request(
    request: str,
    candidate_symbols: Sequence[str] = (),
    resolved_symbols: Sequence[str] = (),
) -> RoutedCommand:
    text = request.strip()
    lowered = text.lower()
    symbols = _merge_symbols(_extract_symbols(text, candidate_symbols), resolved_symbols)

    if any(keyword in text for keyword in ["代码", "编号", "哪只ETF", "对应ETF", "对应代码", "是什么ETF"]) and symbols:
        return RoutedCommand("lookup", [text], "识别为标的代码/编号查询。")

    if any(keyword in text for keyword in ["晨报", "早报", "今天的财经", "今日财经", "日报"]):
        return RoutedCommand("briefing", ["daily"], "识别为晨报/日度简报需求。")
    if any(keyword in text for keyword in ["周报", "周度简报"]):
        return RoutedCommand("briefing", ["weekly"], "识别为周报需求。")
    if any(keyword in text for keyword in ["机会", "发现", "关注什么", "值得看什么", "主动发现", "今日扫描", "扫一下", "有什么值得看的"]):
        event = _extract_event_keyword(text)
        return RoutedCommand("discover", [event] if event else [], "识别为机会发现或关注方向筛选。")
    if any(keyword in text for keyword in ["体制", "regime", "宏观环境"]) and not symbols:
        return RoutedCommand("regime", [], "识别为宏观体制判断。")
    if any(keyword in text for keyword in ["政策", "解读"]) and text:
        target = _extract_policy_target(text)
        return RoutedCommand("policy", [target], "识别为政策解读需求。")
    if any(keyword in text for keyword in ["相关性", "相关矩阵"]):
        return RoutedCommand("risk", ["correlation"], "识别为组合相关性分析。")
    scenario = _detect_scenario(text)
    if scenario and any(keyword in text for keyword in ["压力", "风险", "组合", "会怎样", "跌20", "贬值", "飙升"]):
        return RoutedCommand("risk", ["stress", scenario], "识别为压力测试场景。")
    if any(keyword in text for keyword in ["风险报告", "组合风险", "回撤风险", "var", "cvar"]):
        return RoutedCommand("risk", ["report"], "识别为组合风险报告。")
    if any(keyword in lowered for keyword in ["持仓", "组合状态", "仓位"]) and not scenario:
        return RoutedCommand("portfolio", ["status"], "识别为组合状态查看。")
    if any(keyword in text for keyword in ["复盘"]) and re.search(r"20\d{2}-\d{2}", text):
        month = re.search(r"(20\d{2}-\d{2})", text).group(1)
        return RoutedCommand("portfolio", ["review", month], "识别为月度复盘。")
    if any(keyword in text for keyword in ["比较", "对比", "怎么选", "哪个更好", "vs", "VS"]) and len(symbols) >= 2:
        return RoutedCommand("compare", symbols[:4], "识别为同类标的比较。")
    if any(keyword in text for keyword in ["盘中", "快照", "分时"]) and symbols:
        return RoutedCommand("snap", [symbols[0]], "识别为盘中快照。")
    if any(keyword in text for keyword in ["回测", "规则", "因子验证"]):
        rule = _detect_rule(text)
        period = _detect_period(text)
        if symbols:
            return RoutedCommand("backtest", [rule, symbols[0], period], "识别为规则回测。")
    if symbols and any(keyword in text for keyword in ["扫描", "看看", "分析", "打分卡", "标的", "怎么样", "能不能买", "深度分析"]):
        return RoutedCommand("scan", [symbols[0]], "识别为单标的扫描。")
    if symbols and len(symbols) == 1 and len(text) <= 24:
        return RoutedCommand("scan", [symbols[0]], "检测到单个标的，默认执行扫描。")
    return RoutedCommand("research", [text], "无法稳定匹配到专用命令，回退到研究问答。")


def _extract_symbols(text: str, candidates: Sequence[str]) -> List[str]:
    matched_positions: List[tuple[int, str]] = []
    upper_text = text.upper()
    for symbol in candidates:
        position = upper_text.find(symbol.upper())
        if position >= 0:
            matched_positions.append((position, symbol))

    generic_patterns = [
        r"\b[A-Z]{1,5}\b",
        r"(?<!\d)\d{5,6}(?!\d)",
        r"\b[A-Z]{1,2}\d\b",
    ]
    for pattern in generic_patterns:
        for match in re.finditer(pattern, upper_text):
            matched_positions.append((match.start(), match.group(0)))

    ordered: List[str] = []
    for _, symbol in sorted(matched_positions, key=lambda item: item[0]):
        if symbol in SYMBOL_STOPWORDS:
            continue
        if symbol not in ordered:
            ordered.append(symbol)
    return ordered


def _merge_symbols(detected: Sequence[str], resolved: Sequence[str]) -> List[str]:
    ordered: List[str] = []
    for symbol in list(detected) + list(resolved):
        if symbol not in ordered:
            ordered.append(symbol)
    return ordered


def _detect_rule(text: str) -> str:
    lowered = text.lower()
    for rule_name, aliases in RULE_ALIASES.items():
        if any(alias.lower() in lowered for alias in aliases):
            return rule_name
    return "macd_golden_cross"


def _detect_scenario(text: str) -> str:
    lowered = text.lower()
    for scenario, aliases in SCENARIO_ALIASES.items():
        if any(alias.lower() in lowered for alias in aliases):
            return scenario
    return ""


def _detect_period(text: str) -> str:
    match = re.search(r"(\d+)\s*([ym])", text.lower())
    if match:
        return f"{match.group(1)}{match.group(2)}"
    if "一年" in text:
        return "1y"
    if "两年" in text:
        return "2y"
    if "三年" in text:
        return "3y"
    return "3y"


def _extract_event_keyword(text: str) -> str:
    cleaned = re.sub(r"(帮我|看看|发现|机会|关注|什么|今天|值得|主动)", " ", text)
    tokens = [token.strip() for token in re.split(r"[\s，。,.；;]+", cleaned) if token.strip()]
    for token in tokens:
        if len(token) >= 2:
            return token
    return ""


def _extract_policy_target(text: str) -> str:
    cleaned = re.sub(r"(帮我|看看|解读|政策|一下|分析)", " ", text)
    tokens = [token.strip() for token in re.split(r"[\s，。,.；;]+", cleaned) if token.strip()]
    return tokens[-1] if tokens else text
