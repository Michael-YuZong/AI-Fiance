"""Policy keyword matching and heuristic parsing."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from src.utils.config import PROJECT_ROOT
from src.utils.data import load_json, load_watchlist


@dataclass
class PolicyContext:
    title: str
    source: str
    text: str


@dataclass
class PolicyTemplateMatch:
    template: Dict[str, Any]
    score: int
    matched_aliases: List[str]
    confidence_label: str


class PolicyEngine:
    """Heuristic policy analysis without requiring an LLM key."""

    def __init__(self) -> None:
        self.library = load_json(PROJECT_ROOT / "data" / "policy_library.json", default=[]) or []

    def load_context(self, target: str) -> PolicyContext:
        if target.startswith("http://") or target.startswith("https://"):
            response = requests.get(target, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            title = soup.title.text.strip() if soup.title and soup.title.text else urlparse(target).netloc
            text = " ".join(node.get_text(" ", strip=True) for node in soup.find_all(["p", "li", "h1", "h2", "h3"]))
            return PolicyContext(title=title, source=target, text=text)
        return PolicyContext(title=target, source="keyword", text=target)

    def match_policy(self, text: str) -> Optional[PolicyTemplateMatch]:
        lower_text = text.lower()
        best_template: Optional[Dict[str, Any]] = None
        best_aliases: List[str] = []
        best_score = -1
        for item in self.library:
            aliases = [str(alias).strip() for alias in item.get("aliases", []) if str(alias).strip()]
            matched_aliases = [alias for alias in aliases if alias.lower() in lower_text]
            score = len(matched_aliases) * 3
            name = str(item.get("name", "")).strip()
            if name and name.lower() in lower_text:
                score += 2
            if score > best_score:
                best_score = score
                best_template = item
                best_aliases = matched_aliases
        if best_template is None or best_score <= 0:
            return None
        confidence = "高" if best_score >= 6 or len(best_aliases) >= 3 else "中" if best_score >= 3 else "低"
        return PolicyTemplateMatch(
            template=best_template,
            score=best_score,
            matched_aliases=best_aliases,
            confidence_label=confidence,
        )

    def best_match(self, text: str) -> Optional[Dict[str, Any]]:
        matched = self.match_policy(text)
        return matched.template if matched else None

    def extract_numbers(self, text: str) -> List[str]:
        matches = re.findall(r"[0-9]+(?:\.[0-9]+)?[%万亿亿元万千]+", text)
        unique: List[str] = []
        for item in matches:
            if item not in unique:
                unique.append(item)
        return unique[:6]

    def classify_policy_direction(self, text: str) -> str:
        lowered = str(text or "").lower()
        support_hits = sum(
            lowered.count(token)
            for token in ("支持", "鼓励", "推进", "加快", "完善", "实施", "提升", "增强", "扩大")
        )
        restrict_hits = sum(
            lowered.count(token)
            for token in ("限制", "压降", "严控", "约束", "禁止", "整治", "从严", "防止")
        )
        if support_hits and restrict_hits:
            return "支持与约束并存"
        if support_hits:
            return "偏支持"
        if restrict_hits:
            return "偏约束"
        return "中性/待原文确认"

    def infer_policy_stage(self, title: str, text: str) -> str:
        combined = f"{title} {text}".lower()
        if "征求意见" in combined:
            return "征求意见阶段"
        if "实施细则" in combined or "细则" in combined or "办法" in combined or "申报" in combined:
            return "执行细则/落地阶段"
        if "行动计划" in combined or "方案" in combined or "规划" in combined:
            return "顶层规划/行动方案"
        if "通知" in combined or "意见" in combined or "决定" in combined:
            return "政策通知/执行部署"
        if "试点" in combined:
            return "试点推进阶段"
        return "阶段待原文确认"

    def extract_timeline_points(self, text: str) -> List[str]:
        patterns = [
            r"\d{4}年\d{1,2}月\d{1,2}日",
            r"\d{4}年\d{1,2}月",
            r"\d{1,2}月\d{1,2}日",
            r"\d+年内",
            r"\d+个月内",
            r"月底前",
            r"年内",
        ]
        seen: List[str] = []
        for pattern in patterns:
            for match in re.finditer(pattern, text):
                start = max(match.start() - 16, 0)
                end = min(match.end() + 24, len(text))
                snippet = re.sub(r"\s+", " ", text[start:end]).strip(" ：:;；，,。")
                if snippet and snippet not in seen:
                    seen.append(snippet)
        return seen[:5]

    def watchlist_impact(self, policy: Dict[str, Any], holdings: Iterable[Dict[str, Any]]) -> List[str]:
        watchlist = load_watchlist()
        candidates = list(holdings) + watchlist
        impact_lines: List[str] = []
        beneficiary_nodes = set(policy.get("beneficiary_nodes", []))
        risk_nodes = set(policy.get("risk_nodes", []))
        mapped_assets = [str(item).strip().lower() for item in policy.get("mapped_assets", []) if str(item).strip()]

        seen = set()
        for item in candidates:
            symbol = item.get("symbol")
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            chain_nodes = set(item.get("chain_nodes", []))
            name = str(item.get("name", symbol))
            matched_asset = any(mapped == str(symbol).lower() or mapped in name.lower() for mapped in mapped_assets)
            beneficiary_match = chain_nodes & beneficiary_nodes
            risk_match = chain_nodes & risk_nodes
            if matched_asset or beneficiary_match:
                reason = " / ".join(sorted(beneficiary_match)) if beneficiary_match else "模板显式映射"
                impact_lines.append(f"{symbol} ({name}) 命中受益方向 `{reason}`，适合进入重点跟踪。")
            elif risk_match:
                reason = " / ".join(sorted(risk_match))
                impact_lines.append(f"{symbol} ({name}) 暴露在风险链条 `{reason}` 上，需要观察兑现节奏。")
        return impact_lines
