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

    def best_match(self, text: str) -> Optional[Dict[str, Any]]:
        lower_text = text.lower()
        best = None
        best_score = -1
        for item in self.library:
            aliases = item.get("aliases", [])
            score = sum(1 for alias in aliases if alias.lower() in lower_text)
            if score > best_score:
                best_score = score
                best = item
        return best if best_score > 0 else None

    def extract_numbers(self, text: str) -> List[str]:
        matches = re.findall(r"[0-9]+(?:\.[0-9]+)?[%万亿亿元万千]+", text)
        unique: List[str] = []
        for item in matches:
            if item not in unique:
                unique.append(item)
        return unique[:6]

    def watchlist_impact(self, policy: Dict[str, Any], holdings: Iterable[Dict[str, Any]]) -> List[str]:
        watchlist = load_watchlist()
        candidates = list(holdings) + watchlist
        impact_lines: List[str] = []
        beneficiary_nodes = set(policy.get("beneficiary_nodes", []))
        risk_nodes = set(policy.get("risk_nodes", []))
        mapped_assets = set(policy.get("mapped_assets", []))

        seen = set()
        for item in candidates:
            symbol = item.get("symbol")
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            chain_nodes = set(item.get("chain_nodes", []))
            name = item.get("name", symbol)
            if symbol in mapped_assets or chain_nodes & beneficiary_nodes:
                impact_lines.append(f"{symbol} ({name}) 与政策受益方向匹配，适合进入重点跟踪。")
            elif chain_nodes & risk_nodes:
                impact_lines.append(f"{symbol} ({name}) 暴露在潜在风险点上，需要观察兑现节奏。")
        return impact_lines
