"""Industry chain graph utilities."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from src.utils.config import PROJECT_ROOT


class IndustryChainAnalyzer:
    """Simple directed graph traversal for sector transmission analysis."""

    def __init__(self, graph_path: Path = PROJECT_ROOT / "data" / "industry_chain.json") -> None:
        with graph_path.open("r", encoding="utf-8") as handle:
            self.graph = json.load(handle)
        self.edges = self.graph.get("edges", [])

    def propagate(self, source: str) -> List[Dict[str, object]]:
        return [edge for edge in self.edges if edge.get("from") == source]

    def related_nodes(self, keywords: List[str]) -> List[str]:
        nodes = set()
        for edge in self.edges:
            for keyword in keywords:
                if keyword in str(edge.get("from", "")) or keyword in str(edge.get("to", "")):
                    nodes.add(str(edge.get("from")))
                    nodes.add(str(edge.get("to")))
        return sorted(nodes)
