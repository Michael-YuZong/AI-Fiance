"""Asset keyword lookup with alias and live ETF search fallback."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Sequence

from src.collectors.base import BaseCollector
from src.utils.config import resolve_project_path
from src.utils.data import load_watchlist, load_yaml

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None


class AssetLookupCollector(BaseCollector):
    """Resolve natural-language ETF keywords into symbols."""

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(dict(config or {}), name="AssetLookupCollector")
        alias_file = self.config.get("asset_aliases_file", "config/asset_aliases.yaml")
        self.alias_path = resolve_project_path(alias_file)

    def search(self, keyword: str, limit: int = 8) -> List[Dict[str, Any]]:
        """Search aliases first, then try a live ETF universe lookup."""
        cleaned = self._normalize_query(keyword)
        if not cleaned:
            return []

        alias_matches = self._search_aliases(cleaned)
        live_matches: List[Dict[str, Any]] = []
        if not alias_matches:
            for term in self._candidate_terms(cleaned):
                for item in self._search_live_fund_names(term):
                    if item not in live_matches:
                        live_matches.append(item)
                if len(live_matches) >= limit:
                    break
                if self._looks_like_symbol(term):
                    for item in self._search_live_etf(term):
                        if item not in live_matches:
                            live_matches.append(item)
                    if len(live_matches) >= limit:
                        break

        combined: List[Dict[str, Any]] = []
        seen = set()
        for item in alias_matches + live_matches:
            key = (item.get("symbol"), item.get("name"))
            if key in seen:
                continue
            seen.add(key)
            combined.append(item)
        return combined[:limit]

    def resolve_best(self, keyword: str) -> Optional[Dict[str, Any]]:
        """Return a unique best match when possible."""
        matches = self.search(keyword, limit=5)
        if not matches:
            return None
        exact = [item for item in matches if item.get("match_type") == "exact_alias"]
        if len(exact) == 1:
            return exact[0]
        if len(matches) == 1:
            return matches[0]
        return None

    def _search_aliases(self, keyword: str) -> List[Dict[str, Any]]:
        payload = load_yaml(self.alias_path, default={"aliases": []}) or {"aliases": []}
        records = list(payload.get("aliases", []))
        records.extend(self._watchlist_aliases())
        lowered = keyword.lower()
        exact: List[tuple[int, Dict[str, Any]]] = []
        fuzzy: List[tuple[int, Dict[str, Any]]] = []

        for item in records:
            aliases = [str(item.get("name", ""))] + [str(alias) for alias in item.get("aliases", [])]
            aliases = [alias for alias in aliases if alias]
            exact_match = next((alias for alias in aliases if lowered == alias.lower()), "")
            if exact_match:
                exact.append(
                    (
                        keyword.lower().find(exact_match.lower()) if exact_match else 0,
                        {
                            "symbol": item["symbol"],
                            "name": item.get("name", item["symbol"]),
                            "asset_type": item.get("asset_type", ""),
                            "source": "alias",
                            "match_type": "exact_alias",
                            "matched_alias": exact_match,
                        },
                    )
                )
                continue
            fuzzy_match = next(
                (
                    alias
                    for alias in aliases
                    if lowered in alias.lower() or alias.lower() in lowered
                ),
                "",
            )
            if fuzzy_match:
                position = lowered.find(fuzzy_match.lower())
                if position < 0:
                    position = 9999
                fuzzy.append(
                    (
                        position,
                        {
                            "symbol": item["symbol"],
                            "name": item.get("name", item["symbol"]),
                            "asset_type": item.get("asset_type", ""),
                            "source": "alias",
                            "match_type": "fuzzy_alias",
                            "matched_alias": fuzzy_match,
                        },
                    )
                )

        ordered: List[Dict[str, Any]] = []
        seen = set()
        for _, item in sorted(exact + fuzzy, key=lambda row: row[0]):
            if item["symbol"] in seen:
                continue
            seen.add(item["symbol"])
            ordered.append(item)
        return ordered

    def _watchlist_aliases(self) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for item in load_watchlist():
            result.append(
                {
                    "symbol": item["symbol"],
                    "name": item.get("name", item["symbol"]),
                    "asset_type": item.get("asset_type", ""),
                    "aliases": [item["symbol"], item.get("name", item["symbol"])],
                }
            )
        return result

    def _search_live_etf(self, keyword: str) -> List[Dict[str, Any]]:
        if ak is None:
            return []
        try:
            frame = self.cached_call("asset_lookup:cn_etf_spot", ak.fund_etf_spot_em, ttl_hours=12)
        except Exception:
            return []

        name_col = "名称" if "名称" in frame.columns else None
        code_col = "代码" if "代码" in frame.columns else None
        if not name_col or not code_col:
            return []

        mask = frame[name_col].astype(str).str.contains(keyword, case=False, na=False) | frame[code_col].astype(str).str.contains(keyword, case=False, na=False)
        result = []
        for _, row in frame[mask].head(8).iterrows():
            result.append(
                {
                    "symbol": str(row[code_col]),
                    "name": str(row[name_col]),
                    "asset_type": "cn_etf",
                    "source": "live_etf_search",
                    "match_type": "live_search",
                }
            )
        return result

    def _search_live_fund_names(self, keyword: str) -> List[Dict[str, Any]]:
        if ak is None:
            return []
        try:
            frame = self.cached_call("asset_lookup:fund_name_em", ak.fund_name_em, ttl_hours=12)
        except Exception:
            return []

        name_col = "基金简称" if "基金简称" in frame.columns else None
        code_col = "基金代码" if "基金代码" in frame.columns else None
        if not name_col or not code_col:
            return []

        mask = frame[name_col].astype(str).str.contains(keyword, case=False, na=False)
        subset = frame[mask].copy()
        if subset.empty:
            return []

        def _rank(row: Any) -> tuple[int, int]:
            name = str(row[name_col])
            score = 0
            if keyword.lower() == name.lower():
                score += 4
            if "ETF" in name.upper():
                score += 3
            if "LOF" in name.upper() or "联接" in name:
                score -= 1
            if len(str(row[code_col])) == 6:
                score += 1
            return (-score, len(name))

        subset = subset.sort_values(by=name_col, key=lambda series: series.astype(str).str.len())
        ranked_rows = sorted(subset.to_dict("records"), key=_rank)[:8]
        return [
            {
                "symbol": str(row[code_col]),
                "name": str(row[name_col]),
                "asset_type": "cn_etf",
                "source": "fund_name_em",
                "match_type": "live_name_search",
            }
            for row in ranked_rows
        ]

    def _normalize_query(self, keyword: str) -> str:
        cleaned = keyword.strip()
        cleaned = re.sub(r"[，。,.；;:：!?？()（）]", " ", cleaned)
        cleaned = re.sub(
            r"(帮我|请|一下|分析|看看|研究|扫描|对比|比较|晨报|周报|写|生成|编号|代码|对应|是哪个|哪只|ETF编号)",
            " ",
            cleaned,
        )
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned or keyword.strip()

    def _candidate_terms(self, keyword: str) -> Sequence[str]:
        terms = [keyword]
        chunks = [token.strip() for token in re.split(r"\s+", keyword) if token.strip()]
        ordered = sorted(chunks, key=len, reverse=True)
        for token in ordered:
            if len(token) >= 2 and token not in terms:
                terms.append(token)
        return terms[:4]

    def _looks_like_symbol(self, term: str) -> bool:
        return bool(re.fullmatch(r"[A-Z0-9.\-]{2,10}", term.upper()))
