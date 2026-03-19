"""Asset keyword lookup with alias and live ETF search fallback — Tushare-first."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Sequence

from src.collectors.base import BaseCollector
from src.utils.config import resolve_project_path
from src.utils.data import load_watchlist, load_yaml


THEME_QUERY_EXPANSIONS = {
    "商业航天": ["商业航天", "卫星", "卫星产业", "航天", "商用卫星"],
    "航天": ["航天", "航空航天", "卫星"],
    "卫星": ["卫星", "卫星产业", "商用卫星"],
    "低空经济": ["低空经济", "通用航空", "无人机"],
    "沪深300": ["沪深300", "沪深300ETF", "300ETF"],
    "中证A500": ["中证A500", "中证A500ETF", "A500", "A500ETF"],
}


class AssetLookupCollector(BaseCollector):
    """Resolve natural-language ETF keywords into symbols. Tushare stock_basic/fund_basic 优先。"""

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        super().__init__(dict(config or {}), name="AssetLookupCollector")
        alias_file = self.config.get("asset_aliases_file", "config/asset_aliases.yaml")
        self.alias_path = resolve_project_path(alias_file)

    def search(self, keyword: str, limit: int = 8) -> List[Dict[str, Any]]:
        """Search aliases first, then Tushare stock/fund snapshots."""
        cleaned = self._normalize_query(keyword)
        if not cleaned:
            return []

        alias_matches = self._search_aliases(cleaned)
        live_matches: List[Dict[str, Any]] = []
        if not alias_matches:
            live_matches = self._search_tushare(cleaned)

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
            aliases = [str(item.get("symbol", "")), str(item.get("name", ""))] + [str(alias) for alias in item.get("aliases", [])]
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
                            "sector": item.get("sector", ""),
                            "chain_nodes": item.get("chain_nodes", []),
                            "region": item.get("region", ""),
                            "proxy_symbol": item.get("proxy_symbol", ""),
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
                            "sector": item.get("sector", ""),
                            "chain_nodes": item.get("chain_nodes", []),
                            "region": item.get("region", ""),
                            "proxy_symbol": item.get("proxy_symbol", ""),
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
                    "sector": item.get("sector", ""),
                    "chain_nodes": item.get("chain_nodes", []),
                    "region": item.get("region", ""),
                    "proxy_symbol": item.get("proxy_symbol", ""),
                    "aliases": [item["symbol"], item.get("name", item["symbol"])],
                }
            )
        return result

    def _search_tushare(self, keyword: str) -> List[Dict[str, Any]]:
        """Search Tushare stock_basic and fund_basic for matching assets."""
        results: List[Dict[str, Any]] = []

        # 股票搜索
        try:
            stock_df = self._ts_call("stock_basic", fields="ts_code,symbol,name,industry,list_status")
            if stock_df is not None and not stock_df.empty:
                mask = (
                    stock_df["name"].astype(str).str.contains(keyword, case=False, na=False)
                    | stock_df["symbol"].astype(str).str.contains(keyword, case=False, na=False)
                )
                for _, row in stock_df[mask].head(4).iterrows():
                    results.append({
                        "symbol": self._from_ts_code(str(row["ts_code"])),
                        "name": str(row.get("name", "")),
                        "asset_type": "cn_stock",
                        "sector": str(row.get("industry", "")),
                        "source": "tushare_stock_basic",
                        "match_type": "live_search",
                    })
        except Exception:
            pass

        for market in ("E", "L", "O"):
            try:
                fund_df = self._ts_fund_basic_snapshot(market)
            except Exception:
                fund_df = None
            if fund_df is None or fund_df.empty:
                continue
            name_col = "name" if "name" in fund_df.columns else None
            code_col = "ts_code" if "ts_code" in fund_df.columns else None
            if not name_col or not code_col:
                continue
            mask = (
                fund_df[name_col].astype(str).str.contains(keyword, case=False, na=False)
                | fund_df[code_col].astype(str).str.contains(keyword, case=False, na=False)
            )
            matched = fund_df[mask].copy()
            if matched.empty:
                continue
            for _, row in matched.head(6).iterrows():
                results.append(
                    {
                        "symbol": self._from_ts_code(str(row["ts_code"])),
                        "name": str(row.get("name", "")),
                        "asset_type": self._infer_tushare_fund_asset_type(row, market),
                        "source": f"tushare_fund_basic_{market}",
                        "match_type": "live_search",
                    }
                )

        return results[:8]

    def _infer_tushare_fund_asset_type(self, row: Mapping[str, Any], market: str) -> str:
        name = str(row.get("name", "")).strip()
        fund_type = str(row.get("fund_type", "")).strip()
        symbol = self._from_ts_code(str(row.get("ts_code", "")).strip())
        upper_name = name.upper()
        if market == "E":
            return "cn_etf"
        if market == "L":
            return "cn_fund"
        if "ETF" in upper_name and "联接" not in name and "LOF" not in upper_name:
            return "cn_etf"
        if "LOF" in upper_name:
            return "cn_fund"
        if symbol.startswith(("15", "16", "50", "51", "52", "53", "56", "58", "59")) and "联接" not in name:
            return "cn_etf"
        if "联接" in name or "发起" in name or "混合" in fund_type or "债券" in fund_type or "QDII" in fund_type:
            return "cn_fund"
        return "cn_fund"

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
        expanded = []
        for theme, mapped_terms in THEME_QUERY_EXPANSIONS.items():
            if theme.lower() in keyword.lower():
                expanded.extend(mapped_terms)
        for token in expanded:
            if token not in terms:
                terms.append(token)
        chunks = [token.strip() for token in re.split(r"\s+", keyword) if token.strip()]
        ordered = sorted(chunks, key=len, reverse=True)
        for token in ordered:
            if len(token) >= 2 and token not in terms:
                terms.append(token)
        return terms[:4]

    def _looks_like_symbol(self, term: str) -> bool:
        return bool(re.fullmatch(r"[A-Z0-9.\-]{2,10}", term.upper()))
