"""Valuation, index metadata, and financial proxy collectors."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Optional, Sequence

import pandas as pd

from .base import BaseCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None


class ValuationCollector(BaseCollector):
    """Collect ETF scale, index valuation snapshots, and financial proxies."""

    def _require_ak(self):
        if ak is None:
            raise RuntimeError("akshare is not installed")
        return ak

    def get_cn_etf_nav_history(self, symbol: str, days: int = 120) -> pd.DataFrame:
        client = self._require_ak()
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        return self.cached_call(
            f"valuation:nav:{symbol}:{start_date}:{end_date}",
            client.fund_etf_fund_info_em,
            fund=symbol,
            start_date=start_date,
            end_date=end_date,
        )

    def get_cn_etf_scale(self, symbol: str) -> Optional[dict]:
        client = self._require_ak()
        date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        candidates = []
        try:
            candidates.append(self.cached_call(f"valuation:scale:sse:{date}", client.fund_etf_scale_sse, date=date))
        except Exception:
            pass
        try:
            candidates.append(self.cached_call("valuation:scale:szse", client.fund_etf_scale_szse))
        except Exception:
            pass
        for frame in candidates:
            code_column = "基金代码" if "基金代码" in frame.columns else None
            if code_column is None:
                continue
            filtered = frame[frame[code_column].astype(str) == str(symbol)]
            if not filtered.empty:
                return filtered.iloc[0].to_dict()
        return None

    def get_cn_index_snapshot(self, keywords: Sequence[str]) -> Optional[Dict[str, Any]]:
        """Find a CSI/CNI index valuation snapshot by keyword heuristics."""
        client = self._require_ak()
        cleaned = [str(item).strip() for item in keywords if str(item).strip()]
        if not cleaned:
            return None
        frame = self.cached_call("valuation:index_all_cni", client.index_all_cni, ttl_hours=12)
        if frame is None or frame.empty:
            return None
        if "指数简称" not in frame.columns or "指数代码" not in frame.columns:
            return None

        ranked: list[tuple[tuple[int, int, int], Dict[str, Any]]] = []
        for _, row in frame.iterrows():
            name = str(row.get("指数简称", "")).strip()
            code = str(row.get("指数代码", "")).strip()
            if not name or not code:
                continue
            lowered = name.lower()
            matched_keywords = [keyword for keyword in cleaned if keyword.lower() in lowered]
            if not matched_keywords:
                continue
            pe_value = pd.to_numeric(pd.Series([row.get("PE滚动")]), errors="coerce").iloc[0]
            has_pe = not pd.isna(pe_value)
            score = 0
            if has_pe:
                score += 4
            score += min(len(matched_keywords), 3) * 2
            if "港股" in name or "港币" in name or "人民币" in name:
                score -= 2
            if name.endswith("R"):
                score -= 1
            ranked.append(
                (
                    (-score, len(name), 0 if has_pe else 1),
                    {
                        "index_code": code,
                        "index_name": name,
                        "pe_ttm": None if pd.isna(pe_value) else float(pe_value),
                    },
                )
            )
        if not ranked:
            return None
        ranked.sort(key=lambda item: item[0])
        return ranked[0][1]

    def get_cn_index_value_history(self, index_code: str) -> pd.DataFrame:
        """Fetch CSI/CNI index valuation history."""
        client = self._require_ak()
        fetcher = getattr(client, "stock_zh_index_value_csindex", None)
        if not callable(fetcher):
            raise RuntimeError("AKShare function not available: stock_zh_index_value_csindex")
        return self.cached_call(
            f"valuation:index_value:{index_code}",
            fetcher,
            symbol=index_code,
            ttl_hours=12,
        )

    def get_cn_index_constituent_weights(self, index_code: str, top_n: int = 10) -> pd.DataFrame:
        """Fetch the latest index constituent weights."""
        client = self._require_ak()
        fetcher = getattr(client, "index_stock_cons_weight_csindex", None)
        if not callable(fetcher):
            raise RuntimeError("AKShare function not available: index_stock_cons_weight_csindex")
        frame = self.cached_call(
            f"valuation:index_constituents:{index_code}",
            fetcher,
            symbol=index_code,
            ttl_hours=24,
        )
        code_col = self._first_existing_column(frame, ("成分券代码", "样本代码", "证券代码", "股票代码"))
        name_col = self._first_existing_column(frame, ("成分券名称", "样本简称", "证券名称", "股票名称"))
        weight_col = self._first_existing_column(frame, ("权重", "权重(%)", "weight"))
        if not code_col or not weight_col:
            raise ValueError("Index constituent weight frame missing required columns")
        normalized = frame.copy()
        normalized["symbol"] = normalized[code_col].astype(str).str.extract(r"(\d{6})", expand=False).fillna("")
        normalized["name"] = normalized[name_col].astype(str) if name_col else normalized["symbol"]
        normalized["weight"] = pd.to_numeric(normalized[weight_col], errors="coerce")
        normalized = normalized.dropna(subset=["weight"])
        normalized = normalized[normalized["symbol"] != ""].sort_values("weight", ascending=False)
        if top_n > 0:
            normalized = normalized.head(top_n)
        return normalized[["symbol", "name", "weight"]].reset_index(drop=True)

    def get_cn_stock_financial_proxy(self, symbol: str) -> Dict[str, Any]:
        """Fetch the latest single-stock financial proxy metrics."""
        client = self._require_ak()
        fetchers: list[tuple[str, Any, Dict[str, Any]]] = []
        abstract_fetcher = getattr(client, "stock_financial_abstract_new_ths", None)
        if callable(abstract_fetcher):
            fetchers.append(
                (
                    f"valuation:stock_financial:ths:{symbol}",
                    abstract_fetcher,
                    {"symbol": symbol, "indicator": "按报告期"},
                )
            )
        indicator_fetcher = getattr(client, "stock_financial_analysis_indicator_em", None)
        if callable(indicator_fetcher):
            fetchers.append(
                (
                    f"valuation:stock_financial:em:{symbol}",
                    indicator_fetcher,
                    {"symbol": self._eastmoney_symbol(symbol), "indicator": "按报告期"},
                )
            )

        last_error: Optional[Exception] = None
        for cache_key, fetcher, kwargs in fetchers:
            try:
                frame = self.cached_call(cache_key, fetcher, ttl_hours=24, **kwargs)
                parsed = self._parse_stock_financial_frame(frame)
                if parsed:
                    return parsed
            except Exception as exc:  # pragma: no cover - network/source variance
                last_error = exc
        if last_error is not None:
            raise last_error
        raise RuntimeError("No stock financial proxy source available")

    def get_cn_index_financial_proxies(self, index_code: str, top_n: int = 5) -> Dict[str, Any]:
        """Aggregate weighted financial proxies from top constituents."""
        constituents = self.get_cn_index_constituent_weights(index_code=index_code, top_n=top_n)
        if constituents.empty:
            return {}

        total_weight = float(pd.to_numeric(constituents["weight"], errors="coerce").fillna(0.0).sum())
        if total_weight <= 0:
            return {}

        rows: list[Dict[str, Any]] = []
        for _, row in constituents.iterrows():
            symbol = str(row.get("symbol", "")).strip()
            if not symbol:
                continue
            try:
                snapshot = self.get_cn_stock_financial_proxy(symbol)
            except Exception:
                continue
            snapshot["weight"] = float(row.get("weight", 0.0))
            snapshot["symbol"] = symbol
            snapshot["name"] = str(row.get("name", symbol))
            rows.append(snapshot)

        if not rows:
            return {
                "top_concentration": float(pd.to_numeric(constituents["weight"], errors="coerce").fillna(0.0).sum()),
                "coverage_weight": 0.0,
                "coverage_count": 0,
            }

        metrics = {
            "revenue_yoy": self._weighted_average(rows, "revenue_yoy"),
            "profit_yoy": self._weighted_average(rows, "profit_yoy"),
            "roe": self._weighted_average(rows, "roe"),
            "gross_margin": self._weighted_average(rows, "gross_margin"),
        }
        report_dates = [str(item.get("report_date", "")).strip() for item in rows if str(item.get("report_date", "")).strip()]
        coverage_weight = float(sum(float(item.get("weight", 0.0)) for item in rows))
        return {
            **metrics,
            "top_concentration": float(pd.to_numeric(constituents["weight"], errors="coerce").fillna(0.0).sum()),
            "coverage_weight": coverage_weight,
            "coverage_ratio": coverage_weight / total_weight if total_weight else 0.0,
            "coverage_count": len(rows),
            "report_date": max(report_dates) if report_dates else "",
            "constituents": constituents.to_dict("records"),
        }

    def _eastmoney_symbol(self, symbol: str) -> str:
        if symbol.startswith(("6", "9")):
            return f"{symbol}.SH"
        return f"{symbol}.SZ"

    def _weighted_average(self, rows: Iterable[Dict[str, Any]], field: str) -> Optional[float]:
        pairs: list[tuple[float, float]] = []
        for row in rows:
            value = pd.to_numeric(pd.Series([row.get(field)]), errors="coerce").iloc[0]
            weight = pd.to_numeric(pd.Series([row.get("weight")]), errors="coerce").iloc[0]
            if pd.isna(value) or pd.isna(weight) or float(weight) <= 0:
                continue
            pairs.append((float(value), float(weight)))
        if not pairs:
            return None
        total_weight = sum(weight for _, weight in pairs)
        if total_weight <= 0:
            return None
        return sum(value * weight for value, weight in pairs) / total_weight

    def _parse_stock_financial_frame(self, frame: pd.DataFrame) -> Dict[str, Any]:
        if frame is None or frame.empty:
            return {}
        if {"metric_name", "value"}.issubset(frame.columns):
            return self._parse_long_financial_frame(frame)
        return self._parse_wide_financial_frame(frame)

    def _parse_long_financial_frame(self, frame: pd.DataFrame) -> Dict[str, Any]:
        normalized = frame.copy()
        if "report_date" in normalized.columns:
            normalized["report_date"] = pd.to_datetime(normalized["report_date"], errors="coerce")
            normalized = normalized.sort_values("report_date", ascending=False)
            latest_date = normalized["report_date"].dropna().iloc[0] if normalized["report_date"].notna().any() else pd.NaT
            if pd.notna(latest_date):
                normalized = normalized[normalized["report_date"] == latest_date]
        report_date = ""
        if "report_date" in normalized.columns and normalized["report_date"].notna().any():
            report_date = normalized["report_date"].dropna().iloc[0].strftime("%Y-%m-%d")

        metric_col = normalized["metric_name"].astype(str)
        value_col = pd.to_numeric(normalized.get("value", pd.Series(dtype=float)), errors="coerce")
        yoy_col = pd.to_numeric(normalized.get("yoy", pd.Series(dtype=float)), errors="coerce")

        return {
            "report_date": report_date,
            "revenue_yoy": self._pick_metric(metric_col, yoy_col, ("营业总收入", "营业收入", "营收")),
            "profit_yoy": self._pick_metric(metric_col, yoy_col, ("归母净利润", "净利润", "扣非净利润")),
            "roe": self._pick_metric(metric_col, value_col, ("净资产收益率", "ROE", "净资收益率")),
            "gross_margin": self._pick_metric(metric_col, value_col, ("销售毛利率", "毛利率")),
        }

    def _parse_wide_financial_frame(self, frame: pd.DataFrame) -> Dict[str, Any]:
        normalized = frame.copy()
        date_col = self._first_existing_column(normalized, ("报告期", "REPORT_DATE", "日期", "date"))
        if date_col:
            normalized["_report_date"] = pd.to_datetime(normalized[date_col], errors="coerce")
            normalized = normalized.sort_values("_report_date", ascending=False)
        latest = normalized.iloc[0]
        report_date = ""
        if "_report_date" in normalized.columns and pd.notna(latest.get("_report_date")):
            report_date = pd.to_datetime(latest["_report_date"]).strftime("%Y-%m-%d")

        return {
            "report_date": report_date,
            "revenue_yoy": self._row_value(latest, ("营业总收入同比增长", "营业收入同比增长", "营收同比增长", "TOTALOPERATEREVETZ")),
            "profit_yoy": self._row_value(latest, ("归母净利润同比增长", "净利润同比增长", "扣非净利润同比增长", "PARENTNETPROFITTZ")),
            "roe": self._row_value(latest, ("净资产收益率", "净资产收益率加权", "加权净资产收益率", "ROEJQ")),
            "gross_margin": self._row_value(latest, ("销售毛利率", "毛利率", "XSMLL")),
        }

    def _pick_metric(self, metric_names: pd.Series, values: pd.Series, keywords: Sequence[str]) -> Optional[float]:
        lowered_keywords = [str(item).lower() for item in keywords]
        for metric_name, value in zip(metric_names, values):
            label = str(metric_name).strip().lower()
            if any(keyword in label for keyword in lowered_keywords) and pd.notna(value):
                return float(value)
        return None

    def _row_value(self, row: pd.Series, candidates: Sequence[str]) -> Optional[float]:
        for candidate in candidates:
            if candidate in row.index:
                value = pd.to_numeric(pd.Series([row.get(candidate)]), errors="coerce").iloc[0]
                if pd.notna(value):
                    return float(value)
        lowered_map = {str(column).lower(): column for column in row.index}
        for candidate in candidates:
            matched = lowered_map.get(str(candidate).lower())
            if matched is None:
                continue
            value = pd.to_numeric(pd.Series([row.get(matched)]), errors="coerce").iloc[0]
            if pd.notna(value):
                return float(value)
        return None

    def _first_existing_column(self, frame: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
        for candidate in candidates:
            if candidate in frame.columns:
                return candidate
        lowered = {str(column).lower(): column for column in frame.columns}
        for candidate in candidates:
            matched = lowered.get(str(candidate).lower())
            if matched:
                return matched
        return None
