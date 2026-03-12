"""Valuation, index metadata, and financial proxy collectors — Tushare-first."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Optional, Sequence

import pandas as pd

from .base import BaseCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None

try:
    import baostock as bs
except ImportError:  # pragma: no cover
    bs = None


GENERIC_INDEX_KEYWORDS = {
    "科技",
    "成长",
    "价值",
    "红利",
    "消费",
    "医药",
    "金融",
    "地产",
    "周期",
    "制造",
    "材料",
}


def _normalize_index_label(value: str) -> str:
    text = str(value).strip().lower()
    for token in ("收益率", "价格指数", "指数", " ", "\t", "\n", "*", "×", "+", "/", "-", "（", "）", "(", ")", "·"):
        text = text.replace(token, "")
    return text


def _keyword_specificity(value: str) -> int:
    normalized = _normalize_index_label(value)
    if not normalized:
        return 0
    return 1 if normalized in GENERIC_INDEX_KEYWORDS else len(normalized)


class ValuationCollector(BaseCollector):
    """Collect ETF scale, index valuation snapshots, and financial proxies.

    Tushare 优先（daily_basic / fina_indicator / index_weight），AKShare 兜底。
    """

    def _require_ak(self):
        if ak is None:
            raise RuntimeError("akshare is not installed")
        return ak

    # ── ETF NAV ──────────────────────────────────────────────

    def get_cn_etf_nav_history(self, symbol: str, days: int = 120) -> pd.DataFrame:
        """ETF 净值历史。Tushare fund_nav 优先。"""
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        ts_code = self._resolve_tushare_fund_code(symbol, preferred_markets=("E", "O", "L"))

        # Tushare
        try:
            raw = self._ts_call("fund_nav", ts_code=ts_code)
            if raw is not None and not raw.empty:
                date_col = "nav_date" if "nav_date" in raw.columns else "end_date"
                if date_col not in raw.columns:
                    raise ValueError("fund_nav missing nav_date/end_date")
                raw[date_col] = pd.to_datetime(raw[date_col], format="%Y%m%d", errors="coerce")
                mask = raw[date_col] >= pd.to_datetime(start_date, format="%Y%m%d")
                filtered = raw[mask].sort_values(date_col).reset_index(drop=True)
                if not filtered.empty:
                    return filtered
        except Exception:
            pass

        # AKShare fallback
        client = self._require_ak()
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

    # ── 指数估值快照 ─────────────────────────────────────────

    def get_cn_index_snapshot(self, keywords: Sequence[str]) -> Optional[Dict[str, Any]]:
        """Find a CSI/CNI index valuation snapshot by keyword heuristics."""
        client = self._require_ak()
        cleaned = [str(item).strip() for item in keywords if str(item).strip()]
        if not cleaned:
            return None
        normalized_keywords = [(_normalize_index_label(item), index, _keyword_specificity(item), item) for index, item in enumerate(cleaned)]
        frame = self.cached_call("valuation:index_all_cni", client.index_all_cni, ttl_hours=12)
        if frame is None or frame.empty:
            return None
        if "指数简称" not in frame.columns or "指数代码" not in frame.columns:
            return None

        ranked_exact: list[tuple[tuple[int, int, int, int, int], Dict[str, Any]]] = []
        ranked_proxy: list[tuple[tuple[int, int, int, int, int], Dict[str, Any]]] = []
        exact_without_pe: list[tuple[tuple[int, int, int, int, int], Dict[str, Any]]] = []
        for _, row in frame.iterrows():
            name = str(row.get("指数简称", "")).strip()
            code = str(row.get("指数代码", "")).strip()
            if not name or not code:
                continue
            lowered = name.lower()
            normalized_name = _normalize_index_label(name)
            matched_keywords = [raw for normalized, _, _, raw in normalized_keywords if normalized and (normalized == normalized_name or normalized in normalized_name or raw.lower() in lowered)]
            if not matched_keywords:
                continue
            pe_value = pd.to_numeric(pd.Series([row.get("PE滚动")]), errors="coerce").iloc[0]
            has_pe = not pd.isna(pe_value)
            exact_match = any(_normalize_index_label(keyword) == normalized_name for keyword in matched_keywords)
            specificities = [_keyword_specificity(keyword) for keyword in matched_keywords]
            best_specificity = max(specificities) if specificities else 0
            total_specificity = sum(specificities)
            keyword_index = min(index for normalized, index, _, raw in normalized_keywords if raw in matched_keywords)
            penalty = 0
            if "港股" in name or "港币" in name or "人民币" in name:
                penalty += 2
            if name.endswith("R"):
                penalty += 1
            candidate = {
                "index_code": code,
                "index_name": name,
                "pe_ttm": None if pd.isna(pe_value) else float(pe_value),
                "matched_keywords": matched_keywords,
                "match_quality": "exact" if exact_match else "theme_proxy",
                "display_label": "真实指数估值" if exact_match else "指数估值代理",
            }
            rank_key = (-best_specificity, -total_specificity, keyword_index, penalty, len(name))
            if exact_match and has_pe:
                ranked_exact.append((rank_key, candidate))
            elif exact_match:
                exact_without_pe.append((rank_key, candidate))
            elif has_pe:
                ranked_proxy.append((rank_key, candidate))

        if ranked_exact:
            ranked_exact.sort(key=lambda item: item[0])
            selected = ranked_exact[0][1]
            selected["match_note"] = "估值库已匹配到与目标基准高度一致的指数名称。"
            return selected
        if ranked_proxy:
            ranked_proxy.sort(key=lambda item: item[0])
            selected = ranked_proxy[0][1]
            if exact_without_pe:
                exact_without_pe.sort(key=lambda item: item[0])
                selected["exact_benchmark_name"] = exact_without_pe[0][1]["index_name"]
                selected["match_note"] = f"估值库未提供 `{selected['exact_benchmark_name']}` 的滚动PE，当前改用最接近的主题指数代理。"
            else:
                selected["match_note"] = "估值库未直接命中精确基准，当前使用最接近的主题指数代理。"
            return selected
        if exact_without_pe:
            exact_without_pe.sort(key=lambda item: item[0])
            selected = exact_without_pe[0][1]
            selected["match_quality"] = "exact_no_pe"
            selected["display_label"] = "真实指数估值"
            selected["match_note"] = "估值库已命中基准指数，但当前缺少可用滚动PE。"
            return selected
        return None

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

    # ── 指数成分权重 ─────────────────────────────────────────

    def get_cn_index_constituent_weights(self, index_code: str, top_n: int = 10) -> pd.DataFrame:
        """Fetch the latest index constituent weights. Tushare index_weight 优先。"""
        # ── Tushare (primary) ──
        try:
            frame = self._ts_index_weight(index_code, top_n)
            if frame is not None and not frame.empty:
                return frame
        except Exception:
            pass

        # ── AKShare (fallback) ──
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

    def _ts_index_weight(self, index_code: str, top_n: int) -> pd.DataFrame | None:
        """Tushare index_weight — 指数成分权重。"""
        for ts_code in self._ts_index_code_candidates(index_code):
            cache_key = f"valuation:ts_index_weight:{ts_code}"
            cached = self._load_cache(cache_key, ttl_hours=24)
            if cached is not None:
                return cached

            raw = self._ts_call("index_weight", index_code=ts_code)
            if raw is None or raw.empty:
                continue

            # 取最新一期
            if "trade_date" in raw.columns:
                latest_date = raw["trade_date"].max()
                raw = raw[raw["trade_date"] == latest_date]

            frame = pd.DataFrame({
                "symbol": raw["con_code"].apply(self._from_ts_code),
                "name": raw.get("con_name", raw["con_code"].apply(self._from_ts_code)),
                "weight": pd.to_numeric(raw["weight"], errors="coerce"),
            })
            frame = frame.dropna(subset=["weight"]).sort_values("weight", ascending=False)
            if top_n > 0:
                frame = frame.head(top_n)
            result = frame.reset_index(drop=True)
            self._save_cache(cache_key, result)
            return result
        return None

    # ── 个股财务代理指标 ─────────────────────────────────────

    def get_cn_stock_financial_proxy(self, symbol: str) -> Dict[str, Any]:
        """Fetch the latest single-stock financial proxy metrics.

        Tushare fina_indicator 优先 → AKShare THS/EM 兜底。
        """
        # ── Tushare daily_basic (补充 PE/PB) ──
        try:
            basic = self._ts_daily_basic_for_stock(symbol)
        except Exception:
            basic = {}

        # ── Tushare fina_indicator (primary) ──
        try:
            result = self._tushare_stock_financial(symbol)
            if result:
                merged = dict(basic)
                merged.update(result)
                return merged
        except Exception:
            pass

        # ── AKShare THS/EM (fallback) ──
        if ak is not None:
            fetchers: list[tuple[str, Any, Dict[str, Any]]] = []
            abstract_fetcher = getattr(ak, "stock_financial_abstract_new_ths", None)
            if callable(abstract_fetcher):
                fetchers.append(
                    (
                        f"valuation:stock_financial:ths:{symbol}",
                        abstract_fetcher,
                        {"symbol": symbol, "indicator": "按报告期"},
                    )
                )
            indicator_fetcher = getattr(ak, "stock_financial_analysis_indicator_em", None)
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
                        # 合并 daily_basic 的 PE/PB
                        if basic:
                            parsed.setdefault("pe_ttm", basic.get("pe_ttm"))
                            parsed.setdefault("pb", basic.get("pb"))
                            parsed.setdefault("ps_ttm", basic.get("ps_ttm"))
                            parsed.setdefault("dv_ratio", basic.get("dv_ratio"))
                            parsed.setdefault("total_mv", basic.get("total_mv"))
                        return parsed
                except Exception as exc:
                    last_error = exc

        # 如果只有 daily_basic 有数据
        if basic:
            return basic

        raise RuntimeError("No stock financial proxy source available")

    def _tushare_stock_financial(self, symbol: str) -> Dict[str, Any]:
        """Tushare fina_indicator — 直接获取 ROE/毛利率/营收增速等高阶指标。

        2000 积分只能单只循环拉取，不能全市场一次性下载。
        """
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_fina_indicator:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached

        raw = self._ts_call("fina_indicator", ts_code=ts_code)
        if raw is None or raw.empty:
            return {}

        # 取最新一期
        if "end_date" in raw.columns:
            raw = raw.sort_values("end_date", ascending=False)
        latest = raw.iloc[0]

        def _safe_float(val: Any) -> Optional[float]:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        result: Dict[str, Any] = {
            "report_date": str(latest.get("end_date", "")),
            "roe": _safe_float(latest.get("roe")),
            "roe_dt": _safe_float(latest.get("roe_dt")),
            "gross_margin": _safe_float(latest.get("grossprofit_margin")),
            "revenue_yoy": _safe_float(latest.get("or_yoy")),  # 营业收入同比
            "profit_yoy": _safe_float(latest.get("netprofit_yoy")),  # 净利同比
            "profit_dedt_yoy": _safe_float(latest.get("dt_netprofit_yoy")),  # 扣非净利同比
            "debt_to_assets": _safe_float(latest.get("debt_to_assets")),
            "current_ratio": _safe_float(latest.get("current_ratio")),
            "eps": _safe_float(latest.get("eps")),
            "bps": _safe_float(latest.get("bps")),
            "cfps": _safe_float(latest.get("cfps")),
            "op_income_yoy": _safe_float(latest.get("op_yoy")),  # 营业利润同比
            "netprofit_margin": _safe_float(latest.get("netprofit_margin")),  # 净利率
        }
        # 去除所有 None 值
        result = {k: v for k, v in result.items() if v is not None}
        if result:
            result["report_date"] = str(latest.get("end_date", ""))
            self._save_cache(cache_key, result)
        return result

    def _ts_daily_basic_for_stock(self, symbol: str) -> Dict[str, Any]:
        """从 Tushare daily_basic 获取个股最新 PE/PB/PS/换手率/市值。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_daily_basic:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=4)
        if cached is not None:
            return cached

        raw = self._ts_call("daily_basic", ts_code=ts_code)
        if raw is None or raw.empty:
            return {}

        latest = raw.sort_values("trade_date", ascending=False).iloc[0]

        def _sf(val: Any) -> Optional[float]:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        result = {}
        pe = _sf(latest.get("pe_ttm"))
        if pe is not None:
            result["pe_ttm"] = pe
        pb = _sf(latest.get("pb"))
        if pb is not None:
            result["pb"] = pb
        ps = _sf(latest.get("ps_ttm"))
        if ps is not None:
            result["ps_ttm"] = ps
        dv = _sf(latest.get("dv_ttm"))
        if dv is not None:
            result["dv_ratio"] = dv
        mv = _sf(latest.get("total_mv"))
        if mv is not None:
            result["total_mv"] = mv * 10_000.0
        turnover = _sf(latest.get("turnover_rate_f"))
        if turnover is not None:
            result["turnover_rate"] = turnover

        if result:
            self._save_cache(cache_key, result)
        return result

    # ── 业绩预告 / 快报 ──────────────────────────────────────

    def get_cn_stock_forecast(self, symbol: str) -> Dict[str, Any]:
        """Tushare forecast — 业绩预告。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_forecast:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        raw = self._ts_call("forecast", ts_code=ts_code)
        if raw is None or raw.empty:
            return {}

        latest = raw.sort_values("ann_date", ascending=False).iloc[0]
        result = {
            "ann_date": str(latest.get("ann_date", "")),
            "end_date": str(latest.get("end_date", "")),
            "type": str(latest.get("type", "")),
            "change_reason": str(latest.get("change_reason", "")),
            "net_profit_min": latest.get("net_profit_min"),
            "net_profit_max": latest.get("net_profit_max"),
        }
        self._save_cache(cache_key, result)
        return result

    def get_cn_stock_express(self, symbol: str) -> Dict[str, Any]:
        """Tushare express — 业绩快报。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_express:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        raw = self._ts_call("express", ts_code=ts_code)
        if raw is None or raw.empty:
            return {}

        latest = raw.sort_values("ann_date", ascending=False).iloc[0]
        result = {
            "ann_date": str(latest.get("ann_date", "")),
            "end_date": str(latest.get("end_date", "")),
            "revenue": latest.get("revenue"),
            "operate_profit": latest.get("operate_profit"),
            "total_profit": latest.get("total_profit"),
            "n_income": latest.get("n_income"),
            "revenue_yoy": latest.get("yoy_sales"),
            "profit_yoy": latest.get("yoy_net_profit"),
        }
        self._save_cache(cache_key, result)
        return result

    def get_cn_stock_disclosure_dates(self, symbol: str) -> list[Dict[str, Any]]:
        """Tushare disclosure_date — 财报披露计划。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_disclosure_date:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        raw = self._ts_call("disclosure_date", ts_code=ts_code)
        if raw is None or raw.empty:
            return []

        frame = raw.copy()
        for column in ("ann_date", "end_date", "pre_date", "actual_date"):
            if column in frame.columns:
                frame[column] = frame[column].map(self._normalize_date_text)
        frame = frame.sort_values(["end_date", "pre_date", "actual_date"], ascending=False).reset_index(drop=True)
        records = frame.to_dict("records")
        self._save_cache(cache_key, records)
        return records

    def get_cn_stock_holder_trades(self, symbol: str, start_date: str = "", end_date: str = "") -> list[Dict[str, Any]]:
        """Tushare stk_holdertrade — 大股东/高管增减持。"""
        ts_code = self._to_ts_code(symbol)
        end = end_date or datetime.now().strftime("%Y%m%d")
        start = start_date or (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")
        cache_key = f"valuation:ts_stk_holdertrade:{ts_code}:{start}:{end}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        raw = self._ts_call("stk_holdertrade", ts_code=ts_code, start_date=start, end_date=end)
        if raw is None or raw.empty:
            return []

        frame = raw.copy()
        if "ann_date" in frame.columns:
            frame["ann_date"] = frame["ann_date"].map(self._normalize_date_text)
        for column in ("change_vol", "change_ratio", "after_share", "after_ratio", "avg_price", "total_share"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.sort_values("ann_date", ascending=False).reset_index(drop=True)
        records = frame.to_dict("records")
        self._save_cache(cache_key, records)
        return records

    def get_cn_stock_dividend(self, symbol: str) -> list[Dict[str, Any]]:
        """Tushare dividend — 分红送转。"""
        ts_code = self._to_ts_code(symbol)
        cache_key = f"valuation:ts_dividend:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        raw = self._ts_call("dividend", ts_code=ts_code)
        if raw is None or raw.empty:
            return []

        frame = raw.copy()
        for column in ("end_date", "ann_date", "record_date", "ex_date", "pay_date", "div_listdate", "imp_ann_date"):
            if column in frame.columns:
                frame[column] = frame[column].map(self._normalize_date_text)
        for column in ("cash_div", "cash_div_tax", "stk_div", "stk_bo_rate", "stk_co_rate"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.sort_values(["ann_date", "end_date"], ascending=False).reset_index(drop=True)
        records = frame.to_dict("records")
        self._save_cache(cache_key, records)
        return records

    def get_cn_stock_repurchase(self, symbol: str, start_date: str = "", end_date: str = "") -> list[Dict[str, Any]]:
        """Tushare repurchase — 回购进展。"""
        ts_code = self._to_ts_code(symbol)
        end = end_date or datetime.now().strftime("%Y%m%d")
        start = start_date or (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        cache_key = f"valuation:ts_repurchase:{ts_code}:{start}:{end}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached

        raw = self._ts_call("repurchase", ts_code=ts_code, start_date=start, end_date=end)
        if raw is None or raw.empty:
            return []

        frame = raw.copy()
        for column in ("ann_date", "end_date", "exp_date"):
            if column in frame.columns:
                frame[column] = frame[column].map(self._normalize_date_text)
        for column in ("vol", "amount", "high_limit", "low_limit"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame = frame.sort_values("ann_date", ascending=False).reset_index(drop=True)
        records = frame.to_dict("records")
        self._save_cache(cache_key, records)
        return records

    # ── 指数聚合财务 ─────────────────────────────────────────

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

    def get_weighted_stock_financial_proxies(
        self,
        holdings: Sequence[Dict[str, Any]],
        symbol_key: str = "symbol",
        name_key: str = "name",
        weight_key: str = "weight",
        top_n: int = 5,
    ) -> Dict[str, Any]:
        """Aggregate weighted financial proxies from an arbitrary holdings list."""
        normalized_rows: list[Dict[str, Any]] = []
        for raw in list(holdings)[:top_n]:
            symbol = str(raw.get(symbol_key, "")).strip()
            weight = pd.to_numeric(pd.Series([raw.get(weight_key)]), errors="coerce").iloc[0]
            if not symbol or pd.isna(weight) or float(weight) <= 0:
                continue
            normalized_rows.append(
                {
                    "symbol": symbol,
                    "name": str(raw.get(name_key, symbol)).strip() or symbol,
                    "weight": float(weight),
                }
            )
        if not normalized_rows:
            return {}

        total_weight = float(sum(float(item["weight"]) for item in normalized_rows))
        if total_weight <= 0:
            return {}

        rows: list[Dict[str, Any]] = []
        for row in normalized_rows:
            try:
                snapshot = self.get_cn_stock_financial_proxy(row["symbol"])
            except Exception:
                continue
            snapshot["weight"] = float(row["weight"])
            snapshot["symbol"] = row["symbol"]
            snapshot["name"] = row["name"]
            rows.append(snapshot)

        if not rows:
            return {
                "top_concentration": total_weight,
                "coverage_weight": 0.0,
                "coverage_ratio": 0.0,
                "coverage_count": 0,
                "constituents": normalized_rows,
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
            "top_concentration": total_weight,
            "coverage_weight": coverage_weight,
            "coverage_ratio": coverage_weight / total_weight if total_weight else 0.0,
            "coverage_count": len(rows),
            "report_date": max(report_dates) if report_dates else "",
            "constituents": normalized_rows,
        }

    # ── HK/US yfinance 估值 ──────────────────────────────────

    def get_yf_fundamental(self, symbol: str, asset_type: str) -> Dict[str, Any]:
        """Fetch fundamental metrics for HK/US stocks via yfinance Ticker.info."""
        if yf is None:
            return {}
        ticker = self._yf_ticker(symbol, asset_type)
        if not ticker:
            return {}
        try:
            info = self.cached_call(
                f"valuation:yf_fundamental:{ticker}",
                lambda: yf.Ticker(ticker).info,
                ttl_hours=12,
            )
        except Exception:
            return {}
        if not isinstance(info, dict):
            return {}
        result: Dict[str, Any] = {}
        pe = info.get("trailingPE")
        if pe is not None:
            try:
                result["pe_ttm"] = float(pe)
            except (ValueError, TypeError):
                pass
        ps = info.get("priceToSalesTrailing12Months")
        if ps is not None:
            try:
                result["ps_ttm"] = float(ps)
            except (ValueError, TypeError):
                pass
        roe = info.get("returnOnEquity")
        if roe is not None:
            try:
                result["roe"] = float(roe) * 100  # decimal → percentage
            except (ValueError, TypeError):
                pass
        rev_growth = info.get("revenueGrowth")
        if rev_growth is not None:
            try:
                result["revenue_yoy"] = float(rev_growth) * 100
            except (ValueError, TypeError):
                pass
        gross = info.get("grossMargins")
        if gross is not None:
            try:
                result["gross_margin"] = float(gross) * 100
            except (ValueError, TypeError):
                pass
        peg = info.get("trailingPegRatio")
        if peg is not None:
            try:
                result["peg"] = float(peg)
            except (ValueError, TypeError):
                pass
        return result

    def _yf_ticker(self, symbol: str, asset_type: str) -> str:
        """Convert symbol to yfinance ticker format."""
        if asset_type == "hk":
            if symbol.upper().endswith(".HK"):
                code = symbol[:-3].lstrip("0") or "0"
                return f"{code.zfill(4)}.HK"
            if symbol.isdigit():
                code = symbol.lstrip("0") or "0"
                return f"{code.zfill(4)}.HK"
            return symbol
        if asset_type == "us":
            return symbol.upper().replace(".US", "")
        return symbol

    def _eastmoney_symbol(self, symbol: str) -> str:
        if symbol.startswith(("6", "9")):
            return f"{symbol}.SH"
        return f"{symbol}.SZ"

    # ── 工具方法 ─────────────────────────────────────────────

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
