"""Open-end fund profile collector — Tushare-first for basic/NAV, AKShare for details."""

from __future__ import annotations

import io
import re
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd

from .base import BaseCollector

try:
    import akshare as ak
except ImportError:  # pragma: no cover
    ak = None


FUND_THEME_RULES = [
    (("科技", "半导体", "芯片", "ai", "人工智能", "软件", "算力", "恒生科技"), ("科技", ["AI算力", "半导体", "成长股估值修复"])),
    (("军工", "国防", "航天", "卫星", "商业航天"), ("军工", ["军工", "地缘风险", "商业航天"])),
    (("黄金", "贵金属"), ("黄金", ["黄金", "通胀预期"])),
    (("电网", "电力", "储能", "特高压"), ("电网", ["AI算力", "电力需求", "电网设备"])),
    (("有色", "铜", "铝", "黄金股"), ("有色", ["铜铝", "顺周期"])),
    (("医药", "医疗", "创新药"), ("医药", ["医药", "老龄化"])),
    (("消费", "食品", "饮料", "家电", "零售"), ("消费", ["内需", "消费修复"])),
    (("红利", "高股息", "股息", "银行", "公用事业"), ("高股息", ["高股息", "防守"])),
    (("能源", "原油", "煤炭", "油气"), ("能源", ["原油", "能源安全", "通胀预期"])),
    (("沪深300", "中证a500", "中证500", "上证50"), ("宽基", ["宽基", "大盘蓝筹", "内需"])),
]


class FundProfileCollector(BaseCollector):
    """场外基金画像数据采集。Tushare fund_basic/fund_nav 优先。"""

    def _ak_function(self, name: str):
        if ak is None:
            raise RuntimeError("akshare is not installed")
        func = getattr(ak, name, None)
        if not callable(func):
            raise RuntimeError(f"AKShare function not available: {name}")
        return func

    # ── Tushare: 基金基础信息 ─────────────────────────────────

    def get_fund_basic(self, market: str = "O") -> pd.DataFrame:
        """Tushare fund_basic — 公募基金列表与管理人信息。

        market: E=场内, O=场外/开放式, L=LOF
        """
        cache_key = f"fund_profile:ts_fund_basic:{market}"
        cached = self._load_cache(cache_key, ttl_hours=24)
        if cached is not None:
            return cached
        raw = self._ts_call("fund_basic", market=market)
        if raw is not None and not raw.empty:
            self._save_cache(cache_key, raw)
            return raw
        return pd.DataFrame()

    def get_fund_nav_ts(self, symbol: str) -> pd.DataFrame:
        """Tushare fund_nav — 基金历史净值。"""
        ts_code = self._resolve_tushare_fund_code(symbol, preferred_markets=("O", "L", "E"))
        cache_key = f"fund_profile:ts_fund_nav:{ts_code}"
        cached = self._load_cache(cache_key, ttl_hours=12)
        if cached is not None:
            return cached
        raw = self._ts_call("fund_nav", ts_code=ts_code)
        if raw is not None and not raw.empty:
            self._save_cache(cache_key, raw)
            return raw
        return pd.DataFrame()

    def get_overview(self, symbol: str) -> pd.DataFrame:
        fetcher = self._ak_function("fund_overview_em")
        return self.cached_call(f"fund_profile:overview:{symbol}", fetcher, symbol=symbol, ttl_hours=24)

    def get_achievement(self, symbol: str) -> pd.DataFrame:
        fetcher = self._ak_function("fund_individual_achievement_xq")
        return self.cached_call(f"fund_profile:achievement:{symbol}", fetcher, symbol=symbol, ttl_hours=12)

    def get_asset_allocation(self, symbol: str) -> pd.DataFrame:
        fetcher = self._ak_function("fund_individual_detail_hold_xq")
        return self.cached_call(f"fund_profile:asset_mix:{symbol}", fetcher, symbol=symbol, ttl_hours=12)

    def get_portfolio_hold(self, symbol: str, years: Optional[Sequence[str]] = None) -> pd.DataFrame:
        raw_fetcher = self._ak_function("fund_portfolio_hold_em")

        def fetcher(**kwargs) -> pd.DataFrame:
            try:
                return raw_fetcher(**kwargs)
            except Exception as exc:
                if self._is_known_empty_detail_error(exc):
                    return pd.DataFrame()
                raise

        for year in years or self._year_candidates():
            try:
                frame = self.cached_call(f"fund_profile:holdings:{symbol}:{year}", fetcher, symbol=symbol, date=year, ttl_hours=24)
            except Exception:
                continue
            latest = self._latest_quarter_frame(frame, "季度")
            if not latest.empty:
                return latest.reset_index(drop=True)
        return pd.DataFrame()

    def get_industry_allocation(self, symbol: str, years: Optional[Sequence[str]] = None) -> pd.DataFrame:
        raw_fetcher = self._ak_function("fund_portfolio_industry_allocation_em")

        def fetcher(**kwargs) -> pd.DataFrame:
            try:
                return raw_fetcher(**kwargs)
            except Exception as exc:
                if self._is_known_empty_detail_error(exc):
                    return pd.DataFrame()
                raise

        for year in years or self._year_candidates():
            try:
                frame = self.cached_call(f"fund_profile:industry:{symbol}:{year}", fetcher, symbol=symbol, date=year, ttl_hours=24)
            except Exception:
                continue
            latest = self._latest_cutoff_frame(frame, "截止时间")
            if not latest.empty:
                return latest.reset_index(drop=True)
        return pd.DataFrame()

    def get_manager_directory(self) -> pd.DataFrame:
        def fetcher() -> pd.DataFrame:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                return self._ak_function("fund_manager_em")()

        return self.cached_call("fund_profile:manager_directory", fetcher, ttl_hours=24)

    def get_rating_table(self) -> pd.DataFrame:
        fetcher = self._ak_function("fund_rating_all")
        return self.cached_call("fund_profile:rating_all", fetcher, ttl_hours=24)

    def collect_profile(self, symbol: str) -> Dict[str, Any]:
        notes: List[str] = []
        overview_df = self._safe_frame(self.get_overview, symbol)
        achievement_df = self._safe_frame(self.get_achievement, symbol)
        asset_mix_df = self._safe_frame(self.get_asset_allocation, symbol)
        holdings_df = self._safe_frame(self.get_portfolio_hold, symbol)
        industry_df = self._safe_frame(self.get_industry_allocation, symbol)
        manager_df = self._safe_frame(self.get_manager_directory)
        rating_df = self._safe_frame(self.get_rating_table)

        overview = overview_df.iloc[0].to_dict() if not overview_df.empty else {}
        overview = self._merge_overview_with_tushare(overview, symbol)
        if not overview:
            notes.append("基金概况缺失")
        achievement = self._achievement_snapshot(achievement_df)
        top_holdings = self._top_holdings(holdings_df)
        top_industries = self._top_industries(industry_df)
        asset_mix = self._asset_mix(asset_mix_df)
        rating = self._rating_snapshot(rating_df, symbol)
        manager = self._manager_snapshot(manager_df, overview)
        style = self._derive_style(overview, top_holdings, top_industries, asset_mix, manager)

        if not top_holdings:
            notes.append("基金持仓明细缺失")
        if not manager:
            notes.append("基金经理画像缺失")
        if not rating:
            notes.append("基金评级缺失")

        return {
            "overview": overview,
            "achievement": achievement,
            "top_holdings": top_holdings,
            "industry_allocation": top_industries,
            "asset_allocation": asset_mix,
            "manager": manager,
            "rating": rating,
            "style": style,
            "latest_quarter": str(top_holdings[0].get("季度", "")) if top_holdings else "",
            "notes": notes,
        }

    def _merge_overview_with_tushare(self, overview: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        basic = self._tushare_fund_basic_row(symbol)
        merged = self._overview_from_tushare_basic(basic) if basic else {}
        if not overview:
            return merged
        if not merged:
            return dict(overview)

        for key, value in dict(overview).items():
            if merged.get(key) in (None, "", "—"):
                if value not in (None, "", "—"):
                    merged[key] = value
        return merged

    def _overview_from_tushare_basic(self, basic: Dict[str, Any]) -> Dict[str, Any]:
        overview: Dict[str, Any] = {}
        if not basic:
            return overview

        fund_type = str(basic.get("fund_type", "")).strip()
        invest_type = str(basic.get("invest_type", "")).strip()
        type_text = fund_type or invest_type
        if fund_type and invest_type and invest_type not in fund_type:
            type_text = f"{fund_type} / {invest_type}"

        found_date = self._normalize_compact_date(basic.get("found_date"))
        issue_date = self._normalize_compact_date(basic.get("issue_date"))
        list_date = self._normalize_compact_date(basic.get("list_date"))
        issue_amount = self._to_float(basic.get("issue_amount"))
        if found_date and issue_amount is not None and pd.notna(issue_amount):
            founding_text = f"{found_date} / {issue_amount:.4f}亿份"
        else:
            founding_text = found_date or ""

        if str(basic.get("name", "")).strip():
            overview["基金简称"] = str(basic.get("name", "")).strip()
        ts_code = str(basic.get("ts_code", "")).split(".")[0]
        if ts_code:
            overview["基金代码"] = ts_code
        if type_text:
            overview["基金类型"] = type_text
        if str(basic.get("management", "")).strip():
            overview["基金管理人"] = str(basic.get("management", "")).strip()
        custodian = str(basic.get("custodian", "") or basic.get("trustee", "")).strip()
        if custodian:
            overview["基金托管人"] = custodian
        if issue_date:
            overview["发行日期"] = issue_date
        if founding_text:
            overview["成立日期/规模"] = founding_text
        benchmark = str(basic.get("benchmark", "")).strip()
        if benchmark:
            overview["业绩比较基准"] = benchmark
            overview["跟踪标的"] = benchmark
        if list_date:
            overview["上市日期"] = list_date
        if basic.get("m_fee") not in (None, ""):
            overview["管理费率"] = f"{float(basic.get('m_fee')):.2f}%（每年）"
        if basic.get("c_fee") not in (None, ""):
            overview["托管费率"] = f"{float(basic.get('c_fee')):.2f}%（每年）"
        return overview

    def _tushare_fund_basic_row(self, symbol: str) -> Dict[str, Any]:
        bare_symbol = str(symbol).split(".")[0]
        for market in ("E", "O", "L"):
            frame = self.get_fund_basic(market)
            if frame.empty or "ts_code" not in frame.columns:
                continue
            matched = frame[frame["ts_code"].astype(str).str.startswith(f"{bare_symbol}.", na=False)]
            if not matched.empty:
                return matched.iloc[0].to_dict()
        return {}

    def _normalize_compact_date(self, value: Any) -> str:
        text = str(value or "").strip()
        if not text or text.lower() == "nan":
            return ""
        if text.isdigit() and len(text) == 8:
            return f"{text[:4]}-{text[4:6]}-{text[6:]}"
        return text

    def _safe_frame(self, method, *args) -> pd.DataFrame:  # noqa: ANN001
        try:
            frame = method(*args)
            return frame if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def _year_candidates(self) -> List[str]:
        year = datetime.now().year
        return [str(year - offset) for offset in range(0, 4)]

    def _is_known_empty_detail_error(self, exc: Exception) -> bool:
        text = str(exc)
        return any(
            token in text
            for token in (
                "Length mismatch",
                "Excel file format cannot be determined",
                "CERTIFICATE_VERIFY_FAILED",
                "'data'",
            )
        )

    def _latest_quarter_frame(self, frame: pd.DataFrame, column: str) -> pd.DataFrame:
        if frame.empty or column not in frame.columns:
            return pd.DataFrame()
        scored = frame.copy()
        scored["_quarter_score"] = scored[column].astype(str).map(self._quarter_score)
        latest = scored["_quarter_score"].max()
        if pd.isna(latest):
            return pd.DataFrame()
        return scored[scored["_quarter_score"] == latest].drop(columns="_quarter_score")

    def _latest_cutoff_frame(self, frame: pd.DataFrame, column: str) -> pd.DataFrame:
        if frame.empty or column not in frame.columns:
            return pd.DataFrame()
        scored = frame.copy()
        scored["_cutoff"] = pd.to_datetime(scored[column], errors="coerce")
        latest = scored["_cutoff"].max()
        if pd.isna(latest):
            return pd.DataFrame()
        return scored[scored["_cutoff"] == latest].drop(columns="_cutoff")

    def _quarter_score(self, value: str) -> float:
        match = re.search(r"(\d{4})年(\d)季度", str(value))
        if not match:
            return float("-inf")
        return float(int(match.group(1)) * 10 + int(match.group(2)))

    def _achievement_snapshot(self, frame: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        if frame.empty:
            return {}
        result: Dict[str, Dict[str, Any]] = {}
        period_col = "周期" if "周期" in frame.columns else None
        if not period_col:
            return result
        for _, row in frame.iterrows():
            period = str(row.get(period_col, "")).strip()
            if not period or period in result:
                continue
            result[period] = {
                "return_pct": self._to_float(row.get("本产品区间收益")),
                "max_drawdown_pct": self._to_float(row.get("本产品最大回撒")),
                "peer_rank": str(row.get("周期收益同类排名", "")).strip(),
            }
        return result

    def _top_holdings(self, frame: pd.DataFrame) -> List[Dict[str, Any]]:
        if frame.empty:
            return []
        result: List[Dict[str, Any]] = []
        sorted_frame = frame.copy()
        if "占净值比例" in sorted_frame.columns:
            sorted_frame["占净值比例"] = pd.to_numeric(sorted_frame["占净值比例"], errors="coerce")
            sorted_frame = sorted_frame.sort_values("占净值比例", ascending=False)
        for _, row in sorted_frame.head(10).iterrows():
            result.append(
                {
                    "股票代码": str(row.get("股票代码", "")).strip(),
                    "股票名称": str(row.get("股票名称", "")).strip(),
                    "占净值比例": self._to_float(row.get("占净值比例")),
                    "持股数": self._to_float(row.get("持股数")),
                    "持仓市值": self._to_float(row.get("持仓市值")),
                    "季度": str(row.get("季度", "")).strip(),
                }
            )
        return result

    def _top_industries(self, frame: pd.DataFrame) -> List[Dict[str, Any]]:
        if frame.empty:
            return []
        result: List[Dict[str, Any]] = []
        sorted_frame = frame.copy()
        if "占净值比例" in sorted_frame.columns:
            sorted_frame["占净值比例"] = pd.to_numeric(sorted_frame["占净值比例"], errors="coerce")
            sorted_frame = sorted_frame.sort_values("占净值比例", ascending=False)
        for _, row in sorted_frame.head(8).iterrows():
            result.append(
                {
                    "行业类别": str(row.get("行业类别", "")).strip(),
                    "占净值比例": self._to_float(row.get("占净值比例")),
                    "市值": self._to_float(row.get("市值")),
                    "截止时间": str(row.get("截止时间", "")).strip(),
                }
            )
        return result

    def _asset_mix(self, frame: pd.DataFrame) -> List[Dict[str, Any]]:
        if frame.empty:
            return []
        result: List[Dict[str, Any]] = []
        for _, row in frame.iterrows():
            result.append(
                {
                    "资产类型": str(row.get("资产类型", "")).strip(),
                    "仓位占比": self._to_float(row.get("仓位占比")),
                }
            )
        return result

    def _manager_snapshot(self, frame: pd.DataFrame, overview: Dict[str, Any]) -> Dict[str, Any]:
        manager_name = str(overview.get("基金经理人", "")).strip()
        if not manager_name or frame.empty or "姓名" not in frame.columns:
            return {}
        manager_names = [name.strip() for name in re.split(r"[,，、/]+", manager_name) if name.strip()]
        matched = frame[frame["姓名"].astype(str).isin(manager_names)].copy()
        if matched.empty:
            return {}
        primary = matched.iloc[0]
        return {
            "name": manager_name,
            "company": str(primary.get("所属公司", "")).strip(),
            "tenure_days": self._to_float(primary.get("累计从业时间")),
            "aum_billion": self._to_float(primary.get("现任基金资产总规模")),
            "best_return_pct": self._to_float(primary.get("现任基金最佳回报")),
            "current_fund_count": int(matched["现任基金代码"].astype(str).nunique()) if "现任基金代码" in matched.columns else len(matched),
            "peer_funds": list(dict.fromkeys(matched.get("现任基金", pd.Series(dtype=str)).astype(str).tolist())),
        }

    def _rating_snapshot(self, frame: pd.DataFrame, symbol: str) -> Dict[str, Any]:
        if frame.empty or "代码" not in frame.columns:
            return {}
        matched = frame[frame["代码"].astype(str) == str(symbol)]
        if matched.empty:
            return {}
        row = matched.iloc[0]
        return {
            "five_star_count": self._to_float(row.get("5星评级家数")),
            "shanghai": self._to_float(row.get("上海证券")),
            "zhaoshang": self._to_float(row.get("招商证券")),
            "jiaan": self._to_float(row.get("济安金信")),
            "morningstar": self._to_float(row.get("晨星评级")),
            "category": str(row.get("类型", "")).strip(),
            "fee": self._to_float(row.get("手续费")),
        }

    def _derive_style(
        self,
        overview: Dict[str, Any],
        top_holdings: List[Dict[str, Any]],
        top_industries: List[Dict[str, Any]],
        asset_mix: List[Dict[str, Any]],
        manager: Dict[str, Any],
    ) -> Dict[str, Any]:
        fund_name = str(overview.get("基金简称", "")).strip()
        fund_type = str(overview.get("基金类型", "")).strip()
        benchmark_note = str(overview.get("业绩比较基准", "")).strip() or "未披露业绩比较基准"
        tracking_target = str(overview.get("跟踪标的", "")).strip()
        passive_text = f"{fund_name} {fund_type}".lower()
        is_passive = any(token in passive_text for token in ("指数", "etf", "联接"))
        core_text_parts = [
            fund_name,
            fund_type,
            benchmark_note,
            tracking_target,
        ]
        secondary_text_parts = [
            " ".join(item.get("行业类别", "") for item in top_industries),
            " ".join(item.get("股票名称", "") for item in top_holdings),
        ]
        sector, chain_nodes = self._infer_theme(" ".join(core_text_parts))
        if sector == "综合":
            sector, chain_nodes = self._infer_theme(" ".join(secondary_text_parts))
        if sector == "综合":
            fallback_text = " ".join([*core_text_parts, *secondary_text_parts, " ".join(manager.get("peer_funds", []))])
            sector, chain_nodes = self._infer_theme(fallback_text)
        stock_ratio = self._asset_ratio(asset_mix, "股票")
        cash_ratio = self._asset_ratio(asset_mix, "现金")
        top5 = sum(item.get("占净值比例") or 0.0 for item in top_holdings[:5])
        tags: List[str] = []
        if sector != "综合":
            tags.append(f"{sector}主题")
        if is_passive:
            tags.append("被动跟踪")
        elif stock_ratio >= 80:
            tags.append("高仓位主动")
        elif stock_ratio >= 60:
            tags.append("偏股进攻")
        elif stock_ratio > 0:
            tags.append("仓位灵活")
        if cash_ratio >= 15:
            tags.append("保留机动仓位")
        if not is_passive and top5 >= 40:
            tags.append("高集中选股")
        elif not is_passive and top5 >= 25:
            tags.append("中等集中")
        if not is_passive and self._manager_style_consistent(manager.get("peer_funds", []), sector):
            tags.append("风格稳定")

        if is_passive:
            positioning = "这类基金更看跟踪标的暴露、跟踪误差和申赎效率，不以基金经理主观择时择股为核心。"
        elif stock_ratio >= 80:
            positioning = f"股票仓位约 {stock_ratio:.1f}% ，整体是高仓位进攻框架。"
        elif stock_ratio > 0:
            positioning = f"股票仓位约 {stock_ratio:.1f}% ，仓位并不保守。"
        else:
            positioning = "当前仓位信息不足，无法稳定判断进攻/防守倾向。"
        if cash_ratio >= 15:
            positioning += f" 同时保留约 {cash_ratio:.1f}% 现金，机动性不低。"

        if is_passive:
            if sector != "综合":
                selection = f"核心不是基金经理主动选股，而是跟踪 `{sector}` 暴露及其对应基准。"
            else:
                selection = "核心不是基金经理主动选股，而是跟踪对应指数/标的本身。"
        elif top5 >= 40:
            selection = f"前五大重仓合计约 {top5:.1f}% ，选股集中度较高，本质上是在买基金经理的高 conviction 组合。"
        elif top5 > 0:
            selection = f"前五大重仓合计约 {top5:.1f}% ，持仓集中度中等，更像主题内的主动均衡配置。"
        else:
            selection = "当前没有拿到稳定的前十大持仓，选股风格暂时无法下强结论。"

        if is_passive:
            consistency = "这类产品更重要的是跟踪误差、费率和标的暴露是否清晰，基金经理风格漂移不是核心变量。"
        elif manager:
            consistency = (
                f"经理当前在管约 {manager.get('current_fund_count', 0)} 只产品，"
                f"在管规模约 {manager.get('aum_billion', 0.0):.2f} 亿。"
            )
            if "风格稳定" in tags:
                consistency += " 从在管产品命名和重仓暴露看，风格一致性较强。"
        else:
            consistency = "基金经理画像缺失，无法评估风格一致性。"

        summary = "这只基金更像在买"
        if sector != "综合":
            summary += f"`{sector}`方向"
            if is_passive:
                summary += "的被动暴露"
        else:
            summary += "对应指数/标的本身" if is_passive else "基金经理的主动选股框架"
        if tags:
            summary += "，当前标签是 `" + " / ".join(tags) + "`。"
        else:
            summary += "。"

        return {
            "sector": sector,
            "chain_nodes": chain_nodes,
            "tags": tags,
            "summary": summary,
            "positioning": positioning,
            "selection": selection,
            "consistency": consistency,
            "benchmark_note": benchmark_note,
            "top5_concentration": round(top5, 2),
            "stock_ratio": round(stock_ratio, 2),
            "cash_ratio": round(cash_ratio, 2),
        }

    def _infer_theme(self, text: str) -> tuple[str, List[str]]:
        lowered = str(text).lower()
        for keywords, payload in FUND_THEME_RULES:
            if any(keyword.lower() in lowered for keyword in keywords):
                return payload[0], list(payload[1])
        return "综合", ["主动管理", "组合配置"]

    def _manager_style_consistent(self, peer_funds: Iterable[str], sector: str) -> bool:
        peer_text = " ".join(str(item) for item in peer_funds).lower()
        if not peer_text or sector == "综合":
            return False
        for keywords, payload in FUND_THEME_RULES:
            if payload[0] == sector:
                return sum(1 for keyword in keywords if keyword.lower() in peer_text) >= 1
        return False

    def _asset_ratio(self, rows: Iterable[Dict[str, Any]], label: str) -> float:
        for row in rows:
            if str(row.get("资产类型", "")).strip() == label:
                return float(row.get("仓位占比") or 0.0)
        return 0.0

    def _to_float(self, value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip().replace(",", "")
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        return float(match.group(0)) if match else None
