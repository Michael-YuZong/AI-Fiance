"""Portfolio JSON storage and basic analytics."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from collections import Counter
from typing import Any, Dict, List, Mapping, Optional, Sequence

from src.utils.config import PROJECT_ROOT
from src.utils.data import load_json, save_json


DEFAULT_PORTFOLIO = {"base_currency": "CNY", "holdings": []}


def _top_exposure(exposure: Dict[str, float]) -> tuple[str, float]:
    if not exposure:
        return "UNKNOWN", 0.0
    key, value = max(exposure.items(), key=lambda item: float(item[1]))
    return str(key), float(value)


def _format_holdings_slice(holdings: List[Dict[str, Any]], limit: int = 3) -> str:
    items = []
    for row in sorted(holdings, key=lambda item: float(item.get("weight", 0.0)), reverse=True)[:limit]:
        symbol = str(row.get("symbol", ""))
        name = str(row.get("name", "") or symbol)
        weight = float(row.get("weight", 0.0))
        if symbol and name and name != symbol:
            items.append(f"{symbol} ({name}, {weight * 100:.1f}%)")
        elif symbol:
            items.append(f"{symbol} ({weight * 100:.1f}%)")
        elif name:
            items.append(f"{name} ({weight * 100:.1f}%)")
    return " / ".join(items) if items else "—"


def _format_exposure_slice(exposure: Dict[str, float], limit: int = 3) -> str:
    items = []
    for key, value in sorted(exposure.items(), key=lambda item: float(item[1]), reverse=True)[:limit]:
        weight = float(value)
        if weight <= 0:
            continue
        items.append(f"{key} {weight * 100:.1f}%")
    return " / ".join(items) if items else "—"


def _style_bucket(sector: str, asset_type: str = "") -> tuple[str, float]:
    text = f"{sector} {asset_type}".lower()
    if any(term in text for term in ("红利", "高股息", "银行", "电力", "公用", "黄金", "债", "货币", "低波", "防御")):
        return "防守", -1.0
    if any(term in text for term in ("科技", "ai", "半导体", "通信", "软件", "互联网", "成长", "新能源", "创新药", "军工", "机器人", "芯片")):
        return "进攻", 1.0
    if any(term in text for term in ("有色", "煤炭", "化工", "周期", "基建", "地产", "券商", "金融")):
        return "顺周期", 0.4
    if any(term in text for term in ("宽基", "指数", "中证", "沪深", "上证", "全指", "核心")):
        return "均衡", 0.0
    return "中性", 0.0


def _safe_text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text or default


def _analysis_sector_label(row: Mapping[str, Any]) -> str:
    metadata = dict(row.get("metadata") or {})
    return (
        _safe_text(row.get("sector"))
        or _safe_text(metadata.get("sector"))
        or _safe_text(metadata.get("hard_sector_label"))
        or "UNKNOWN"
    )


def _analysis_region_label(row: Mapping[str, Any]) -> str:
    metadata = dict(row.get("metadata") or {})
    region = _safe_text(row.get("region")) or _safe_text(metadata.get("region"))
    if region:
        return region.upper()
    asset_type = _safe_text(row.get("asset_type")).lower()
    if asset_type in {"cn_stock", "cn_etf", "cn_fund", "future", "futures"}:
        return "CN"
    if asset_type == "hk":
        return "HK"
    if asset_type == "us":
        return "US"
    return "UNKNOWN"


def build_candidate_set_linkage_summary(candidates: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    rows = [dict(item or {}) for item in list(candidates or []) if dict(item or {})]
    if len(rows) < 2:
        return {}

    normalized: List[Dict[str, str]] = []
    for row in rows:
        symbol = _safe_text(row.get("symbol"), "—")
        sector = _analysis_sector_label(row)
        region = _analysis_region_label(row)
        asset_type = _safe_text(row.get("asset_type"))
        style_bucket, _ = _style_bucket(sector, asset_type)
        normalized.append(
            {
                "symbol": symbol,
                "sector": sector,
                "region": region,
                "style_bucket": style_bucket,
            }
        )

    known_sectors = [row["sector"] for row in normalized if row["sector"] != "UNKNOWN"]
    known_regions = [row["region"] for row in normalized if row["region"] != "UNKNOWN"]
    sector_counts = Counter(known_sectors)
    region_counts = Counter(known_regions)
    style_counts = Counter(row["style_bucket"] for row in normalized if row["style_bucket"])

    overlap_label = "跨主线对比"
    summary_line = "这组候选来自不同主线，更适合做跨方向取舍。"
    if known_sectors and len(set(known_sectors)) == 1 and len(known_sectors) == len(normalized):
        overlap_label = "同一行业主线对比"
        summary_line = f"这组候选本质上都在比较 `{known_sectors[0]}` 这条线里的内部优先级，不是跨主线切换。"
    elif sector_counts:
        top_sector, top_sector_count = sector_counts.most_common(1)[0]
        if top_sector_count >= 2:
            overlap_label = "部分同主线重合"
            summary_line = f"这组候选里有 `{top_sector_count}` 只都指向 `{top_sector}`，分散效果有限，更像同一主线内部比较。"

    region_line = "地区上有分散，能帮助判断是同市场内部优先级，还是跨市场取舍。"
    if known_regions and len(set(known_regions)) == 1 and len(known_regions) == len(normalized):
        region_line = f"地区上都在 `{known_regions[0]}`，地域分散度有限。"
    elif region_counts:
        top_region, top_region_count = region_counts.most_common(1)[0]
        if top_region_count >= 2:
            region_line = f"地区上有 `{top_region_count}` 只都在 `{top_region}`，区域分散效果一般。"

    style_summary_line = "风格上存在分化，更适合作为组合补位比较。"
    style_priority_hint = "如果只选一个，先看哪条线的技术、催化和基本面共振更完整。"
    if style_counts:
        top_style, top_style_count = style_counts.most_common(1)[0]
        if len(style_counts) == 1:
            style_summary_line = f"风格上都偏 `{top_style}`，更适合比较执行节奏和景气强弱，而不是把它们当成完全分散的两条线。"
            style_priority_hint = "如果只想选一个，优先看谁的确认条件更完整，不宜把它们当成完全分散的双押。"
        elif top_style_count >= 2:
            style_summary_line = f"风格上至少有 `{top_style_count}` 只都偏 `{top_style}`，但还存在一定分化。"
            style_priority_hint = "这组更像主线相近、风格部分重合的选择题；除非组合本来缺这类暴露，否则不宜同时重仓。"

    detail_lines = [
        "主线分布: " + " / ".join(f"`{row['symbol']}` {row['sector']}" for row in normalized),
        "地区分布: " + " / ".join(f"`{row['symbol']}` {row['region']}" for row in normalized),
        "风格分布: " + " / ".join(f"`{row['symbol']}` {row['style_bucket']}" for row in normalized),
    ]
    return {
        "overlap_label": overlap_label,
        "summary_line": summary_line,
        "region_line": region_line,
        "style_summary_line": style_summary_line,
        "style_priority_hint": style_priority_hint,
        "detail_lines": detail_lines,
    }


def build_portfolio_overlap_summary(
    status: Dict[str, Any],
    *,
    candidate_symbol: str = "",
    candidate_name: str = "",
    candidate_sector: str = "",
    candidate_region: str = "",
    candidate_asset_type: str = "",
    existing_symbol_weight: float | None = None,
    projected_weight: float | None = None,
    projected_sector_weight: float | None = None,
    projected_region_weight: float | None = None,
    suggested_max_weight: float | None = None,
    sector_limit: float | None = None,
    region_limit: float | None = None,
) -> Dict[str, Any]:
    holdings = list(status.get("holdings", []) or [])
    sector_exposure = {str(key): float(value) for key, value in dict(status.get("sector_exposure", {}) or {}).items()}
    region_exposure = {str(key): float(value) for key, value in dict(status.get("region_exposure", {}) or {}).items()}
    top_sector, top_sector_weight = _top_exposure(sector_exposure)
    top_region, top_region_weight = _top_exposure(region_exposure)

    candidate_symbol = str(candidate_symbol or "").strip()
    candidate_name = str(candidate_name or "").strip()
    candidate_sector = str(candidate_sector or "").strip() or "UNKNOWN"
    candidate_region = str(candidate_region or "").strip() or "UNKNOWN"
    existing_symbol_weight = float(existing_symbol_weight or 0.0)

    same_symbol_rows = (
        [row for row in holdings if str(row.get("symbol")) == candidate_symbol] if candidate_symbol and existing_symbol_weight > 0 else []
    )
    same_sector_rows = [row for row in holdings if str(row.get("sector", "UNKNOWN") or "UNKNOWN") == candidate_sector]
    same_region_rows = [row for row in holdings if str(row.get("region", "UNKNOWN") or "UNKNOWN") == candidate_region]
    same_sector_weight = sum(float(row.get("weight", 0.0)) for row in same_sector_rows)
    same_region_weight = sum(float(row.get("weight", 0.0)) for row in same_region_rows)
    same_sector_count = len(same_sector_rows)
    same_region_count = len(same_region_rows)

    style_exposure: Dict[str, float] = {}
    style_weighted_score = 0.0
    for row in holdings:
        bucket, score = _style_bucket(str(row.get("sector", "UNKNOWN") or "UNKNOWN"), str(row.get("asset_type", "") or ""))
        weight = float(row.get("weight", 0.0))
        style_exposure[bucket] = style_exposure.get(bucket, 0.0) + weight
        style_weighted_score += weight * score
    top_style_bucket, top_style_weight = _top_exposure(style_exposure)
    if style_weighted_score >= 0.25:
        style_direction_label = "进攻偏重"
    elif style_weighted_score <= -0.25:
        style_direction_label = "防守偏重"
    else:
        style_direction_label = "均衡"
    style_summary_line = (
        f"当前组合风格偏 `{style_direction_label}`，最重风格是 `{top_style_bucket}` `{top_style_weight * 100:.1f}%`。"
    )
    style_detail_line = f"风格暴露: {_format_exposure_slice(style_exposure)}"
    candidate_style_bucket, candidate_style_score = _style_bucket(candidate_sector, candidate_asset_type)
    candidate_style_line = ""
    style_conflict_label = "暂未看到明显风格冲突"
    style_priority_hint = "当前风格已足够分散，优先级仍以主线判断为准。"
    if candidate_symbol:
        candidate_style_line = f"候选风格: `{candidate_style_bucket}`"
        if candidate_style_bucket == top_style_bucket and top_style_weight >= 0.35:
            style_conflict_label = "同风格重复较高"
            style_priority_hint = "如果只是同风格加码，优先级低于补新方向。"
        elif candidate_style_bucket == top_style_bucket:
            style_conflict_label = "同风格延伸"
            style_priority_hint = "这是同风格加码，适合在确认同主线后再做。"
        elif candidate_style_bucket == "均衡" and top_style_bucket != "均衡":
            style_conflict_label = "可做风格补位"
            style_priority_hint = "这条更像补组合风格缺口，而不是继续压同一风格。"
        elif candidate_style_score > 0 and style_weighted_score < 0:
            style_conflict_label = "风格补位"
            style_priority_hint = "候选更偏进攻，有助于补当前防守风格的缺口。"
        elif candidate_style_score < 0 and style_weighted_score > 0:
            style_conflict_label = "风格补位"
            style_priority_hint = "候选更偏防守，有助于补当前进攻风格的缺口。"

    overlap_label = "重复度较低"
    conflict_label = "暂未看到明显主线冲突"
    summary_line = (
        f"当前组合最集中在 `{top_sector}` `{top_sector_weight * 100:.1f}%` / "
        f"`{top_region}` `{top_region_weight * 100:.1f}%`。"
    )
    if candidate_symbol:
        if same_symbol_rows:
            overlap_label = "同一标的加码"
            summary_line = (
                f"这条建议是现有持仓 `{candidate_symbol}` 的加码，不是新方向；"
                f"当前同一标的权重约 `{float(same_symbol_rows[0].get('weight', 0.0)) * 100:.1f}%`。"
            )
        elif candidate_sector != "UNKNOWN" and candidate_sector == top_sector:
            overlap_label = "同一行业主线加码"
            summary_line = (
                f"这条建议和现有组合最重的行业 `{candidate_sector}` 同线，重复度较高，"
                "更像同一主线延伸，而不是完全新方向。"
            )
        elif same_sector_weight >= 0.20:
            overlap_label = "主题/行业重复较高"
            summary_line = (
                f"这条建议在 `{candidate_sector}` 上已有约 `{same_sector_weight * 100:.1f}%` 的组合暴露，"
                "新增更像同主题加码。"
            )
        elif candidate_region != "UNKNOWN" and candidate_region == top_region:
            overlap_label = "地区暴露偏重"
            summary_line = (
                f"这条建议和现有组合的 `{candidate_region}` 暴露重合度较高，"
                "分散效果有限。"
            )
        elif same_region_weight >= 0.35:
            overlap_label = "地区重复度偏高"
            summary_line = (
                f"这条建议在 `{candidate_region}` 上已有约 `{same_region_weight * 100:.1f}%` 的组合暴露，"
                "新增更像同区域再加码。"
            )
        if projected_weight is not None and suggested_max_weight is not None and projected_weight > suggested_max_weight + 1e-9:
            conflict_label = (
                f"预演仓位 `{projected_weight * 100:.1f}%` 已高于建议上限 `{suggested_max_weight * 100:.1f}%`。"
            )
        elif projected_sector_weight is not None and sector_limit is not None and projected_sector_weight > sector_limit + 1e-9:
            conflict_label = (
                f"预演后行业 `{candidate_sector}` 暴露 `{projected_sector_weight * 100:.1f}%`，"
                f"高于上限 `{sector_limit * 100:.1f}%`。"
            )
        elif projected_region_weight is not None and region_limit is not None and projected_region_weight > region_limit + 1e-9:
            conflict_label = (
                f"预演后地区 `{candidate_region}` 暴露 `{projected_region_weight * 100:.1f}%`，"
                f"高于上限 `{region_limit * 100:.1f}%`。"
            )
        if style_conflict_label == "同风格重复较高":
            conflict_label = f"{conflict_label} 风格上也在继续加同一主线。"
        elif style_conflict_label in {"同风格延伸", "风格补位", "可做风格补位"} and conflict_label == "暂未看到明显主线冲突":
            conflict_label = style_priority_hint
    else:
        if top_sector_weight >= 0.30:
            overlap_label = "行业集中度偏高"
            summary_line = (
                f"当前组合最集中在 `{top_sector}` `{top_sector_weight * 100:.1f}%`，"
                "后续如果再加同主题建议，更多是在加码同一主线。"
            )
        elif top_region_weight >= 0.40:
            overlap_label = "地区集中度偏高"
            summary_line = (
                f"当前组合最集中在 `{top_region}` `{top_region_weight * 100:.1f}%`，"
                "新增同区域建议的分散效果会比较有限。"
            )

    detail_lines = [
        f"当前最重行业: `{top_sector}` `{top_sector_weight * 100:.1f}%`",
        f"当前最重地区: `{top_region}` `{top_region_weight * 100:.1f}%`",
        style_detail_line,
        f"同主题/行业持仓: {_format_holdings_slice(same_sector_rows)}",
        f"同地区持仓: {_format_holdings_slice(same_region_rows)}",
    ]
    if candidate_symbol:
        detail_lines.append(f"候选风格: `{candidate_style_bucket}`")
    if same_symbol_rows:
        same_symbol_row = same_symbol_rows[0]
        detail_lines.append(
            f"同一标的当前权重: `{float(same_symbol_row.get('weight', 0.0)) * 100:.1f}%`"
            + (f"（{candidate_name}）" if candidate_name else "")
        )
    if projected_sector_weight is not None:
        detail_lines.append(f"预演后行业暴露: `{projected_sector_weight * 100:.1f}%`")
    if projected_region_weight is not None:
        detail_lines.append(f"预演后地区暴露: `{projected_region_weight * 100:.1f}%`")

    return {
        "candidate_symbol": candidate_symbol,
        "candidate_name": candidate_name,
        "candidate_sector": candidate_sector,
        "candidate_region": candidate_region,
        "top_sector": top_sector,
        "top_sector_weight": top_sector_weight,
        "top_region": top_region,
        "top_region_weight": top_region_weight,
        "style_direction_label": style_direction_label,
        "style_summary_line": style_summary_line,
        "style_detail_line": style_detail_line,
        "candidate_style_bucket": candidate_style_bucket,
        "candidate_style_line": candidate_style_line,
        "style_conflict_label": style_conflict_label,
        "style_priority_hint": style_priority_hint,
        "same_symbol_weight": float(same_symbol_rows[0].get("weight", 0.0)) if same_symbol_rows else 0.0,
        "same_sector_weight": same_sector_weight,
        "same_region_weight": same_region_weight,
        "same_sector_count": same_sector_count,
        "same_region_count": same_region_count,
        "overlap_label": overlap_label,
        "conflict_label": conflict_label,
        "summary_line": summary_line,
        "detail_lines": detail_lines,
    }


class PortfolioRepository:
    """JSON-backed portfolio repository."""

    def __init__(
        self,
        portfolio_path: Path = PROJECT_ROOT / "data" / "portfolio.json",
        trade_log_path: Path = PROJECT_ROOT / "data" / "trade_log.json",
    ) -> None:
        self.portfolio_path = portfolio_path
        self.trade_log_path = trade_log_path

    def load(self) -> Dict[str, Any]:
        payload = load_json(self.portfolio_path, default=DEFAULT_PORTFOLIO)
        if not payload:
            payload = dict(DEFAULT_PORTFOLIO)
        payload.setdefault("holdings", [])
        return payload

    def save(self, payload: Dict[str, Any]) -> None:
        save_json(self.portfolio_path, payload)

    def list_holdings(self) -> List[Dict[str, Any]]:
        return list(self.load().get("holdings", []))

    def upsert_holding(self, holding: Dict[str, Any]) -> None:
        payload = self.load()
        holdings = payload.get("holdings", [])
        for index, existing in enumerate(holdings):
            if existing["symbol"] == holding["symbol"]:
                holdings[index] = holding
                payload["holdings"] = holdings
                self.save(payload)
                return
        holdings.append(holding)
        payload["holdings"] = holdings
        self.save(payload)

    def remove_holding(self, symbol: str) -> None:
        payload = self.load()
        payload["holdings"] = [item for item in payload.get("holdings", []) if item["symbol"] != symbol]
        self.save(payload)

    def set_target_weight(self, symbol: str, weight: float) -> Dict[str, Any]:
        holdings = self.list_holdings()
        for holding in holdings:
            if holding["symbol"] == symbol:
                holding["target_weight"] = weight
                self.upsert_holding(holding)
                return holding
        raise ValueError(f"Holding not found: {symbol}")

    def log_trade(
        self,
        action: str,
        symbol: str,
        name: str,
        asset_type: str,
        price: float,
        amount: float,
        region: str = "",
        sector: str = "",
        basis: str = "rule",
        note: str = "",
        signal_snapshot: Optional[Dict[str, Any]] = None,
        thesis_snapshot: Optional[Dict[str, Any]] = None,
        decision_snapshot: Optional[Dict[str, Any]] = None,
        execution_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload = self.load()
        holdings = {item["symbol"]: item for item in payload.get("holdings", [])}
        quantity_delta = amount / price if price else 0.0
        holding = holdings.get(
            symbol,
            {
                "symbol": symbol,
                "name": name or symbol,
                "asset_type": asset_type,
                "quantity": 0.0,
                "cost_basis": 0.0,
                "region": region,
                "sector": sector,
                "target_weight": None,
            },
        )

        current_qty = float(holding.get("quantity", 0.0))
        current_cost = float(holding.get("cost_basis", 0.0))
        if action == "buy":
            new_qty = current_qty + quantity_delta
            new_cost = ((current_qty * current_cost) + amount) / new_qty if new_qty > 0 else price
            holding["quantity"] = new_qty
            holding["cost_basis"] = new_cost
        elif action == "sell":
            if quantity_delta > current_qty + 1e-9:
                raise ValueError(f"Sell quantity exceeds current holding for {symbol}")
            new_qty = current_qty - quantity_delta
            if new_qty <= 1e-9:
                self.remove_holding(symbol)
                holding["quantity"] = 0.0
            else:
                holding["quantity"] = new_qty
                self.upsert_holding(holding)
        else:
            raise ValueError(f"Unsupported action: {action}")

        if action == "buy":
            self.upsert_holding(holding)

        trades = load_json(self.trade_log_path, default=[]) or []
        trades.append(
            {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "action": action,
                "symbol": symbol,
                "name": holding.get("name", name or symbol),
                "asset_type": asset_type,
                "price": price,
                "amount": amount,
                "quantity": quantity_delta,
                "basis": basis,
                "note": note,
                "signal_snapshot": signal_snapshot or {},
                "thesis_snapshot": thesis_snapshot or {},
                "decision_snapshot": decision_snapshot or {},
                "execution_snapshot": execution_snapshot or {},
            }
        )
        save_json(self.trade_log_path, trades)
        return holding

    def list_trades(self) -> List[Dict[str, Any]]:
        return list(load_json(self.trade_log_path, default=[]) or [])

    def monthly_review(self, month: str, latest_prices: Dict[str, float]) -> Dict[str, Any]:
        trades = [trade for trade in self.list_trades() if str(trade.get("timestamp", "")).startswith(month)]
        by_basis: Dict[str, Dict[str, float]] = {}
        detailed: List[Dict[str, Any]] = []
        for trade in trades:
            symbol = trade["symbol"]
            latest_price = latest_prices.get(symbol, trade["price"])
            action = trade["action"]
            trade_return = (latest_price / trade["price"] - 1) if trade["price"] else 0.0
            outcome = trade_return if action == "buy" else -trade_return
            basis = trade.get("basis", "unknown")
            stats = by_basis.setdefault(basis, {"count": 0, "avg_outcome": 0.0, "wins": 0})
            stats["count"] += 1
            stats["avg_outcome"] += outcome
            stats["wins"] += 1 if outcome > 0 else 0
            detailed.append({**trade, "latest_price": latest_price, "outcome": outcome})

        for basis, stats in by_basis.items():
            count = stats["count"] or 1
            stats["avg_outcome"] /= count
            stats["win_rate"] = stats["wins"] / count

        return {"month": month, "trades": detailed, "basis_stats": by_basis}

    def build_status(self, latest_prices: Dict[str, float]) -> Dict[str, Any]:
        holdings = self.list_holdings()
        rows: List[Dict[str, Any]] = []
        total_value = 0.0
        for holding in holdings:
            latest = latest_prices.get(holding["symbol"], holding.get("cost_basis", 0.0))
            market_value = latest * float(holding.get("quantity", 0.0))
            total_value += market_value
            pnl = (latest - float(holding.get("cost_basis", 0.0))) * float(holding.get("quantity", 0.0))
            rows.append(
                {
                    **holding,
                    "latest_price": latest,
                    "market_value": market_value,
                    "pnl": pnl,
                }
            )

        for row in rows:
            row["weight"] = row["market_value"] / total_value if total_value else 0.0

        region_exposure: Dict[str, float] = {}
        sector_exposure: Dict[str, float] = {}
        for row in rows:
            region = row.get("region", "UNKNOWN") or "UNKNOWN"
            sector = row.get("sector", "UNKNOWN") or "UNKNOWN"
            region_exposure[region] = region_exposure.get(region, 0.0) + row["weight"]
            sector_exposure[sector] = sector_exposure.get(sector, 0.0) + row["weight"]

        return {
            "base_currency": self.load().get("base_currency", "CNY"),
            "total_value": total_value,
            "holdings": rows,
            "region_exposure": region_exposure,
            "sector_exposure": sector_exposure,
        }

    def rebalance_suggestions(self, latest_prices: Dict[str, float], threshold: float = 0.05) -> List[Dict[str, Any]]:
        status = self.build_status(latest_prices)
        total_value = status["total_value"]
        suggestions: List[Dict[str, Any]] = []
        for row in status["holdings"]:
            target = row.get("target_weight")
            if target is None:
                continue
            diff = row["weight"] - target
            if abs(diff) < threshold:
                continue
            suggestions.append(
                {
                    "symbol": row["symbol"],
                    "current_weight": row["weight"],
                    "target_weight": target,
                    "difference": diff,
                    "action": "reduce" if diff > 0 else "add",
                    "amount": abs(diff) * total_value,
                }
            )
        return suggestions
