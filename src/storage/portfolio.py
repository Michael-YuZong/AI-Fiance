"""Portfolio JSON storage and basic analytics."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.utils.config import PROJECT_ROOT
from src.utils.data import load_json, save_json


DEFAULT_PORTFOLIO = {"base_currency": "CNY", "holdings": []}


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
