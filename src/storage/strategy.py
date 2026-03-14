"""JSON-backed storage for strategy prediction ledger."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from src.utils.config import PROJECT_ROOT
from src.utils.data import load_json, save_json


DEFAULT_STRATEGY_LEDGER = {"predictions": []}


class StrategyRepository:
    """Persist strategy prediction snapshots as JSON."""

    def __init__(
        self,
        ledger_path: Path = PROJECT_ROOT / "data" / "strategy_predictions.json",
    ) -> None:
        self.ledger_path = ledger_path

    def load(self) -> Dict[str, Any]:
        payload = load_json(self.ledger_path, default=DEFAULT_STRATEGY_LEDGER)
        if not payload:
            payload = dict(DEFAULT_STRATEGY_LEDGER)
        payload.setdefault("predictions", [])
        return payload

    def save(self, payload: Dict[str, Any]) -> None:
        save_json(self.ledger_path, payload)

    def list_predictions(
        self,
        *,
        symbol: str = "",
        status: str = "",
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        rows = list(self.load().get("predictions", []))
        if symbol:
            rows = [row for row in rows if str(row.get("symbol", "")).strip() == str(symbol).strip()]
        if status and status != "all":
            rows = [row for row in rows if str(row.get("status", "")) == status]
        rows.sort(
            key=lambda row: (
                str(row.get("as_of", "")),
                str(row.get("created_at", "")),
                str(row.get("prediction_id", "")),
            ),
            reverse=True,
        )
        if limit is not None:
            rows = rows[: max(int(limit), 0)]
        return rows

    def get_prediction(self, prediction_id: str) -> Optional[Dict[str, Any]]:
        for row in self.load().get("predictions", []):
            if str(row.get("prediction_id", "")) == str(prediction_id):
                return dict(row)
        return None

    def upsert_prediction(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ledger = self.load()
        rows = list(ledger.get("predictions", []))
        prediction_id = str(payload.get("prediction_id", "")).strip()
        if not prediction_id:
            raise ValueError("prediction_id is required")
        for index, existing in enumerate(rows):
            if str(existing.get("prediction_id", "")) == prediction_id:
                rows[index] = payload
                ledger["predictions"] = rows
                self.save(ledger)
                return payload
        rows.append(payload)
        ledger["predictions"] = rows
        self.save(ledger)
        return payload
