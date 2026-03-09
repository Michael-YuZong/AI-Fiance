"""Thesis JSON storage."""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List

from src.utils.config import PROJECT_ROOT
from src.utils.data import load_json, save_json


class ThesisRepository:
    """JSON-backed thesis storage."""

    def __init__(self, thesis_path: Path = PROJECT_ROOT / "data" / "thesis.json") -> None:
        self.thesis_path = thesis_path

    def load(self) -> Dict[str, Any]:
        return load_json(self.thesis_path, default={}) or {}

    def save(self, payload: Dict[str, Any]) -> None:
        save_json(self.thesis_path, payload)

    def get(self, symbol: str):
        return self.load().get(symbol)

    def list_all(self) -> List[Dict[str, Any]]:
        payload = self.load()
        return [{**value, "symbol": key} for key, value in sorted(payload.items())]

    def upsert(
        self,
        symbol: str,
        core_assumption: str,
        validation_metric: str,
        stop_condition: str,
        holding_period: str,
    ) -> Dict[str, Any]:
        payload = self.load()
        existing = payload.get(symbol, {})
        record = {
            "core_assumption": core_assumption,
            "validation_metric": validation_metric,
            "stop_condition": stop_condition,
            "holding_period": holding_period,
            "created_at": existing.get("created_at", datetime.now().strftime("%Y-%m-%d")),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        payload[symbol] = record
        self.save(payload)
        return {"symbol": symbol, **record}

    def delete(self, symbol: str) -> bool:
        payload = self.load()
        if symbol not in payload:
            return False
        del payload[symbol]
        self.save(payload)
        return True
