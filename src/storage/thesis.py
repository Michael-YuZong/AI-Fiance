"""Thesis JSON storage."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

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
