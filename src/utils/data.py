"""Helpers for YAML/JSON project data files."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from .config import PROJECT_ROOT


def load_json(path: Path, default: Optional[Any] = None) -> Any:
    if not path.exists():
        return deepcopy(default)
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def load_yaml(path: Path, default: Optional[Any] = None) -> Any:
    if not path.exists():
        return deepcopy(default)
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def load_watchlist(path: Path = PROJECT_ROOT / "config" / "watchlist.yaml") -> List[Dict[str, Any]]:
    payload = load_yaml(path, default={"watchlist": []}) or {"watchlist": []}
    return list(payload.get("watchlist", []))


def load_strategy_batches(path: Path = PROJECT_ROOT / "config" / "strategy_batches.yaml") -> Dict[str, Any]:
    payload = load_yaml(path, default={"batch_sources": {}, "cohort_recipes": {}}) or {"batch_sources": {}, "cohort_recipes": {}}
    payload.setdefault("batch_sources", {})
    payload.setdefault("cohort_recipes", {})
    return payload


def load_asset_aliases(path: Path = PROJECT_ROOT / "config" / "asset_aliases.yaml") -> List[Dict[str, Any]]:
    payload = load_yaml(path, default={"aliases": []}) or {"aliases": []}
    return list(payload.get("aliases", []))
