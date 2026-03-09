"""Configuration loading utilities."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping, Optional, Union

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"
EXAMPLE_CONFIG_PATH = PROJECT_ROOT / "config" / "config.example.yaml"


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Config file must contain a mapping: {path}")
    return loaded


def _deep_merge(base: MutableMapping[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = _deep_merge(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged


def resolve_project_path(path_value: Union[str, Path]) -> Path:
    candidate = Path(path_value)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def ensure_runtime_directories(config: Mapping[str, Any]) -> None:
    storage = config.get("storage", {})
    db_path = resolve_project_path(storage.get("db_path", "data/investment.db"))
    cache_dir = resolve_project_path(storage.get("cache_dir", "data/cache"))
    db_path.parent.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)


def load_config(config_path: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
    """Load runtime configuration and resolve relative paths from project root."""
    base_config = _read_yaml(EXAMPLE_CONFIG_PATH)
    target_path = resolve_project_path(config_path) if config_path else DEFAULT_CONFIG_PATH
    target_config = _read_yaml(target_path)
    config = _deep_merge(base_config, target_config)
    storage = config.setdefault("storage", {})
    storage["db_path"] = str(resolve_project_path(storage.get("db_path", "data/investment.db")))
    storage["cache_dir"] = str(resolve_project_path(storage.get("cache_dir", "data/cache")))
    ensure_runtime_directories(config)
    return config


def detect_asset_type(symbol: str, config: Mapping[str, Any]) -> str:
    """Infer asset type from configured regex mapping."""
    mapping = config.get("asset_type_mapping", {})
    for pattern, asset_type in mapping.items():
        if re.match(pattern, symbol):
            return str(asset_type)
    return "unknown"
