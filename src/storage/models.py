"""Dataclass models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass(slots=True)
class MarketBar:
    symbol: str
    asset_type: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    amount: Optional[float] = None
    interval: str = "1d"


@dataclass(slots=True)
class MacroObservation:
    series_name: str
    observation_date: str
    value: float
    source: str
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class Holding:
    symbol: str
    name: str
    weight: float
    cost_basis: float


@dataclass(slots=True)
class Thesis:
    symbol: str
    core_assumption: str
    validation_metric: str
    stop_condition: str
    holding_period: str
    created_at: str
