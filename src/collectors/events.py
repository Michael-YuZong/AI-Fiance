"""Local event calendar collector for briefing generation."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from src.utils.config import resolve_project_path
from src.utils.data import load_yaml


class EventsCollector:
    """Load manually maintained event calendars from local YAML."""

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        self.config = dict(config or {})
        self.calendar_path = resolve_project_path(
            self.config.get("briefing_calendar_file", "config/event_calendar.yaml")
        )

    def collect(self, mode: str = "daily", as_of: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Return one-off and recurring event items for the target day."""
        now = as_of or datetime.now()
        payload = load_yaml(self.calendar_path, default={}) or {}
        events: List[Dict[str, Any]] = []

        for item in payload.get("one_off", []) or []:
            if str(item.get("date", "")) == now.strftime("%Y-%m-%d"):
                events.append(dict(item))

        for item in payload.get("recurring", []) or []:
            weekdays = [int(day) for day in item.get("weekdays", [])]
            if weekdays and now.weekday() not in weekdays:
                continue
            if item.get("mode") not in {"", None, mode, "both"}:
                continue
            record = dict(item)
            record.setdefault("date", now.strftime("%Y-%m-%d"))
            events.append(record)

        importance_rank = {"high": 0, "medium": 1, "low": 2}
        return sorted(
            events,
            key=lambda row: (
                str(row.get("time", "99:99")),
                importance_rank.get(str(row.get("importance", "")).lower(), 9),
            ),
        )
