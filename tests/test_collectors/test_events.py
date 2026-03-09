"""Tests for local event calendar collection."""

from __future__ import annotations

from datetime import datetime

from src.collectors.events import EventsCollector


def test_events_collector_reads_recurring_calendar():
    collector = EventsCollector({"briefing_calendar_file": "config/event_calendar.yaml"})
    events = collector.collect(mode="daily", as_of=datetime(2026, 3, 9, 8, 0, 0))
    assert events
    assert "title" in events[0]
