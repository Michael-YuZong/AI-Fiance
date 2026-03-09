"""Tests for SQLite storage layer."""

from __future__ import annotations

import pandas as pd

from src.storage.db import DatabaseManager


def test_database_initialization_and_market_upsert(tmp_path):
    db = DatabaseManager(str(tmp_path / "investment.db"))
    db.initialize()
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=3, freq="D"),
            "open": [1.0, 1.1, 1.2],
            "high": [1.1, 1.2, 1.3],
            "low": [0.9, 1.0, 1.1],
            "close": [1.05, 1.15, 1.25],
            "volume": [100, 110, 120],
        }
    )
    inserted = db.save_market_data("561380", "cn_etf", frame)
    stored = db.fetch_latest_market_data("561380", limit=5)
    assert inserted == 3
    assert len(stored) == 3
