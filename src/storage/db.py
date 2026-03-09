"""SQLite storage layer."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd

from src.processors.technical import normalize_ohlcv_frame


class DatabaseManager:
    """SQLite database manager."""

    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.db_path)
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS market_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    asset_type TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    interval TEXT NOT NULL DEFAULT '1d',
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL,
                    amount REAL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(symbol, asset_type, trade_date, interval)
                );

                CREATE TABLE IF NOT EXISTS macro_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    series_name TEXT NOT NULL,
                    observation_date TEXT NOT NULL,
                    value REAL NOT NULL,
                    source TEXT NOT NULL,
                    metadata_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(series_name, observation_date)
                );
                """
            )

    def save_market_data(self, symbol: str, asset_type: str, df: pd.DataFrame, interval: str = "1d") -> int:
        normalized = normalize_ohlcv_frame(df)
        now = datetime.utcnow().isoformat(timespec="seconds")
        rows = [
            (
                symbol,
                asset_type,
                row["date"].strftime("%Y-%m-%d"),
                interval,
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]),
                None if pd.isna(row["amount"]) else float(row["amount"]),
                now,
                now,
            )
            for _, row in normalized.iterrows()
        ]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO market_data (
                    symbol, asset_type, trade_date, interval, open, high, low, close,
                    volume, amount, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, asset_type, trade_date, interval)
                DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    amount = excluded.amount,
                    updated_at = excluded.updated_at
                """,
                rows,
            )
        return len(rows)

    def save_macro_data(
        self,
        series_name: str,
        observation_date: str,
        value: float,
        source: str,
        metadata: Optional[dict] = None,
    ) -> None:
        now = datetime.utcnow().isoformat(timespec="seconds")
        metadata_json = json.dumps(metadata or {}, ensure_ascii=False)
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO macro_data (
                    series_name, observation_date, value, source, metadata_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(series_name, observation_date)
                DO UPDATE SET
                    value = excluded.value,
                    source = excluded.source,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (series_name, observation_date, value, source, metadata_json, now, now),
            )

    def fetch_latest_market_data(self, symbol: str, limit: int = 30) -> pd.DataFrame:
        with self.connect() as connection:
            return pd.read_sql_query(
                """
                SELECT symbol, asset_type, trade_date, interval, open, high, low, close, volume, amount
                FROM market_data
                WHERE symbol = ?
                ORDER BY trade_date DESC
                LIMIT ?
                """,
                connection,
                params=(symbol, limit),
            )
