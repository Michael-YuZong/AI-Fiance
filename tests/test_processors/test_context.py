"""Tests for macro context snapshots."""

from __future__ import annotations

import pandas as pd

from src.processors.context import load_china_macro_snapshot


class _FakeMacroCollector:
    def __init__(self, config):  # noqa: ANN001, D401
        self.config = config

    def get_pmi(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "MONTH": ["202512", "202511"],
                "PMI010000": [50.1, 49.2],
            }
        )

    def get_cpi(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "month": ["202602", "202601"],
                "nt_yoy": [1.3, 0.2],
            }
        )

    def get_lpr(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": ["20260120", "20251222"],
                "1y": [3.0, 3.1],
            }
        )


def test_load_china_macro_snapshot_supports_tushare_columns(monkeypatch) -> None:
    monkeypatch.setattr("src.processors.context.ChinaMacroCollector", _FakeMacroCollector)

    snapshot = load_china_macro_snapshot({})

    assert snapshot["pmi"] == 50.1
    assert snapshot["pmi_prev"] == 49.2
    assert snapshot["cpi_monthly"] == 1.3
    assert snapshot["cpi_prev"] == 0.2
    assert snapshot["lpr_1y"] == 3.0
    assert snapshot["lpr_prev"] == 3.1


def test_load_china_macro_snapshot_supports_legacy_columns(monkeypatch) -> None:
    class _LegacyMacroCollector(_FakeMacroCollector):
        def get_pmi(self) -> pd.DataFrame:
            return pd.DataFrame({"月份": ["2026-02", "2026-01"], "制造业-指数": [50.6, 50.2]})

        def get_cpi(self) -> pd.DataFrame:
            return pd.DataFrame({"月份": ["2026-02", "2026-01"], "今值": [0.7, 0.5]})

        def get_lpr(self) -> pd.DataFrame:
            return pd.DataFrame({"日期": ["2026-02-20", "2026-01-20"], "LPR1Y": [3.1, 3.2]})

    monkeypatch.setattr("src.processors.context.ChinaMacroCollector", _LegacyMacroCollector)

    snapshot = load_china_macro_snapshot({})

    assert snapshot["pmi"] == 50.6
    assert snapshot["cpi_monthly"] == 0.7
    assert snapshot["lpr_1y"] == 3.1
