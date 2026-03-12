"""Tests for macro context snapshots."""

from __future__ import annotations

import pandas as pd

from src.processors.context import load_china_macro_snapshot, macro_lines


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

    def get_ppi(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "month": ["202602", "202601"],
                "ppi_yoy": [-0.9, -1.4],
            }
        )

    def get_money_supply(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "month": ["202601", "202512"],
                "m1_yoy": [4.9, 3.8],
                "m2_yoy": [9.0, 8.5],
            }
        )

    def get_social_financing(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "月份": ["202601", "202512", "202511", "202510", "202509", "202508"],
                "社会融资规模增量": [70600, 32286, 23958, 18627, 27566, 30300],
            }
        )


def test_load_china_macro_snapshot_supports_tushare_columns(monkeypatch) -> None:
    monkeypatch.setattr("src.processors.context.ChinaMacroCollector", _FakeMacroCollector)

    snapshot = load_china_macro_snapshot({})

    assert snapshot["pmi"] == 50.1
    assert snapshot["pmi_prev"] == 49.2
    assert snapshot["cpi_monthly"] == 1.3
    assert snapshot["cpi_prev"] == 0.2
    assert snapshot["ppi_yoy"] == -0.9
    assert snapshot["ppi_prev"] == -1.4
    assert snapshot["m1_m2_spread"] == -4.1
    assert snapshot["credit_impulse"] == "expanding"
    assert snapshot["lpr_1y"] == 3.0
    assert snapshot["lpr_prev"] == 3.1
    assert snapshot["demand_state"] == "improving"


def test_load_china_macro_snapshot_supports_legacy_columns(monkeypatch) -> None:
    class _LegacyMacroCollector(_FakeMacroCollector):
        def get_pmi(self) -> pd.DataFrame:
            return pd.DataFrame({"月份": ["2026-02", "2026-01"], "制造业-指数": [50.6, 50.2]})

        def get_cpi(self) -> pd.DataFrame:
            return pd.DataFrame({"月份": ["2026-02", "2026-01"], "今值": [0.7, 0.5]})

        def get_lpr(self) -> pd.DataFrame:
            return pd.DataFrame({"日期": ["2026-02-20", "2026-01-20"], "LPR1Y": [3.1, 3.2]})

        def get_ppi(self) -> pd.DataFrame:
            return pd.DataFrame({"月份": ["2026-02", "2026-01"], "PPI同比": [-0.8, -1.2]})

    monkeypatch.setattr("src.processors.context.ChinaMacroCollector", _LegacyMacroCollector)

    snapshot = load_china_macro_snapshot({})

    assert snapshot["pmi"] == 50.6
    assert snapshot["cpi_monthly"] == 0.7
    assert snapshot["ppi_yoy"] == -0.8
    assert snapshot["lpr_1y"] == 3.1


def test_macro_lines_surface_leading_indicators() -> None:
    lines = macro_lines(
        {
            "pmi": 50.1,
            "pmi_trend": "rising",
            "pmi_new_orders": 50.8,
            "pmi_production": 51.7,
            "ppi_yoy": -0.9,
            "ppi_trend": "rising",
            "cpi_monthly": 1.3,
            "cpi_trend": "rising",
            "m1_m2_spread": -4.1,
            "m1_m2_spread_trend": "rising",
            "social_financing_3m_avg_text": "2.56 万亿元",
            "credit_impulse": "expanding",
            "lpr_1y": 3.0,
        },
        {"dxy": 100.2, "dxy_20d_change": -0.02},
    )

    assert any("新订单" in line for line in lines)
    assert any("PPI" in line for line in lines)
    assert any("M1-M2" in line for line in lines)
    assert any("信用脉冲" in line for line in lines)
