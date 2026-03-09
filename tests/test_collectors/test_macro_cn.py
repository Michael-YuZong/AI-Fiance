"""Tests for China macro collector."""

from __future__ import annotations

import pandas as pd

import src.collectors.macro_cn as macro_cn_module
from src.collectors.macro_cn import ChinaMacroCollector


class _FakeAk:
    @staticmethod
    def macro_china_pmi():
        return pd.DataFrame({"月份": ["2026-01"], "PMI": [50.2]})


def test_get_pmi_returns_dataframe(monkeypatch, tmp_path):
    monkeypatch.setattr(macro_cn_module, "ak", _FakeAk)
    collector = ChinaMacroCollector(config={"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 1}})
    result = collector.get_pmi()
    assert not result.empty
    assert "PMI" in result.columns
