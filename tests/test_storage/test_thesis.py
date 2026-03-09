"""Tests for thesis repository."""

from __future__ import annotations

from pathlib import Path

from src.storage.thesis import ThesisRepository


def test_thesis_repository_upsert_and_delete(tmp_path: Path):
    repo = ThesisRepository(thesis_path=tmp_path / "thesis.json")
    record = repo.upsert(
        symbol="561380",
        core_assumption="电网投资提升",
        validation_metric="投资完成额同比 > 10%",
        stop_condition="估值过高且增速下滑",
        holding_period="6-12个月",
    )
    assert record["symbol"] == "561380"
    assert repo.get("561380")["core_assumption"] == "电网投资提升"
    assert len(repo.list_all()) == 1
    assert repo.delete("561380") is True
    assert repo.get("561380") is None
