from __future__ import annotations

import json
from pathlib import Path

from src.commands import catalyst_review


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_catalyst_review_next_with_prompt(monkeypatch, tmp_path: Path, capsys) -> None:
    payload = tmp_path / "reports/scans/etfs/internal/scan_512480_2026-03-26_catalyst_web_review_payload.json"
    prompt = tmp_path / "reports/scans/etfs/internal/scan_512480_2026-03-26_catalyst_web_review_prompt.md"
    _write(
        payload,
        json.dumps(
            {
                "report_type": "scan",
                "subject": "半导体ETF",
                "generated_at": "2026-03-26 10:00:00",
                "items": [{"symbol": "512480"}],
            },
            ensure_ascii=False,
        ),
    )
    _write(prompt, "# Catalyst Web Review Prompt\n\n- demo")

    monkeypatch.setattr(catalyst_review, "resolve_project_path", lambda path="": tmp_path / str(path))
    monkeypatch.setattr("sys.argv", ["catalyst_review", "next", "--root", "reports", "--with-prompt"])
    catalyst_review.main()
    output = capsys.readouterr().out
    assert "# Next Catalyst Web Review Task" in output
    assert "半导体ETF" in output
    assert "## Prompt" in output


def test_catalyst_review_list_filters_status(monkeypatch, tmp_path: Path, capsys) -> None:
    payload = tmp_path / "reports/scans/etfs/internal/scan_512480_2026-03-26_catalyst_web_review_payload.json"
    review = tmp_path / "reports/scans/etfs/internal/scan_512480_2026-03-26_catalyst_web_review.md"
    _write(
        payload,
        json.dumps(
            {
                "report_type": "scan",
                "subject": "半导体ETF",
                "generated_at": "2026-03-26 10:00:00",
                "items": [{"symbol": "512480"}],
            },
            ensure_ascii=False,
        ),
    )
    _write(
        review,
        "\n".join(
            [
                "# Catalyst Web Review | scan | 2026-03-26",
                "",
                "## 1. 半导体ETF (512480)",
                "",
                "### 复核结论",
                "",
                "- 结论：待补",
            ]
        ),
    )
    monkeypatch.setattr(catalyst_review, "resolve_project_path", lambda path="": tmp_path / str(path))
    monkeypatch.setattr("sys.argv", ["catalyst_review", "list", "--root", "reports", "--status", "pending_template"])
    catalyst_review.main()
    output = capsys.readouterr().out
    assert "| pending_template | scan | 半导体ETF |" in output
