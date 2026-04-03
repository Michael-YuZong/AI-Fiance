from __future__ import annotations

import json
from pathlib import Path

from src.reporting.catalyst_review_queue import build_catalyst_review_queue, next_pending_task


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_build_catalyst_review_queue_tracks_pending_and_completed(tmp_path: Path) -> None:
    payload_root = tmp_path / "reports/scans/etfs/internal"
    payload1 = payload_root / "scan_512480_2026-03-26_catalyst_web_review_payload.json"
    payload2 = payload_root / "scan_159570_2026-03-26_catalyst_web_review_payload.json"
    _write(
        payload1,
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
        payload2,
        json.dumps(
            {
                "report_type": "scan",
                "subject": "创新药ETF",
                "generated_at": "2026-03-26 10:00:00",
                "items": [{"symbol": "159570"}],
            },
            ensure_ascii=False,
        ),
    )
    _write(
        payload1.with_name("scan_512480_2026-03-26_catalyst_web_review.md"),
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
    _write(
        payload2.with_name("scan_159570_2026-03-26_catalyst_web_review.md"),
        "\n".join(
            [
                "# Catalyst Web Review | scan | 2026-03-26",
                "",
                "## 1. 创新药ETF (159570)",
                "",
                "### 复核结论",
                "",
                "- 结论：只有主题级催化",
            ]
        ),
    )

    queue = build_catalyst_review_queue(tmp_path / "reports")
    statuses = {task["subject"]: task["status"] for task in queue["tasks"]}
    assert statuses["半导体ETF"] == "pending_template"
    assert statuses["创新药ETF"] == "completed"


def test_next_pending_task_returns_first_pending(tmp_path: Path) -> None:
    payload = tmp_path / "reports/scans/etfs/internal/scan_512480_2026-03-26_catalyst_web_review_payload.json"
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
    queue = build_catalyst_review_queue(tmp_path / "reports")
    task = next_pending_task(queue)
    assert task is not None
    assert task["subject"] == "半导体ETF"
    assert task["status"] == "missing_review"
