from __future__ import annotations

import sys
from pathlib import Path

from src.commands import review_ledger as review_ledger_module


def test_review_ledger_main_prints_summary_and_writes_outputs(tmp_path: Path, monkeypatch, capsys) -> None:
    review_root = tmp_path / "reviews"
    review_root.mkdir(parents=True)
    (review_root / "demo_round1.md").write_text(
        "\n".join(
            [
                "# Demo review",
                "",
                "- review_target：`docs/demo.md`",
                "- review_prompt：`docs/prompts/demo.md`",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：PASS",
                "- 本轮新增 P0/P1：否",
                "- 上一轮 P0/P1 是否已关闭：是",
                "- 本轮是否收敛：是",
                "- 是否建议继续下一轮：否",
            ]
        ),
        encoding="utf-8",
    )
    json_out = tmp_path / "review_index.json"
    markdown_out = tmp_path / "review_index.md"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "review_ledger",
            "--root",
            str(review_root),
            "--json-out",
            str(json_out),
            "--markdown-out",
            str(markdown_out),
        ],
    )

    review_ledger_module.main()

    captured = capsys.readouterr()
    assert "# External Review Ledger" in captured.out
    assert json_out.exists()
    assert markdown_out.exists()
    assert '"total_records": 1' in json_out.read_text(encoding="utf-8")
