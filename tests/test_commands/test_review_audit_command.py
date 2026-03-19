from __future__ import annotations

import sys
from pathlib import Path

from src.commands import review_audit as review_audit_module


def test_review_audit_main_prints_summary_and_writes_outputs(tmp_path: Path, monkeypatch, capsys) -> None:
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
                "## 结论",
                "",
                "`go with conditions`",
                "",
                "## 主要问题",
                "",
                "1. `P1`：还有阻塞项",
                "",
                "## 框架外问题",
                "",
                "1. 还有额外风险",
                "",
                "## 建议沉淀",
                "",
                "- prompt",
                "  - 补 reviewer 规则",
                "",
                "## 收敛结论",
                "",
                "- round：1",
                "- 状态：IN_REVIEW",
                "- 本轮新增 P0/P1：是",
                "- 上一轮 P0/P1 是否已关闭：不适用",
                "- 本轮是否收敛：否",
                "- 是否建议继续下一轮：是",
            ]
        ),
        encoding="utf-8",
    )
    json_out = tmp_path / "review_audit.json"
    markdown_out = tmp_path / "review_audit.md"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "review_audit",
            "--root",
            str(review_root),
            "--json-out",
            str(json_out),
            "--markdown-out",
            str(markdown_out),
        ],
    )

    review_audit_module.main()

    captured = capsys.readouterr()
    assert "# External Review Audit" in captured.out
    assert json_out.exists()
    assert markdown_out.exists()
    assert '"total_findings"' in json_out.read_text(encoding="utf-8")
