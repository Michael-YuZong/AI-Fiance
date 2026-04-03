from __future__ import annotations

import sys
from pathlib import Path

from src.commands import export_review_kit as export_review_kit_module


def test_export_review_kit_copies_bundle_and_writes_archive(tmp_path: Path, monkeypatch, capsys) -> None:
    target = tmp_path / "portable_review_kit"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "export_review_kit",
            "--out",
            str(target),
            "--archive",
        ],
    )

    export_review_kit_module.main()

    captured = capsys.readouterr()
    assert "Exported portable review kit to:" in captured.out
    assert target.exists()
    assert (target / "README.md").exists()
    assert (target / "SKILL.md").exists()
    assert (target / "prompts" / "generic_structural_reviewer.md").exists()
    assert (target / "templates" / "review_record_template.md").exists()
    assert (target / "python" / "final_gate.py").exists()
    assert (target.parent / "portable_review_kit.tar.gz").exists()
