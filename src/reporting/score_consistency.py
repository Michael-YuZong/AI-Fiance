"""Shared score/conclusion consistency checks for final reports."""

from __future__ import annotations

import re
from typing import Any, Dict


STOCK_SIGNAL_GATE_THRESHOLDS = {
    "技术面": 30,
    "催化面": 20,
    "风险特征": 20,
}


def parse_score_value(value: str) -> int | None:
    match = re.search(r"(-?\d+)\s*/\s*100", str(value or ""))
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def extract_dimension_scores(markdown_text: str) -> Dict[str, int]:
    scores: Dict[str, int] = {}
    for raw_line in str(markdown_text or "").splitlines():
        if "|" not in raw_line:
            continue
        cells = [cell.strip().strip("`") for cell in raw_line.strip().strip("|").split("|")]
        if len(cells) < 2:
            continue
        label = cells[0]
        if label not in STOCK_SIGNAL_GATE_THRESHOLDS:
            continue
        score = parse_score_value(cells[1])
        if score is not None:
            scores[label] = score
    return scores


def strong_opportunity_label_present(markdown_text: str) -> bool:
    for raw_line in str(markdown_text or "").splitlines()[:120]:
        line = raw_line.strip()
        if not line:
            continue
        if any(token in line for token in ("不允许", "不能", "不得", "压回", "封顶")):
            continue
        if not (line.startswith("#") or line.startswith("**") or line.startswith("|")):
            continue
        if "⭐⭐⭐⭐" in line or "⭐⭐⭐" in line or "较强机会" in line:
            return True
        if re.search(r"(?<!较)强机会", line):
            return True
    return False


def stock_signal_gate_problem(markdown_text: str) -> Dict[str, Any]:
    scores = extract_dimension_scores(markdown_text)
    failed = [
        {"dimension": label, "score": score, "threshold": threshold}
        for label, threshold in STOCK_SIGNAL_GATE_THRESHOLDS.items()
        if (score := scores.get(label)) is not None and score < threshold
    ]
    if not failed or not strong_opportunity_label_present(markdown_text):
        return {}
    return {"scores": scores, "failed": failed}


def format_stock_signal_gate_problem(problem: Dict[str, Any]) -> str:
    failed = problem.get("failed") or []
    if not failed:
        return ""
    return "、".join(
        f"{item.get('dimension')}{item.get('score')}/{item.get('threshold')}"
        for item in failed
    )
