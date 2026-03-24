"""Shared parsing helpers for round-based review records."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterable, List, Mapping


FILE_ROUND_RE = re.compile(r"_round(\d+)$")
LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
SECTION_PREFIX_RE = re.compile(r"^[0-9０-９]+[.．、)]\s*")
SECTION_ALIASES = {
    "一句话总评": "结论",
    "总评": "结论",
    "零提示审稿": "零提示发散审",
    "零提示评审": "零提示发散审",
    "零提示复核": "零提示发散审",
}
STATUS_PREFIXES = ("PASS", "BLOCKED", "IN_REVIEW")
YES_PREFIXES = ("是", "否", "不适用", "N/A")
EMPTY_ACTIONABLE_MARKERS = (
    "无新增",
    "没有新增",
    "没有新的",
    "没有再发现新的",
    "未发现新的",
    "无新的",
    "不适用",
    "已满足",
)
EMPTY_BLOCK_ITEM_MARKERS = {"无", "none", "n/a", "不适用", "无新增", "没有新的", "无新的"}


def clean_text(value: str) -> str:
    text = str(value or "").strip()
    if text.startswith("`") and text.endswith("`") and len(text) >= 2:
        text = text[1:-1].strip()
    return text


def extract_link(value: str) -> tuple[str, str]:
    match = LINK_RE.search(value or "")
    if not match:
        return clean_text(value), ""
    label, target = match.groups()
    return clean_text(label), clean_text(target)


def parse_bullet_mapping(lines: Iterable[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line.startswith("- "):
            continue
        body = line[2:].strip()
        if "：" in body:
            key, value = body.split("：", 1)
        elif ":" in body:
            key, value = body.split(":", 1)
        else:
            continue
        mapping[clean_text(key)] = clean_text(value)
    return mapping


def split_sections(text: str) -> Dict[str, str]:
    sections: Dict[str, List[str]] = {}
    current: str | None = None
    for raw_line in text.splitlines():
        if raw_line.startswith("## "):
            current = raw_line[3:].strip()
            sections.setdefault(current, [])
            continue
        if current is not None:
            sections[current].append(raw_line)
    return {key: "\n".join(lines).strip() for key, lines in sections.items()}


def normalize_section_title(value: str) -> str:
    title = clean_text(value)
    title = SECTION_PREFIX_RE.sub("", title)
    return SECTION_ALIASES.get(title, title)


def canonicalize_sections(sections: Mapping[str, str]) -> Dict[str, str]:
    canonical: Dict[str, str] = {}
    for key, value in sections.items():
        normalized = normalize_section_title(key)
        content = clean_text(value)
        if not normalized:
            continue
        if normalized in canonical and content:
            if canonical[normalized]:
                canonical[normalized] = canonical[normalized] + "\n\n" + content
            else:
                canonical[normalized] = content
            continue
        canonical[normalized] = content
    return canonical


def top_metadata(text: str) -> Dict[str, str]:
    lines = text.splitlines()
    metadata_lines: List[str] = []
    started = False
    for line in lines:
        if line.startswith("# "):
            started = True
            continue
        if not started:
            continue
        if line.startswith("## "):
            break
        metadata_lines.append(line)
    return parse_bullet_mapping(metadata_lines)


def series_id_for(path: Path) -> str:
    return FILE_ROUND_RE.sub("", path.stem)


def round_from_text(value: str) -> int | None:
    if not value:
        return None
    value = clean_text(value)
    if value.isdigit():
        return int(value)
    match = re.search(r"round(\d+)", value, re.I)
    if match:
        return int(match.group(1))
    return None


def normalize_status(value: str) -> str:
    text = clean_text(value)
    for prefix in STATUS_PREFIXES:
        if text.startswith(prefix):
            return prefix
    return text


def normalize_yes_no(value: str) -> str:
    text = clean_text(value)
    for prefix in YES_PREFIXES:
        if text.startswith(prefix):
            return prefix
    return text


def meaningful_lines(text: str) -> List[str]:
    result: List[str] = []
    for raw_line in str(text or "").splitlines():
        line = clean_text(raw_line)
        if not line:
            continue
        if line in {"`go`", "`hold`", "`blocked`", "无", "无新增阻塞项"}:
            continue
        result.append(line)
    return result


def has_actionable_content(text: str) -> bool:
    lines = meaningful_lines(text)
    if not lines:
        return False
    normalized_lines = [re.sub(r"^[\-*]\s*", "", line).strip() for line in lines]
    if all(any(marker in line for marker in EMPTY_ACTIONABLE_MARKERS) for line in normalized_lines):
        return False
    normalized = " ".join(normalized_lines)
    return not any(marker in normalized for marker in EMPTY_ACTIONABLE_MARKERS)


def bullet_block_items(text: str, key: str) -> List[str]:
    target = clean_text(key)
    if not target:
        return []

    items: List[str] = []
    collecting = False
    base_indent = -1

    for raw_line in str(text or "").splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue

        indent = len(raw_line) - len(raw_line.lstrip(" "))
        if stripped.startswith("- "):
            body = stripped[2:].strip()
            split_char = "：" if "：" in body else ":" if ":" in body else ""
            if split_char:
                block_key, value = body.split(split_char, 1)
                normalized_key = clean_text(block_key)
                if collecting and indent <= base_indent and normalized_key != target:
                    break
                if normalized_key == target:
                    collecting = True
                    base_indent = indent
                    inline_value = clean_text(value)
                    if inline_value:
                        items.append(inline_value)
                    continue
            if collecting:
                if indent <= base_indent:
                    break
                items.append(clean_text(body))
            continue

        if collecting:
            if indent <= base_indent:
                break
            continuation = clean_text(stripped)
            if not continuation:
                continue
            if items:
                items[-1] = items[-1] + " " + continuation
            else:
                items.append(continuation)

    cleaned_items = [clean_text(item) for item in items]
    return [item for item in cleaned_items if item and item.lower() not in EMPTY_BLOCK_ITEM_MARKERS]


def decision_from_sections(sections: Mapping[str, str]) -> str:
    normalized_sections = canonicalize_sections(sections)
    for key in ("结论",):
        content = clean_text(normalized_sections.get(key, ""))
        if not content:
            continue
        for line in content.splitlines():
            line = clean_text(line)
            if line and not line.startswith("- "):
                return line
            if line.startswith("- "):
                return clean_text(line[2:])
    return ""
