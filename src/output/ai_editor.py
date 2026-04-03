"""Optional AI editor hook for homepage rewriting.

Current default path is repo-level sidecar generation plus external subagent editing.
This module keeps the command contract stable and safely falls back to the rule-based
homepage when no external editor is enabled.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class AIEditorResult:
    markdown: str
    applied: bool
    provider: str | None
    model: str | None
    reason: str
    response_markdown: str | None = None


def _is_valid_homepage_markdown(markdown_text: str) -> bool:
    required = (
        "## 首页判断",
        "### 宏观面",
        "### 板块 / 主题认知",
        "### 情绪与热度",
        "### 微观面",
        "### 动作建议与结论",
    )
    return all(token in markdown_text for token in required)


def maybe_apply_ai_editor(rule_based_markdown: str, *, prompt_text: str, packet: Mapping[str, Any]) -> AIEditorResult:
    _ = prompt_text
    _ = packet
    return AIEditorResult(
        markdown=rule_based_markdown,
        applied=False,
        provider="subagent",
        model=None,
        reason="editor_disabled",
        response_markdown="",
    )


def ai_editor_run_payload(result: AIEditorResult, packet: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **asdict(result),
        "packet_version": packet.get("packet_version"),
        "report_type": packet.get("report_type"),
    }
