"""Shared chart attachment helpers for pick reports."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, MutableMapping, Sequence, Tuple

from src.output.analysis_charts import AnalysisChartRenderer


def _analysis_visual_key(analysis: Mapping[str, Any]) -> Tuple[str, str, str]:
    return (
        str(analysis.get("asset_type", "")).strip(),
        str(analysis.get("symbol", "")).strip(),
        str(analysis.get("generated_at", "")).strip(),
    )


def attach_visuals_to_analyses(analyses: Sequence[MutableMapping[str, Any]] | Iterable[MutableMapping[str, Any]]) -> None:
    """Render chart assets for the provided analyses in-place.

    The helper is intentionally selective: callers should only pass analyses that
    will actually be surfaced in client/internal pick reports.
    """

    renderer = AnalysisChartRenderer()
    seen: set[Tuple[str, str, str]] = set()
    for analysis in analyses:
        if not isinstance(analysis, dict):
            continue
        key = _analysis_visual_key(analysis)
        if key in seen:
            continue
        seen.add(key)
        visuals = renderer.render(analysis)
        if visuals:
            analysis["visuals"] = visuals
