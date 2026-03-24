"""Point-in-time evidence provenance helpers."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence

import pandas as pd


def _to_timestamp(value: Any) -> pd.Timestamp | None:
    if value is None or value == "":
        return None
    try:
        stamp = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(stamp):
        return None
    if stamp.tzinfo is not None:
        try:
            stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
        except TypeError:
            stamp = stamp.tz_localize(None)
    return stamp


def format_as_of(value: Any) -> str:
    stamp = _to_timestamp(value)
    if stamp is None:
        return "—"
    if stamp.hour == 0 and stamp.minute == 0 and stamp.second == 0:
        return stamp.strftime("%Y-%m-%d")
    return stamp.strftime("%Y-%m-%d %H:%M")


def history_as_of(history: Any) -> str:
    if isinstance(history, pd.DataFrame) and not history.empty and "date" in history.columns:
        return format_as_of(history["date"].iloc[-1])
    return "—"


def latest_evidence_as_of(evidence: Sequence[Mapping[str, Any]]) -> str:
    stamps = [_to_timestamp(item.get("date")) for item in evidence if item.get("date")]
    stamps = [stamp for stamp in stamps if stamp is not None]
    if not stamps:
        return "未命中显式日期"
    return format_as_of(max(stamps))


def unique_sources(items: Sequence[Mapping[str, Any]], *, limit: int = 4) -> List[str]:
    sources: List[str] = []
    for item in items:
        source = str(item.get("source") or item.get("configured_source") or "").strip()
        if source and source not in sources:
            sources.append(source)
        if len(sources) >= limit:
            break
    return sources


def build_analysis_provenance(analysis: Mapping[str, Any]) -> Dict[str, Any]:
    catalyst = dict(dict(analysis.get("dimensions") or {}).get("catalyst") or {})
    relative = dict(dict(analysis.get("dimensions") or {}).get("relative_strength") or {})
    coverage = dict(catalyst.get("coverage") or {})
    evidence = list(catalyst.get("evidence") or [])
    intraday = dict(analysis.get("intraday") or {})
    metadata = dict(analysis.get("metadata") or {})
    history_source = str(metadata.get("history_source", "")).strip()
    history_source_label = str(metadata.get("history_source_label", "")).strip()

    intraday_as_of = "未启用"
    if intraday.get("enabled"):
        intraday_as_of = format_as_of(
            intraday.get("updated_at")
            or intraday.get("snapshot_time")
            or intraday.get("data_date")
            or intraday.get("trade_date")
        )

    notes: List[str] = []
    if bool(analysis.get("history_fallback_mode")) or bool(metadata.get("history_fallback")):
        notes.append("完整日线历史当前不可用，行情时点已退化为本地实时快照或缓存日线。")
    if coverage.get("degraded"):
        notes.append("催化/新闻覆盖当前存在降级，证据时点更接近结构化事件和可用缓存，而不是完整实时新闻流。")
    if intraday.get("enabled") and intraday.get("fallback_mode"):
        notes.append("盘中快照当前退化为最近一根日K快照，盘中时点只适合作参考。")

    market_source = history_source_label or "Tushare 优先日线；失败时 AKShare / Yahoo / 本地实时快照回退"
    if str(analysis.get("asset_type", "")) == "cn_fund" and not history_source_label:
        market_source = "基金净值/日线历史；失败时本地缓存或快照回退"

    sources = unique_sources(evidence)
    return {
        "analysis_generated_at": format_as_of(analysis.get("generated_at")),
        "market_data_as_of": history_as_of(analysis.get("history")),
        "market_data_source": market_source,
        "market_data_source_code": history_source or "unknown",
        "relative_benchmark_name": str(relative.get("benchmark_name") or analysis.get("benchmark_name") or "未显式标注"),
        "relative_benchmark_symbol": str(relative.get("benchmark_symbol") or analysis.get("benchmark_symbol") or ""),
        "intraday_as_of": intraday_as_of,
        "intraday_source": "AKShare 分钟线 / 实时行情" if intraday.get("enabled") else "未启用",
        "catalyst_evidence_as_of": latest_evidence_as_of(evidence),
        "catalyst_sources": sources,
        "catalyst_sources_text": " / ".join(sources) if sources else "未命中高置信直连源",
        "news_mode": str(coverage.get("news_mode", "unknown") or "unknown"),
        "point_in_time_note": "默认只使用生成时点前可见的行情、结构化事件和新闻覆盖；显式降级和缺失会单独写出。",
        "notes": notes,
    }
