"""JSON-backed storage for strategy prediction ledger."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from src.utils.config import PROJECT_ROOT
from src.utils.data import load_json, save_json


DEFAULT_STRATEGY_LEDGER = {"predictions": []}


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _strategy_row_has_degradation(row: Mapping[str, Any]) -> bool:
    payload = dict(row or {})
    if bool(payload.get("degraded")):
        return True
    validation = dict(payload.get("validation") or {})
    if _safe_text(validation.get("validation_status")) == "not_evaluable_due_to_data_quality":
        return True
    benchmark_fixture = dict(payload.get("benchmark_fixture") or {})
    if _safe_text(benchmark_fixture.get("status")) in {"partial", "missing"}:
        return True
    lag_visibility_fixture = dict(payload.get("lag_visibility_fixture") or {})
    if _safe_text(lag_visibility_fixture.get("status")) in {"partial", "blocked", "missing"}:
        return True
    overlap_fixture = dict(payload.get("overlap_fixture") or {})
    if _safe_text(overlap_fixture.get("status")) == "blocked":
        return True
    flags = {_safe_text(item) for item in list(payload.get("downgrade_flags") or []) if _safe_text(item)}
    return any(
        flag
        for flag in flags
        if flag.startswith("benchmark_fixture_")
        or flag.startswith("lag_visibility_fixture_")
        or flag.startswith("overlap_fixture_")
    )


def summarize_strategy_background_confidence(
    rows: List[Mapping[str, Any]] | None,
    *,
    symbol: str = "",
    lookback: int = 8,
) -> Dict[str, Any]:
    scoped_rows = [dict(row or {}) for row in list(rows or []) if dict(row or {})]
    symbol_text = _safe_text(symbol)
    if symbol_text:
        scoped_rows = [row for row in scoped_rows if _safe_text(row.get("symbol")) == symbol_text]
    recent_rows = scoped_rows[: max(int(lookback), 0)]
    if not recent_rows:
        return {}

    validated_rows = [
        row
        for row in recent_rows
        if _safe_text(dict(row.get("validation") or {}).get("validation_status")) == "validated"
    ]
    no_prediction_rows = [
        row
        for row in recent_rows
        if _safe_text(row.get("status")) == "no_prediction"
        or _safe_text(dict(row.get("validation") or {}).get("validation_status")) == "skipped_no_prediction"
    ]
    degraded_rows = [row for row in recent_rows if _strategy_row_has_degradation(row)]

    validated_count = len(validated_rows)
    sample_count = len(recent_rows)
    no_prediction_count = len(no_prediction_rows)
    degraded_count = len(degraded_rows)
    hit_rate = (
        sum(1 for row in validated_rows if bool(dict(row.get("validation") or {}).get("hit"))) / validated_count
        if validated_count
        else 0.0
    )
    avg_net = (
        sum(_safe_float(dict(row.get("validation") or {}).get("cost_adjusted_directional_return")) for row in validated_rows)
        / validated_count
        if validated_count
        else 0.0
    )

    if (
        validated_count >= 4
        and hit_rate >= 0.55
        and avg_net > 0
        and degraded_count == 0
        and no_prediction_count <= 1
    ):
        status = "stable"
        label = "稳定"
        summary = "这类 setup 过去验证仍稳定。"
        reason = (
            f"最近 `{validated_count}` 个可验证样本命中率 `{hit_rate:.0%}`，"
            f"成本后方向收益 `{avg_net:+.1%}`，未见明显 fixture 降级。"
        )
    elif (
        (validated_count >= 3 and (hit_rate < 0.45 or avg_net < 0))
        or degraded_count >= 2
        or no_prediction_count >= max(2, sample_count // 2)
    ):
        status = "degraded"
        label = "退化"
        summary = "这类 setup 过去有效，但最近退化。"
        if validated_count >= 3 and (hit_rate < 0.45 or avg_net < 0):
            reason = (
                f"最近 `{validated_count}` 个可验证样本命中率 `{hit_rate:.0%}`，"
                f"成本后方向收益 `{avg_net:+.1%}`，稳定性开始走弱。"
            )
        elif degraded_count >= 2:
            reason = f"最近 `{degraded_count}` 条样本带明显 fixture / 数据降级，当前不能把它当成稳定 setup。"
        else:
            reason = f"最近 `{sample_count}` 条样本里有 `{no_prediction_count}` 条直接退成 no_prediction，信号连续性开始变差。"
    else:
        status = "watch"
        label = "观察"
        summary = "这类 setup 当前还在观察。"
        if validated_count < 4:
            reason = f"最近只有 `{validated_count}` 个可验证样本，暂时不够把它当成稳定策略，只能做辅助说明。"
        elif degraded_count > 0:
            reason = f"最近样本里已有 `{degraded_count}` 条出现 fixture / 数据降级，先按观察处理。"
        else:
            reason = "最近验证还没有坏到退化，但也还不足以给出稳定结论。"

    return {
        "symbol": symbol_text or _safe_text(recent_rows[0].get("symbol")),
        "status": status,
        "label": label,
        "summary": summary,
        "reason": reason,
        "validated_count": validated_count,
        "sample_count": sample_count,
        "hit_rate": round(hit_rate, 4),
        "avg_cost_adjusted_directional_return": round(avg_net, 4),
        "degraded_count": degraded_count,
        "no_prediction_count": no_prediction_count,
        "latest_as_of": _safe_text(recent_rows[0].get("as_of")),
    }


class StrategyRepository:
    """Persist strategy prediction snapshots as JSON."""

    def __init__(
        self,
        ledger_path: Path = PROJECT_ROOT / "data" / "strategy_predictions.json",
    ) -> None:
        self.ledger_path = ledger_path

    def load(self) -> Dict[str, Any]:
        payload = load_json(self.ledger_path, default=DEFAULT_STRATEGY_LEDGER)
        if not payload:
            payload = dict(DEFAULT_STRATEGY_LEDGER)
        payload.setdefault("predictions", [])
        return payload

    def save(self, payload: Dict[str, Any]) -> None:
        save_json(self.ledger_path, payload)

    def list_predictions(
        self,
        *,
        symbol: str = "",
        status: str = "",
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        rows = list(self.load().get("predictions", []))
        if symbol:
            rows = [row for row in rows if str(row.get("symbol", "")).strip() == str(symbol).strip()]
        if status and status != "all":
            rows = [row for row in rows if str(row.get("status", "")) == status]
        rows.sort(
            key=lambda row: (
                str(row.get("as_of", "")),
                str(row.get("created_at", "")),
                str(row.get("prediction_id", "")),
            ),
            reverse=True,
        )
        if limit is not None:
            rows = rows[: max(int(limit), 0)]
        return rows

    def get_prediction(self, prediction_id: str) -> Optional[Dict[str, Any]]:
        for row in self.load().get("predictions", []):
            if str(row.get("prediction_id", "")) == str(prediction_id):
                return dict(row)
        return None

    def upsert_prediction(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ledger = self.load()
        rows = list(ledger.get("predictions", []))
        prediction_id = str(payload.get("prediction_id", "")).strip()
        if not prediction_id:
            raise ValueError("prediction_id is required")
        for index, existing in enumerate(rows):
            if str(existing.get("prediction_id", "")) == prediction_id:
                rows[index] = payload
                ledger["predictions"] = rows
                self.save(ledger)
                return payload
        rows.append(payload)
        ledger["predictions"] = rows
        self.save(ledger)
        return payload

    def summarize_background_confidence(self, symbol: str, *, lookback: int = 8) -> Dict[str, Any]:
        symbol_text = _safe_text(symbol)
        if not symbol_text:
            return {}
        rows = self.list_predictions(symbol=symbol_text, status="all", limit=max(int(lookback), 0))
        return summarize_strategy_background_confidence(rows, symbol=symbol_text, lookback=lookback)
