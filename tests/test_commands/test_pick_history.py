"""Tests for shared pick history helpers."""

from __future__ import annotations

from pathlib import Path

from src.commands.pick_history import enrich_pick_payload_with_score_history


def _sample_payload(score: int, signal: str, generated_at: str, *, degraded: bool = False) -> dict:
    coverage = {
        "news_mode": "proxy" if degraded else "live",
        "degraded": degraded,
        "structured_rate": 0.5,
        "direct_news_rate": 0.5,
        "total": 1,
    }
    catalyst_coverage = {
        "news_mode": "proxy" if degraded else "live",
        "high_confidence_company_news": not degraded,
        "structured_event": False,
        "forward_event": False,
        "degraded": degraded,
    }
    return {
        "generated_at": generated_at,
        "data_coverage": coverage,
        "top": [
            {
                "symbol": "159981",
                "name": "能源化工ETF",
                "rating": {"rank": 3},
                "dimensions": {
                    "technical": {"score": 52, "core_signal": "趋势未坏", "factors": [{"name": "趋势", "display_score": "18/20", "signal": "MA20 上方"}]},
                    "fundamental": {"score": 20, "core_signal": "指数估值中性", "factors": [{"name": "估值", "display_score": "12/20", "signal": "PB 中位"}]},
                    "catalyst": {
                        "score": score,
                        "core_signal": signal,
                        "coverage": catalyst_coverage,
                        "factors": [{"name": "主题催化", "display_score": f"{score}/100", "signal": signal}],
                    },
                    "relative_strength": {"score": 58, "core_signal": "相对强势", "factors": [{"name": "超额拐点", "display_score": "20/30", "signal": "近 5 日跑赢"}]},
                    "chips": {"score": None, "core_signal": "缺失", "factors": []},
                    "risk": {"score": 64, "core_signal": "波动可控", "factors": [{"name": "回撤", "display_score": "18/30", "signal": "近期回撤一般"}]},
                    "seasonality": {"score": 32, "core_signal": "中性", "factors": []},
                    "macro": {"score": 18, "core_signal": "顺风", "factors": []},
                },
            }
        ],
    }


def _rank_key(item: dict) -> tuple[float, float, float, float]:
    dims = item["dimensions"]
    return (
        float(item["rating"]["rank"]),
        float(dims["technical"]["score"] + dims["relative_strength"]["score"] + dims["risk"]["score"]),
        float(dims["relative_strength"]["score"]),
        float(dims["catalyst"]["score"]),
    )


def test_enrich_pick_payload_with_score_history_adds_dimension_change_reason(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "pick_history.json"
    first = enrich_pick_payload_with_score_history(
        _sample_payload(35, "催化偏弱", "2026-03-13 09:00:00"),
        scope="theme:*",
        snapshot_path=snapshot_path,
        model_version="test-pick-model-v1",
        model_changelog=["第一次口径"],
        rank_key=_rank_key,
    )
    assert first["top"][0]["score_changes"] == []
    assert first["is_daily_baseline"] is True

    second = enrich_pick_payload_with_score_history(
        _sample_payload(65, "化工链催化增强", "2026-03-13 14:00:00"),
        scope="theme:*",
        snapshot_path=snapshot_path,
        model_version="test-pick-model-v1",
        model_changelog=["第一次口径"],
        rank_key=_rank_key,
    )
    assert second["is_daily_baseline"] is False
    assert second["comparison_basis_label"] == "当日基准版"
    changes = second["top"][0]["score_changes"]
    assert len(changes) == 1
    assert changes[0]["dimension"] == "catalyst"
    assert changes[0]["previous"] == 35
    assert changes[0]["current"] == 65


def test_enrich_pick_payload_with_score_history_applies_catalyst_fallback_when_degraded(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "pick_history.json"
    enrich_pick_payload_with_score_history(
        _sample_payload(55, "主题催化存在", "2026-03-12 20:00:00"),
        scope="theme:*",
        snapshot_path=snapshot_path,
        model_version="test-pick-model-v1",
        model_changelog=["第一次口径"],
        rank_key=_rank_key,
    )

    degraded = enrich_pick_payload_with_score_history(
        _sample_payload(12, "信息不足", "2026-03-13 10:00:00", degraded=True),
        scope="theme:*",
        snapshot_path=snapshot_path,
        model_version="test-pick-model-v1",
        model_changelog=["第一次口径"],
        rank_key=_rank_key,
    )
    catalyst = degraded["top"][0]["dimensions"]["catalyst"]
    assert catalyst["score"] > 12
    assert catalyst["coverage"]["fallback_applied"] is True


def test_enrich_pick_payload_with_score_history_prefers_full_coverage_population(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "pick_history.json"
    payload = _sample_payload(35, "催化偏弱", "2026-03-13 09:00:00")
    extra = _sample_payload(20, "信息一般", "2026-03-13 09:00:00")["top"][0]
    extra["symbol"] = "510880"
    extra["dimensions"]["catalyst"]["coverage"]["structured_event"] = True
    extra["dimensions"]["catalyst"]["coverage"]["high_confidence_company_news"] = False
    extra["rating"]["rank"] = 0
    payload["coverage_analyses"] = [payload["top"][0], extra]

    enriched = enrich_pick_payload_with_score_history(
        payload,
        scope="theme:*",
        snapshot_path=snapshot_path,
        model_version="test-pick-model-v1",
        model_changelog=["第一次口径"],
        rank_key=_rank_key,
    )

    assert enriched["pick_coverage"]["total"] == 2
    assert enriched["pick_coverage"]["structured_rate"] == 0.5
