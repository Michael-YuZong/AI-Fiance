"""Tests for stock-pick command helpers."""

from __future__ import annotations

import json
from pathlib import Path

from src.commands.stock_pick import enrich_payload_with_score_history


def _sample_payload(score: int, signal: str, generated_at: str) -> dict:
    return {
        "generated_at": generated_at,
        "top": [
            {
                "symbol": "00700.HK",
                "name": "腾讯控股",
                "rating": {"rank": 3},
                "dimensions": {
                    "technical": {"score": 62, "core_signal": "趋势未坏", "factors": [{"name": "趋势", "display_score": "20/20", "signal": "MA20 上方"}]},
                    "fundamental": {"score": 61, "core_signal": "估值中性", "factors": [{"name": "估值", "display_score": "20/30", "signal": "PE 21x"}]},
                    "catalyst": {
                        "score": score,
                        "core_signal": signal,
                        "factors": [
                            {"name": "海外映射", "display_score": "20/20" if score >= 75 else "0/20", "signal": signal},
                            {"name": "负面事件", "display_score": "信息项" if score >= 75 else "-15", "signal": "近 30 日未命中明确稀释/监管负面" if score >= 75 else "Review risk"},
                        ],
                    },
                    "relative_strength": {"score": 47, "core_signal": "相对中性", "factors": [{"name": "超额拐点", "display_score": "15/30", "signal": "5日跑赢"}]},
                    "chips": {"score": None, "core_signal": "缺失", "factors": [{"name": "北向/南向", "display_score": "缺失", "signal": "缺失"}]},
                    "risk": {
                        "score": 50,
                        "core_signal": "波动偏高",
                        "factors": [
                            {"name": "回撤分位", "display_score": "15/30", "signal": "当前回撤 12%"},
                            {"name": "波动率分位", "display_score": "10/25", "signal": "年化波动偏高"},
                        ],
                    },
                    "seasonality": {"score": 40, "core_signal": "中性", "factors": [{"name": "月度胜率", "display_score": "10/30", "signal": "同月胜率一般"}]},
                    "macro": {"score": 10, "core_signal": "轻度逆风", "factors": [{"name": "敏感度向量", "display_score": "10/40", "signal": "rate 逆风"}]},
                },
            }
        ],
    }


def test_enrich_payload_with_score_history_adds_dimension_change_reason(tmp_path: Path):
    snapshot_path = tmp_path / "stock_pick_score_history.json"
    first = enrich_payload_with_score_history(
        _sample_payload(55, "催化不足", "2026-03-09 20:00:00"),
        market="hk",
        sector_filter="",
        snapshot_path=snapshot_path,
    )
    assert first["top"][0]["score_changes"] == []
    assert first["is_daily_baseline"] is True
    assert first["baseline_snapshot_at"] == "2026-03-09 20:00:00"

    second = enrich_payload_with_score_history(
        _sample_payload(55, "催化不足", "2026-03-10 20:00:00"),
        market="hk",
        sector_filter="",
        snapshot_path=snapshot_path,
    )
    assert second["top"][0]["score_changes"] == []
    assert second["is_daily_baseline"] is True
    assert second["baseline_snapshot_at"] == "2026-03-10 20:00:00"

    third = enrich_payload_with_score_history(
        _sample_payload(75, "海外映射增强", "2026-03-10 22:00:00"),
        market="hk",
        sector_filter="",
        snapshot_path=snapshot_path,
    )
    changes = third["top"][0]["score_changes"]
    assert len(changes) == 1
    assert third["is_daily_baseline"] is False
    assert third["comparison_basis_label"] == "当日基准版"
    assert third["comparison_basis_at"] == "2026-03-10 20:00:00"
    assert changes[0]["dimension"] == "catalyst"
    assert changes[0]["previous"] == 55
    assert changes[0]["current"] == 75
    assert "海外映射" in changes[0]["reason"]
    assert "负面事件" in changes[0]["reason"]


def test_enrich_payload_with_score_history_warns_when_model_version_changes(tmp_path: Path):
    snapshot_path = tmp_path / "stock_pick_score_history.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "hk:*": {
                    "latest": {},
                    "daily_baselines": {
                        "2026-03-10": {
                            "generated_at": "2026-03-10 09:00:00",
                            "model_version": "old-model-version",
                            "items": {
                                "00700.HK": {
                                    "name": "腾讯控股",
                                    "rating_rank": 3,
                                    "dimensions": {
                                        "technical": {"score": 62, "core_signal": "趋势未坏", "factors": {}},
                                        "fundamental": {"score": 61, "core_signal": "估值中性", "factors": {}},
                                        "catalyst": {"score": 55, "core_signal": "催化不足", "factors": {}},
                                        "relative_strength": {"score": 47, "core_signal": "相对中性", "factors": {}},
                                        "chips": {"score": None, "core_signal": "缺失", "factors": {}},
                                        "risk": {"score": 50, "core_signal": "波动偏高", "factors": {}},
                                        "seasonality": {"score": 40, "core_signal": "中性", "factors": {}},
                                        "macro": {"score": 10, "core_signal": "轻度逆风", "factors": {}},
                                    },
                                }
                            },
                        }
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload = enrich_payload_with_score_history(
        _sample_payload(75, "海外映射增强", "2026-03-10 20:00:00"),
        market="hk",
        sector_filter="",
        snapshot_path=snapshot_path,
    )
    assert payload["is_daily_baseline"] is False
    assert payload["comparison_basis_at"] == "2026-03-10 09:00:00"
    assert "old-model-version" in payload["model_version_warning"]
