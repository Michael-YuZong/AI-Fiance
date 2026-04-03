from __future__ import annotations

from src.storage.strategy import StrategyRepository, summarize_strategy_background_confidence


def test_strategy_repository_upsert_and_filter(tmp_path) -> None:
    repository = StrategyRepository(tmp_path / "strategy_predictions.json")
    repository.upsert_prediction(
        {
            "prediction_id": "pred_1",
            "symbol": "600519",
            "status": "predicted",
            "as_of": "2026-03-14",
            "created_at": "2026-03-14T10:00:00+00:00",
        }
    )
    repository.upsert_prediction(
        {
            "prediction_id": "pred_2",
            "symbol": "300750",
            "status": "no_prediction",
            "as_of": "2026-03-13",
            "created_at": "2026-03-14T09:00:00+00:00",
        }
    )

    assert repository.get_prediction("pred_1")["symbol"] == "600519"
    assert [row["prediction_id"] for row in repository.list_predictions(limit=10)] == ["pred_1", "pred_2"]
    assert [row["prediction_id"] for row in repository.list_predictions(status="no_prediction")] == ["pred_2"]
    assert [row["prediction_id"] for row in repository.list_predictions(symbol="600519")] == ["pred_1"]


def test_summarize_strategy_background_confidence_marks_stable_for_recent_positive_validated_rows() -> None:
    rows = [
        {
            "symbol": "600519",
            "as_of": f"2026-03-{day:02d}",
            "status": "predicted",
            "validation": {
                "validation_status": "validated",
                "hit": True,
                "cost_adjusted_directional_return": 0.02 + day * 0.001,
            },
            "benchmark_fixture": {"status": "aligned"},
            "lag_visibility_fixture": {"status": "ready"},
            "overlap_fixture": {"status": "ready"},
        }
        for day in range(1, 5)
    ]

    summary = summarize_strategy_background_confidence(rows, symbol="600519")

    assert summary["status"] == "stable"
    assert summary["label"] == "稳定"
    assert "过去验证仍稳定" in summary["summary"]


def test_summarize_strategy_background_confidence_marks_watch_when_samples_are_thin() -> None:
    rows = [
        {
            "symbol": "600519",
            "as_of": "2026-03-01",
            "status": "predicted",
            "validation": {
                "validation_status": "validated",
                "hit": True,
                "cost_adjusted_directional_return": 0.012,
            },
            "benchmark_fixture": {"status": "aligned"},
            "lag_visibility_fixture": {"status": "ready"},
            "overlap_fixture": {"status": "ready"},
        }
    ]

    summary = summarize_strategy_background_confidence(rows, symbol="600519")

    assert summary["status"] == "watch"
    assert summary["label"] == "观察"
    assert "可验证样本" in summary["reason"]


def test_summarize_strategy_background_confidence_marks_degraded_for_negative_or_degraded_recent_rows() -> None:
    rows = [
        {
            "symbol": "600519",
            "as_of": f"2026-03-{day:02d}",
            "status": "predicted",
            "validation": {
                "validation_status": "validated",
                "hit": False,
                "cost_adjusted_directional_return": -0.03,
            },
            "benchmark_fixture": {"status": "partial" if day in {1, 2} else "aligned"},
            "lag_visibility_fixture": {"status": "blocked" if day in {1, 2} else "ready"},
            "overlap_fixture": {"status": "ready"},
        }
        for day in range(1, 5)
    ] + [
        {
            "symbol": "600519",
            "as_of": "2026-03-05",
            "status": "no_prediction",
            "validation": {"validation_status": "skipped_no_prediction"},
        }
    ]

    summary = summarize_strategy_background_confidence(rows, symbol="600519")

    assert summary["status"] == "degraded"
    assert summary["label"] == "退化"
    assert "过去有效，但最近退化" in summary["summary"]
