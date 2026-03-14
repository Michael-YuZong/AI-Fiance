from __future__ import annotations

from src.storage.strategy import StrategyRepository


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
