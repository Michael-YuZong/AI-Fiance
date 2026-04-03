from __future__ import annotations

from src.processors import opportunity_engine as opportunity_engine_module


def test_compare_opportunities_attaches_strategy_background_confidence_and_linkage(monkeypatch) -> None:
    monkeypatch.setattr(opportunity_engine_module, "build_market_context", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(opportunity_engine_module, "detect_asset_type_for_compare", lambda *_args, **_kwargs: "cn_etf")

    def _fake_analyze(symbol, asset_type, config, context=None):
        technical_score = 80 if symbol == "561380" else 65
        return {
            "symbol": symbol,
            "name": f"标的{symbol}",
            "asset_type": asset_type,
            "rating": {"rank": 3 if symbol == "561380" else 2, "label": "较强机会"},
            "dimensions": {
                "technical": {"score": technical_score},
                "fundamental": {"score": 55},
                "catalyst": {"score": 45},
                "relative_strength": {"score": 60},
                "chips": {"score": 0},
                "risk": {"score": 50},
                "seasonality": {"score": 20},
                "macro": {"score": 25},
            },
            "metadata": {"sector": "电网" if symbol != "512400" else "有色"},
        }

    monkeypatch.setattr(opportunity_engine_module, "analyze_opportunity", _fake_analyze)

    class _FakeStrategyRepository:
        def summarize_background_confidence(self, symbol: str, *, lookback: int = 8):
            return {
                "symbol": symbol,
                "status": "stable" if symbol == "561380" else "watch",
                "label": "稳定" if symbol == "561380" else "观察",
                "reason": "最近验证仍稳定。" if symbol == "561380" else "最近样本偏少。",
            }

    monkeypatch.setattr("src.storage.strategy.StrategyRepository", _FakeStrategyRepository)

    payload = opportunity_engine_module.compare_opportunities(["561380", "512400"], {})

    assert payload["best_symbol"] == "561380"
    assert payload["analyses"][0]["strategy_background_confidence"]["status"] == "stable"
    assert payload["analyses"][1]["strategy_background_confidence"]["status"] == "watch"
    assert payload["compare_linkage_summary"]["overlap_label"] in {"部分同主线重合", "跨主线对比", "同一行业主线对比"}
    assert payload["compare_linkage_summary"]["style_summary_line"]
