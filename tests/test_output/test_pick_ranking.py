from __future__ import annotations

from src.output import pick_ranking


def _watch_candidate(symbol: str) -> dict:
    return {
        "symbol": symbol,
        "rating": {"rank": 0, "label": "无信号"},
        "dimensions": {
            "technical": {"score": 40},
            "fundamental": {"score": 78},
            "catalyst": {"score": 35},
            "relative_strength": {"score": 58},
            "risk": {"score": 60},
        },
    }


def test_recommendation_bucket_lazy_loads_strategy_background_confidence(monkeypatch) -> None:
    pick_ranking._STRATEGY_CONFIDENCE_CACHE.clear()

    class _FakeStrategyRepository:
        def summarize_background_confidence(self, symbol: str, *, lookback: int = 8):
            return {"status": "watch", "label": "观察", "reason": "最近样本偏少。"}

    monkeypatch.setattr(pick_ranking, "StrategyRepository", _FakeStrategyRepository)

    bucket = pick_ranking.recommendation_bucket(_watch_candidate("600519"))

    assert bucket == "观察为主"


def test_recommendation_bucket_downgrades_etf_when_direct_coverage_is_zero_and_setup_is_weak() -> None:
    candidate = _watch_candidate("159516")
    candidate["asset_type"] = "cn_etf"
    candidate["rating"] = {"rank": 3, "label": "较强机会"}
    candidate["dimensions"]["technical"] = {"score": 45}
    candidate["dimensions"]["catalyst"] = {
        "score": 15,
        "coverage": {
            "diagnosis": "proxy_degraded",
            "direct_news_count": 0,
            "fresh_direct_news_count": 0,
            "structured_event": False,
            "effective_structured_event": False,
            "forward_event": False,
            "high_confidence_company_news": False,
            "directional_catalyst_hit": False,
            "theme_news_count": 0,
        },
    }
    candidate["dimensions"]["risk"] = {"score": 35}

    assert pick_ranking.recommendation_bucket(candidate) == "观察为主"


def test_rank_market_items_uses_lazy_strategy_confidence_for_tie_break(monkeypatch) -> None:
    pick_ranking._STRATEGY_CONFIDENCE_CACHE.clear()

    class _FakeStrategyRepository:
        def summarize_background_confidence(self, symbol: str, *, lookback: int = 8):
            if symbol == "600519":
                return {"status": "stable", "label": "稳定", "reason": "验证稳定。"}
            return {"status": "degraded", "label": "退化", "reason": "最近退化。"}

    monkeypatch.setattr(pick_ranking, "StrategyRepository", _FakeStrategyRepository)

    ranked = pick_ranking.rank_market_items(
        [_watch_candidate("000333"), _watch_candidate("600519")],
        watch_symbols=set(),
    )

    assert ranked[0]["symbol"] == "600519"


def test_rank_market_items_prefers_higher_chips_score_when_other_dimensions_tie() -> None:
    first = _watch_candidate("000333")
    second = _watch_candidate("600519")
    first["dimensions"]["chips"] = {"score": 20}
    second["dimensions"]["chips"] = {"score": 80}

    ranked = pick_ranking.rank_market_items([first, second], watch_symbols=set())

    assert ranked[0]["symbol"] == "600519"


def test_rank_market_items_prefers_lower_p1_stock_crowding_risk_when_other_dimensions_tie(monkeypatch) -> None:
    pick_ranking._STRATEGY_CONFIDENCE_CACHE.clear()

    class _FakeStrategyRepository:
        def summarize_background_confidence(self, symbol: str, *, lookback: int = 8):
            return {}

    monkeypatch.setattr(pick_ranking, "StrategyRepository", _FakeStrategyRepository)
    first = _watch_candidate("000333")
    second = _watch_candidate("600519")
    first["dimensions"]["chips"] = {"score": 55}
    second["dimensions"]["chips"] = {"score": 55}
    first["dimensions"]["risk"] = {"score": 72}
    second["dimensions"]["risk"] = {"score": 42}

    ranked = pick_ranking.rank_market_items([second, first], watch_symbols=set())

    assert ranked[0]["symbol"] == "000333"
