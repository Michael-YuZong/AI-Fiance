"""Tests for daily off-exchange fund pick command."""

from __future__ import annotations

from src.commands.fund_pick import _payload_from_analyses, _selection_context


def _analysis(
    symbol: str,
    name: str,
    sector: str,
    technical: int,
    fundamental: int,
    catalyst: int,
    relative: int,
    risk: int,
    macro: int = 10,
) -> dict:
    return {
        "symbol": symbol,
        "name": name,
        "generated_at": "2026-03-11 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "metadata": {"sector": sector, "fund_style_tags": [f"{sector}主题"]},
        "fund_profile": {"overview": {"业绩比较基准": f"{sector} 相关基准"}},
        "narrative": {"judgment": {"state": "持有优于追高"}, "cautions": ["短线不适合追高"]},
        "action": {
            "direction": "回避",
            "entry": "等确认",
            "position": "≤2% 试探",
            "scaling_plan": "仅观察仓，不加仓",
            "stop": "跌破支撑处理",
        },
        "dimensions": {
            "technical": {"score": technical, "max_score": 100, "summary": "技术结构一般"},
            "fundamental": {"score": fundamental, "max_score": 100, "summary": "估值或持仓代理"},
            "catalyst": {"score": catalyst, "max_score": 100, "summary": "短线催化仍在"},
            "relative_strength": {"score": relative, "max_score": 100, "summary": "相对走势比较"},
            "chips": {"score": 0, "max_score": 100, "summary": "筹码一般"},
            "risk": {"score": risk, "max_score": 100, "summary": "回撤和组合风险较可控"},
            "seasonality": {"score": 0, "max_score": 100, "summary": "时间窗口中性"},
            "macro": {"score": macro, "max_score": 40, "summary": "宏观顺风"},
        },
        "rating": {"rank": 0, "label": "无信号"},
    }


def test_payload_prefers_gold_candidate_in_defensive_mode() -> None:
    payload = _payload_from_analyses(
        [
            _analysis("021740", "前海开源黄金ETF联接C", "黄金", technical=44, fundamental=0, catalyst=80, relative=20, risk=75),
            _analysis("022365", "永赢科技智选混合发起C", "科技", technical=35, fundamental=75, catalyst=50, relative=47, risk=85, macro=30),
            _analysis("025832", "天弘电网设备特高压指数A", "电网", technical=50, fundamental=0, catalyst=25, relative=25, risk=10),
        ]
    )

    assert payload["winner"]["symbol"] == "021740"
    assert any("防守" in line or "黄金" in line for line in payload["winner"]["positives"])
    assert payload["alternatives"][0]["symbol"] == "022365"


def test_payload_keeps_selection_context() -> None:
    payload = _payload_from_analyses(
        [_analysis("022365", "永赢科技智选混合发起C", "科技", technical=35, fundamental=75, catalyst=50, relative=47, risk=85, macro=30)],
        selection_context=_selection_context(
            discovery_mode="full_universe",
            scan_pool=12,
            passed_pool=5,
            theme_filter="科技",
            style_filter="active",
            manager_filter="永赢",
            blind_spots=["全市场基金池有 3 只因画像拉取失败被跳过。"],
        ),
    )

    assert payload["selection_context"]["discovery_mode_label"] == "全市场初筛"
    assert payload["selection_context"]["style_filter_label"] == "主动权益"
    assert payload["selection_context"]["blind_spots"] == ["全市场基金池有 3 只因画像拉取失败被跳过。"]


def test_payload_uses_shared_dimension_summary_for_catalyst() -> None:
    analysis = _analysis("021740", "前海开源黄金ETF联接C", "黄金", technical=30, fundamental=45, catalyst=23, relative=35, risk=60)
    analysis["dimensions"]["catalyst"] = {
        "score": 23,
        "max_score": 100,
        "summary": "催化不足，当前更像静态博弈。",
        "factors": [
            {"name": "政策催化", "display_score": "0/30"},
            {"name": "产品/跟踪方向催化", "display_score": "12/12"},
            {"name": "研报/新闻密度", "display_score": "10/10"},
            {"name": "新闻热度", "display_score": "10/10"},
        ],
    }

    payload = _payload_from_analyses([analysis])

    catalyst_row = next(row for row in payload["winner"]["dimension_rows"] if row[0] == "催化面")
    assert catalyst_row[2] == "直接催化偏弱，舆情关注度尚可，因此当前更像静态博弈。"


def test_payload_marks_fund_chips_as_auxiliary_dimension() -> None:
    analysis = _analysis("021740", "前海开源黄金ETF联接C", "黄金", technical=30, fundamental=45, catalyst=23, relative=35, risk=60)

    payload = _payload_from_analyses([analysis])

    chips_row = next(row for row in payload["winner"]["dimension_rows"] if row[0] == "筹码结构（辅助项）")
    assert chips_row[1] == "辅助项"
    assert "主排序不直接使用这项" in chips_row[2]


def test_payload_keeps_regime_context() -> None:
    analysis = _analysis("022365", "永赢科技智选混合发起C", "科技", technical=35, fundamental=75, catalyst=50, relative=47, risk=85, macro=30)

    payload = _payload_from_analyses(
        [analysis],
        regime={
            "current_regime": "recovery",
            "reasoning": ["PMI 回到 50 上方。", "信用脉冲边际改善。"],
        },
        day_theme={"label": "能源冲击 + 地缘风险"},
    )

    assert payload["regime"]["current_regime"] == "recovery"
    assert payload["day_theme"]["label"] == "能源冲击 + 地缘风险"


def test_payload_keeps_provenance_fields_for_renderer() -> None:
    analysis = _analysis("022365", "永赢科技智选混合发起C", "科技", technical=35, fundamental=75, catalyst=50, relative=47, risk=85, macro=30)
    analysis["history"] = {"stub": True}
    analysis["intraday"] = {"enabled": False}
    analysis["metadata"]["history_source_label"] = "基金净值"
    analysis["benchmark_name"] = "中证科技"
    analysis["benchmark_symbol"] = "000985"
    analysis["provenance"] = {
        "analysis_generated_at": "2026-03-11 10:00",
        "market_data_as_of": "2026-03-11",
        "relative_benchmark_name": "中证科技",
        "relative_benchmark_symbol": "000985",
        "news_mode": "proxy",
    }

    payload = _payload_from_analyses([analysis], {})

    assert payload["winner"]["generated_at"] == "2026-03-11 10:00:00"
    assert payload["winner"]["provenance"]["relative_benchmark_symbol"] == "000985"
    assert payload["winner"]["benchmark_name"] == "中证科技"
