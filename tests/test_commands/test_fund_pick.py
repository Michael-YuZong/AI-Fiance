"""Tests for daily off-exchange fund pick command."""

from __future__ import annotations

from src.commands.fund_pick import _payload_from_analyses


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
