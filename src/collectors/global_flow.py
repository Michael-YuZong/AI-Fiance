"""Offline-friendly global flow proxy collector."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional


def _value(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, Mapping):
        return item.get(key, default)
    return getattr(item, key, default)


class GlobalFlowCollector:
    """Infer global flow direction from relative asset strength."""

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        self.config = dict(config or {})

    def collect(self, snapshots: Iterable[Any]) -> Dict[str, Any]:
        rows = [item for item in snapshots]
        if not rows:
            return {
                "risk_bias": "neutral",
                "domestic_bias": "neutral",
                "flow_score": 0.0,
                "lines": ["当前没有可用于推断全球资金流向的资产快照。"],
                "method": "proxy",
                "confidence_score": 0.0,
                "confidence_label": "低",
                "limitations": ["缺少科技/黄金/国内/海外相对样本，当前无法形成稳定的风格代理判断。"],
                "downgrade_impact": "当前不适合把资金风格代理用于主线强弱判断，只能当作缺失提示。",
                "coverage_summary": "无有效代理样本",
            }

        tech = [item for item in rows if _value(item, "sector") == "科技"]
        gold = [item for item in rows if _value(item, "sector") == "黄金"]
        domestic = [item for item in rows if _value(item, "region") == "CN"]
        offshore = [item for item in rows if _value(item, "region") in {"US", "HK"}]

        tech_5d = self._avg(tech, "return_5d")
        gold_5d = self._avg(gold, "return_5d")
        domestic_5d = self._avg(domestic, "return_5d")
        offshore_5d = self._avg(offshore, "return_5d")
        tech_20d = self._avg(tech, "return_20d")
        gold_20d = self._avg(gold, "return_20d")

        risk_score = tech_5d - gold_5d
        domestic_score = domestic_5d - offshore_5d
        medium_term_score = tech_20d - gold_20d
        confidence = self._confidence_payload(
            tech=tech,
            gold=gold,
            domestic=domestic,
            offshore=offshore,
            rows=rows,
        )

        risk_bias = "risk_on" if risk_score > 0.015 else "risk_off" if risk_score < -0.015 else "neutral"
        domestic_bias = (
            "domestic_lead" if domestic_score > 0.015 else "offshore_lead" if domestic_score < -0.015 else "neutral"
        )

        lines: List[str] = []
        if risk_bias == "risk_on":
            lines.append(
                f"成长相对黄金更强，资金风格偏 risk-on（科技 5 日 {tech_5d * 100:+.2f}% vs 黄金 {gold_5d * 100:+.2f}%）。"
            )
        elif risk_bias == "risk_off":
            lines.append(
                f"黄金相对成长更抗跌，资金风格偏防守（科技 5 日 {tech_5d * 100:+.2f}% vs 黄金 {gold_5d * 100:+.2f}%）。"
            )
        else:
            lines.append("成长与黄金的相对强弱接近，当前风格轮动没有形成明确单边。")

        if domestic_bias == "domestic_lead":
            lines.append(
                f"国内资产相对海外更稳，说明资金更愿意回流或留在本土确定性方向（国内 {domestic_5d * 100:+.2f}% vs 海外 {offshore_5d * 100:+.2f}%）。"
            )
        elif domestic_bias == "offshore_lead":
            lines.append(
                f"海外资产相对国内更强，说明资金更偏向外盘成长或离岸弹性（国内 {domestic_5d * 100:+.2f}% vs 海外 {offshore_5d * 100:+.2f}%）。"
            )
        else:
            lines.append("国内与海外的相对表现接近，地域层面的资金迁移暂不明显。")

        if medium_term_score > 0.03:
            lines.append("20 日维度看，成长资产中期仍占优，短线回撤未必改变主线。")
        elif medium_term_score < -0.03:
            lines.append("20 日维度看，防守资产中期更占优，当前更适合先谈回撤控制。")
        else:
            lines.append("20 日维度看，主线尚不稳定，更适合边观察边验证。")

        return {
            "risk_bias": risk_bias,
            "domestic_bias": domestic_bias,
            "flow_score": risk_score + domestic_score,
            "lines": lines,
            "scores": {
                "tech_5d": tech_5d,
                "gold_5d": gold_5d,
                "domestic_5d": domestic_5d,
                "offshore_5d": offshore_5d,
                "tech_20d": tech_20d,
                "gold_20d": gold_20d,
            },
            "method": "proxy",
            "confidence_score": confidence["score"],
            "confidence_label": confidence["label"],
            "limitations": confidence["limitations"],
            "downgrade_impact": confidence["downgrade_impact"],
            "coverage_summary": confidence["coverage_summary"],
        }

    def _avg(self, rows: List[Any], key: str) -> float:
        if not rows:
            return 0.0
        return sum(float(_value(item, key, 0.0)) for item in rows) / len(rows)

    def _confidence_payload(
        self,
        *,
        tech: List[Any],
        gold: List[Any],
        domestic: List[Any],
        offshore: List[Any],
        rows: List[Any],
    ) -> Dict[str, Any]:
        coverage_bits = {
            "科技": bool(tech),
            "黄金": bool(gold),
            "国内": bool(domestic),
            "海外": bool(offshore),
        }
        score = sum(1.0 for present in coverage_bits.values() if present) / len(coverage_bits)
        if len(rows) < 3:
            score *= 0.7
        elif len(rows) >= 5:
            score = min(score + 0.1, 1.0)

        if score >= 0.8:
            label = "高"
        elif score >= 0.5:
            label = "中"
        else:
            label = "低"

        missing = [name for name, present in coverage_bits.items() if not present]
        limitations = [
            "这是相对强弱代理，不是 ETF 申购赎回、北向/南向或机构资金原始流向数据。",
        ]
        if missing:
            limitations.append(f"当前缺少 `{ ' / '.join(missing) }` 样本，风格判断更容易失真。")
        if len(rows) < 3:
            limitations.append("代理样本数偏少，当前更适合看方向提示，不适合下强结论。")

        downgrade_impact = (
            "可作为市场风格和防守/进攻切换的辅助证据，但不应单独决定个股或单只 ETF 的交易动作。"
            if score >= 0.5
            else "当前代理覆盖不足，只适合提示风格分歧，不能当成稳定的主线或资金结论。"
        )
        covered = [name for name, present in coverage_bits.items() if present]
        coverage_summary = " / ".join(covered) if covered else "无有效代理样本"
        return {
            "score": round(score, 2),
            "label": label,
            "limitations": limitations,
            "downgrade_impact": downgrade_impact,
            "coverage_summary": coverage_summary,
        }
