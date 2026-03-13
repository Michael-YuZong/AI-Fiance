"""Policy deep-dive command."""

from __future__ import annotations

import argparse

from src.output.policy_report import PolicyReportRenderer
from src.processors.policy_engine import PolicyEngine
from src.storage.portfolio import PortfolioRepository
from src.utils.config import load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze a policy keyword or URL.")
    parser.add_argument("target", help="Policy keyword or URL")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    load_config(args.config or None)
    engine = PolicyEngine()
    try:
        context = engine.load_context(args.target)
    except Exception:
        context = engine.load_context(args.target if not args.target.startswith("http") else args.target.split("/")[-1])
    matched = engine.match_policy(f"{context.title} {context.text}")
    template = matched.template if matched else {
        "name": context.title,
        "policy_goal": "从原文中未匹配到现成模板，当前使用通用结构化输出。",
        "timeline": "需要结合后续细则和项目进度跟踪。",
        "support_points": ["原文需人工复核重点支持方向"],
        "beneficiary_nodes": [],
        "risk_nodes": ["落地节奏不确定"],
        "mapped_assets": [],
        "headline_numbers": [],
    }

    holdings = PortfolioRepository().list_holdings()
    extracted_numbers = engine.extract_numbers(context.text)
    timeline_points = engine.extract_timeline_points(context.text)
    headline_numbers = list(template.get("headline_numbers", []))
    for item in extracted_numbers:
        if item not in headline_numbers:
            headline_numbers.append(item)

    policy_direction = engine.classify_policy_direction(f"{context.title} {context.text}")
    policy_stage = engine.infer_policy_stage(context.title, context.text)
    benefit_risk_lines = [f"受益方向：{', '.join(template.get('beneficiary_nodes', [])) or '待人工补充'}"]
    benefit_risk_lines.append(f"风险点：{', '.join(template.get('risk_nodes', [])) or '未明显识别'}")

    payload = {
        "title": context.title,
        "source": context.source,
        "theme": template.get("name", context.title),
        "summary": f"该主题的核心在于 {template.get('policy_goal', '')}",
        "match_confidence": matched.confidence_label if matched else "低",
        "matched_aliases": matched.matched_aliases if matched else [],
        "policy_direction": policy_direction,
        "policy_stage": policy_stage,
        "policy_goal": template.get("policy_goal", ""),
        "timeline": template.get("timeline", ""),
        "timeline_points": timeline_points,
        "support_points": template.get("support_points", []),
        "benefit_risk_lines": benefit_risk_lines,
        "headline_numbers": headline_numbers[:6],
        "watchlist_impact": engine.watchlist_impact(template, holdings),
        "raw_excerpt": context.text[:220] + ("..." if len(context.text) > 220 else ""),
    }
    print(PolicyReportRenderer().render(payload))


if __name__ == "__main__":
    main()
