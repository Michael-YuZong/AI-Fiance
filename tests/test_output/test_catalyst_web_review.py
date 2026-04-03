from src.output.catalyst_web_review import (
    attach_catalyst_web_review_to_analysis,
    build_catalyst_web_review_packet,
    catalyst_web_review_has_completed_conclusion,
    load_catalyst_web_review,
    preserve_existing_catalyst_web_review,
    render_catalyst_web_review_prompt,
    render_catalyst_web_review_scaffold,
)


def _analysis(symbol: str, name: str, *, diagnosis: str = "", recommended: bool = False) -> dict:
    return {
        "symbol": symbol,
        "name": name,
        "asset_type": "cn_etf",
        "generated_at": "2026-03-26 10:00:00",
        "history": None,
        "metadata": {
            "sector": "半导体",
            "chain_nodes": ["AI算力", "HBM"],
        },
        "taxonomy_summary": "半导体 / AI算力 / 先进封装",
        "day_theme": {"label": "科技成长"},
        "dimensions": {
            "catalyst": {
                "summary": "当前实时新闻关键词检索未命中高置信标题，先按待 AI 联网复核处理。",
                "coverage": {
                    "diagnosis": diagnosis,
                    "ai_web_search_recommended": recommended,
                    "news_mode": "live",
                    "search_result_count": 0,
                    "search_groups": [["半导体ETF", "512480"], ["HBM", "先进封装"]],
                },
            }
        },
    }


def test_build_catalyst_web_review_packet_collects_flagged_items() -> None:
    packet = build_catalyst_web_review_packet(
        report_type="scan",
        subject="半导体ETF scan",
        generated_at="2026-03-26 10:00:00",
        analyses=[
            _analysis("512480", "半导体ETF", diagnosis="suspected_search_gap", recommended=True),
            _analysis("159938", "医药ETF", diagnosis="", recommended=False),
        ],
    )
    assert packet["packet_version"] == "catalyst-web-review-v1"
    assert len(packet["items"]) == 1
    item = packet["items"][0]
    assert item["symbol"] == "512480"
    assert item["catalyst_diagnosis"] == "suspected_search_gap"
    assert item["sector"] == "半导体"
    assert item["theme_name"] == "先进封装 / HBM / Chiplet"
    assert item["theme_family"] == "技术路线"
    assert "当前更像在交易" in item["playbook_hint"]


def test_render_catalyst_web_review_prompt_and_scaffold_include_context() -> None:
    packet = build_catalyst_web_review_packet(
        report_type="scan",
        subject="半导体ETF scan",
        generated_at="2026-03-26 10:00:00",
        analyses=[_analysis("512480", "半导体ETF", diagnosis="suspected_search_gap", recommended=True)],
    )
    prompt = render_catalyst_web_review_prompt(packet)
    scaffold = render_catalyst_web_review_scaffold(packet)
    assert "docs/prompts/financial_catalyst_web_researcher.md" in prompt
    assert "半导体ETF (512480)" in prompt
    assert "HBM / 先进封装" in prompt
    assert "主题认知提示" in prompt
    assert "待独立 agent / subagent 联网复核" in scaffold
    assert "### 复核结论" in scaffold


def test_load_and_attach_completed_catalyst_web_review(tmp_path) -> None:
    review_path = tmp_path / "scan_512480_2026-03-26_catalyst_web_review.md"
    review_path.write_text(
        "\n".join(
            [
                "# Catalyst Web Review | scan | 2026-03-26",
                "",
                "## 1. 半导体ETF (512480)",
                "",
                "### 复核结论",
                "",
                "- 结论：只有主题级催化",
                "",
                "### 关键证据",
                "",
                "- 证券时报 2026-03-25：半导体设备链继续讨论扩产。",
                "",
                "### 影响判断",
                "",
                "- 不足以把观察稿升级为推荐稿，但不能再写成“零催化”。",
                "",
                "### 边界",
                "",
                "- 主题新闻不等于公司直接催化。",
            ]
        ),
        encoding="utf-8",
    )
    lookup = load_catalyst_web_review(review_path)
    enriched = attach_catalyst_web_review_to_analysis(_analysis("512480", "半导体ETF", diagnosis="suspected_search_gap", recommended=True), lookup)
    assert catalyst_web_review_has_completed_conclusion(review_path.read_text(encoding="utf-8"))
    assert enriched["catalyst_web_review"]["decision"] == "只有主题级催化"
    assert enriched["dimensions"]["catalyst"]["coverage"]["diagnosis"] == "web_review_completed"
    assert "联网复核" in enriched["dimensions"]["catalyst"]["summary"]


def test_preserve_existing_completed_catalyst_web_review(tmp_path) -> None:
    review_path = tmp_path / "scan_512480_2026-03-26_catalyst_web_review.md"
    existing = "\n".join(
        [
            "# Catalyst Web Review | scan | 2026-03-26",
            "",
            "## 1. 半导体ETF (512480)",
            "",
            "### 复核结论",
            "",
            "- 结论：已确认直接催化",
        ]
    )
    review_path.write_text(existing, encoding="utf-8")
    preserved = preserve_existing_catalyst_web_review(review_path, "# scaffold")
    assert preserved == existing
