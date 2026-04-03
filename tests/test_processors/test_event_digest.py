from __future__ import annotations

from src.processors.event_digest import build_analysis_event_digest


def test_event_digest_marks_search_gap_as_pending_review() -> None:
    analysis = {
        "name": "半导体ETF",
        "symbol": "512480",
        "asset_type": "cn_etf",
        "dimensions": {
            "catalyst": {
                "summary": "当前实时新闻关键词检索未命中高置信标题；本次催化维度暂按待 AI 联网复核处理。",
                "coverage": {
                    "diagnosis": "suspected_search_gap",
                    "ai_web_search_recommended": True,
                    "news_mode": "live",
                },
                "evidence": [],
                "theme_news": [],
            }
        },
    }

    digest = build_analysis_event_digest(analysis)

    assert digest["status"] == "待复核"
    assert "这件事改变了什么" in "\n".join(digest["summary_lines"])
    assert "联网复核" in digest["status_reason"]


def test_event_digest_marks_structured_earnings_event_as_digested() -> None:
    analysis = {
        "name": "新易盛",
        "symbol": "300502",
        "asset_type": "cn_stock",
        "dimensions": {
            "catalyst": {
                "summary": "财报窗口已进入前瞻期。",
                "coverage": {
                    "diagnosis": "confirmed_live",
                    "ai_web_search_recommended": False,
                    "news_mode": "live",
                },
                "evidence": [
                    {
                        "layer": "结构化事件",
                        "title": "新易盛预计于 2026-04-15 披露 2026Q1 财报",
                        "source": "Tushare",
                        "date": "2026-04-15",
                    }
                ],
                "theme_news": [],
            }
        },
    }

    digest = build_analysis_event_digest(analysis)

    assert digest["status"] == "已消化"
    assert digest["top_event_type"] == "财报"
    assert "盈利" in digest["what_changed"] or "指引" in digest["what_changed"]


def test_event_digest_marks_stale_live_only_as_pending_fill() -> None:
    analysis = {
        "name": "半导体ETF",
        "symbol": "512480",
        "asset_type": "cn_etf",
        "dimensions": {
            "catalyst": {
                "summary": "当前能命中的多是旧闻回放或背景线索，新增催化仍不足。",
                "coverage": {
                    "diagnosis": "stale_live_only",
                    "ai_web_search_recommended": False,
                    "news_mode": "live",
                },
                "evidence": [],
                "theme_news": [
                    {
                        "layer": "主题级关键新闻",
                        "title": "半导体设备需求持续改善",
                        "source": "Reuters",
                        "date": "2026-03-20",
                    }
                ],
            }
        },
    }

    digest = build_analysis_event_digest(analysis)

    assert digest["status"] == "待补充"
    assert "旧闻回放" in digest["status_reason"]
