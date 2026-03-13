"""Tests for open-end fund pool discovery filters."""

from __future__ import annotations

import pandas as pd

from src.processors.opportunity_engine import build_fund_pool


def test_build_fund_pool_applies_style_and_manager_filters(monkeypatch) -> None:
    frame = pd.DataFrame(
        [
            {
                "ts_code": "000001.OF",
                "name": "科技成长混合A",
                "status": "L",
                "fund_type": "混合型",
                "invest_type": "灵活配置型",
                "management": "永赢基金",
                "benchmark": "科技成长指数收益率",
                "found_date": "20200101",
                "issue_amount": 10.0,
            },
            {
                "ts_code": "000002.OF",
                "name": "半导体指数增强A",
                "status": "L",
                "fund_type": "股票型",
                "invest_type": "增强指数型",
                "management": "易方达基金",
                "benchmark": "中证半导体指数收益率",
                "found_date": "20200101",
                "issue_amount": 12.0,
            },
            {
                "ts_code": "000003.OF",
                "name": "黄金主题联接C",
                "status": "L",
                "fund_type": "商品型",
                "invest_type": "黄金现货合约",
                "management": "前海开源基金",
                "benchmark": "上海金收益率",
                "found_date": "20200101",
                "issue_amount": 8.0,
            },
        ]
    )

    monkeypatch.setattr(
        "src.processors.opportunity_engine.FundProfileCollector.get_fund_basic",
        lambda self, market="O": frame,
    )

    pool, warnings = build_fund_pool(
        {},
        theme_filter="半导体",
        preferred_sectors=["科技"],
        max_candidates=10,
        style_filter="index",
        manager_filter="易方达",
    )

    assert warnings == []
    assert [item.symbol for item in pool] == ["000002"]
    assert pool[0].metadata["management"] == "易方达基金"


def test_build_fund_pool_reports_filters_when_no_candidate_left(monkeypatch) -> None:
    frame = pd.DataFrame(
        [
            {
                "ts_code": "000001.OF",
                "name": "科技成长混合A",
                "status": "L",
                "fund_type": "混合型",
                "invest_type": "灵活配置型",
                "management": "永赢基金",
                "benchmark": "科技成长指数收益率",
                "found_date": "20200101",
                "issue_amount": 10.0,
            }
        ]
    )

    monkeypatch.setattr(
        "src.processors.opportunity_engine.FundProfileCollector.get_fund_basic",
        lambda self, market="O": frame,
    )

    pool, warnings = build_fund_pool({}, theme_filter="黄金", style_filter="commodity", manager_filter="前海")

    assert pool == []
    assert len(warnings) == 1
    assert "主题=黄金" in warnings[0]
    assert "风格=commodity" in warnings[0]
    assert "管理人=前海" in warnings[0]


def test_build_fund_pool_does_not_recreate_nan_row_after_empty_filters(monkeypatch) -> None:
    frame = pd.DataFrame(
        [
            {
                "ts_code": "000001.OF",
                "name": "科技成长混合A",
                "status": "L",
                "fund_type": "混合型",
                "invest_type": "灵活配置型",
                "management": "永赢基金",
                "benchmark": "科技成长指数收益率",
                "found_date": "20200101",
                "issue_amount": 10.0,
            }
        ]
    )

    monkeypatch.setattr(
        "src.processors.opportunity_engine.FundProfileCollector.get_fund_basic",
        lambda self, market="O": frame,
    )

    pool, warnings = build_fund_pool({}, theme_filter="黄金")

    assert pool == []
    assert warnings
