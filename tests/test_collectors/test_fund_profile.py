"""Tests for open-end fund profile collector."""

from __future__ import annotations

import pandas as pd

from src.collectors.fund_profile import FundProfileCollector


def test_fund_profile_collects_holdings_and_manager_style(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})

    monkeypatch.setattr(
        collector,
        "get_overview",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "基金简称": "永赢科技智选混合发起C",
                    "基金类型": "混合型-偏股",
                    "基金管理人": "永赢基金",
                    "基金经理人": "任桀",
                    "净资产规模": "107.79亿元（截止至：2025年12月31日）",
                    "业绩比较基准": "中国战略新兴产业成份指数收益率*70%+恒生科技指数收益率*10%+中债-综合指数(全价)收益率*20%",
                    "成立日期/规模": "2024年10月30日 / 0.103亿份",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_achievement",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"周期": "今年以来", "本产品区间收益": -1.36, "本产品最大回撒": 6.8, "周期收益同类排名": "3780/5246"},
                {"周期": "近1月", "本产品区间收益": -1.06, "本产品最大回撒": None, "周期收益同类排名": "968/5198"},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_asset_allocation",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"资产类型": "股票", "仓位占比": 80.34},
                {"资产类型": "现金", "仓位占比": 18.91},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_portfolio_hold",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"股票代码": "300738", "股票名称": "奥飞数据", "占净值比例": 9.30, "持股数": 96.4, "持仓市值": 2338.66, "季度": "2025年1季度股票投资明细"},
                {"股票代码": "300308", "股票名称": "中际旭创", "占净值比例": 8.40, "持股数": 20.1, "持仓市值": 2110.00, "季度": "2025年1季度股票投资明细"},
                {"股票代码": "688256", "股票名称": "寒武纪", "占净值比例": 7.20, "持股数": 10.3, "持仓市值": 1800.00, "季度": "2025年1季度股票投资明细"},
                {"股票代码": "301269", "股票名称": "华大九天", "占净值比例": 6.80, "持股数": 10.3, "持仓市值": 1700.00, "季度": "2025年1季度股票投资明细"},
                {"股票代码": "688082", "股票名称": "盛美上海", "占净值比例": 6.20, "持股数": 10.3, "持仓市值": 1600.00, "季度": "2025年1季度股票投资明细"},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_industry_allocation",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"行业类别": "科技", "占净值比例": 45.2, "市值": 1000000, "截止时间": "2025-12-31"},
                {"行业类别": "信息传输、软件和信息技术服务业", "占净值比例": 27.0, "市值": 700000, "截止时间": "2025-12-31"},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_manager_directory",
        lambda: pd.DataFrame(
            [
                {"姓名": "任桀", "所属公司": "永赢基金", "现任基金代码": "022365", "现任基金": "永赢科技智选混合发起C", "累计从业时间": 495, "现任基金资产总规模": 161.72, "现任基金最佳回报": 281.99},
                {"姓名": "任桀", "所属公司": "永赢基金", "现任基金代码": "024735", "现任基金": "永赢港股通科技智选混合发起A", "累计从业时间": 495, "现任基金资产总规模": 161.72, "现任基金最佳回报": 281.99},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_rating_table",
        lambda: pd.DataFrame(
            [
                {"代码": "022365", "5星评级家数": 1, "上海证券": 4, "招商证券": 5, "济安金信": 4, "晨星评级": 3, "类型": "混合型-偏股", "手续费": 0.0}
            ]
        ),
    )

    profile = collector.collect_profile("022365")
    assert profile["overview"]["基金简称"] == "永赢科技智选混合发起C"
    assert profile["style"]["sector"] == "科技"
    assert "科技主题" in profile["style"]["tags"]
    assert "高仓位主动" in profile["style"]["tags"]
    assert "保留机动仓位" in profile["style"]["tags"]
    assert profile["manager"]["name"] == "任桀"
    assert len(profile["top_holdings"]) == 5
