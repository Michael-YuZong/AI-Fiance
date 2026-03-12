"""Tests for open-end fund profile collector."""

from __future__ import annotations

import pandas as pd

from src.collectors.fund_profile import FundProfileCollector


def test_fund_profile_collects_holdings_and_manager_style(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "get_fund_basic", lambda market="O": pd.DataFrame())  # noqa: ARG005

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


def test_fund_profile_prefers_fund_benchmark_theme_over_manager_peer_funds(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "get_fund_basic", lambda market="O": pd.DataFrame())  # noqa: ARG005

    monkeypatch.setattr(
        collector,
        "get_overview",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "基金简称": "前海开源黄金ETF联接C",
                    "基金类型": "指数型-其他",
                    "基金管理人": "前海开源基金",
                    "基金经理人": "梁溥森、孔芳",
                    "净资产规模": "9.38亿元（截止至：2025年12月31日）",
                    "业绩比较基准": "上海黄金交易所Au99.99现货实盘合约收益率*90%+人民币活期存款税后利率*10%",
                    "成立日期/规模": "2024年06月19日 / --",
                }
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_asset_allocation",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"资产类型": "债券", "仓位占比": 4.59},
                {"资产类型": "现金", "仓位占比": 1.65},
                {"资产类型": "其他", "仓位占比": 96.06},
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_portfolio_hold", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_manager_directory",
        lambda: pd.DataFrame(
            [
                {"姓名": "梁溥森", "所属公司": "前海开源基金", "现任基金代码": "021740", "现任基金": "前海开源黄金ETF联接C", "累计从业时间": 1742, "现任基金资产总规模": 27.93, "现任基金最佳回报": 183.03},
                {"姓名": "梁溥森", "所属公司": "前海开源基金", "现任基金代码": "022365", "现任基金": "前海开源科技成长混合A", "累计从业时间": 1742, "现任基金资产总规模": 27.93, "现任基金最佳回报": 183.03},
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())

    profile = collector.collect_profile("021740")
    assert profile["style"]["sector"] == "黄金"
    assert "黄金主题" in profile["style"]["tags"]
    assert "被动跟踪" in profile["style"]["tags"]
    assert "被动暴露" in profile["style"]["summary"]
    assert "不是基金经理主动选股" in profile["style"]["selection"]


def test_fund_profile_industry_allocation_swallows_known_shape_errors(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "get_fund_basic", lambda market="O": pd.DataFrame())  # noqa: ARG005

    def broken_fetcher(**kwargs):  # noqa: ARG001
        raise ValueError("Length mismatch: Expected axis has 1 elements, new values have 17 elements")

    monkeypatch.setattr(collector, "_ak_function", lambda name: broken_fetcher)  # noqa: ARG005
    monkeypatch.setattr(collector, "_year_candidates", lambda: ["2025"])

    frame = collector.get_industry_allocation("021740")
    assert isinstance(frame, pd.DataFrame)
    assert frame.empty


def test_fund_profile_prefers_benchmark_theme_over_secondary_industries(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "get_fund_basic", lambda market="O": pd.DataFrame())  # noqa: ARG005

    monkeypatch.setattr(
        collector,
        "get_overview",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "基金简称": "国泰恒生A股电网设备ETF",
                    "基金类型": "指数型-股票",
                    "基金管理人": "国泰基金",
                    "基金经理人": "吴中昊",
                    "净资产规模": "5.52亿元（截止至：2025年12月31日）",
                    "业绩比较基准": "恒生A股电网设备指数收益率",
                    "跟踪标的": "恒生A股电网设备指数",
                    "成立日期/规模": "2024年12月11日 / 2.624亿份",
                }
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_asset_allocation",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"资产类型": "股票", "仓位占比": 99.15},
                {"资产类型": "现金", "仓位占比": 0.90},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_portfolio_hold",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"股票代码": "600089", "股票名称": "特变电工", "占净值比例": 9.76, "持股数": 242.51, "持仓市值": 5388.52, "季度": "2025年4季度股票投资明细"},
                {"股票代码": "600406", "股票名称": "国电南瑞", "占净值比例": 9.51, "持股数": 233.64, "持仓市值": 5252.21, "季度": "2025年4季度股票投资明细"},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_industry_allocation",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"行业类别": "信息传输、软件和信息技术服务业", "占净值比例": 12.0, "市值": 6664.26, "截止时间": "2025-12-31"},
                {"行业类别": "制造业", "占净值比例": 80.0, "市值": 46631.72, "截止时间": "2025-12-31"},
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005

    profile = collector.collect_profile("561380")
    assert profile["style"]["sector"] == "电网"
    assert "电网主题" in profile["style"]["tags"]


def test_fund_profile_backfills_overview_from_tushare_when_overview_empty(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})

    monkeypatch.setattr(collector, "get_overview", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_asset_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_portfolio_hold", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_fund_basic",
        lambda market="O": pd.DataFrame(  # noqa: ARG005
            [
                {
                    "ts_code": "510880.SH",
                    "name": "华泰柏瑞上证红利ETF",
                    "fund_type": "被动指数型",
                    "invest_type": "股票型",
                    "management": "华泰柏瑞基金",
                    "custodian": "招商银行",
                    "issue_date": "20061109",
                    "found_date": "20061117",
                    "issue_amount": 24.723,
                    "benchmark": "上证红利指数",
                    "list_date": "20070118",
                    "m_fee": 0.5,
                    "c_fee": 0.1,
                }
            ]
        ),
    )

    profile = collector.collect_profile("510880")
    overview = profile["overview"]

    assert overview["基金简称"] == "华泰柏瑞上证红利ETF"
    assert overview["基金管理人"] == "华泰柏瑞基金"
    assert overview["基金类型"] == "被动指数型 / 股票型"
    assert overview["业绩比较基准"] == "上证红利指数"
    assert overview["跟踪标的"] == "上证红利指数"
    assert overview["成立日期/规模"] == "2006-11-17 / 24.7230亿份"


def test_fund_profile_prefers_tushare_basic_fields_and_uses_overview_for_missing_details(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})

    monkeypatch.setattr(
        collector,
        "get_overview",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "基金简称": "第三方名称",
                    "基金类型": "第三方类型",
                    "基金管理人": "第三方公司",
                    "基金经理人": "柳军、李茜",
                    "净资产规模": "192.65亿元（截止至：2025年12月31日）",
                    "业绩比较基准": "第三方基准",
                }
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_asset_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_portfolio_hold", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_fund_basic",
        lambda market="O": pd.DataFrame(  # noqa: ARG005
            [
                {
                    "ts_code": "510880.SH",
                    "name": "华泰柏瑞上证红利ETF",
                    "fund_type": "被动指数型",
                    "invest_type": "股票型",
                    "management": "华泰柏瑞基金",
                    "custodian": "招商银行",
                    "issue_date": "20061109",
                    "found_date": "20061117",
                    "issue_amount": 24.723,
                    "benchmark": "上证红利指数",
                    "list_date": "20070118",
                }
            ]
        ),
    )

    profile = collector.collect_profile("510880")
    overview = profile["overview"]

    assert overview["基金简称"] == "华泰柏瑞上证红利ETF"
    assert overview["基金类型"] == "被动指数型 / 股票型"
    assert overview["基金管理人"] == "华泰柏瑞基金"
    assert overview["业绩比较基准"] == "上证红利指数"
    assert overview["基金经理人"] == "柳军、李茜"
    assert overview["净资产规模"] == "192.65亿元（截止至：2025年12月31日）"


def test_fund_profile_does_not_render_nan_issue_amount(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})

    monkeypatch.setattr(collector, "get_overview", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_asset_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_portfolio_hold", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_fund_basic",
        lambda market="O": pd.DataFrame(  # noqa: ARG005
            [
                {
                    "ts_code": "021740.OF",
                    "name": "前海开源黄金ETF联接C",
                    "fund_type": "商品型",
                    "invest_type": "黄金现货合约",
                    "management": "前海开源基金",
                    "found_date": "20240619",
                    "issue_amount": float("nan"),
                    "benchmark": "上海黄金交易所Au99.99现货实盘合约收盘价收益率*90%+人民币活期存款利率(税后)*10%",
                }
            ]
        ),
    )

    profile = collector.collect_profile("021740")
    assert profile["overview"]["成立日期/规模"] == "2024-06-19"
