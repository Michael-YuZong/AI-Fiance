"""Tests for open-end fund profile collector."""

from __future__ import annotations

import pandas as pd
import pytest

from src.collectors import fund_profile as fund_profile_module
from src.collectors.fund_profile import FundProfileCollector
from src.utils.fund_taxonomy import build_standard_fund_taxonomy


def test_fund_profile_shared_tables_reuse_process_cache(monkeypatch):
    fund_profile_module._PROCESS_SHARED_FRAME_CACHE.clear()
    calls = {"manager": 0, "rating": 0}

    def fake_cached_call(self, cache_key, fetcher, *args, **kwargs):  # noqa: ANN001, ARG002
        if cache_key == "fund_profile:manager_directory":
            calls["manager"] += 1
            return pd.DataFrame([{"姓名": "张三"}])
        if cache_key == "fund_profile:rating_all":
            calls["rating"] += 1
            return pd.DataFrame([{"代码": "513120"}])
        raise AssertionError(cache_key)

    monkeypatch.setattr(FundProfileCollector, "cached_call", fake_cached_call)

    first = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    second = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})

    assert not first.get_manager_directory().empty
    assert not second.get_manager_directory().empty
    assert not first.get_rating_table().empty
    assert not second.get_rating_table().empty
    assert calls == {"manager": 1, "rating": 1}


def test_build_standard_fund_taxonomy_identifies_agriculture_theme():
    taxonomy = build_standard_fund_taxonomy(
        name="粮食ETF",
        fund_type="股票型 / 被动指数型",
        benchmark="国证粮食产业指数收益率",
        asset_type="cn_etf",
    )

    assert taxonomy["sector"] == "农业"
    assert taxonomy["exposure_scope"] == "行业主题"
    assert "农业方向" in taxonomy["labels"]


def test_fund_profile_style_refines_broad_ai_index_with_cpo_holdings() -> None:
    collector = FundProfileCollector({})

    style = collector._derive_style(  # noqa: SLF001
        {
            "基金简称": "华宝创业板人工智能ETF",
            "基金类型": "ETF",
            "业绩比较基准": "创业板人工智能指数",
            "跟踪标的": "创业板人工智能指数",
        },
        [
            {"股票名称": "新易盛", "占净值比例": 15.37},
            {"股票名称": "中际旭创", "占净值比例": 14.03},
            {"股票名称": "天孚通信", "占净值比例": 9.80},
        ],
        [],
        [{"资产类型": "股票", "仓位占比": 98.79}],
        {},
        asset_type="cn_etf",
    )

    assert style["sector"] == "通信"
    assert style["taxonomy"]["primary_chain"] == "CPO/光模块"
    assert style["taxonomy"]["theme_role"] == "AI硬件主链"


def test_fund_profile_collects_holdings_and_manager_style(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "get_fund_basic", lambda market="O": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005

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
    assert profile["style"]["taxonomy"]["management_style"] == "主动管理"
    assert profile["style"]["taxonomy"]["sector"] == "科技"
    assert profile["style"]["taxonomy"]["exposure_scope"] == "行业主题"
    assert profile["manager"]["name"] == "任桀"
    assert len(profile["top_holdings"]) == 5


def test_fund_profile_prefers_etf_basic_index_and_share_size(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "get_overview", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_asset_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_portfolio_hold", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_etf_basic_ts",
        lambda symbol="": pd.DataFrame(
            [
                {
                    "ts_code": "510300.SH",
                    "csname": "沪深300ETF",
                    "extname": "沪深300ETF华泰柏瑞",
                    "cname": "华泰柏瑞沪深300ETF",
                    "index_code": "000300.SH",
                    "index_name": "沪深300指数",
                    "setup_date": "20110718",
                    "list_date": "20110802",
                    "mgr_name": "华泰柏瑞基金",
                    "custod_name": "中国银行",
                    "mgt_fee": 0.15,
                    "etf_type": "境内",
                    "exchange": "SH",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_etf_index_ts",
        lambda symbol="": pd.DataFrame(
            [
                {
                    "ts_code": "000300.SH",
                    "indx_name": "沪深300指数",
                    "indx_csname": "沪深300",
                    "pub_party_name": "中证指数有限公司",
                    "pub_date": "20050408",
                    "base_date": "20050408",
                    "bp": 1000.0,
                    "adj_circle": "半年",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_etf_share_size_ts",
        lambda symbol, **kwargs: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "trade_date": "2026-03-12",
                    "ts_code": "510300.SH",
                    "etf_name": "沪深300ETF",
                    "total_share": 4_741_854.98,
                    "total_size": 22_878_980.0,
                    "nav": 4.8332,
                    "close": 4.8500,
                    "exchange": "SH",
                }
            ]
        ),
    )

    profile = collector.collect_profile("510300", asset_type="cn_etf")
    overview = profile["overview"]

    assert overview["基金简称"] == "沪深300ETF"
    assert overview["ETF基准指数代码"] == "000300.SH"
    assert overview["ETF基准指数中文全称"] == "沪深300指数"
    assert overview["ETF基准指数发布机构"] == "中证指数有限公司"
    assert overview["ETF份额规模日期"] == "2026-03-12"
    assert overview["ETF总份额"] == "4741854.98万份"
    assert overview["ETF总规模"] == "22878980.00万元"
    assert overview["净资产规模"] == "22878980.00万元（截止至：2026-03-12）"
    assert overview["管理费率"] == "0.15%（每年）"


def test_fund_profile_fund_factor_pro_reuses_process_cache_and_contracts(monkeypatch, tmp_path):
    collector = FundProfileCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    calls = {"count": 0}

    def fake_snapshot(*, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> pd.DataFrame:  # noqa: ARG001
        calls["count"] += 1
        assert ts_code == "510300.SH"
        assert trade_date == "20260401"
        return pd.DataFrame(
            [
                {"trade_date": "20260401", "ts_code": "510300.SH", "close": 4.05, "pct_change": 1.25, "ma_bfq_20": 4.01},
                {"trade_date": "20260331", "ts_code": "510300.SH", "close": 4.00, "pct_change": 0.50, "ma_bfq_20": 3.99},
            ]
        )

    monkeypatch.setattr(collector, "_ts_fund_factor_pro_snapshot", fake_snapshot)

    frame1 = collector.get_fund_factor_pro_ts("510300", trade_date="2026-04-01")
    frame2 = collector.get_fund_factor_pro_ts("510300", trade_date="2026-04-01")

    assert calls["count"] == 1
    assert list(frame1["trade_date"]) == ["2026-04-01", "2026-03-31"]
    assert not frame2.empty
    assert frame1.attrs["source"] == "tushare.fund_factor_pro"
    assert frame1.attrs["latest_date"] == "2026-04-01"
    assert frame1.attrs["is_fresh"] is True
    assert frame1.attrs["fallback"] == "none"
    assert "fund_factor_pro" in frame1.attrs["disclosure"]


def test_fund_profile_fund_sales_ratio_snapshot_contracts(monkeypatch, tmp_path):
    fund_profile_module._PROCESS_SHARED_FRAME_CACHE.clear()
    collector = FundProfileCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})
    calls = {"count": 0}

    def fake_ts_call(api_name: str, **kwargs):  # noqa: ANN001
        calls["count"] += 1
        assert api_name == "fund_sales_ratio"
        assert kwargs == {}
        return pd.DataFrame(
            [
                {"year": 2020, "bank": 18.25, "sec_comp": 12.0, "fund_comp": 55.5, "indep_comp": 9.25, "rests": 5.0},
                {"year": 2019, "bank": 20.0, "sec_comp": 11.0, "fund_comp": 50.0, "indep_comp": 10.0, "rests": 9.0},
            ]
        )

    monkeypatch.setattr(collector, "_ts_call", fake_ts_call)

    frame1 = collector.get_fund_sales_ratio_ts()
    frame2 = collector.get_fund_sales_ratio_ts()
    snapshot = collector.get_fund_sales_ratio_snapshot()

    assert calls["count"] == 1
    assert list(frame1["year"]) == [2020, 2019]
    assert frame1.attrs["source"] == "tushare.fund_sales_ratio"
    assert frame1.attrs["latest_date"] == "2020-12-31"
    assert frame1.attrs["latest_year"] == "2020"
    assert frame1.attrs["is_fresh"] is False
    assert frame1.attrs["fallback"] == "none"
    assert "fund_sales_ratio" in frame1.attrs["disclosure"]
    assert not frame2.empty
    assert snapshot["status"] == "matched"
    assert snapshot["latest_date"] == "2020-12-31"
    assert snapshot["lead_channel"] == "基金公司直销"
    assert snapshot["channel_mix"][0]["channel"] == "基金公司直销"
    assert snapshot["channel_mix"][0]["ratio"] == 55.5
    assert "年度更新" in snapshot["disclosure"]


def test_get_etf_share_size_ts_auto_window_requests_recent_seven_open_dates(monkeypatch, tmp_path):
    fund_profile_module._PROCESS_SHARED_FRAME_CACHE.clear()
    collector = FundProfileCollector({"storage": {"cache_dir": str(tmp_path), "cache_ttl_hours": 0}})

    monkeypatch.setattr(collector, "_resolve_tushare_etf_code", lambda symbol, preferred_markets=("E", "O", "L"): "510300.SH")  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "_recent_open_trade_dates",
        lambda lookback_days=14: [  # noqa: ARG005
            "20260325",
            "20260326",
            "20260327",
            "20260330",
            "20260331",
            "20260401",
            "20260402",
        ],
    )

    def fake_snapshot(*, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> pd.DataFrame:
        assert ts_code == "510300.SH"
        assert trade_date == ""
        assert start_date == "20260325"
        assert end_date == "20260402"
        return pd.DataFrame(
            [
                {"trade_date": "20260402", "ts_code": "510300.SH", "total_share": 3_087_198.74, "total_size": 4_012_300.0},
                {"trade_date": "20260401", "ts_code": "510300.SH", "total_share": 3_060_100.00, "total_size": 3_980_000.0},
            ]
        )

    monkeypatch.setattr(collector, "_ts_etf_share_size_snapshot", fake_snapshot)

    frame = collector.get_etf_share_size_ts("510300")

    assert list(frame["trade_date"]) == ["2026-04-02", "2026-04-01"]


def test_collect_profile_exposes_fund_factor_snapshot(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "get_overview", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_asset_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_portfolio_hold", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_basic_ts", lambda symbol="": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_index_ts", lambda symbol="": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_share_size_ts", lambda symbol, **kwargs: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_fund_factor_pro_ts",
        lambda symbol, **kwargs: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "trade_date": "2026-04-01",
                    "close": 4.05,
                    "pct_change": 1.25,
                    "ma_bfq_20": 4.01,
                    "ma_bfq_60": 3.95,
                    "macd_bfq": 0.12,
                    "rsi_bfq_6": 66.5,
                }
            ]
        ),
    )

    profile = collector.collect_profile("510300", asset_type="cn_etf")

    assert profile["fund_factor_snapshot"]["trade_date"] == "2026-04-01"
    assert profile["fund_factor_snapshot"]["trend_label"] == "趋势偏强"
    assert profile["fund_factor_snapshot"]["momentum_label"] == "动能改善"


def test_fund_profile_prefers_fund_benchmark_theme_over_manager_peer_funds(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "get_fund_basic", lambda market="O": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005

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
    assert profile["style"]["taxonomy"]["vehicle_role"] == "ETF联接"
    assert profile["style"]["taxonomy"]["share_class"] == "ETF联接C类"


def test_fund_profile_does_not_misclassify_a500_due_to_bank_deposit_tail(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "get_fund_basic", lambda market="O": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005

    monkeypatch.setattr(
        collector,
        "get_overview",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "基金简称": "南方中证A500ETF联接C",
                    "基金类型": "股票型 / 被动指数型",
                    "基金管理人": "南方基金",
                    "基金经理人": "朱恒红",
                    "业绩比较基准": "中证A500指数收益率*95%+银行活期存款利率(税后)*5%",
                    "成立日期/规模": "2024年11月12日 / 41.4147亿份",
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
                {"资产类型": "其他", "仓位占比": 97.81},
                {"资产类型": "现金", "仓位占比": 5.74},
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_portfolio_hold", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005

    profile = collector.collect_profile("022435")
    assert profile["style"]["sector"] == "宽基"
    assert "宽基" in profile["style"]["summary"]


def test_fund_profile_recognizes_sse_composite_etf_as_broad_index(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "get_fund_basic", lambda market="O": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_etf_basic_ts",
        lambda symbol="": pd.DataFrame(  # noqa: ARG005
            [
                {
                    "ts_code": "510210.SH",
                    "csname": "上证指数ETF",
                    "index_code": "000001.SH",
                    "index_name": "上证综合指数",
                    "exchange": "SH",
                    "list_status": "L",
                    "etf_type": "纯境内",
                    "mgr_name": "富国基金",
                }
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_etf_index_ts", lambda symbol="": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_share_size_ts", lambda symbol, **kwargs: pd.DataFrame())  # noqa: ARG005

    monkeypatch.setattr(
        collector,
        "get_overview",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "基金简称": "上证指数ETF",
                    "基金类型": "股票型 / 被动指数型",
                    "基金管理人": "富国基金",
                    "基金经理人": "王保合、方旻",
                    "业绩比较基准": "上证综合指数",
                    "跟踪标的": "上证综合指数",
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
                {"资产类型": "股票", "仓位占比": 99.86},
                {"资产类型": "现金", "仓位占比": 0.24},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_portfolio_hold",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"股票代码": "601288", "股票名称": "农业银行", "占净值比例": 3.83, "持股数": 1, "持仓市值": 1, "季度": "2025年4季度股票投资明细"},
                {"股票代码": "601857", "股票名称": "中国石油", "占净值比例": 3.25, "持股数": 1, "持仓市值": 1, "季度": "2025年4季度股票投资明细"},
                {"股票代码": "600519", "股票名称": "贵州茅台", "占净值比例": 2.26, "持股数": 1, "持仓市值": 1, "季度": "2025年4季度股票投资明细"},
                {"股票代码": "601138", "股票名称": "工业富联", "占净值比例": 2.19, "持股数": 1, "持仓市值": 1, "季度": "2025年4季度股票投资明细"},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_industry_allocation",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"行业类别": "制造业", "占净值比例": 43.9, "市值": 1, "截止时间": "2025-12-31"},
                {"行业类别": "金融业", "占净值比例": 23.63, "市值": 1, "截止时间": "2025-12-31"},
                {"行业类别": "信息传输、软件和信息技术服务业", "占净值比例": 6.1, "市值": 1, "截止时间": "2025-12-31"},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_manager_directory",
        lambda: pd.DataFrame(
            [
                {"姓名": "王保合", "所属公司": "富国基金", "现任基金代码": "510210", "现任基金": "上证指数ETF", "累计从业时间": 4000, "现任基金资产总规模": 100.0, "现任基金最佳回报": 200.0},
                {"姓名": "王保合", "所属公司": "富国基金", "现任基金代码": "588000", "现任基金": "富国上证科创板综合价格指数增强A", "累计从业时间": 4000, "现任基金资产总规模": 100.0, "现任基金最佳回报": 200.0},
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005

    profile = collector.collect_profile("510210", asset_type="cn_etf")
    assert profile["style"]["sector"] == "宽基"
    assert profile["style"]["chain_nodes"] == ["宽基", "大盘蓝筹", "内需"]
    assert "宽基主题" in profile["style"]["tags"]
    assert profile["style"]["taxonomy"]["exposure_scope"] == "宽基"


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
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005

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


def test_fund_profile_recognizes_hang_seng_index_etf_as_broad_index(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "get_fund_basic", lambda market="O": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_etf_basic_ts",
        lambda symbol="": pd.DataFrame(  # noqa: ARG005
            [
                {
                    "ts_code": "513210.SH",
                    "csname": "恒生ETF易方达",
                    "index_code": "HSI.HK",
                    "index_name": "恒生指数",
                    "exchange": "SH",
                    "list_status": "L",
                    "etf_type": "跨境",
                    "mgr_name": "易方达基金",
                }
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_etf_index_ts", lambda symbol="": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_share_size_ts", lambda symbol, **kwargs: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_overview",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "基金简称": "恒生ETF易方达",
                    "基金类型": "股票型 / 被动指数型",
                    "基金管理人": "易方达基金",
                    "基金经理人": "宋钊贤",
                    "业绩比较基准": "恒生指数收益率(使用估值汇率折算)",
                    "跟踪标的": "恒生指数",
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
                {"资产类型": "股票", "仓位占比": 95.48},
                {"资产类型": "现金", "仓位占比": 4.52},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_portfolio_hold",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"股票代码": "0005", "股票名称": "汇丰控股", "占净值比例": 8.0, "持股数": 1, "持仓市值": 1, "季度": "2025年4季度股票投资明细"},
                {"股票代码": "0700", "股票名称": "腾讯控股", "占净值比例": 7.5, "持股数": 1, "持仓市值": 1, "季度": "2025年4季度股票投资明细"},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_industry_allocation",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"行业类别": "非必需消费品", "占净值比例": 23.0, "市值": 1, "截止时间": "2025-12-31"},
                {"行业类别": "金融业", "占净值比例": 21.0, "市值": 1, "截止时间": "2025-12-31"},
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005

    profile = collector.collect_profile("513210", asset_type="cn_etf")
    assert profile["style"]["sector"] == "宽基"
    assert profile["style"]["taxonomy"]["sector"] == "宽基"
    assert profile["style"]["taxonomy"]["exposure_scope"] == "跨境"
    assert "宽基主题" in profile["style"]["tags"]


def test_fund_profile_drops_invalid_asset_mix_and_marks_note(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "get_fund_basic", lambda market="O": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_etf_basic_ts",
        lambda symbol="": pd.DataFrame(  # noqa: ARG005
            [
                {
                    "ts_code": "513210.SH",
                    "csname": "恒生ETF易方达",
                    "index_code": "HSI.HK",
                    "index_name": "恒生指数",
                    "exchange": "SH",
                    "list_status": "L",
                    "etf_type": "跨境",
                    "mgr_name": "易方达基金",
                }
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_etf_index_ts", lambda symbol="": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_share_size_ts", lambda symbol, **kwargs: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_overview",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "基金简称": "恒生ETF易方达",
                    "基金类型": "股票型 / 被动指数型",
                    "基金管理人": "易方达基金",
                    "基金经理人": "宋钊贤",
                    "业绩比较基准": "恒生指数收益率(使用估值汇率折算)",
                    "跟踪标的": "恒生指数",
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
                {"资产类型": "股票", "仓位占比": 95.48},
                {"资产类型": "现金", "仓位占比": 88.85},
                {"资产类型": "其他", "仓位占比": 1.05},
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_portfolio_hold", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005

    profile = collector.collect_profile("513210", asset_type="cn_etf")
    assert profile["asset_allocation"] == []
    assert any("资产配置口径异常" in item for item in profile["notes"])


def test_fund_profile_backfills_overview_from_tushare_when_overview_empty(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})

    monkeypatch.setattr(collector, "get_overview", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
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
    assert overview["成立日期"] == "2006-11-17"
    assert overview["首发规模"] == "24.7230亿份"
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
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
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
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
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


def test_fund_profile_commodity_etf_treats_cash_as_margin_structure(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(
        collector,
        "get_etf_basic_ts",
        lambda symbol="": pd.DataFrame(  # noqa: ARG005
            [
                {
                    "ts_code": "159981.SZ",
                    "csname": "建信能源化工期货ETF",
                    "index_code": "ESCFI.CZCE",
                    "index_name": "易盛郑商所能源化工指数A",
                    "exchange": "SZ",
                    "list_status": "L",
                    "etf_type": "商品",
                    "mgr_name": "建信基金",
                }
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_etf_index_ts", lambda symbol="": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_share_size_ts", lambda symbol, **kwargs: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_overview",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "基金简称": "能源化工ETF",
                    "基金类型": "商品型 / 能源化工期货型",
                    "基金管理人": "建信基金",
                    "基金经理人": "朱金钰、亢豆",
                    "净资产规模": "15.95亿元（截止至：2025年12月31日）",
                    "业绩比较基准": "易盛郑商所能源化工指数A收益率",
                    "成立日期": "2019-12-13",
                }
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_asset_allocation",
        lambda symbol: pd.DataFrame(
            [
                {"资产类型": "债券", "占总资产比例": 5.94},
                {"资产类型": "现金", "占总资产比例": 92.32},
                {"资产类型": "其他", "占总资产比例": 8.23},
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_portfolio_hold", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_basic", lambda market="E": pd.DataFrame())  # noqa: ARG005

    profile = collector.collect_profile("159981", asset_type="cn_etf")
    assert "商品/期货跟踪" in profile["style"]["tags"]
    assert "保证金/备付结构" in profile["style"]["tags"]
    assert "不等于主观空仓" in profile["style"]["positioning"]
    assert profile["style"]["taxonomy"]["product_form"] == "ETF"
    assert profile["style"]["taxonomy"]["vehicle_role"] == "场内ETF"


def test_fund_profile_etf_snapshot_exposes_share_change(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    monkeypatch.setattr(collector, "get_overview", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_asset_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_portfolio_hold", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_etf_basic_ts",
        lambda symbol="": pd.DataFrame(  # noqa: ARG005
            [
                {
                    "ts_code": "563360.SH",
                    "csname": "A500ETF华泰柏瑞",
                    "index_code": "000510.SH",
                    "index_name": "中证A500指数",
                    "list_date": "20241010",
                    "setup_date": "20240930",
                    "etf_type": "境内",
                    "mgr_name": "华泰柏瑞基金",
                    "custod_name": "中国银行",
                    "list_status": "L",
                    "exchange": "SH",
                    "mgt_fee": 0.15,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_etf_index_ts",
        lambda symbol="": pd.DataFrame(  # noqa: ARG005
            [{"ts_code": "000510.SH", "indx_name": "中证A500指数", "indx_csname": "中证A500", "pub_party_name": "中证指数有限公司"}]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_etf_share_size_ts",
        lambda symbol, **kwargs: pd.DataFrame(  # noqa: ARG005
            [
                {"trade_date": "2026-03-31", "ts_code": "563360.SH", "etf_name": "A500ETF华泰柏瑞", "total_share": 3_091_998.74, "total_size": 3_818_927.64, "exchange": "SSE"},
                {"trade_date": "2026-03-30", "ts_code": "563360.SH", "etf_name": "A500ETF华泰柏瑞", "total_share": 3_066_198.74, "total_size": 3_833_361.66, "exchange": "SSE"},
            ]
        ),
    )

    profile = collector.collect_profile("563360", asset_type="cn_etf")
    snapshot = profile["etf_snapshot"]

    assert snapshot["index_code"] == "000510.SH"
    assert snapshot["index_name"] == "中证A500指数"
    assert snapshot["list_status"] == "L"
    assert snapshot["total_share"] == 3_091_998.74
    assert snapshot["total_size"] == 3_818_927.64
    assert snapshot["etf_share_change"] == pytest.approx(2.58, rel=1e-6)
    assert snapshot["etf_size_change"] == pytest.approx(-1.443402, rel=1e-6)


def test_collect_profile_light_mode_keeps_etf_snapshot_without_heavy_profile_calls(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    def _unexpected(*args, **kwargs):  # noqa: ANN001, ARG001
        raise AssertionError("heavy profile endpoint should not be called in light mode")

    monkeypatch.setattr(collector, "get_overview", _unexpected)
    monkeypatch.setattr(collector, "get_etf_basic_ts", lambda symbol="": pd.DataFrame([{"ts_code": "512890.SH", "index_code": "H30269.CSI", "index_name": "中证红利低波动指数", "exchange": "SH", "list_status": "L", "etf_type": "纯境内"}]))  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_index_ts", lambda symbol="": pd.DataFrame([{"ts_code": "H30269.CSI", "indx_csname": "中证红利低波", "pub_party_name": "中证指数有限公司"}]))  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_share_size_ts", lambda symbol, **kwargs: pd.DataFrame([{"trade_date": "2026-04-01", "ts_code": "512890.SH", "total_share": 2613411.08, "total_size": 3118767.355, "exchange": "SSE"}]))  # noqa: ARG005

    monkeypatch.setattr(collector, "get_fund_manager_ts", _unexpected)
    monkeypatch.setattr(collector, "get_fund_company_ts", _unexpected)
    monkeypatch.setattr(collector, "get_fund_div_ts", _unexpected)
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", _unexpected)
    monkeypatch.setattr(collector, "get_achievement", _unexpected)
    monkeypatch.setattr(collector, "get_asset_allocation", _unexpected)
    monkeypatch.setattr(collector, "get_portfolio_hold", _unexpected)
    monkeypatch.setattr(collector, "get_industry_allocation", _unexpected)
    monkeypatch.setattr(collector, "get_manager_directory", _unexpected)
    monkeypatch.setattr(collector, "get_rating_table", _unexpected)

    profile = collector.collect_profile("512890", asset_type="cn_etf", profile_mode="light")

    assert profile["profile_mode"] == "light"
    assert profile["etf_snapshot"]["index_code"] == "H30269.CSI"
    assert profile["etf_snapshot"]["total_share"] == 2613411.08


def test_collect_profile_full_etf_merges_overview_manager_when_tushare_core_exists(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})

    monkeypatch.setattr(
        collector,
        "get_etf_basic_ts",
        lambda symbol="": pd.DataFrame(
            [
                {
                    "ts_code": "512400.OF",
                    "csname": "南方中证申万有色金属ETF",
                    "index_code": "000819.SH",
                    "index_name": "中证申万有色金属指数",
                    "exchange": "SH",
                    "list_status": "L",
                    "etf_type": "纯境内",
                    "mgr_name": "南方基金",
                }
            ]
        ),
    )  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_index_ts", lambda symbol="": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_share_size_ts", lambda symbol, **kwargs: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_overview",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {
                    "基金简称": "有色金属ETF南方",
                    "基金管理人": "南方基金",
                    "基金经理人": "崔蕾",
                    "业绩比较基准": "中证申万有色金属指数",
                }
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_asset_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_portfolio_hold", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_factor_pro_ts", lambda symbol, **kwargs: pd.DataFrame())  # noqa: ARG005

    profile = collector.collect_profile("512400", asset_type="cn_etf", profile_mode="full")

    assert profile["overview"]["基金经理人"] == "崔蕾"
    assert profile["overview"]["基金管理人"] == "南方基金"


def test_collect_profile_etf_prefers_tushare_over_ak_overview(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})

    def _unexpected(*args, **kwargs):  # noqa: ANN001, ARG001
        raise AssertionError("AKShare overview should not run once ETF Tushare core data is available")

    monkeypatch.setattr(collector, "get_overview", _unexpected)
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_asset_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_portfolio_hold", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_basic_ts", lambda symbol="": pd.DataFrame([{"ts_code": "563360.SH", "csname": "A500ETF华泰柏瑞", "index_code": "000510.SH", "index_name": "中证A500指数", "exchange": "SH", "list_status": "L", "etf_type": "境内"}]))  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_index_ts", lambda symbol="": pd.DataFrame([{"ts_code": "000510.SH", "indx_name": "中证A500指数", "indx_csname": "中证A500", "pub_party_name": "中证指数有限公司"}]))  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_share_size_ts", lambda symbol, **kwargs: pd.DataFrame([{"trade_date": "2026-04-01", "ts_code": "563360.SH", "total_share": 3091998.74, "total_size": 3818927.64, "exchange": "SSE"}]))  # noqa: ARG005

    profile = collector.collect_profile("563360", asset_type="cn_etf")

    assert profile["overview"]["ETF基准指数中文全称"] == "中证A500指数"


def test_collect_profile_etf_does_not_fallback_to_akshare_when_tushare_core_is_empty(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})
    calls = {"overview": 0, "holdings": 0}

    def fake_overview(*args, **kwargs):  # noqa: ANN001, ARG001
        calls["overview"] += 1
        return pd.DataFrame()

    def fake_holdings(*args, **kwargs):  # noqa: ANN001, ARG001
        calls["holdings"] += 1
        return pd.DataFrame()

    monkeypatch.setattr(collector, "get_overview", fake_overview)
    monkeypatch.setattr(collector, "get_portfolio_hold", fake_holdings)
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_asset_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_portfolio_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_basic_ts", lambda symbol="": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_index_ts", lambda symbol="": pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_share_size_ts", lambda symbol, **kwargs: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_factor_pro_ts", lambda symbol, **kwargs: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "_tushare_etf_basic_row", lambda symbol: {})  # noqa: ARG005
    monkeypatch.setattr(collector, "_tushare_fund_basic_row", lambda symbol: {})  # noqa: ARG005

    profile = collector.collect_profile("510300", asset_type="cn_etf")

    assert calls == {"overview": 0, "holdings": 0}
    assert profile["overview"] == {}
    assert "基金概况缺失" in profile["notes"]


def test_collect_profile_etf_uses_tushare_holdings_without_akshare_holdings(monkeypatch):
    collector = FundProfileCollector({"storage": {"cache_dir": "data/cache", "cache_ttl_hours": 0}})

    def _unexpected(*args, **kwargs):  # noqa: ANN001, ARG001
        raise AssertionError("AKShare ETF holdings should not run once Tushare holdings and stock_basic are available")

    monkeypatch.setattr(collector, "get_overview", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_asset_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_portfolio_hold", _unexpected)
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_manager_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_fund_company_ts", lambda: pd.DataFrame())
    monkeypatch.setattr(collector, "get_fund_div_ts", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_fund_portfolio_ts",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"symbol": "300308.SZ", "stk_mkv_ratio": 9.2, "stk_amount": 20.1, "stk_mkv": 2_110.0, "end_date": "20251231"},
                {"symbol": "300738.SZ", "stk_mkv_ratio": 8.5, "stk_amount": 19.5, "stk_mkv": 1_980.0, "end_date": "20251231"},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_stock_basic_ts",
        lambda: pd.DataFrame(
            [
                {"ts_code": "300308.SZ", "symbol": "300308", "name": "中际旭创"},
                {"ts_code": "300738.SZ", "symbol": "300738", "name": "奥飞数据"},
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_manager_directory", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_basic_ts", lambda symbol="": pd.DataFrame([{"ts_code": "563360.SH", "csname": "A500ETF华泰柏瑞", "index_code": "000510.SH", "index_name": "中证A500指数", "exchange": "SH", "list_status": "L", "etf_type": "境内"}]))  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_index_ts", lambda symbol="": pd.DataFrame([{"ts_code": "000510.SH", "indx_name": "中证A500指数", "indx_csname": "中证A500", "pub_party_name": "中证指数有限公司"}]))  # noqa: ARG005
    monkeypatch.setattr(collector, "get_etf_share_size_ts", lambda symbol, **kwargs: pd.DataFrame([{"trade_date": "2026-04-01", "ts_code": "563360.SH", "total_share": 3091998.74, "total_size": 3818927.64, "exchange": "SSE"}]))  # noqa: ARG005

    profile = collector.collect_profile("563360", asset_type="cn_etf")

    assert [row["股票名称"] for row in profile["top_holdings"][:2]] == ["中际旭创", "奥飞数据"]


def test_standard_taxonomy_does_not_treat_etf_suffix_as_f_share_class() -> None:
    taxonomy = build_standard_fund_taxonomy(
        name="港股创新药ETF",
        fund_type="股票型",
        invest_type="被动指数型",
        benchmark="中证香港创新药指数收益率(人民币计价)",
        asset_type="cn_etf",
    )

    assert taxonomy["product_form"] == "ETF"
    assert taxonomy["share_class"] == "未分级"


def test_standard_taxonomy_prefers_explicit_financial_theme_over_wrong_sector_hint() -> None:
    taxonomy = build_standard_fund_taxonomy(
        name="香港证券ETF易方达",
        fund_type="股票型",
        invest_type="被动指数型",
        benchmark="中证香港证券投资主题指数收益率(使用估值汇率折算)",
        asset_type="cn_etf",
        sector_hint="科技",
    )

    assert taxonomy["sector"] == "金融"
    assert taxonomy["exposure_scope"] == "行业主题"
    assert "券商" in taxonomy["chain_nodes"]


def test_fund_profile_uses_tushare_manager_company_dividend_and_holdings(monkeypatch):
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
                    "基金经理人": "",
                    "业绩比较基准": "中国战略新兴产业成份指数收益率",
                }
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_achievement", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(collector, "get_asset_allocation", lambda symbol: pd.DataFrame([{"资产类型": "股票", "仓位占比": 85.0}]))  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_fund_portfolio_ts",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"symbol": "300308.SZ", "stk_mkv_ratio": 8.4, "mkv": 2110.0, "amount": 20.1, "end_date": "20251231"},
                {"symbol": "300738.SZ", "stk_mkv_ratio": 9.3, "mkv": 2338.66, "amount": 96.4, "end_date": "20251231"},
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_stock_basic_ts", lambda: pd.DataFrame([{"ts_code": "300308.SZ", "symbol": "300308", "name": "中际旭创"}, {"ts_code": "300738.SZ", "symbol": "300738", "name": "奥飞数据"}]))
    monkeypatch.setattr(
        collector,
        "get_portfolio_hold",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("AKShare holdings should not run when Tushare fund_portfolio is available")),
    )
    monkeypatch.setattr(collector, "get_industry_allocation", lambda symbol: pd.DataFrame())  # noqa: ARG005
    monkeypatch.setattr(
        collector,
        "get_fund_manager_ts",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"name": "任桀", "ann_date": "20260110", "begin_date": "20241030", "end_date": "", "edu": "硕士", "nationality": "中国", "resume": "长期覆盖科技成长方向。"},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_manager_directory",
        lambda: pd.DataFrame(
            [
                {"姓名": "任桀", "所属公司": "永赢基金", "现任基金代码": "022365", "现任基金": "永赢科技智选混合发起C", "累计从业时间": 495, "现任基金资产总规模": 161.72, "现任基金最佳回报": 281.99},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_fund_company_ts",
        lambda: pd.DataFrame(
            [
                {"name": "永赢基金", "short_name": "永赢", "province": "上海", "city": "上海", "website": "https://example.com", "manager": "王某"},
            ]
        ),
    )
    monkeypatch.setattr(
        collector,
        "get_fund_div_ts",
        lambda symbol: pd.DataFrame(  # noqa: ARG005
            [
                {"ann_date": "20251220", "ex_date": "20251225", "pay_date": "20251226", "div_cash": 0.12, "progress": "实施"},
            ]
        ),
    )
    monkeypatch.setattr(collector, "get_rating_table", lambda: pd.DataFrame())  # noqa: ARG005

    profile = collector.collect_profile("022365")

    assert profile["overview"]["基金经理人"] == "任桀"
    assert profile["manager"]["education"] == "硕士"
    assert profile["manager"]["ann_date"] == "2026-01-10"
    assert profile["company"]["short_name"] == "永赢"
    assert profile["company"]["general_manager"] == "王某"
    assert profile["dividends"]["latest_ex_date"] == "2025-12-25"
    assert [row["股票名称"] for row in profile["top_holdings"][:2]] == ["奥飞数据", "中际旭创"]
