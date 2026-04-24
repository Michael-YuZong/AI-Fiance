"""Tests for shared fund taxonomy contracts."""

from __future__ import annotations

from src.utils.fund_taxonomy import build_standard_fund_taxonomy, uses_index_mainline


def test_uses_index_mainline_skips_active_fund_with_index_benchmark_text() -> None:
    payload = {
        "asset_type": "cn_fund",
        "name": "永赢科技智选混合发起C",
        "metadata": {"fund_management_style": "主动管理"},
        "fund_profile": {
            "overview": {
                "基金类型": "混合型-偏股",
                "业绩比较基准": "中国战略新兴产业成份指数收益率",
            },
            "style": {
                "tags": ["科技主题"],
                "taxonomy": {"management_style": "主动管理"},
            },
        },
    }

    assert uses_index_mainline(payload) is False


def test_uses_index_mainline_keeps_passive_linked_fund_on_index_mainline() -> None:
    payload = {
        "asset_type": "cn_fund",
        "name": "前海开源黄金ETF联接C",
        "fund_profile": {
            "overview": {"基金类型": "商品型 / 黄金现货合约"},
            "style": {"taxonomy": {"management_style": "被动跟踪"}},
        },
    }

    assert uses_index_mainline(payload) is True


def test_build_standard_fund_taxonomy_keeps_hk_innovative_drug_specific_chain_nodes() -> None:
    taxonomy = build_standard_fund_taxonomy(
        name="广发中证香港创新药(QDII-ETF)",
        fund_type="股票型",
        invest_type="被动指数型(QDII)",
        benchmark="中证香港创新药指数收益率(人民币计价)",
        asset_type="cn_etf",
    )

    assert taxonomy["sector"] == "医药"
    assert taxonomy["chain_nodes"] == ["创新药", "港股医药", "FDA"]


def test_build_standard_fund_taxonomy_promotes_hardtech_family_into_specific_buckets() -> None:
    cases = [
        ("通信ETF", "中证全指通信设备主题指数收益率", "通信", "光模块"),
        ("5GETF", "中证5G通信主题指数收益率", "通信", "5G/6G"),
        ("光模块ETF", "中证光模块主题指数收益率", "通信", "CPO"),
        ("数据中心ETF", "中证数据中心主题指数收益率", "通信", "数据中心"),
        ("半导体ETF", "中证全指半导体产品与设备指数收益率", "半导体", "半导体"),
    ]

    for name, benchmark, expected_sector, expected_node in cases:
        taxonomy = build_standard_fund_taxonomy(
            name=name,
            fund_type="股票型",
            invest_type="被动指数型",
            benchmark=benchmark,
            asset_type="cn_etf",
        )

        assert taxonomy["sector"] == expected_sector
        assert expected_node in taxonomy["chain_nodes"]


def test_build_standard_fund_taxonomy_keeps_satellite_communication_distinct_from_cpo_chain() -> None:
    taxonomy = build_standard_fund_taxonomy(
        name="卫星ETF",
        fund_type="股票型",
        invest_type="被动指数型",
        benchmark="国证商用卫星通信产业指数收益率",
        asset_type="cn_etf",
    )

    assert taxonomy["sector"] == "通信"
    assert taxonomy["chain_nodes"] == ["卫星通信", "卫星互联网", "商业航天"]


def test_build_standard_fund_taxonomy_classifies_media_family_without_falling_back_to_composite() -> None:
    taxonomy = build_standard_fund_taxonomy(
        name="游戏ETF",
        fund_type="股票型",
        invest_type="被动指数型",
        benchmark="中证动漫游戏指数收益率",
        asset_type="cn_etf",
    )

    assert taxonomy["sector"] == "传媒"
    assert taxonomy["chain_nodes"] == ["游戏", "传媒", "AI应用"]


def test_build_standard_fund_taxonomy_covers_mainline_etf_taxonomy_expansion() -> None:
    cases = [
        ("国泰中证半导体材料设备主题ETF", "中证半导体材料设备主题指数", "半导体", "半导体设备"),
        ("嘉实上证科创板芯片ETF", "上证科创板芯片指数收益率", "半导体", "芯片"),
        ("智能电网ETF", "中证智能电网主题指数收益率", "电网", "智能电网"),
        ("特高压ETF", "中证特高压主题指数收益率", "电网", "特高压"),
        ("储能电池ETF", "中证储能主题指数收益率", "电网", "储能并网"),
        ("医疗器械ETF", "中证医疗器械指数收益率", "医药", "医疗器械"),
        ("CXO ETF", "中证医药外包指数收益率", "医药", "CXO"),
        ("创新药ETF", "中证创新药产业指数收益率", "医药", "BD授权"),
    ]

    for name, benchmark, expected_sector, expected_node in cases:
        taxonomy = build_standard_fund_taxonomy(
            name=name,
            fund_type="股票型",
            invest_type="被动指数型",
            benchmark=benchmark,
            asset_type="cn_etf",
        )

        assert taxonomy["sector"] == expected_sector
        assert expected_node in taxonomy["chain_nodes"]


def test_build_standard_fund_taxonomy_emits_upper_theme_profile_for_cpo() -> None:
    taxonomy = build_standard_fund_taxonomy(
        name="通信ETF",
        fund_type="股票型",
        invest_type="被动指数型",
        benchmark="中证全指通信设备指数收益率",
        asset_type="cn_etf",
    )

    assert taxonomy["sector"] == "通信"
    assert taxonomy["theme_family"] == "硬科技"
    assert taxonomy["primary_chain"] == "CPO/光模块"
    assert taxonomy["theme_role"] == "AI硬件主链"
    assert taxonomy["theme_directness"] == "direct"
    assert "CPO" in taxonomy["evidence_keywords"]
    assert "光模块" in taxonomy["evidence_keywords"]
    assert "科技" in taxonomy["preferred_sector_aliases"]
    assert "AI硬件链" in taxonomy["mainline_tags"]


def test_build_standard_fund_taxonomy_lets_holdings_hint_refine_broad_ai_index() -> None:
    taxonomy = build_standard_fund_taxonomy(
        name="创业板人工智能ETF",
        fund_type="ETF",
        invest_type="被动指数型",
        benchmark="创业板人工智能指数",
        asset_type="cn_etf",
        sector_hint="通信 CPO 光模块 新易盛 中际旭创 天孚通信",
    )

    assert taxonomy["sector"] == "通信"
    assert taxonomy["primary_chain"] == "CPO/光模块"
    assert taxonomy["theme_role"] == "AI硬件主链"


def test_build_standard_fund_taxonomy_keeps_sidechains_distinct_in_theme_profile() -> None:
    satellite = build_standard_fund_taxonomy(
        name="卫星互联网ETF",
        fund_type="股票型",
        invest_type="被动指数型",
        benchmark="中证卫星互联网主题指数收益率",
        asset_type="cn_etf",
    )
    grid = build_standard_fund_taxonomy(
        name="智能电网ETF",
        fund_type="股票型",
        invest_type="被动指数型",
        benchmark="中证智能电网主题指数收益率",
        asset_type="cn_etf",
    )

    assert satellite["primary_chain"] == "卫星互联网"
    assert satellite["theme_directness"] == "adjacent"
    assert "商业航天" in satellite["preferred_sector_aliases"]
    assert grid["primary_chain"] == "智能电网/特高压"
    assert grid["theme_role"] == "AI电力侧链"
    assert grid["theme_directness"] == "sidechain"
    assert "AI用电" in grid["evidence_keywords"]


def test_build_standard_fund_taxonomy_emits_medical_and_game_evidence_keywords() -> None:
    innovation = build_standard_fund_taxonomy(
        name="港股创新药ETF",
        fund_type="股票型",
        invest_type="被动指数型",
        benchmark="中证香港创新药指数收益率",
        asset_type="cn_etf",
    )
    game = build_standard_fund_taxonomy(
        name="游戏ETF",
        fund_type="股票型",
        invest_type="被动指数型",
        benchmark="中证动漫游戏指数收益率",
        asset_type="cn_etf",
    )

    assert innovation["theme_family"] == "医药成长"
    assert innovation["primary_chain"] == "创新药"
    assert "license-out" in innovation["evidence_keywords"]
    assert "医药" in innovation["preferred_sector_aliases"]
    assert game["theme_family"] == "传媒应用"
    assert game["theme_directness"] == "application"
    assert "版号" in game["evidence_keywords"]
    assert "AI应用" in game["preferred_sector_aliases"]
