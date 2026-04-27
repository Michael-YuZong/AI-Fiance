import json

import pandas as pd

from src.output.client_report import ClientReportRenderer
from src.output.editor_payload import (
    _action_lines,
    _ensure_homepage_news_signal_bundle,
    _format_homepage_evidence_line,
    _load_previous_editor_payload_context,
    _subject_theme_context,
    _subject_intelligence_keywords,
    _humanize_news_summary_line,
    _news_lines_with_event_digest,
    _theme_lines,
    build_what_changed_summary,
    build_briefing_editor_packet,
    build_etf_pick_editor_packet,
    build_fund_pick_editor_packet,
    build_scan_editor_packet,
    build_stock_analysis_editor_packet,
    build_stock_pick_editor_packet,
    render_financial_editor_prompt,
    render_editor_homepage,
    summarize_theme_playbook_contract,
)
from src.output.event_digest import summarize_event_digest_contract
from src.output.theme_playbook import (
    HARD_SECTOR_REGISTRY,
    PLAYBOOK_REGISTRY,
    build_theme_playbook_context,
    classify_hard_sector,
    infer_theme_trading_role,
    load_sector_playbook,
    load_theme_playbook,
    playbook_hint_line,
    sector_subtheme_bridge_items,
    summarize_sector_subtheme_bridge,
)


def _sample_dimensions() -> dict:
    return {
        "technical": {"score": 32, "max_score": 100, "summary": "技术结构还没确认。", "factors": []},
        "fundamental": {"score": 72, "max_score": 100, "summary": "底层逻辑和产品质量仍有支撑。", "factors": []},
        "catalyst": {"score": 28, "max_score": 100, "summary": "直接催化偏弱，更像静态博弈。", "factors": [], "evidence": []},
        "relative_strength": {"score": 35, "max_score": 100, "summary": "相对强弱还不够支撑右侧动作。", "factors": []},
        "risk": {"score": 58, "max_score": 100, "summary": "风险不算最差，但赔率一般。", "factors": []},
        "macro": {"score": 24, "max_score": 40, "summary": "宏观不逆风，但也不是全面顺风。", "factors": []},
        "chips": {"score": 20, "max_score": 100, "summary": "筹码只是辅助项。", "factors": []},
        "seasonality": {"score": 22, "max_score": 100, "summary": "季节性没有明显加分。", "factors": []},
    }


def _sample_history() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.date_range("2025-10-01", periods=120, freq="B"),
            "open": [1.0 + i * 0.004 for i in range(120)],
            "high": [1.02 + i * 0.004 for i in range(120)],
            "low": [0.98 + i * 0.004 for i in range(120)],
            "close": [1.0 + i * 0.004 for i in range(120)],
            "volume": [8_000_000 + i * 1_000 for i in range(120)],
            "amount": [16_000_000 + i * 2_000 for i in range(120)],
        }
    )


def test_build_theme_playbook_context_resolves_semiconductor() -> None:
    playbook = build_theme_playbook_context("半导体ETF", "AI 算力和晶圆厂扩产")
    assert playbook["key"] == "semiconductor"
    assert playbook["market_logic"]
    assert playbook["risks"]
    assert playbook["trading_role_label"] in {"主线核心", "主线扩散"}
    assert playbook["trading_position_label"] in {"主线仓", "卫星仓"}


def test_subject_intelligence_keywords_use_taxonomy_profile_evidence_terms() -> None:
    keywords = _subject_intelligence_keywords(
        {
            "asset_type": "cn_etf",
            "name": "通信ETF",
            "metadata": {
                "sector": "信息技术",
                "taxonomy": {
                    "theme_profile": {
                        "theme_family": "硬科技",
                        "primary_chain": "CPO/光模块",
                        "theme_role": "AI硬件主链",
                        "evidence_keywords": ["CPO", "光模块", "800G", "AI算力"],
                        "preferred_sector_aliases": ["科技", "通信", "AI硬件"],
                    }
                },
            },
        },
        {},
    )

    assert "CPO" in keywords
    assert "光模块" in keywords
    assert "800G" in keywords


def test_theme_lines_surface_taxonomy_primary_chain_for_fund_like_subject() -> None:
    lines = _theme_lines(
        {
            "hard_sector_label": "信息技术",
            "theme_family": "技术路线",
            "playbook_level": "theme",
            "label": "AI算力",
        },
        {
            "asset_type": "cn_etf",
            "metadata": {
                "sector": "通信",
                "primary_chain": "CPO/光模块",
                "theme_role": "AI硬件主链",
            },
        },
    )

    joined = "\n".join(lines)
    assert "标准分类补充" in joined
    assert "CPO/光模块" in joined
    assert "AI硬件主链" in joined


def test_news_lines_with_event_digest_prioritize_linked_news_before_digest_summary() -> None:
    subject = {
        "symbol": "588170",
        "generated_at": "2026-04-02 20:00:00",
        "news_report": {
            "items": [
                {
                    "title": "财联社：半导体材料设备链关注度回升",
                    "source": "财联社",
                    "date": "2026-04-02",
                    "link": "https://example.com/semiconductor",
                }
            ]
        },
        "dimensions": {
            "catalyst": {
                "evidence": [
                    {
                        "title": "这条结构化证据不应抢到新闻位前面",
                        "source": "财联社",
                        "date": "2026-04-02",
                    }
                ]
            }
        },
    }
    digest = {
        "status": "已消化",
        "lead_layer": "行业主题事件",
        "lead_detail": "主题事件：成分权重结构；结论：指数主链已明确核心成分和权重结构。",
        "lead_title": "指数成分权重：前十权重合计 74.3%",
        "signal_type": "成分权重结构",
        "signal_strength": "强",
        "signal_conclusion": "指数主链已明确核心成分和权重结构。",
        "changed_what": "这条线当前更直接影响景气 / 资金偏好，但暂不单独升级动作。",
    }

    lines = _news_lines_with_event_digest(subject, digest)

    assert lines
    assert any(line.startswith("结构证据：") for line in lines)
    assert any(line.startswith("外部情报：") for line in lines)
    assert any("信号强弱" in line or "结论：" in line for line in lines[:2])
    assert any("传导：" in line for line in lines[:2])
    assert any("https://example.com/semiconductor" in line for line in lines)
    assert any("财联社" in line for line in lines)
    assert sum("成分权重结构" in line for line in lines) == 1


def test_news_lines_with_event_digest_interprets_raw_news_without_signal_fields() -> None:
    subject = {
        "asset_type": "cn_stock",
        "name": "新易盛",
        "symbol": "300502",
        "generated_at": "2026-04-24 08:30:00",
        "metadata": {"sector": "AI算力"},
        "news_report": {
            "items": [
                {
                    "title": "新易盛预约披露年报和一季报，市场关注AI算力订单兑现",
                    "source": "财联社",
                    "date": "2026-04-24",
                    "link": "https://example.com/ai-compute",
                }
            ]
        },
        "dimensions": {"catalyst": {"evidence": []}},
    }

    lines = _news_lines_with_event_digest(subject, {})
    joined = "\n".join(lines)

    assert "信号类型" in joined
    assert "财报摘要" in joined or "AI硬件" in joined
    assert "主要影响" in joined
    assert "结论：" in joined
    assert "传导：" in joined


def test_news_lines_with_event_digest_stock_prioritizes_company_event_over_theme_links() -> None:
    subject = {
        "asset_type": "cn_stock",
        "name": "新易盛",
        "symbol": "300502",
        "generated_at": "2026-04-24 18:30:00",
        "metadata": {"sector": "AI算力"},
        "news_report": {
            "items": [
                {
                    "title": "亚马逊云科技与全球AI伙伴共话AI应用生态",
                    "source": "新浪财经",
                    "date": "2026-04-23",
                    "link": "https://example.com/ai-app",
                },
                {
                    "title": "新易盛光模块需求延续，市场关注算力订单",
                    "source": "财联社",
                    "date": "2026-04-23",
                    "link": "https://example.com/eoptolink",
                },
            ]
        },
        "dimensions": {"catalyst": {"evidence": []}},
    }
    digest = {
        "status": "已消化",
        "lead_layer": "公告",
        "lead_title": "新易盛 披露现金分红预案（每10股派现 1.00 元）",
        "lead_link": "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=300502",
        "signal_type": "公告类型：分红/回报",
        "signal_strength": "强",
        "signal_conclusion": "偏利多，已开始改写 `估值 / 资金偏好` 这层。",
        "impact_summary": "估值 / 资金偏好",
        "latest_signal_at": "2026-04-24",
        "thesis_scope": "thesis变化",
        "importance": "high",
    }

    lines = _news_lines_with_event_digest(subject, digest)

    assert lines
    assert "新易盛 披露现金分红预案" in lines[0]
    assert "主要影响：`估值 / 资金偏好`" in lines[0]
    assert "传导：" in lines[0]
    assert not lines[0].startswith("结构证据：信号类型")


def test_news_lines_with_event_digest_for_etf_prefers_linked_news_before_digest_lead() -> None:
    subject = {
        "asset_type": "cn_etf",
        "symbol": "159980",
        "generated_at": "2026-04-06 02:28:00",
        "news_report": {
            "items": [
                {
                    "title": "几内亚考虑收紧铝土矿供应，铝价中枢或继续抬升",
                    "source": "新浪财经",
                    "date": "2026-04-06",
                    "link": "https://example.com/bauxite",
                }
            ]
        },
        "dimensions": {"catalyst": {"evidence": [], "coverage": {"directional_catalyst_hit": True}}},
    }
    digest = {
        "status": "已消化",
        "lead_layer": "行业主题事件",
        "lead_detail": "主题事件：标准指数框架；结论：先按跟踪指数主链理解。",
        "lead_title": "跟踪指数框架：大成有色金属期货ETF 跟踪 上海期货交易所有色金属期货价格指数",
        "signal_type": "标准指数框架",
        "signal_strength": "中",
        "signal_conclusion": "先按跟踪指数主链理解。",
        "changed_what": "这条线当前更直接影响景气 / 资金偏好，但暂不单独升级动作。",
    }

    lines = _news_lines_with_event_digest(subject, digest)

    assert lines
    assert "https://example.com/bauxite" in lines[0]


def test_news_lines_with_event_digest_prioritizes_subject_news_over_market_background() -> None:
    subject = {
        "asset_type": "cn_etf",
        "name": "国泰中证半导体材料设备主题ETF",
        "symbol": "159516",
        "day_theme": {"label": "硬科技 / AI硬件链"},
        "metadata": {"sector": "信息技术", "chain_nodes": ["AI算力", "半导体"]},
        "dimensions": {"catalyst": {"coverage": {"directional_catalyst_hit": True}}},
        "news_report": {
            "items": [
                {
                    "title": "是“真和平”还是“假希望”？一文盘点：投行如何看待美伊临时停火！",
                    "source": "财联社",
                    "date": "2026-04-08",
                    "link": "https://example.com/geopolitics",
                },
                {
                    "title": "半导体设备ETF招商逆势3连阳，国产化与存储扩产双催化",
                    "source": "财经号",
                    "date": "2026-04-10",
                    "link": "https://example.com/semiconductor-equipment",
                },
                {
                    "title": "A股放量大涨，机构眼中的后市机会在哪？",
                    "source": "新浪财经",
                    "date": "2026-04-08",
                    "link": "https://example.com/market-wide",
                },
            ]
        },
    }
    digest = {
        "status": "已消化",
        "lead_layer": "公告",
        "lead_title": "半导体设备ETF招商逆势3连阳，国产化与存储扩产双催化",
        "lead_link": "https://example.com/semiconductor-equipment",
        "theme_label": "半导体",
        "signal_type": "公告类型：扩产/投产",
        "signal_strength": "强",
        "signal_conclusion": "偏利多，已开始改写 `盈利 / 景气` 这层。",
        "thesis_scope": "thesis变化",
    }

    lines = _news_lines_with_event_digest(subject, digest)

    assert "半导体设备ETF招商" in lines[0]
    assert "美伊" not in lines[0]


def test_news_lines_with_event_digest_filters_etf_proxy_news_when_coverage_is_zero() -> None:
    subject = {
        "asset_type": "cn_etf",
        "name": "国泰中证半导体材料设备主题ETF",
        "symbol": "159516",
        "metadata": {"tracked_index_name": "中证半导体材料设备主题指数"},
        "dimensions": {
            "catalyst": {
                "coverage": {"directional_catalyst_hit": False, "direct_news_count": 0},
                "theme_news": [
                    {"title": "北方华创股价连续上涨，嘉实基金旗下11只基金重仓持有", "source": "财联社", "date": "2026-04-10"},
                ],
            }
        },
        "news_report": {
            "items": [
                {
                    "title": "半导体设备ETF易方达（159558）盘中获1800万份净申购",
                    "source": "财联社",
                    "date": "2026-04-10",
                    "link": "https://example.com/159558",
                },
                {
                    "title": "【早报】消费领域多个利好来袭，多家A股公司被证监会立案",
                    "source": "财联社",
                    "date": "2026-04-10",
                    "link": "https://example.com/generic",
                },
            ]
        },
    }
    digest = {
        "status": "已消化",
        "lead_layer": "行业主题事件",
        "lead_title": "跟踪指数/行业框架：中证半导体材料设备主题指数对应申万二级行业半导体（+1.96%）",
        "lead_detail": "主题事件：行业/指数框架；结论：只作为跟踪指数背景。",
        "signal_type": "行业/指数框架",
        "signal_strength": "中",
        "signal_conclusion": "只作为跟踪指数背景。",
    }

    lines = _news_lines_with_event_digest(subject, digest)
    joined = "\n".join(lines)

    assert any(line.startswith("结构证据：") for line in lines)
    assert "159558" not in joined
    assert "消费领域多个利好" not in joined
    assert "北方华创股价连续上涨" not in joined

    packet = build_scan_editor_packet(
        {
            **subject,
            "generated_at": "2026-04-11 10:00:00",
            "trade_state": "观察为主",
            "action": {"direction": "观察为主", "entry": "等右侧确认"},
        },
        bucket="观察为主",
    )
    subject_sidecar = json.dumps(packet["subject"], ensure_ascii=False)
    assert "theme_news" not in dict(packet["subject"]["dimensions"]["catalyst"] or {})
    assert "159558" not in subject_sidecar
    assert "北方华创股价连续上涨" not in subject_sidecar


def test_news_lines_with_event_digest_filters_fund_brand_noise_links() -> None:
    subject = {
        "asset_type": "cn_fund",
        "name": "南方创业板人工智能ETF联接A",
        "symbol": "024725",
        "metadata": {
            "sector": "科技",
            "benchmark": "创业板人工智能指数收益率*95%+银行活期存款利率(税后)*5%",
            "chain_nodes": ["AI算力", "软件服务"],
            "taxonomy": {
                "theme_profile": {
                    "primary_chain": "AI/成长科技",
                    "evidence_keywords": ["人工智能", "AI算力"],
                }
            },
        },
        "news_report": {
            "items": [
                {
                    "title": "全国首单数字人民币跨境双边电力结算业务落地南方电网",
                    "source": "新浪财经",
                    "date": "2026-04-23",
                    "link": "https://example.com/power-grid",
                },
                {
                    "title": "AI服务器需求拉动算力产业链景气",
                    "source": "财联社",
                    "date": "2026-04-23",
                    "link": "https://example.com/ai-server",
                },
            ]
        },
    }
    digest = {
        "status": "已消化",
        "lead_layer": "行业主题事件",
        "lead_title": "AI服务器需求拉动算力产业链景气",
        "lead_link": "https://example.com/ai-server",
        "signal_type": "主题事件：产业链映射",
        "signal_strength": "中",
        "signal_conclusion": "中性偏多，先看景气能否继续确认。",
        "thesis_scope": "thesis变化",
    }

    lines = _news_lines_with_event_digest(subject, digest)
    joined = "\n".join(lines)

    assert "AI服务器需求拉动算力产业链景气" in joined
    assert "南方电网" not in joined


def test_news_lines_with_event_digest_fund_relevance_uses_product_benchmark_not_relative_benchmark() -> None:
    subject = {
        "asset_type": "cn_fund",
        "name": "南方创业板人工智能ETF联接A",
        "symbol": "024725",
        "benchmark_name": "沪深300ETF",
        "metadata": {
            "sector": "科技",
            "benchmark": "创业板人工智能指数收益率*95%+银行活期存款利率(税后)*5%",
            "taxonomy": {"theme_profile": {"primary_chain": "AI/成长科技"}},
        },
    }
    digest = {
        "status": "已消化",
        "lead_layer": "行业主题事件",
        "lead_title": "AI服务器需求拉动算力产业链景气",
        "lead_link": "https://example.com/ai-server",
        "signal_type": "主题事件：产业链映射",
        "signal_strength": "中",
        "signal_conclusion": "中性偏多，先看景气能否继续确认。",
    }

    lines = _news_lines_with_event_digest(subject, digest)
    joined = "\n".join(lines)

    assert "创业板人工智能指数收益率" in joined
    assert "沪深300ETF" not in joined


def test_news_lines_with_event_digest_filters_irrelevant_market_background_when_only_structure_evidence_exists() -> None:
    subject = {
        "asset_type": "cn_stock",
        "name": "中信证券",
        "symbol": "600030",
        "day_theme": {"label": "背景宏观主导"},
        "metadata": {"sector": "金融", "industry": "证券"},
        "news_report": {
            "items": [
                {
                    "title": "是“真和平”还是“假希望”？一文盘点：投行如何看待美伊临时停火！",
                    "source": "财联社",
                    "date": "2026-04-08",
                    "link": "https://example.com/geopolitics",
                },
                {
                    "title": "美伊冲突“迎曙光”？特朗普同意停火2周，伊朗态度大反转！",
                    "source": "财联社",
                    "date": "2026-04-08",
                    "link": "https://example.com/geopolitics-2",
                },
            ]
        },
    }
    digest = {
        "status": "已消化",
        "lead_layer": "公告",
        "lead_title": "中信证券披露现金分红预案",
        "signal_type": "公告类型：分红/回报",
        "signal_strength": "中",
        "signal_conclusion": "中性，当前更多是历史基线，不把它直接当成新增催化。",
        "thesis_scope": "历史基线",
    }

    lines = _news_lines_with_event_digest(subject, digest)

    assert all("美伊" not in line and "特朗普" not in line for line in lines)
    assert any(line.startswith("结构证据：") for line in lines)
    assert any("本轮未拿到可点击外部情报" in line for line in lines)
    assert any(
        "中信证券披露现金分红预案" in line and "信号类型：" in line and "结论：" in line
        for line in lines
    )


def test_news_lines_with_event_digest_filters_stock_day_theme_background_links() -> None:
    subject = {
        "asset_type": "cn_stock",
        "name": "紫金矿业",
        "symbol": "601899",
        "day_theme": {"label": "硬科技 / AI硬件链"},
        "metadata": {
            "sector": "有色",
            "industry": "铜",
            "chain_nodes": ["工业金属", "铜"],
        },
        "theme_playbook": {"label": "黄金 / 有色资源"},
        "news_report": {
            "items": [
                {
                    "title": "国内创新药“大单品”竞相涌现 商业化进程全面提速 - 证券时报",
                    "source": "证券时报",
                    "published_at": "2026-04-21T22:56:00",
                    "link": "https://example.com/biotech",
                },
                {
                    "title": "国务院重磅部署！事关AI、算力、6G、卫星互联网等 - 财联社",
                    "source": "财联社",
                    "published_at": "2026-04-21T09:09:05",
                    "link": "https://example.com/ai-policy",
                },
            ]
        },
    }
    digest = {
        "status": "已消化",
        "lead_layer": "财报",
        "lead_title": "紫金矿业 已于 2026-04-22 披露 2026年一季报",
        "lead_link": "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=601899",
        "theme_label": "黄金 / 有色资源",
        "signal_type": "财报摘要：盈利/指引",
        "signal_strength": "强",
        "signal_conclusion": "偏利多，已开始改写 `盈利 / 估值` 这层。",
        "thesis_scope": "thesis变化",
    }

    lines = _news_lines_with_event_digest(subject, digest)
    joined = "\n".join(lines)

    assert "创新药" not in joined
    assert "AI、算力" not in joined
    assert any("紫金矿业 已于 2026-04-22 披露 2026年一季报" in line for line in lines)
    assert any(line.startswith("结构证据：") for line in lines)


def test_news_lines_with_event_digest_keeps_structured_label_when_summary_lines_exist() -> None:
    subject = {
        "asset_type": "cn_stock",
        "name": "中信证券",
        "symbol": "600030",
        "news_report": {
            "summary_lines": [
                "主题聚类：背景线索 7 条",
                "来源分层：媒体 2 条",
            ],
            "items": [],
        },
    }
    digest = {
        "status": "已消化",
        "lead_layer": "公告",
        "lead_title": "中信证券披露现金分红预案",
        "lead_link": "https://example.com/dividend",
        "signal_type": "公告类型：分红/回报",
        "signal_strength": "中",
        "signal_conclusion": "中性，当前更多是历史基线，不把它直接当成新增催化。",
        "latest_signal_at": "2026-04-25",
    }

    lines = _news_lines_with_event_digest(subject, digest)

    assert any(line.startswith("结构证据：") for line in lines)
    assert any(line.startswith("外部情报：") for line in lines)


def test_news_lines_with_event_digest_drops_irrelevant_market_lead_event_link() -> None:
    subject = {
        "asset_type": "cn_fund",
        "name": "天弘国证港股通科技ETF联接A",
        "symbol": "024885",
        "day_theme": {"label": "硬科技 / AI硬件链"},
        "metadata": {"sector": "信息技术"},
        "news_report": {"items": []},
    }
    digest = {
        "status": "已消化",
        "lead_layer": "新闻",
        "lead_title": "是“真和平”还是“假希望”？一文盘点：投行如何看待美伊临时停火！",
        "lead_link": "https://example.com/geopolitics",
        "signal_type": "信息环境：新闻/舆情脉冲",
        "signal_strength": "弱",
        "signal_conclusion": "中性，别把它单独升级成动作。",
        "latest_signal_at": "2026-04-08T13:04:00",
    }

    lines = _news_lines_with_event_digest(subject, digest)

    assert all("美伊" not in line and "投行如何看待" not in line for line in lines)
    assert any(line.startswith("结构证据：") for line in lines)
    assert any("本轮未拿到可点击外部情报" in line for line in lines)


def test_news_lines_with_event_digest_filters_old_news_lines_for_history_baseline() -> None:
    subject = {
        "symbol": "600313",
        "generated_at": "2026-04-02 20:00:00",
        "news_report": {
            "items": [
                {
                    "title": "旧闻回放：农发种业历史分红预案再被提及",
                    "source": "财联社",
                    "date": "2025-08-23",
                    "link": "https://example.com/old-dividend",
                },
                {
                    "title": "农发种业回应新产品进展",
                    "source": "财联社",
                    "date": "2026-04-02",
                    "link": "https://example.com/new-progress",
                },
            ]
        },
    }
    digest = {
        "status": "已消化",
        "thesis_scope": "历史基线",
        "lead_layer": "公告",
        "lead_detail": "公告类型：回购/分红",
        "lead_title": "农发种业披露现金分红预案",
        "signal_strength": "中",
        "signal_conclusion": "中性，当前更多是历史基线，不把它直接当成新增催化。",
    }

    lines = _news_lines_with_event_digest(subject, digest)

    assert all("旧闻回放" not in line for line in lines)
    assert any("https://example.com/new-progress" in line for line in lines)


def test_news_lines_with_event_digest_surfaces_news_summary_lines() -> None:
    subject = {
        "symbol": "512400",
        "generated_at": "2026-04-05 20:00:00",
        "news_report": {
            "summary_lines": [
                "主题聚类：价格/供需 2 条，产业/公司 1 条",
                "来源分层：主流媒体 1 条，行业/协会 2 条",
            ],
            "items": [
                {
                    "title": "铜价上行带动有色链活跃",
                    "source": "财联社",
                    "date": "2026-04-05",
                    "link": "https://example.com/nonferrous",
                }
            ],
        },
        "dimensions": {"catalyst": {"evidence": []}},
    }
    digest = {"status": "待补充"}

    lines = _news_lines_with_event_digest(subject, digest)

    assert any("情报摘要：" in line and "价格/供需" in line and "产业/公司" in line for line in lines)
    assert any("铜价上行带动有色链活跃" in line for line in lines)


def test_news_lines_with_event_digest_allows_more_etf_news_and_deduplicates_same_title() -> None:
    subject = {
        "asset_type": "cn_etf",
        "symbol": "588170",
        "generated_at": "2026-04-02 20:00:00",
        "news_report": {
            "items": [
                {
                    "title": "SEMI：未来四年12英寸晶圆厂设备支出持续增长 - api3.cls.cn",
                    "source": "api3.cls.cn",
                    "published_at": "2026-04-02 14:28:00",
                    "link": "https://example.com/semi-a",
                },
                {
                    "title": "SEMI：未来四年12英寸晶圆厂设备支出持续增长 - 财联社",
                    "source": "财联社",
                    "published_at": "2026-04-02 14:28:00",
                    "link": "https://example.com/semi-b",
                },
                {
                    "title": "三星西安晶圆厂制程升级正式量产",
                    "source": "新浪财经",
                    "published_at": "2026-04-01 10:31:00",
                    "link": "https://example.com/samsung-xian",
                },
                {
                    "title": "AI算力扩张拉动半导体设备需求，机构继续看多国产替代",
                    "source": "证券时报",
                    "published_at": "2026-04-02 11:20:00",
                    "link": "https://example.com/ai-chip",
                },
            ]
        },
    }
    digest = {
        "status": "已消化",
        "lead_layer": "行业主题事件",
        "lead_detail": "主题事件：成分权重结构；结论：指数主链已明确核心成分和权重结构。",
        "lead_title": "指数成分权重：前十权重合计 74.3%",
        "signal_type": "成分权重结构",
        "signal_strength": "强",
        "signal_conclusion": "指数主链已明确核心成分和权重结构。",
        "changed_what": "这条线当前更直接影响景气 / 资金偏好，但暂不单独升级动作。",
    }

    lines = _news_lines_with_event_digest(subject, digest)

    assert len(lines) >= 4
    joined = "\n".join(lines)
    assert joined.count("SEMI：未来四年12英寸晶圆厂设备支出持续增长") == 1
    assert "三星西安晶圆厂制程升级正式量产" in joined
    assert "AI算力扩张拉动半导体设备需求" in joined


def test_news_lines_with_event_digest_deduplicates_timestamped_mirror_titles() -> None:
    subject = {
        "asset_type": "cn_etf",
        "symbol": "588170",
        "generated_at": "2026-04-02 20:00:00",
        "news_report": {
            "items": [
                {
                    "title": "SEMI：未来四年12英寸晶圆厂设备支出持续增长 - 亿欧网",
                    "source": "亿欧网",
                    "published_at": "2026-04-02 06:40:05",
                    "link": "https://example.com/semi-iyiou",
                },
                {
                    "title": "14:15:50【SEMI：未来四年12英寸晶圆厂设备支出持续增长】 - api3.cls.cn",
                    "source": "api3.cls.cn",
                    "published_at": "2026-04-02 02:28:00",
                    "link": "https://example.com/semi-cls",
                },
            ]
        },
    }
    digest = {
        "status": "已消化",
        "lead_layer": "行业主题事件",
        "lead_detail": "主题事件：成分权重结构；结论：指数主链已明确核心成分和权重结构。",
        "lead_title": "指数成分权重：前十权重合计 74.3%",
        "signal_type": "成分权重结构",
        "signal_strength": "强",
        "signal_conclusion": "指数主链已明确核心成分和权重结构。",
        "changed_what": "这条线当前更直接影响景气 / 资金偏好，但暂不单独升级动作。",
    }

    lines = _news_lines_with_event_digest(subject, digest)

    joined = "\n".join(lines)
    assert joined.count("SEMI：未来四年12英寸晶圆厂设备支出持续增长") == 1


def test_build_theme_playbook_context_resolves_ai_computing() -> None:
    playbook = build_theme_playbook_context("AI算力ETF", "光模块和服务器景气仍在")
    assert playbook["key"] == "ai_computing"
    assert playbook["bullish_drivers"]


def test_build_theme_playbook_context_prefers_ai_index_over_chuangye_beta() -> None:
    playbook = build_theme_playbook_context(
        {
            "name": "南方创业板人工智能ETF联接A",
            "sector": "科技",
            "benchmark": "创业板人工智能指数收益率*95%+银行活期存款利率(税后)*5%",
            "chain_nodes": ["AI算力", "软件服务", "成长股估值修复"],
            "primary_chain": "AI/成长科技",
            "theme_family": "泛科技",
            "theme_role": "主题成长",
            "evidence_keywords": ["人工智能", "AI算力"],
        }
    )
    assert playbook["key"] == "ai_computing"
    assert playbook["label"] == "AI算力"
    assert playbook["hard_sector_label"] == "信息技术"


def test_build_theme_playbook_context_distinguishes_hardtech_expansion_from_core() -> None:
    playbook = build_theme_playbook_context(
        "CPO / 光模块 / 液冷 / PCB / 存储",
        "硬科技 / AI硬件链",
        "细分扩散、接力走强、订单验证，当前更像第二梯队而不是绝对核心",
        {
            "name": "光模块ETF",
            "day_theme": {"label": "硬科技 / AI硬件链"},
            "notes": "CPO、液冷、PCB、存储都在细分扩散",
            "narrative": {
                "headline": "AI硬件链细分扩散",
                "summary_lines": ["第二梯队接力", "景气与订单线索延续"],
                "drivers": ["扩散", "接力", "细分", "订单"],
                "judgment": {"state": "扩散走强"},
            },
            "dimensions": {
                "technical": {"score": 56},
                "fundamental": {"score": 65},
                "catalyst": {"score": 54},
                "relative_strength": {"score": 69},
                "risk": {"score": 58},
            },
            "action": {"horizon": {"code": "position_trade", "label": "主线扩散", "style": "趋势参与"}},
        },
    )
    assert playbook["key"] == "ai_computing"
    assert playbook["trading_role_label"] == "主线扩散"
    assert playbook["trading_position_label"] == "卫星仓"


def test_build_theme_playbook_context_respects_rotation_and_secondary_swing_negation() -> None:
    rotation = build_theme_playbook_context(
        "电网 / 特高压 / 配网",
        "硬科技 / AI硬件链",
        "防守承接，结构性轮动，高低切，不是当前主线核心",
        {
            "name": "电网ETF",
            "day_theme": {"label": "硬科技 / AI硬件链"},
            "notes": "高低切、轮动承接、防守属性更明显",
            "narrative": {
                "headline": "电网更像轮动承接方向",
                "summary_lines": ["结构性轮动", "不宜当主攻仓"],
                "drivers": ["轮动", "高低切", "防守"],
                "judgment": {"state": "轮动承接"},
            },
            "dimensions": {
                "technical": {"score": 49},
                "fundamental": {"score": 63},
                "catalyst": {"score": 26},
                "relative_strength": {"score": 52},
                "risk": {"score": 55},
            },
            "action": {"horizon": {"code": "swing", "label": "轮动低吸", "style": "低吸分批"}},
        },
    )
    swing = build_theme_playbook_context(
        "有色 / 黄金 / 铜",
        "硬科技 / AI硬件链",
        "资源涨价，副主线，强波段，不是科技主线本体",
        {
            "name": "有色ETF",
            "day_theme": {"label": "硬科技 / AI硬件链"},
            "notes": "涨价驱动、副主线、顺势但非第一主攻",
            "narrative": {
                "headline": "有色更像副主线或强波段",
                "summary_lines": ["涨价驱动", "顺势但非第一主攻"],
                "drivers": ["涨价", "副主线", "强波段"],
                "judgment": {"state": "偏强波段"},
            },
            "dimensions": {
                "technical": {"score": 57},
                "fundamental": {"score": 66},
                "catalyst": {"score": 41},
                "relative_strength": {"score": 58},
                "risk": {"score": 56},
            },
            "action": {"horizon": {"code": "swing", "label": "强波段", "style": "顺势参与"}},
        },
    )
    assert rotation["key"] == "power_grid"
    assert rotation["trading_role_label"] == "轮动"
    assert rotation["trading_position_label"] == "轮动仓"
    assert swing["key"] == "gold_nonferrous"
    assert swing["trading_role_label"] == "强波段 / 副主线"
    assert swing["trading_position_label"] == "波段仓"


def test_scan_editor_packet_uses_structured_event_evidence_for_mainline_expansion() -> None:
    packet = build_scan_editor_packet(
        {
            "name": "国泰中证半导体材料设备主题ETF",
            "symbol": "159516",
            "asset_type": "cn_etf",
            "day_theme": {"label": "硬科技 / AI硬件链"},
            "metadata": {
                "sector": "信息技术",
                "chain_nodes": ["AI算力", "半导体"],
            },
            "notes": ["半导体主线轮动，材料设备分支开始扩散。"],
            "narrative": {
                "headline": "半导体材料设备分支承接硬科技扩散。",
                "summary_lines": ["相对强弱在线", "扩产公告给到景气确认"],
                "drivers": ["扩散", "订单", "景气", "主线轮动"],
                "judgment": {"state": "回调更优"},
            },
            "action": {
                "direction": "回调更优",
                "entry": "等回踩确认",
                "stop": "跌破支撑重评",
                "horizon": {"code": "swing", "label": "主线轮动", "style": "几周波段跟踪"},
            },
            "dimensions": {
                "technical": {"score": 34, "max_score": 100},
                "fundamental": {"score": 53, "max_score": 100},
                "catalyst": {"score": 23, "max_score": 100},
                "relative_strength": {"score": 75, "max_score": 100},
                "risk": {"score": 35, "max_score": 100},
                "macro": {"score": 7, "max_score": 40},
            },
            "market_event_rows": [
                [
                    "2026-04-10",
                    "半导体扩产公告",
                    "公告专题",
                    "强",
                    "半导体",
                    "",
                    "公告类型：扩产/投产",
                    "偏利多，已开始改写 盈利 / 景气 这层。",
                ],
            ],
        },
        bucket="正式推荐",
    )

    assert packet["theme_playbook"]["trading_role_label"] == "主线扩散"
    assert packet["theme_playbook"]["trading_position_label"] == "卫星仓"


def test_build_scan_editor_packet_keeps_mainline_expansion_for_etf_with_structural_confirmation() -> None:
    packet = build_scan_editor_packet(
        {
            "name": "国泰中证半导体材料设备主题ETF",
            "symbol": "159516",
            "asset_type": "cn_etf",
            "day_theme": {"label": "硬科技 / AI硬件链"},
            "trade_state": "回调更优",
            "metadata": {
                "sector": "科技",
                "chain_nodes": ["AI算力", "半导体", "成长股估值修复"],
            },
            "notes": [
                "本轮 `client-final` 保留主题情报扩搜能力，只对全局新闻源走轻量配置，避免把热点方向静默写成零催化。",
                "本轮 `client-final` 已自动切到轻量新闻源配置，避免单标的扫描稿被全局新闻拉取慢链拖住。",
            ],
            "narrative": {
                "headline": "这是一个中期偏多，但短线仍在整理的标的。",
                "judgment": {"state": "回调更优"},
                "drivers": {
                    "relative": "相对强弱有改善，但行业宽度和龙头确认仍缺失，当前更适合作为低置信代理去看，而不是把它写成完整扩散确认。",
                    "technical": "技术面最值得看的不是强趋势，而是价格已经回到关键支撑附近；但短线动能还需要再修复。",
                },
                "contradiction": "方向并不差，但当前位置赔率已经被明显压缩。",
                "playbook": {
                    "trend": "更适合等短线动能重新修复后再跟随，而不是在趋势尚未顺畅时提前抢跑。",
                    "allocation": "如果你看重的是中期逻辑而不是短线节奏，可以按首次建仓 ≤3% 的框架分批，但不适合一次性追价。",
                },
            },
            "action": {
                "direction": "观望偏多",
                "entry": "先等顶背离/假突破消化、MACD/OBV 重新同步，再考虑分批介入",
                "position": "首次建仓 ≤3%，等结构进一步确认后再加仓",
                "stop": "跌破支撑重评",
                "horizon": {
                    "code": "swing",
                    "label": "波段跟踪（2-6周）",
                    "style": "更适合按几周级别的波段节奏去跟踪，等确认和回踩，不靠单日冲动去追。",
                },
            },
            "dimensions": {
                "technical": {"score": 34, "max_score": 100},
                "fundamental": {"score": 53, "max_score": 100},
                "catalyst": {"score": 15, "max_score": 100},
                "relative_strength": {"score": 75, "max_score": 100},
                "risk": {"score": 35, "max_score": 100},
                "macro": {"score": 7, "max_score": 40},
            },
            "market_event_rows": [
                [
                    "2026-04-11 19:22:46",
                    "跟踪指数/行业框架：国泰中证半导体材料设备主题ETF 对应 申万二级行业·半导体（+1.96%）",
                    "申万行业框架",
                    "中",
                    "半导体",
                    "",
                    "行业/指数框架",
                    "偏利多，先按 `申万二级行业` 对应的 `半导体` 去理解它的相对强弱和行业扩散。",
                ],
                [
                    "2026-04-11 19:22:46",
                    "指数技术面：半导体材料设备 修复中 / 动能偏强",
                    "指数技术面",
                    "中",
                    "半导体材料设备",
                    "",
                    "技术确认",
                    "优先按 `修复中` 理解跟踪指数的相对强弱和节奏。",
                ],
                [
                    "2026-04-11 19:22:46",
                    "份额申赎确认：国泰中证半导体材料设备主题ETF 最近净创设 +0.10 亿份 (+0.04%)",
                    "ETF份额规模",
                    "中",
                    "国泰中证半导体材料设备主题ETF",
                    "",
                    "份额净创设",
                    "偏利多，ETF 份额扩张说明场外申购在配合当前主线，不只是价格抬升。",
                ],
            ],
        },
        bucket="正式推荐",
    )

    assert packet["theme_playbook"]["trading_role_label"] == "主线扩散"
    assert packet["theme_playbook"]["trading_position_label"] == "卫星仓"


def test_theme_trading_role_uses_event_digest_before_demoting_to_rotation() -> None:
    playbook = build_theme_playbook_context("国泰中证半导体材料设备主题ETF", "硬科技 / AI硬件链")
    subject = {
        "name": "国泰中证半导体材料设备主题ETF",
        "symbol": "159516",
        "asset_type": "cn_etf",
        "day_theme": {"label": "硬科技 / AI硬件链"},
        "metadata": {"sector": "信息技术"},
        "notes": ["轮动、高低切、观察、等确认、修复、暂不、先看"],
        "narrative": {
            "headline": "半导体分支还在轮动修复，等待确认。",
            "summary_lines": ["观察", "修复"],
            "drivers": ["轮动", "主线轮动"],
            "judgment": {"state": "回调更优"},
        },
        "action": {
            "direction": "观望偏多",
            "horizon": {
                "code": "swing",
                "label": "波段跟踪（2-6周）",
                "style": "更适合按几周级别的波段节奏去跟踪，等确认和回踩，不靠单日冲动去追。",
            },
        },
        "dimensions": {
            "technical": {"score": 34},
            "fundamental": {"score": 53},
            "catalyst": {"score": 23},
            "relative_strength": {"score": 75},
            "risk": {"score": 35},
        },
    }

    stale_role = infer_theme_trading_role(playbook, subject, subject=subject)
    enriched_subject = {
        **subject,
        "event_digest": {
            "signal_type": "公告类型：扩产/投产",
            "signal_strength": "强",
            "signal_conclusion": "偏利多，已开始改写 `盈利 / 景气` 这层。",
            "thesis_scope": "thesis变化",
            "importance": "high",
        },
    }
    enriched_role = infer_theme_trading_role(playbook, enriched_subject, subject=enriched_subject)

    assert stale_role["trading_role_label"] == "轮动"
    assert enriched_role["trading_role_label"] == "主线扩散"
    assert enriched_role["trading_position_label"] == "卫星仓"


def test_build_theme_playbook_context_resolves_policy_and_event_chain() -> None:
    policy = build_theme_playbook_context("信创", "数据要素和国产替代")
    chain = build_theme_playbook_context("事件产业链", "发布会和供应链脉冲")
    assert policy["key"] == "policy_substitution"
    assert chain["key"] == "event_supply_chain"


def test_build_theme_playbook_context_resolves_new_high_frequency_themes() -> None:
    dividend = build_theme_playbook_context("红利ETF", "高股息和分红防守风格")
    coal = build_theme_playbook_context("煤炭ETF", "能源安全和煤价韧性")
    gold = build_theme_playbook_context("黄金ETF", "金价和黄金股避险交易")
    low_alt = build_theme_playbook_context("低空经济ETF", "eVTOL 和卫星互联网进展")
    data = build_theme_playbook_context("数据要素ETF", "国资云和数据资产入表")
    assert dividend["key"] == "dividend_value"
    assert coal["key"] == "coal_energy_security"
    assert gold["key"] == "gold_nonferrous"
    assert low_alt["key"] == "low_altitude_satellite"
    assert data["key"] == "data_elements"


def test_build_theme_playbook_context_resolves_next_wave_technology_themes() -> None:
    solid_state = build_theme_playbook_context("固态电池ETF", "全固态路线和硫化物体系")
    perovskite = build_theme_playbook_context("钙钛矿设备", "叠层电池和蒸镀设备")
    quantum = build_theme_playbook_context("量子科技概念", "量子通信和量子芯片")
    event_chain = build_theme_playbook_context("FSD 概念", "特斯拉 FSD 和 Dojo")
    assert solid_state["key"] == "solid_state_battery"
    assert perovskite["key"] == "perovskite"
    assert quantum["key"] == "quantum_computing"
    assert event_chain["key"] == "tesla_chain"


def test_build_theme_playbook_context_resolves_renewable_subthemes() -> None:
    solar = build_theme_playbook_context("光伏ETF", "组件排产和装机预期")
    storage = build_theme_playbook_context("储能概念", "工商业储能和 PCS 出货")
    lithium = build_theme_playbook_context("锂电池ETF", "正极材料和电解液价格")
    grid = build_theme_playbook_context("电网设备", "特高压和变压器订单")
    assert solar["key"] == "solar_mainchain"
    assert storage["key"] == "energy_storage"
    assert lithium["key"] == "lithium_battery"
    assert grid["key"] == "power_grid"


def test_build_theme_playbook_context_resolves_advanced_packaging_and_autonomous_driving() -> None:
    packaging = build_theme_playbook_context("先进封装", "HBM 和 CoWoS 扩产")
    driving = build_theme_playbook_context("自动驾驶ETF", "车路云和激光雷达订单")
    assert packaging["key"] == "advanced_packaging"
    assert driving["key"] == "autonomous_driving"


def test_build_theme_playbook_context_resolves_specific_event_chains() -> None:
    huawei = build_theme_playbook_context("鸿蒙概念", "Mate 和麒麟新品周期")
    apple = build_theme_playbook_context("苹果概念", "iPhone 和 Vision Pro 备货")
    nvidia = build_theme_playbook_context("英伟达映射", "GB200 和 NVLink 链条")
    assert huawei["key"] == "huawei_chain"
    assert apple["key"] == "apple_chain"
    assert nvidia["key"] == "nvidia_chain"


def test_build_theme_playbook_context_exposes_hard_sector_and_family() -> None:
    playbook = build_theme_playbook_context("中信证券", "证券龙头和央企市值管理")
    assert playbook["hard_sector_label"] == "金融"
    assert playbook["theme_family"] == "政策驱动"


def test_build_theme_playbook_context_defers_dividend_overlay_for_broker_financials() -> None:
    playbook = build_theme_playbook_context(
        {
            "name": "中信证券",
            "sector": "金融",
            "industry": "证券Ⅱ",
            "industry_framework_label": "证券Ⅱ",
            "chain_nodes": ["证券Ⅱ"],
        },
        {
            "headline": "分红预案落地",
            "summary_lines": ["分红预案带来防守讨论", "但本体仍是券商龙头"],
        },
        {
            "lead_title": "中信证券披露现金分红预案",
            "signal_type": "公告类型：分红/回报",
        },
    )
    assert playbook["key"] == "sector::financials"
    assert playbook["theme_match_status"] == "hard_sector_guarded"
    assert "高股息 / 红利" in list(playbook["theme_match_candidates"] or [])


def test_build_theme_playbook_context_allows_dividend_overlay_for_bank_financials() -> None:
    playbook = build_theme_playbook_context(
        {
            "name": "招商银行",
            "sector": "金融",
            "industry": "银行",
            "industry_framework_label": "银行",
            "chain_nodes": ["银行"],
        },
        {
            "headline": "高股息风格回暖",
            "summary_lines": ["高股息和分红防守重新回流", "银行权重获得承接"],
        },
    )
    assert playbook["key"] == "dividend_value"


def test_build_theme_playbook_context_exposes_extended_theme_metadata() -> None:
    playbook = build_theme_playbook_context("黄金ETF", "金价和央行购金")
    assert playbook["transmission_path"]
    assert playbook["stage_pattern"]
    assert playbook["rotation_and_crowding"]
    assert playbook["falsifiers"]


def test_build_theme_playbook_context_resolves_consumer_subthemes() -> None:
    baijiu = build_theme_playbook_context("白酒ETF", "高端白酒批价和渠道库存")
    appliance = build_theme_playbook_context("家电龙头", "以旧换新和出海订单")
    travel = build_theme_playbook_context("免税概念", "暑期出行和机场客流")
    mass = build_theme_playbook_context("大众消费", "社零和啤酒餐饮修复")
    assert baijiu["key"] == "baijiu"
    assert appliance["key"] == "home_appliance"
    assert travel["key"] == "travel_retail"
    assert mass["key"] == "mass_consumption"


def test_build_theme_playbook_context_resolves_biotech_subthemes() -> None:
    bd = build_theme_playbook_context("创新药出海", "license-out 和首付款条款")
    clinical = build_theme_playbook_context("临床读数", "ASCO 和 III期终点")
    cxo = build_theme_playbook_context("CXO板块", "海外客户订单和产能利用率")
    assert bd["key"] == "bd_out_licensing"
    assert clinical["key"] == "clinical_readout"
    assert cxo["key"] == "cxo_service"


def test_build_theme_playbook_context_resolves_market_beta() -> None:
    playbook = build_theme_playbook_context("中证A500ETF", "宽基轮动和风格 beta 修复")
    assert playbook["key"] == "market_beta"
    assert playbook["market_logic"]
    assert playbook["rotation_and_crowding"]


def test_build_theme_playbook_context_falls_back_to_sector_playbook() -> None:
    playbook = build_theme_playbook_context("银行ETF")
    assert playbook["key"] == "sector::financials"
    assert playbook["playbook_level"] == "sector"
    assert playbook["hard_sector_label"] == "金融"
    assert playbook["market_logic"]
    assert playbook["transmission_path"]


def test_build_theme_playbook_context_falls_back_to_remaining_broad_sectors() -> None:
    technology = build_theme_playbook_context("软件ETF")
    staples = build_theme_playbook_context("食品ETF")
    agriculture = build_theme_playbook_context("农发种业", "种植业")
    real_estate = build_theme_playbook_context("房屋租赁概念")
    assert technology["key"] == "sector::information_technology"
    assert staples["key"] == "sector::consumer_staples"
    assert agriculture["key"] == "sector::agriculture"
    assert real_estate["key"] == "sector::real_estate"
    assert technology["market_logic"]
    assert staples["rotation_and_crowding"]
    assert agriculture["falsifiers"]
    assert real_estate["falsifiers"]


def test_build_theme_playbook_context_prefers_structured_agriculture_over_market_theme_noise() -> None:
    values = (
        {
            "name": "盐湖股份",
            "symbol": "000792",
            "sector": "农业",
            "industry": "农药化肥",
            "industry_framework_label": "粮食安全",
            "business_scope": "旅游业务 住宿服务 餐饮服务 再生资源回收",
        },
        "硬科技 / AI硬件链",
        "总体来看，盐湖股份 的核心逻辑在于 硬科技 / AI硬件链 主线下的 粮食安全 暴露仍有跟踪价值；",
    )
    hard_sector = classify_hard_sector(*values)
    playbook = build_theme_playbook_context(*values)

    assert hard_sector["key"] == "agriculture"
    assert playbook["key"] == "sector::agriculture"
    assert playbook["hard_sector_label"] == "农业 / 种植链"


def test_build_theme_playbook_context_keeps_legitimate_lithium_battery_soft_theme_for_materials_stock() -> None:
    values = (
        {
            "name": "天赐材料",
            "symbol": "002709",
            "sector": "材料",
            "industry": "化工原料",
            "industry_framework_label": "电解液 / 锂电材料",
            "business_scope": "电池制造 电解液 新材料",
        },
        "",
        "",
    )
    hard_sector = classify_hard_sector(*values)
    playbook = build_theme_playbook_context(*values)

    assert hard_sector["key"] == "materials"
    assert playbook["key"] == "lithium_battery"
    assert playbook["hard_sector_label"] == "材料"
    assert playbook["theme_match_status"] == "resolved"


def test_subject_theme_context_uses_company_profile_for_same_sector_soft_theme() -> None:
    subject = {
        "name": "天赐材料",
        "symbol": "002709",
        "metadata": {
            "name": "天赐材料",
            "symbol": "002709",
            "sector": "化工原料",
            "industry_framework_label": "煤化工",
            "chain_nodes": ["煤化工", "化工材料", "顺周期"],
            "business_scope": "基础化学原料制造;电池制造;电池零配件生产;新材料技术推广服务",
            "company_intro": "核心业务为锂离子电池材料和日化材料及特种化学品两大板块，深耕锂电池电解液业务十余载。",
            "company_profile_source": "tushare_stock_company_cache",
        },
    }

    context = _subject_theme_context(subject)

    assert context["key"] == "lithium_battery"
    assert context["hard_sector_key"] == "materials"
    assert context["theme_match_status"] == "resolved"


def test_subject_theme_context_refreshes_stale_sector_level_theme_context() -> None:
    subject = {
        "name": "天赐材料",
        "symbol": "002709",
        "theme_playbook": {
            "key": "sector::materials",
            "label": "材料",
            "playbook_level": "sector",
            "hard_sector_key": "materials",
            "hard_sector_label": "材料",
            "theme_match_status": "none",
        },
        "metadata": {
            "name": "天赐材料",
            "symbol": "002709",
            "sector": "化工原料",
            "industry_framework_label": "煤化工",
            "chain_nodes": ["煤化工", "化工材料", "顺周期"],
            "business_scope": "基础化学原料制造;电池制造;电池零配件生产;新材料技术推广服务",
            "company_intro": "核心业务为锂离子电池材料和日化材料及特种化学品两大板块，深耕锂电池电解液业务十余载。",
            "company_profile_source": "tushare_stock_company_cache",
        },
    }

    context = _subject_theme_context(subject)

    assert context["key"] == "lithium_battery"
    assert context["hard_sector_key"] == "materials"
    assert context["theme_match_status"] == "resolved"


def test_subject_theme_context_blocks_cross_sector_company_profile_noise() -> None:
    subject = {
        "name": "云天化",
        "symbol": "600096",
        "metadata": {
            "name": "云天化",
            "symbol": "600096",
            "sector": "农药化肥",
            "industry_framework_label": "农资 / 钾肥",
            "business_scope": "化肥,化工原料,新材料,新能源的研发及产品的生产,销售",
            "company_intro": "重点发展新材料、新能源等新兴产业，但公司主体仍以磷产业和化肥为核心。",
            "company_profile_source": "tushare_stock_company_cache",
        },
    }

    context = _subject_theme_context(subject)

    assert context["key"] == "sector::agriculture"
    assert context["hard_sector_label"] == "农业 / 种植链"
    assert context["theme_match_status"] == "hard_sector_guarded"


def test_build_theme_playbook_context_maps_a_share_power_equipment_sector_names() -> None:
    playbook = build_theme_playbook_context("阳光电源", "电气设备")
    bridge_labels = [item["label"] for item in playbook["subtheme_bridge"]]
    assert playbook["key"] == "sector::power_equipment"
    assert playbook["hard_sector_label"] == "电力设备 / 新能源设备"
    assert bridge_labels[:3] == ["光伏主链", "储能", "电网设备"]


def test_build_theme_playbook_context_sector_fallback_exposes_subtheme_bridge() -> None:
    playbook = build_theme_playbook_context("软件ETF")
    bridge_labels = [item["label"] for item in playbook["subtheme_bridge"]]
    assert playbook["playbook_level"] == "sector"
    assert bridge_labels[:3] == ["半导体", "先进封装 / HBM / Chiplet", "AI算力"]
    assert playbook["subtheme_bridge_confidence"] == "none"


def test_build_theme_playbook_context_defers_to_sector_when_conflict_group_is_ambiguous() -> None:
    playbook = build_theme_playbook_context("科技ETF", "HBM、CoWoS 和光模块、服务器一起走强")
    assert playbook["key"] == "sector::information_technology"
    assert playbook["theme_match_status"] == "ambiguous_conflict"
    assert "半导体" in playbook["theme_match_candidates"] or "先进封装 / HBM / Chiplet" in playbook["theme_match_candidates"]
    assert "AI算力" in playbook["theme_match_candidates"]


def test_build_theme_playbook_context_defers_to_sector_for_event_chain_conflicts() -> None:
    playbook = build_theme_playbook_context("科技映射ETF", "鸿蒙新品、英伟达链和发布会供应链一起走强")
    assert playbook["key"] == "sector::information_technology"
    assert playbook["theme_match_status"] == "ambiguous_conflict"
    assert "华为链" in playbook["theme_match_candidates"]
    assert "英伟达链" in playbook["theme_match_candidates"]


def test_build_theme_playbook_context_defers_to_sector_for_consumer_conflicts() -> None:
    playbook = build_theme_playbook_context("消费ETF", "白酒批价回暖和家电以旧换新一起走强")
    assert playbook["key"] == "sector::consumer_discretionary"
    assert playbook["theme_match_status"] == "ambiguous_conflict"
    assert "白酒 / 高端消费" in playbook["theme_match_candidates"]
    assert "家电 / 出海制造消费" in playbook["theme_match_candidates"]


def test_build_theme_playbook_context_defers_to_sector_for_policy_conflicts() -> None:
    playbook = build_theme_playbook_context("政策主线ETF", "一带一路、新质生产力和中特估一起走强")
    assert playbook["key"] == "sector::industrials"
    assert playbook["theme_match_status"] == "ambiguous_conflict"
    assert "政策驱动 / 国产替代" in playbook["theme_match_candidates"]
    assert "央企市值管理 / 中特估" in playbook["theme_match_candidates"]


def test_sector_subtheme_bridge_reorders_with_context_signals() -> None:
    bridge = sector_subtheme_bridge_items("information_technology", "AI算力主线延续，光模块和服务器景气还在")
    summary = summarize_sector_subtheme_bridge(bridge)
    assert bridge[0]["key"] == "ai_computing"
    assert "AI算力" in bridge[0]["matched_tokens"]
    assert "光模块" in bridge[0]["matched_tokens"]
    assert summary["confidence"] == "high"
    assert summary["top_key"] == "ai_computing"


def test_all_registered_theme_playbooks_load_with_sections() -> None:
    required_sections = {
        "market_logic",
        "bullish_drivers",
        "risks",
        "variables",
        "transmission_path",
        "stage_pattern",
        "rotation_and_crowding",
        "falsifiers",
        "guardrails",
        "style_notes",
    }
    for key in PLAYBOOK_REGISTRY:
        playbook = load_theme_playbook(key)
        assert playbook, key
        assert playbook["sections"], key
        assert required_sections.issubset(playbook["sections"]), key


def test_all_registered_sector_playbooks_load_with_sections() -> None:
    required_sections = {
        "market_logic",
        "bullish_drivers",
        "risks",
        "variables",
        "transmission_path",
        "stage_pattern",
        "rotation_and_crowding",
        "falsifiers",
        "guardrails",
        "style_notes",
    }
    sector_keys = [key for key, meta in HARD_SECTOR_REGISTRY.items() if meta.get("path")]
    for key in sector_keys:
        playbook = load_sector_playbook(key)
        assert playbook, key
        assert playbook["sections"], key
        assert required_sections.issubset(playbook["sections"]), key


def test_stock_analysis_editor_packet_prefers_identity_theme_before_note_overlap() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "创新药ETF",
            "symbol": "159992",
            "asset_type": "cn_etf",
            "metadata": {"sector": "医药"},
            "notes": ["今天讨论也提到了 license-out 和首付款条款。"],
            "day_theme": {"label": "成长修复"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看主线能否延续。"},
            },
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )
    assert packet["theme_playbook"]["key"] == "innovative_drug"


def test_stock_analysis_editor_packet_uses_sector_fallback_when_theme_missing() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "银行ETF",
            "symbol": "512800",
            "asset_type": "cn_etf",
            "metadata": {"sector": "金融"},
            "day_theme": {"label": "背景宏观主导"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看行业轮动是否延续。"},
            },
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )
    assert packet["theme_playbook"]["key"] == "sector::financials"
    assert packet["theme_playbook"]["playbook_level"] == "sector"
    assert any("行业层" in line for line in packet["homepage"]["theme_lines"])
    assert any("下钻方向" in line or "细分方向" in line for line in packet["homepage"]["theme_lines"])


def test_stock_analysis_editor_packet_surfaces_theme_conflict_before_subtheme_bridge() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "科技ETF",
            "symbol": "515000",
            "asset_type": "cn_etf",
            "metadata": {"sector": "信息技术"},
            "notes": ["HBM、CoWoS 和光模块、服务器一起走强。"],
            "day_theme": {"label": "科技轮动"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看科技内部到底往哪条线收敛。"},
            },
            "narrative": {"headline": "科技内部几条线同时活跃。", "judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )
    assert packet["theme_playbook"]["key"] == "sector::information_technology"
    assert packet["theme_playbook"]["theme_match_status"] == "ambiguous_conflict"
    assert any("还在打架" in line for line in packet["homepage"]["theme_lines"])


def test_build_stock_analysis_editor_packet_surfaces_strategy_background_confidence(monkeypatch) -> None:
    class _FakeStrategyRepo:
        def summarize_background_confidence(self, symbol, lookback=8):
            assert symbol == "600519"
            assert lookback == 8
            return {
                "symbol": "600519",
                "status": "degraded",
                "label": "退化",
                "summary": "这类 setup 过去有效，但最近退化。",
                "reason": "最近 `4` 个可验证样本命中率 `25%`，成本后方向收益 `-2.5%`，稳定性开始走弱。",
            }

    monkeypatch.setattr("src.output.editor_payload.StrategyRepository", lambda: _FakeStrategyRepo())

    packet = build_stock_analysis_editor_packet(
        {
            "name": "贵州茅台",
            "symbol": "600519",
            "asset_type": "cn_stock",
            "metadata": {"sector": "消费"},
            "day_theme": {"label": "消费分化"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看确认。"},
            },
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )

    assert packet["homepage"]["micro_lines"][0].startswith("策略后台置信度：`退化`。")
    assert "排序不直接翻空，但当前应下调置信度" in packet["homepage"]["micro_lines"][0]


def test_build_stock_analysis_editor_packet_skips_strategy_background_confidence_when_missing(monkeypatch) -> None:
    class _FakeStrategyRepo:
        def summarize_background_confidence(self, symbol, lookback=8):
            assert symbol == "600519"
            return {}

    monkeypatch.setattr("src.output.editor_payload.StrategyRepository", lambda: _FakeStrategyRepo())

    packet = build_stock_analysis_editor_packet(
        {
            "name": "贵州茅台",
            "symbol": "600519",
            "asset_type": "cn_stock",
            "metadata": {"sector": "消费"},
            "day_theme": {"label": "消费分化"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看确认。"},
            },
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )

    assert not any("策略后台置信度" in line for line in packet["homepage"]["micro_lines"])


def test_summarize_theme_playbook_contract_keeps_conflict_and_bridge_fields() -> None:
    playbook = build_theme_playbook_context("科技ETF", "HBM、CoWoS 和光模块、服务器一起走强")
    contract = summarize_theme_playbook_contract(playbook)
    assert contract["contract_version"] == "theme_playbook.v1"
    assert contract["playbook_level"] == "sector"
    assert contract["label"] == "信息技术"
    assert contract["theme_match_status"] == "ambiguous_conflict"
    assert set(contract["theme_match_candidates"]) == {"先进封装 / HBM / Chiplet", "AI算力"}
    assert contract["subtheme_bridge_confidence"] in {"high", "medium"}
    assert contract["subtheme_bridge_top_label"] in {"半导体", "先进封装 / HBM / Chiplet", "AI算力"}
    assert "AI算力" in contract["subtheme_bridge_candidates"]
    assert contract["trading_role_label"] in {"观察", "主线扩散"}


def test_scan_editor_packet_builds_event_digest_contract_from_announcement() -> None:
    dimensions = _sample_dimensions()
    dimensions["catalyst"] = {
        "score": 58,
        "max_score": 100,
        "summary": "公司级事件开始出现。",
        "factors": [
            {
                "name": "结构化事件",
                "signal": "公司发布 800G 光模块新品公告",
                "detail": "",
                "display_score": "12/20",
            }
        ],
        "evidence": [
            {
                "layer": "结构化事件",
                "title": "新易盛公告：800G 光模块新品发布",
                "source": "证券时报",
                "date": "2026-03-28",
            }
        ],
    }
    packet = build_scan_editor_packet(
        {
            "name": "新易盛",
            "symbol": "300502",
            "asset_type": "cn_stock",
            "generated_at": "2026-03-29 10:00:00",
            "day_theme": {"label": "AI算力"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看事件兑现。"},
            },
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": dimensions,
        },
        bucket="观察稿",
    )
    contract = summarize_event_digest_contract(packet.get("event_digest") or {})
    assert contract["status"] == "已消化"
    assert contract["lead_layer"] == "公告"
    assert contract["lead_detail"].startswith("公告类型：")
    assert contract["impact_summary"]
    assert contract["thesis_scope"] == "待确认"
    assert "产品验证、客户反馈和需求映射层" in contract["changed_what"]
    assert any("事件状态 `已消化`" in line for line in packet["homepage"]["news_lines"])


def test_scan_editor_packet_homepage_keeps_related_intelligence_item() -> None:
    dimensions = _sample_dimensions()
    dimensions["catalyst"] = {
        "score": 62,
        "max_score": 100,
        "summary": "公司级情报已开始转向执行验证。",
        "factors": [],
        "evidence": [
            {
                "layer": "公告",
                "title": "关于举办投资者关系活动的公告",
                "source": "CNINFO",
                "configured_source": "CNINFO::direct",
                "source_note": "official_direct",
                "link": "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600519",
                "date": "2026-03-31",
            },
            {
                "layer": "公告",
                "title": "投资者关系活动记录表：管理层交流渠道库存与发货节奏",
                "source": "CNINFO",
                "configured_source": "CNINFO::direct",
                "source_note": "official_direct",
                "link": "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600519&plate=szse",
                "date": "2026-03-31",
            },
        ],
    }
    packet = build_scan_editor_packet(
        {
            "name": "贵州茅台",
            "symbol": "600519",
            "asset_type": "cn_stock",
            "generated_at": "2026-03-31 10:00:00",
            "day_theme": {"label": "白酒"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等右侧确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看管理层口径和渠道节奏。"},
            },
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": dimensions,
            "taxonomy_summary": "主暴露属于白酒。",
        },
        bucket="观察稿",
    )

    news_lines = list(packet["homepage"]["news_lines"])
    assert any("关于举办投资者关系活动的公告" in line for line in news_lines)
    assert any("投资者关系活动记录表" in line for line in news_lines)


def test_scan_editor_packet_marks_event_digest_pending_review_for_search_gap() -> None:
    dimensions = _sample_dimensions()
    dimensions["catalyst"] = {
        "score": 30,
        "max_score": 100,
        "summary": "主题链条有线索，但直连覆盖不足。",
        "factors": [],
        "evidence": [],
        "theme_news": [{"title": "AI算力链条热度继续扩散", "source": "财联社", "date": "2026-03-29"}],
    }
    packet = build_scan_editor_packet(
        {
            "name": "AI算力ETF",
            "symbol": "515000",
            "asset_type": "cn_etf",
            "generated_at": "2026-03-29 10:00:00",
            "day_theme": {"label": "AI算力"},
            "regime": {"current_regime": "recovery"},
            "provenance": {"catalyst_diagnosis": "suspected_search_gap / 待 AI 联网复核"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看复核结果。"},
            },
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": dimensions,
        },
        bucket="观察稿",
    )
    contract = summarize_event_digest_contract(packet.get("event_digest") or {})
    assert contract["status"] == "待复核"
    assert contract["lead_layer"] == "行业主题事件"
    assert any("待复核" in line for line in packet["homepage"]["news_lines"])


def test_scan_editor_packet_builds_what_changed_from_prior_thesis(monkeypatch) -> None:
    class _FakeThesisRepo:
        def get(self, symbol):
            assert symbol == "300502"
            return {
                "core_assumption": "800G 光模块放量兑现",
                "validation_metric": "订单和毛利率同步改善",
                "holding_period": "1-3个月",
                "event_digest_snapshot": {
                    "status": "待补充",
                    "lead_layer": "行业主题事件",
                    "lead_detail": "主题事件：主题热度/映射",
                    "lead_title": "AI算力链热度扩散",
                    "impact_summary": "资金偏好 / 景气",
                    "thesis_scope": "待确认",
                    "importance_reason": "先放在观察前排，因为它更多是在改写 `资金偏好 / 景气` 的主题理解，还没下沉成公司级兑现。",
                },
            }

    monkeypatch.setattr("src.output.editor_payload.ThesisRepository", lambda: _FakeThesisRepo())
    dimensions = _sample_dimensions()
    dimensions["catalyst"] = {
        "score": 58,
        "max_score": 100,
        "summary": "公司级事件开始出现。",
        "factors": [
            {
                "name": "结构化事件",
                "signal": "公司发布 800G 光模块新品公告",
                "detail": "",
                "display_score": "12/20",
            }
        ],
        "evidence": [
            {
                "layer": "结构化事件",
                "title": "新易盛公告：800G 光模块新品发布",
                "source": "证券时报",
                "date": "2026-03-28",
            }
        ],
    }

    packet = build_scan_editor_packet(
        {
            "name": "新易盛",
            "symbol": "300502",
            "asset_type": "cn_stock",
            "generated_at": "2026-03-29 10:00:00",
            "day_theme": {"label": "AI算力"},
            "regime": {"current_regime": "recovery"},
            "action": {"direction": "观察为主"},
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": dimensions,
        },
        bucket="观察稿",
    )

    assert packet["what_changed"]["conclusion_label"] == "升级"
    assert "核心假设是" in packet["what_changed"]["previous_view"]
    assert "主题事件：主题热度/映射" in packet["what_changed"]["previous_view"]
    assert "当时的优先级判断是：" not in packet["what_changed"]["previous_view"]
    assert "事件状态从 `待补充` 升到 `已消化`" in packet["what_changed"]["change_summary"]
    assert "当前更该前置的是" in packet["what_changed"]["change_summary"]
    assert "更直接影响 `" in packet["what_changed"]["current_event_understanding"]
    assert "当前更像 `" in packet["what_changed"]["current_event_understanding"]
    assert "优先级判断是：" not in packet["what_changed"]["current_event_understanding"]
    assert packet["what_changed"]["state_trigger"] == "事件完成消化"


def test_build_what_changed_summary_uses_state_machine_for_theme_focus_shift(monkeypatch) -> None:
    class _FakeThesisRepo:
        def get(self, symbol):
            assert symbol == "515000"
            return {
                "core_assumption": "AI算力景气会继续扩散",
                "event_digest_snapshot": {
                    "status": "已消化",
                    "lead_layer": "行业主题事件",
                    "lead_detail": "主题事件：情绪热度",
                    "lead_title": "AI算力链热度扩散",
                    "impact_summary": "资金偏好",
                    "thesis_scope": "待确认",
                },
            }

    monkeypatch.setattr("src.output.editor_payload.ThesisRepository", lambda: _FakeThesisRepo())

    summary = build_what_changed_summary(
        {"symbol": "515000", "action": {"direction": "观察为主"}, "narrative": {"judgment": {"state": "中期逻辑未坏"}}},
        {
            "status": "已消化",
            "lead_layer": "行业主题事件",
            "lead_detail": "主题事件：价格/排产验证",
            "lead_title": "算力链涨价与排产验证",
            "impact_summary": "盈利 / 景气",
            "thesis_scope": "thesis变化",
            "changed_what": "主题热度已下沉到供需与排产验证。",
        },
    )

    assert summary["conclusion_label"] == "升级"
    assert summary["state_trigger"] == "主题从热度切到景气验证"
    assert "供需" in summary["change_summary"] or "价格/排产验证" in summary["change_summary"]


def test_build_what_changed_summary_prefers_subject_theme_over_day_theme_in_current_view() -> None:
    summary = build_what_changed_summary(
        {
            "symbol": "300274",
            "day_theme": {"label": "背景宏观主导"},
            "theme_playbook": {
                "key": "solar_mainchain",
                "label": "光伏主链",
                "playbook_level": "theme",
                "hard_sector_label": "电力设备 / 新能源设备",
            },
            "metadata": {
                "sector": "电力设备",
                "industry_framework_label": "光伏主链",
                "chain_nodes": ["光伏主链", "储能", "电网设备"],
            },
            "action": {"direction": "回避"},
            "narrative": {"judgment": {"state": "观察为主"}},
        },
        {
            "status": "待补充",
            "lead_layer": "行业主题事件",
            "lead_detail": "主题事件：景气验证",
            "lead_title": "光伏链景气验证",
            "impact_summary": "盈利 / 景气",
            "thesis_scope": "thesis变化",
        },
    )

    assert "光伏主链" in summary["current_view"]
    assert "背景宏观主导" not in summary["current_view"]


def test_scan_editor_packet_carries_since_last_review_intelligence_note(monkeypatch) -> None:
    class _FakeThesisRepo:
        def get(self, symbol):
            assert symbol == "512480"
            return {
                "event_digest_snapshot": {
                    "status": "待补充",
                    "recorded_at": "2026-03-26 09:00:00",
                }
            }

    monkeypatch.setattr("src.output.editor_payload.ThesisRepository", lambda: _FakeThesisRepo())
    dimensions = _sample_dimensions()
    dimensions["catalyst"] = {
        "score": 0,
        "max_score": 100,
        "summary": "当前能命中的多是旧闻回放或背景线索，新增催化仍不足。",
        "coverage": {
            "diagnosis": "stale_live_only",
            "latest_news_at": "2026-03-20",
        },
        "theme_news": [
            {
                "layer": "主题级关键新闻",
                "title": "半导体设备需求持续改善",
                "source": "Reuters",
                "date": "2026-03-20",
            }
        ],
        "evidence": [],
        "factors": [],
    }

    packet = build_scan_editor_packet(
        {
            "name": "半导体ETF",
            "symbol": "512480",
            "asset_type": "cn_etf",
            "generated_at": "2026-03-29 10:00:00",
            "day_theme": {"label": "半导体"},
            "regime": {"current_regime": "recovery"},
            "provenance": {"catalyst_diagnosis": "stale_live_only"},
            "action": {"direction": "观察为主"},
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": dimensions,
        },
        bucket="观察稿",
    )

    assert "自上次复查" in str(packet["event_digest"].get("history_note"))
    assert any("自上次复查" in line for line in packet["homepage"]["news_lines"])


def test_render_financial_editor_prompt_includes_what_changed_block(monkeypatch) -> None:
    class _FakeThesisRepo:
        def get(self, symbol):
            assert symbol == "300502"
            return {
                "core_assumption": "800G 光模块放量兑现",
                "validation_metric": "订单和毛利率同步改善",
                "holding_period": "1-3个月",
                "event_digest_snapshot": {
                    "status": "待补充",
                    "lead_layer": "行业主题事件",
                    "lead_detail": "主题事件：主题热度/映射",
                    "lead_title": "AI算力链热度扩散",
                    "impact_summary": "资金偏好 / 景气",
                    "thesis_scope": "待确认",
                },
            }

    monkeypatch.setattr("src.output.editor_payload.ThesisRepository", lambda: _FakeThesisRepo())
    packet = build_scan_editor_packet(
        {
            "name": "新易盛",
            "symbol": "300502",
            "asset_type": "cn_stock",
            "generated_at": "2026-03-29 10:00:00",
            "day_theme": {"label": "AI算力"},
            "regime": {"current_regime": "recovery"},
            "action": {"direction": "观察为主"},
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        },
        bucket="观察稿",
    )

    rendered = render_financial_editor_prompt(packet)

    assert "## Event Digest" in rendered
    assert "- 事件细分：" in rendered
    assert "- 影响层：" in rendered
    assert "- 影响性质：" in rendered
    assert "- 优先级判断：" in rendered
    assert "## What Changed" in rendered
    assert "上次怎么看：核心假设是 `800G 光模块放量兑现`" in rendered
    assert "这次什么变了：" in rendered
    assert "当前事件理解：" in rendered
    assert "结论变化：`" in rendered
    assert "触发：" in rendered
    assert "状态解释：" in rendered


def test_render_financial_editor_prompt_surfaces_latest_signal_and_review_history(monkeypatch) -> None:
    class _FakeThesisRepo:
        def get(self, symbol):
            assert symbol == "512480"
            return {
                "event_digest_snapshot": {
                    "status": "待补充",
                    "recorded_at": "2026-03-26 09:00:00",
                }
            }

    monkeypatch.setattr("src.output.editor_payload.ThesisRepository", lambda: _FakeThesisRepo())
    dimensions = _sample_dimensions()
    dimensions["catalyst"] = {
        "score": 32,
        "max_score": 100,
        "summary": "新增催化开始出现，但还不够直接。",
        "coverage": {
            "latest_news_at": "2026-03-29 08:15:00",
        },
        "theme_news": [
            {
                "title": "半导体设备链早盘继续走强",
                "source": "财联社",
                "date": "2026-03-29 08:15:00",
            }
        ],
        "evidence": [],
        "factors": [],
    }

    packet = build_scan_editor_packet(
        {
            "name": "半导体ETF",
            "symbol": "512480",
            "asset_type": "cn_etf",
            "generated_at": "2026-03-29 10:00:00",
            "day_theme": {"label": "半导体"},
            "regime": {"current_regime": "recovery"},
            "action": {"direction": "观察为主"},
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": dimensions,
        },
        bucket="观察稿",
    )

    rendered = render_financial_editor_prompt(packet)

    assert "最新情报时点：2026-03-29 08:15:00" in rendered
    assert "上次复查时间：2026-03-26 09:00:00" in rendered
    assert "与上次复查相比：自上次复查" in rendered


def test_action_lines_prepend_event_digest_boundary_when_present() -> None:
    lines = _action_lines(
        {
            "trade_state": "观察为主",
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破关键支撑重评",
            },
        },
        observe_only=True,
        event_digest={
            "status": "已消化",
            "lead_detail": "财报摘要：盈利/指引上修",
            "impact_summary": "盈利 / 估值",
            "thesis_scope": "thesis变化",
            "next_step": "继续盯下一季指引、利润兑现质量和价格确认。",
        },
    )

    assert lines[0].startswith("这次 `财报摘要：盈利/指引上修` 已开始改写 `盈利 / 估值`")
    assert any("空仓先别急着直接找买点" in line for line in lines)


def test_stock_analysis_editor_packet_surfaces_event_chain_conflict() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "科技映射ETF",
            "symbol": "516000",
            "asset_type": "cn_etf",
            "metadata": {"sector": "信息技术"},
            "notes": ["鸿蒙新品、英伟达链和发布会供应链一起走强。"],
            "day_theme": {"label": "科技轮动"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看映射链到底往哪条线收敛。"},
            },
            "narrative": {"headline": "事件链和映射链同时活跃。", "judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )
    assert packet["theme_playbook"]["theme_match_status"] == "ambiguous_conflict"
    assert any("华为链 / 英伟达链" in line or "还在打架" in line for line in packet["homepage"]["theme_lines"])


def test_stock_analysis_editor_packet_surfaces_policy_conflict() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "政策主线ETF",
            "symbol": "560001",
            "asset_type": "cn_etf",
            "metadata": {"sector": "工业"},
            "notes": ["一带一路、新质生产力和中特估一起走强。"],
            "day_theme": {"label": "政策主线"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看政策主线内部先往哪条线收敛。"},
            },
            "narrative": {"headline": "政策主题内部几条线同时活跃。", "judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )
    assert packet["theme_playbook"]["theme_match_status"] == "ambiguous_conflict"
    assert any("政策驱动 / 国产替代" in line or "还在打架" in line for line in packet["homepage"]["theme_lines"])


def test_stock_analysis_editor_packet_reorders_sector_bridge_from_day_theme_and_notes() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "软件ETF",
            "symbol": "159852",
            "asset_type": "cn_etf",
            "metadata": {"sector": "信息技术"},
            "notes": ["今天更像光模块和服务器链条继续扩散。"],
            "day_theme": {"label": "AI算力"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看行业轮动是否往细分方向收敛。"},
            },
            "narrative": {"headline": "景气还在向算力链条集中。", "judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )
    bridge = list(packet["theme_playbook"]["subtheme_bridge"])
    assert packet["theme_playbook"]["key"] == "sector::information_technology"
    assert bridge[0]["key"] == "ai_computing"
    assert packet["theme_playbook"]["subtheme_bridge_confidence"] == "high"
    assert any("AI算力" in line for line in packet["homepage"]["theme_lines"])
    assert any("仍先按行业层来写" in line for line in packet["homepage"]["theme_lines"])


def test_render_financial_editor_prompt_includes_sector_subtheme_bridge() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "软件ETF",
            "symbol": "159852",
            "asset_type": "cn_etf",
            "metadata": {"sector": "信息技术"},
            "day_theme": {"label": "背景宏观主导"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看行业轮动是否往细分方向收敛。"},
            },
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )
    rendered = render_financial_editor_prompt(packet)
    assert "行业层下钻方向" in rendered
    assert "半导体 / 先进封装 / HBM / Chiplet / AI算力" in rendered
    assert "行业层下钻置信度：none" in rendered
    assert "当前只允许把细分方向写成观察清单" in rendered


def test_render_financial_editor_prompt_includes_dynamic_sector_subtheme_signals() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "软件ETF",
            "symbol": "159852",
            "asset_type": "cn_etf",
            "metadata": {"sector": "信息技术"},
            "notes": ["今天更像光模块和服务器链条继续扩散。"],
            "day_theme": {"label": "AI算力"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看行业轮动是否往细分方向收敛。"},
            },
            "narrative": {"headline": "景气还在向算力链条集中。", "judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )
    rendered = render_financial_editor_prompt(packet)
    assert "行业层下钻置信度：high" in rendered
    assert "当前下钻线索" in rendered
    assert "下钻判断依据" in rendered
    assert "AI算力 <- AI算力, 光模块" in rendered
    assert "更偏向/可优先留意某条细分线" in rendered


def test_render_financial_editor_prompt_surfaces_theme_conflict_guard() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "科技ETF",
            "symbol": "515000",
            "asset_type": "cn_etf",
            "metadata": {"sector": "信息技术"},
            "notes": ["HBM、CoWoS 和光模块、服务器一起走强。"],
            "day_theme": {"label": "科技轮动"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看科技内部到底往哪条线收敛。"},
            },
            "narrative": {"headline": "科技内部几条线同时活跃。", "judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )
    rendered = render_financial_editor_prompt(packet)
    assert "主题匹配状态：ambiguous_conflict" in rendered
    assert "易混主题候选" in rendered
    assert "先不要硬落单一细主题" in rendered


def test_render_financial_editor_prompt_surfaces_consumer_conflict_guard() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "消费ETF",
            "symbol": "159928",
            "asset_type": "cn_etf",
            "metadata": {"sector": "可选消费"},
            "notes": ["白酒批价回暖和家电以旧换新一起走强。"],
            "day_theme": {"label": "消费修复"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看消费内部先往哪条线收敛。"},
            },
            "narrative": {"headline": "可选消费内部几条线一起修复。", "judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )
    rendered = render_financial_editor_prompt(packet)
    assert "主题匹配状态：ambiguous_conflict" in rendered
    assert "白酒 / 高端消费" in rendered
    assert "家电 / 出海制造消费" in rendered


def test_render_financial_editor_prompt_surfaces_policy_conflict_guard() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "政策主线ETF",
            "symbol": "560001",
            "asset_type": "cn_etf",
            "metadata": {"sector": "工业"},
            "notes": ["一带一路、新质生产力和中特估一起走强。"],
            "day_theme": {"label": "政策主线"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看政策主线内部先往哪条线收敛。"},
            },
            "narrative": {"headline": "政策主题内部几条线同时活跃。", "judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )
    rendered = render_financial_editor_prompt(packet)
    assert "主题匹配状态：ambiguous_conflict" in rendered
    assert "政策驱动 / 国产替代" in rendered
    assert "央企市值管理 / 中特估" in rendered


def test_build_stock_analysis_editor_packet_uses_v2_homepage_sections() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "中金黄金",
            "symbol": "600489",
            "asset_type": "cn_stock",
            "day_theme": {"label": "黄金 / 有色资源"},
            "regime": {"current_regime": "stagflation"},
            "action": {
                "direction": "观察为主",
                "entry": "等 MA20 重新走平后再看",
                "stop": "跌破关键支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "当前更适合继续跟，不适合直接抬仓位。"},
            },
            "narrative": {"judgment": {"state": "持有优于追高"}},
            "dimensions": _sample_dimensions(),
        }
    )
    homepage = packet["homepage"]
    assert packet["packet_version"] == "editor-v2"
    assert homepage["version"] == "thesis-first-v2"
    assert homepage["macro_lines"]
    assert homepage["theme_lines"]
    assert homepage["sentiment_lines"]
    assert homepage["micro_lines"]
    assert homepage["action_lines"]
    assert any(line.startswith("赛道判断：") for line in homepage["theme_lines"])
    assert any(line.startswith("载体判断：") for line in homepage["micro_lines"])
    assert any(line.startswith("执行卡：") for line in homepage["action_lines"])
    assert any(line.startswith("尾部风险：") for line in [*homepage["macro_lines"], *homepage["micro_lines"], *homepage["action_lines"]])
    assert any("硬分类" in line or "行业层" in line for line in homepage["theme_lines"])
    assert any("更像样的理解路径" in line or "轮动和拥挤度" in line for line in homepage["theme_lines"])


def test_build_stock_analysis_editor_packet_micro_lines_include_shared_technical_labels_when_history_exists() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "中金黄金",
            "symbol": "600489",
            "asset_type": "cn_stock",
            "history": _sample_history(),
            "day_theme": {"label": "黄金 / 有色资源"},
            "regime": {"current_regime": "stagflation"},
            "action": {
                "direction": "观察为主",
                "entry": "等 MA20 重新走平后再看",
                "stop": "跌破关键支撑重评",
            },
            "narrative": {"judgment": {"state": "持有优于追高"}},
            "dimensions": _sample_dimensions(),
        }
    )
    assert any("当前图形标签：" in line for line in packet["homepage"]["micro_lines"])


def test_build_stock_analysis_editor_packet_surfaces_portfolio_overlap_in_micro_lines() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "国电南瑞",
            "symbol": "600406",
            "asset_type": "cn_stock",
            "day_theme": {"label": "电网设备"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等 MA20 重新走平后再看",
                "stop": "跌破关键支撑重评",
            },
            "narrative": {"judgment": {"state": "持有优于追高"}},
            "dimensions": _sample_dimensions(),
            "portfolio_overlap_summary": {
                "overlap_label": "同一行业主线加码",
                "summary_line": "这条建议和现有组合最重的行业 `电网` 同线，重复度较高，更像同一主线延伸，而不是完全新方向。",
                "style_priority_hint": "如果只是同风格加码，优先级低于补新方向。",
            },
        }
    )
    assert any("和现有持仓的关系上，这条更像 `同一行业主线加码`" in line for line in packet["homepage"]["micro_lines"])
    assert any("优先级低于补新方向" in line for line in packet["homepage"]["micro_lines"])


def test_action_lines_normalize_double_prefixed_observe_entry() -> None:
    lines = _action_lines(
        {
            "trade_state": "观察为主",
            "action": {
                "direction": "观察为主",
                "entry": "先等顶背离/假突破消化、MACD/OBV 重新同步，再考虑分批介入",
                "stop": "跌破关键支撑重评",
            },
        },
        observe_only=True,
    )
    assert lines[0] == "空仓先别急着直接找买点，升级触发器先看 `顶背离/假突破消化、MACD/OBV 重新同步`。"
    assert "先看 `先等" not in lines[0]


def test_action_lines_skip_observe_only_placeholder_position() -> None:
    lines = _action_lines(
        {
            "trade_state": "观察为主",
            "action": {
                "direction": "回避",
                "entry": "等确认后再看",
                "position": "暂不出手",
                "stop": "跌破关键支撑重评",
            },
        },
        observe_only=True,
    )
    assert all("首次仓位按 `暂不出手`" not in line for line in lines)
    assert all("止损按" not in line for line in lines)


def test_build_stock_analysis_editor_packet_softens_observe_only_action_template() -> None:
    packet = build_stock_analysis_editor_packet(
        {
            "name": "农发种业",
            "symbol": "600313",
            "asset_type": "cn_stock",
            "trade_state": "回避",
            "generated_at": "2026-04-01 10:00:00",
            "dimensions": _sample_dimensions(),
            "action": {
                "direction": "回避",
                "entry": "等站回 MA20 后再决定是否给买入区间。",
                "position": "首次仓位先按观察仓理解。",
                "stop": "跌破 7.654 或主线/催化失效时重新评估",
                "stop_ref": 7.654,
                "target_ref": 9.800,
            },
        }
    )

    lines = list(dict(packet.get("homepage") or {}).get("action_lines") or [])

    assert any("只按首页的触发、建仓、仓位和失效条件复核" in line for line in lines)
    assert all("止损按" not in line for line in lines)
    assert all("下沿先看" not in line for line in lines)
    assert "观察为主（偏回避）" in str(dict(packet.get("homepage") or {}).get("conclusion"))


def test_playbook_hint_line_normalizes_logic_and_stage_punctuation() -> None:
    line = playbook_hint_line(
        {
            "label": "农业 / 种植链",
            "playbook_level": "sector",
            "market_logic": ["市场在交易的是农产品价格、种植收益和补贴。"],
            "stage_pattern": ["更常见的是“价格或政策先动 -> 验证 -> 盈利兑现”"],
        }
    )
    assert "。常见阶段往往是" in line
    assert "。；" not in line
    assert "常见阶段往往是 `更常见的是" not in line


def test_theme_lines_normalize_falsifier_sentence_without_double_prefix() -> None:
    lines = _theme_lines(
        {
            "label": "农业 / 种植链",
            "hard_sector_label": "可选消费",
            "theme_family": "周期/宏观类",
            "playbook_level": "sector",
            "falsifiers": ["如果价格、库存和种植收益都没有改善"],
        },
        {"name": "农发种业", "symbol": "600313"},
    )
    assert any("如果价格、库存和种植收益都没有改善，这类首页就不能再往乐观方向写。" in line for line in lines)
    assert all("如果出现 `如果" not in line for line in lines)


def test_theme_lines_keep_existing_falsifier_conclusion_without_repeating_suffix() -> None:
    lines = _theme_lines(
        {
            "label": "农业 / 种植链",
            "hard_sector_label": "可选消费",
            "theme_family": "周期/宏观类",
            "playbook_level": "sector",
            "falsifiers": ["如果价格、库存和种植收益都没有改善，这条行业逻辑就不能继续往乐观方向写"],
        },
        {"name": "农发种业", "symbol": "600313"},
    )
    assert any("如果价格、库存和种植收益都没有改善，这条行业逻辑就不能继续往乐观方向写。" == line for line in lines)


def test_theme_lines_normalize_rotation_and_crowding_prefix() -> None:
    lines = _theme_lines(
        {
            "label": "宽基 / 市场Beta",
            "hard_sector_label": "宽基 / 市场Beta",
            "theme_family": "周期/宏观类",
            "playbook_level": "resolved",
            "rotation_and_crowding": ["观察宽基时重点看：现在是大票占优、小票补涨、成长修复，还是防御抱团。"],
        },
        {"name": "A500ETF华泰柏瑞", "symbol": "563360"},
    )
    assert any("轮动和拥挤度上，要重点看：现在是大票占优、小票补涨、成长修复，还是防御抱团" in line for line in lines)
    assert all("要重点看：观察宽基时重点看" not in line for line in lines)


def test_render_editor_homepage_renders_v2_sections() -> None:
    packet = build_etf_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "day_theme": {"label": "能源冲击 + 地缘风险"},
            "regime": {"current_regime": "stagflation"},
            "selection_context": {
                "delivery_observe_only": True,
                "coverage_lines": ["结构化事件覆盖 67%（2/3）", "高置信直接新闻覆盖 0%（0/3）"],
                "proxy_contract": {
                    "market_flow": {"interpretation": "资金更偏防守，不是全面 risk-on。"},
                },
            },
            "winner": {
                "name": "有色金属ETF",
                "symbol": "512400",
                "asset_type": "cn_etf",
                "trade_state": "观察为主",
                "action": {"direction": "观察为主", "entry": "等回踩再看", "stop": "跌破支撑重评"},
                "dimensions": _sample_dimensions(),
                "proxy_signals": {
                    "social_sentiment": {
                        "aggregate": {
                            "interpretation": "情绪指数 61，热度偏高，需防拥挤交易。",
                            "limitations": ["这是价格和量能行为推导出的情绪代理，不是真实社媒抓取。"],
                        }
                    }
                },
                "narrative": {"playbook": {"trend": "回踩确认后再跟。"}},
                "taxonomy_summary": "主暴露属于资源。",
            },
        }
    )
    rendered = render_editor_homepage(packet)
    assert "## 首页判断" in rendered
    assert "本页重点看 `有色金属ETF (512400)`" in rendered
    assert "### 宏观面" in rendered
    assert "### 板块 / 主题认知" in rendered
    assert "### 关键新闻 / 关键证据" in rendered
    assert (
        "当前更依赖现有结构化事件和代理证据来理解" in rendered
        or "本轮实时新闻/事件覆盖存在降级" in rendered
        or "事件状态 `待补充`" in rendered
        or "行业主题事件" in rendered
        or "当前可前置的外部情报仍偏少" in rendered
    )
    assert "### 情绪与热度" in rendered
    assert "### 微观面" in rendered
    assert "### 动作建议与结论" in rendered
    assert "结论：" in rendered


def test_render_editor_homepage_preserves_required_headings_when_macro_is_empty() -> None:
    packet = build_fund_pick_editor_packet(
        {
            "generated_at": "2026-03-31 10:00:00",
            "day_theme": {"label": "背景宏观主导"},
            "regime": {"current_regime": "recovery"},
            "selection_context": {"delivery_observe_only": True},
            "winner": {
                "name": "测试场外基金",
                "symbol": "022365",
                "asset_type": "cn_fund",
                "trade_state": "观察为主",
                "action": {"direction": "观察为主", "entry": "等确认再看", "stop": "跌破支撑重评"},
                "dimensions": _sample_dimensions(),
                "proxy_signals": {
                    "social_sentiment": {
                        "aggregate": {
                            "interpretation": "情绪指数 48，当前未出现极端一致预期。",
                            "limitations": ["这是价格和量能行为推导出的情绪代理，不是真实社媒抓取。"],
                        }
                    }
                },
                "narrative": {"playbook": {}, "judgment": {"state": "观察为主"}},
            },
        }
    )
    packet["homepage"]["macro_lines"] = []
    rendered = render_editor_homepage(packet)
    assert "### 宏观面" in rendered
    assert "先按中性背景理解" in rendered
    assert "### 板块 / 主题认知" in rendered
    assert "### 情绪与热度" in rendered
    assert "### 微观面" in rendered
    assert "### 动作建议与结论" in rendered


def test_render_editor_homepage_surfaces_key_news_when_evidence_exists() -> None:
    payload = {
        "generated_at": "2026-03-26 10:00:00",
        "day_theme": {"label": "AI / 半导体催化"},
        "regime": {"current_regime": "stagflation"},
        "selection_context": {"delivery_observe_only": True},
        "winner": {
            "name": "半导体ETF",
            "symbol": "512480",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "action": {"direction": "观察为主", "entry": "等回踩再看", "stop": "跌破支撑重评"},
            "dimensions": {
                **_sample_dimensions(),
                "catalyst": {
                    "score": 18,
                    "max_score": 100,
                    "summary": "直接催化一般。",
                    "factors": [],
                    "evidence": [
                        {"title": "台积电上调先进封装资本开支", "source": "Reuters", "date": "2026-03-25"},
                    ],
                },
            },
            "taxonomy_summary": "主暴露属于半导体。",
        },
        "alternatives": [],
        "notes": [],
    }
    rendered = render_editor_homepage(build_etf_pick_editor_packet(payload))
    assert "### 关键新闻 / 关键证据" in rendered
    assert "台积电上调先进封装资本开支" in rendered
    assert "新鲜情报" in rendered
    assert "媒体直连" in rendered


def test_render_editor_homepage_sanitizes_sentiment_lines_that_sound_like_direct_catalyst() -> None:
    packet = {
        "homepage": {
            "version": "thesis-first-v2",
            "total_judgment": "先观察。",
            "macro_lines": ["宏观中性。"],
            "theme_lines": ["主题仍在。"],
            "news_lines": ["证据一般。"],
            "sentiment_lines": ["热度已经形成直接催化，说明现在可以直接追。"],
            "micro_lines": ["微观确认不足。"],
            "action_lines": ["先观察。"],
            "conclusion": "结论：继续观察。",
        }
    }
    rendered = render_editor_homepage(packet)
    assert "### 情绪与热度" in rendered
    assert "直接催化" not in rendered
    assert "可以直接追" not in rendered
    assert "更多反映关注度和拥挤度变化" in rendered


def test_build_etf_pick_editor_packet_consumes_news_report_items_when_direct_evidence_missing() -> None:
    payload = {
        "generated_at": "2026-04-01 10:00:00",
        "day_theme": {"label": "黄金 / 有色资源"},
        "regime": {"current_regime": "recovery"},
        "selection_context": {"delivery_observe_only": True},
        "winner": {
            "name": "有色金属ETF",
            "symbol": "512400",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "action": {"direction": "观察为主", "entry": "等确认再看", "stop": "跌破支撑重评"},
            "dimensions": {
                **_sample_dimensions(),
                "catalyst": {
                    "score": 12,
                    "max_score": 100,
                    "summary": "直接催化偏弱。",
                    "evidence": [],
                    "theme_news": [],
                },
            },
            "news_report": {
                "items": [
                    {
                        "title": "有色金属板块走强，工业金属价格继续修复",
                        "source": "财联社",
                        "published_at": "2026-04-01 14:00:00",
                        "link": "https://example.com/nonferrous",
                    }
                ]
            },
            "taxonomy_summary": "主暴露属于有色。",
        },
        "alternatives": [],
        "notes": [],
    }

    packet = build_etf_pick_editor_packet(payload)

    assert packet["event_digest"]["lead_title"] == "有色金属板块走强，工业金属价格继续修复"
    assert packet["event_digest"]["lead_link"] == "https://example.com/nonferrous"
    rendered = render_editor_homepage(packet)
    assert "有色金属板块走强，工业金属价格继续修复" in rendered


def test_build_etf_pick_editor_packet_falls_back_to_market_event_rows_when_news_missing() -> None:
    payload = {
        "generated_at": "2026-04-01 10:00:00",
        "day_theme": {"label": "黄金 / 有色资源"},
        "regime": {"current_regime": "recovery"},
        "selection_context": {"delivery_observe_only": True},
        "winner": {
            "name": "有色金属ETF",
            "symbol": "512400",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "action": {"direction": "观察为主", "entry": "等确认再看", "stop": "跌破支撑重评"},
            "dimensions": {
                **_sample_dimensions(),
                "catalyst": {
                    "score": 12,
                    "max_score": 100,
                    "summary": "直接催化偏弱。",
                    "evidence": [],
                    "theme_news": [],
                },
            },
            "news_report": {"items": []},
            "market_event_rows": [
                [
                    "2026-04-01",
                    "A股行业走强：有色（+2.65%）；领涨 紫金矿业",
                    "A股行业/盘面",
                    "高",
                    "有色",
                    "",
                    "主线增强",
                    "偏利多，先看 `有色` 能否继续扩散。",
                ]
            ],
            "taxonomy_summary": "主暴露属于有色。",
        },
        "alternatives": [],
        "notes": [],
    }

    packet = build_etf_pick_editor_packet(payload)

    assert packet["event_digest"]["lead_title"] == "A股行业走强：有色（+2.65%）；领涨 紫金矿业"
    rendered = render_editor_homepage(packet)
    assert "A股行业走强：有色（+2.65%）；领涨 紫金矿业" in rendered


def test_build_etf_pick_editor_packet_uses_subject_dimensions_to_upgrade_semiconductor_role() -> None:
    payload = {
        "generated_at": "2026-04-11 10:00:00",
        "day_theme": {"label": "背景宏观主导"},
        "regime": {"current_regime": "recovery"},
        "selection_context": {"delivery_observe_only": False},
        "winner": {
            "name": "国泰中证半导体材料设备主题ETF",
            "symbol": "159516",
            "asset_type": "cn_etf",
            "trade_state": "回调更优",
            "action": {
                "direction": "观望偏多",
                "entry": "先等顶背离/假突破消化后再看",
                "stop": "跌破支撑重评",
                "position": "首次建仓 ≤3%",
                "horizon": {
                    "code": "swing",
                    "label": "波段跟踪（2-6周）",
                    "style": "当前更像主线轮动和催化共振驱动的波段跟踪，核心在未来几周能否继续得到资金确认。",
                },
            },
            "dimensions": {
                **_sample_dimensions(),
                "technical": {"score": 34, "max_score": 100, "summary": "技术结构仍偏弱。", "factors": []},
                "fundamental": {"score": 49, "max_score": 100, "summary": "产品质量中性。", "factors": []},
                "catalyst": {"score": 64, "max_score": 100, "summary": "有催化苗头。", "factors": [], "evidence": []},
                "relative_strength": {"score": 75, "max_score": 100, "summary": "相对强弱明显改善。", "factors": []},
                "risk": {"score": 35, "max_score": 100, "summary": "赔率仍需控制。", "factors": []},
                "macro": {"score": 7, "max_score": 40, "summary": "宏观并非顺风。", "factors": []},
            },
            "metadata": {"chain_nodes": ["AI算力", "半导体", "成长股估值修复"]},
            "taxonomy_summary": "主暴露属于行业主题。",
            "narrative": {"judgment": {"state": "回调更优"}},
        },
        "alternatives": [],
        "notes": [],
    }

    packet = build_etf_pick_editor_packet(payload)

    assert packet["theme_playbook"]["trading_role_label"] == "主线扩散"
    assert packet["theme_playbook"]["trading_position_label"] == "卫星仓"
    assert packet["subject"]["dimensions"]["catalyst"]["score"] == 64
    assert packet["subject"]["day_theme"]["label"] == "背景宏观主导"


def test_build_etf_pick_editor_packet_does_not_demote_evidence_rich_mainline_to_rotation() -> None:
    payload = {
        "generated_at": "2026-04-11 11:00:00",
        "day_theme": {"label": "硬科技 / AI硬件链"},
        "regime": {"current_regime": "recovery"},
        "selection_context": {"delivery_observe_only": True},
        "winner": {
            "name": "国泰中证半导体材料设备主题ETF",
            "symbol": "159516",
            "asset_type": "cn_etf",
            "trade_state": "回调更优",
            "action": {
                "direction": "观望偏多",
                "entry": "先等顶背离/假突破消化后再看",
                "position": "首次建仓 ≤3%",
                "horizon": {
                    "code": "swing",
                    "label": "波段跟踪（2-6周）",
                    "style": "当前更像主线轮动和催化共振驱动的波段跟踪，核心在未来几周能否继续得到资金确认。",
                    "fit_reason": "催化和相对强弱都在线，优势主要集中在未来几周的轮动延续，而不是长周期兑现。",
                },
            },
            "dimensions": {
                **_sample_dimensions(),
                "technical": {"score": 34, "max_score": 100, "summary": "技术结构仍偏弱。", "factors": []},
                "fundamental": {"score": 53, "max_score": 100, "summary": "产品质量中性。", "factors": []},
                "catalyst": {"score": 64, "max_score": 100, "summary": "有催化苗头。", "factors": [], "evidence": []},
                "relative_strength": {
                    "score": 75,
                    "max_score": 100,
                    "summary": "相对强弱明显改善。",
                    "factors": [{"name": "超额拐点", "detail": "相对基准从负转正更接近轮动切换窗口。"}],
                },
                "risk": {"score": 35, "max_score": 100, "summary": "赔率仍需控制。", "factors": []},
                "macro": {"score": 7, "max_score": 40, "summary": "宏观并非顺风。", "factors": []},
            },
            "metadata": {"chain_nodes": ["AI算力", "半导体", "成长股估值修复"]},
            "notes": ["先看右侧确认，暂不追价。"],
            "narrative": {
                "judgment": {"state": "回调更优"},
                "summary_lines": ["外部覆盖不足，但这只是证据降级，不等于这条主线失效。"],
                "playbook": {"trend": "更适合等短线动能重新修复后再跟随。"},
            },
        },
        "alternatives": [],
        "notes": [],
    }

    packet = build_etf_pick_editor_packet(payload)

    assert packet["theme_playbook"]["trading_role_label"] == "主线扩散"
    assert packet["theme_playbook"]["trading_position_label"] == "卫星仓"
    assert "普通轮动" in packet["theme_playbook"]["trading_role_summary"]
    assert any("再小仓试" in line and "不是现在就挂单位" in line for line in packet["homepage"]["action_lines"])
    assert any("机械挂单位" in line and "买点、止损和目标" in line for line in packet["homepage"]["action_lines"])


def test_render_financial_editor_prompt_surfaces_horizon_expression_contract() -> None:
    payload = {
        "generated_at": "2026-04-23 10:00:00",
        "day_theme": {"label": "AI硬件链 / 通信设备"},
        "regime": {"current_regime": "recovery"},
        "selection_context": {"delivery_observe_only": True},
        "winner": {
            "name": "国泰中证全指通信设备ETF",
            "symbol": "515880",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "action": {
                "direction": "观望",
                "entry": "先等拥挤消化后的再确认",
                "position": "观察仓",
                "horizon": {
                    "code": "watch_crowded_mainline_consolidation",
                    "family_code": "watch",
                    "label": "观察期",
                    "setup_code": "crowded_mainline_consolidation",
                    "setup_label": "高位拥挤主线分歧",
                    "fit_reason": "主线和相对强弱未必坏，但高位分歧还在，先等再确认。",
                },
            },
            "dimensions": _sample_dimensions(),
            "metadata": {"chain_nodes": ["CPO", "通信设备", "AI算力"]},
            "taxonomy_summary": "主暴露属于通信设备。",
        },
        "alternatives": [],
        "notes": [],
    }

    packet = build_etf_pick_editor_packet(payload)
    rendered = render_financial_editor_prompt(packet)

    assert packet["subject"]["horizon_expression"]["setup_code"] == "crowded_mainline_consolidation"
    assert "## Horizon Expression Contract" in rendered
    assert "高位拥挤主线分歧" in rendered
    assert "修复早期" in rendered
    assert "不能改变推荐等级、动作方向" in rendered


def test_render_editor_homepage_uses_top_level_winner_evidence_when_dimension_evidence_missing() -> None:
    payload = {
        "generated_at": "2026-03-26 10:00:00",
        "day_theme": {"label": "背景宏观主导"},
        "regime": {"current_regime": "deflation"},
        "selection_context": {"delivery_observe_only": True},
        "winner": {
            "name": "券商ETF华宝",
            "symbol": "512000",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "action": {"direction": "观察为主", "entry": "等回踩再看", "stop": "跌破支撑重评"},
            "dimensions": _sample_dimensions(),
            "evidence": [
                {"title": "当前可前置的一手情报有限，判断更多参考结构化事件和行业线索。", "source": "覆盖率摘要"},
            ],
            "taxonomy_summary": "主暴露属于金融。",
        },
        "alternatives": [{"name": "证券ETF国泰", "symbol": "512880"}],
        "notes": [],
    }
    rendered = render_editor_homepage(build_etf_pick_editor_packet(payload))
    assert "本页重点看 `券商ETF华宝 (512000)`" in rendered
    assert "### 关键新闻 / 关键证据" in rendered
    assert "当前可前置的外部情报仍偏少" in rendered


def test_render_editor_homepage_falls_back_to_theme_news_when_direct_evidence_missing() -> None:
    payload = {
        "generated_at": "2026-03-26 10:00:00",
        "day_theme": {"label": "AI / 半导体催化"},
        "regime": {"current_regime": "stagflation"},
        "selection_context": {"delivery_observe_only": True},
        "winner": {
            "name": "半导体ETF",
            "symbol": "512480",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "action": {"direction": "观察为主", "entry": "等回踩再看", "stop": "跌破支撑重评"},
            "dimensions": {
                **_sample_dimensions(),
                "catalyst": {
                    "score": 12,
                    "max_score": 100,
                    "summary": "直接催化偏弱。",
                    "factors": [],
                    "evidence": [],
                    "theme_news": [
                        {"title": "AI服务器资本开支延续，先进封装设备链情绪回暖", "source": "财联社", "date": "2026-03-25"},
                    ],
                },
            },
            "taxonomy_summary": "主暴露属于半导体。",
        },
        "alternatives": [],
        "notes": [],
    }
    rendered = render_editor_homepage(build_etf_pick_editor_packet(payload))
    assert "### 关键新闻 / 关键证据" in rendered
    assert "主题级新闻：" in rendered
    assert "先进封装设备链情绪回暖" in rendered
    assert "主题级情报" in rendered


def test_humanize_news_summary_line_explains_cluster_count() -> None:
    assert (
        _humanize_news_summary_line("主题聚类：价格/供需 6 条")
        == "这批外部情报主要围绕 价格/供需；这里的 6 条，是把重复报道合并后的线索组数，不是 6 个独立利好。"
    )


def test_humanize_news_summary_line_handles_generic_bucket() -> None:
    assert (
        _humanize_news_summary_line("主题聚类：综合/其他 7 条")
        == "这批外部情报暂时没能稳定归到单一主题；去重后先留下 7 条背景线索，更适合当背景补充，不直接当成新增催化。"
    )


def test_news_lines_with_event_digest_for_etf_labels_external_news_and_structured_evidence() -> None:
    subject = {
        "name": "大成有色金属期货ETF",
        "symbol": "159980",
        "asset_type": "cn_etf",
        "metadata": {"tracked_index_name": "上海期货交易所有色金属期货价格指数"},
        "news_report": {
            "items": [
                {
                    "title": "几内亚铝土矿供应扰动延续，氧化铝价格中枢抬升",
                    "source": "新浪财经",
                    "published_at": "2026-03-30T23:30:00",
                    "link": "https://example.com/bauxite",
                }
            ],
            "summary_lines": ["主题聚类：价格/供需 6 条"],
        },
    }
    event_digest = {
        "lead_detail": "跟踪指数框架：大成有色金属期货ETF 跟踪 上海期货交易所有色金属期货价格指数",
        "signal_type": "标准指数框架",
        "signal_strength": "中",
        "conclusion": "先按跟踪指数主链理解。",
        "latest_signal_at": "2026-04-06 02:30:32",
    }

    lines = _news_lines_with_event_digest(subject, event_digest)

    assert lines[0].startswith("外部情报：")
    assert any(line.startswith("结构证据：") for line in lines)
    assert any("不是 6 个独立利好" in line for line in lines)
    assert any(line.startswith("关系说明：") for line in lines)


def test_format_homepage_evidence_line_preserves_signal_bundle_for_linked_news() -> None:
    line = (
        "`行业主题事件`：[美伊接近达成停火框架](https://example.com/ceasefire)（华尔街见闻 / 2026-04-15）"
        "；信号类型：`地缘缓和/风险偏好修复`；信号强弱：`中`；结论：中性偏多，先看 `估值 / 资金偏好` 能否继续拿到确认。"
        "；情报属性：`新鲜情报 / 结构化披露`；来源层级：`结构化披露`；事件理解：先按新增情报线索处理。"
    )

    formatted = _format_homepage_evidence_line(line)

    assert formatted.startswith("外部情报：")
    assert "信号类型：`地缘缓和/风险偏好修复`" in formatted
    assert "信号强弱：`中`" in formatted
    assert "结论：中性偏多" in formatted
    assert "情报属性：" in formatted
    assert "来源层级：" not in formatted
    assert "事件理解：" not in formatted


def test_ensure_homepage_news_signal_bundle_adds_conclusion_for_linked_market_news() -> None:
    line = (
        "外部情报：2026-04-15T00:00:00 · 华尔街见闻：[美伊接近达成停火框架](https://example.com/ceasefire)"
        "；信号类型：`地缘缓和`；信号强弱：`中`"
    )

    enriched = _ensure_homepage_news_signal_bundle(line)

    assert enriched.startswith("外部情报：")
    assert "信号类型：" in enriched
    assert "信号强弱：" in enriched
    assert "结论：" in enriched
    assert "传导：" in enriched


def test_render_editor_homepage_marks_since_last_review_search_fallback(monkeypatch) -> None:
    class _FakeThesisRepo:
        def get(self, symbol):
            return {
                "updated_at": "2026-03-30 09:00:00",
                "event_digest_snapshot": {"status": "待补充", "lead_layer": "行业主题事件"},
            }

    monkeypatch.setattr("src.output.editor_payload.ThesisRepository", lambda: _FakeThesisRepo())
    payload = {
        "generated_at": "2026-03-31 10:00:00",
        "day_theme": {"label": "AI / 半导体催化"},
        "regime": {"current_regime": "stagflation"},
        "selection_context": {"delivery_observe_only": True},
        "winner": {
            "name": "半导体ETF",
            "symbol": "512480",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "action": {"direction": "观察为主", "entry": "等回踩再看", "stop": "跌破支撑重评"},
            "dimensions": {
                **_sample_dimensions(),
                "catalyst": {
                    "score": 12,
                    "max_score": 100,
                    "summary": "直接催化偏弱。",
                    "factors": [],
                    "evidence": [],
                    "theme_news": [
                        {
                            "title": "AI服务器资本开支延续，先进封装设备链情绪回暖",
                            "source": "Reuters",
                            "configured_source": "Reuters",
                            "category": "topic_search",
                            "date": "2026-03-29",
                        },
                    ],
                },
            },
            "taxonomy_summary": "主暴露属于半导体。",
        },
        "alternatives": [],
        "notes": [],
    }

    rendered = render_editor_homepage(build_etf_pick_editor_packet(payload))

    assert "旧闻回放" in rendered
    assert "搜索回退" in rendered
    assert "主题级情报" in rendered


def test_render_editor_homepage_prefers_theme_news_over_diagnostic_evidence() -> None:
    payload = {
        "generated_at": "2026-03-26 10:00:00",
        "day_theme": {"label": "AI / 半导体催化"},
        "regime": {"current_regime": "stagflation"},
        "selection_context": {"delivery_observe_only": True},
        "winner": {
            "name": "半导体ETF",
            "symbol": "512480",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "action": {"direction": "观察为主", "entry": "等回踩再看", "stop": "跌破支撑重评"},
            "dimensions": {
                **_sample_dimensions(),
                    "catalyst": {
                        "score": 12,
                        "max_score": 100,
                        "summary": "直接催化偏弱。",
                        "coverage": {"directional_catalyst_hit": True},
                        "factors": [],
                        "evidence": [
                        {"title": "当前可前置的一手情报有限，判断更多参考结构化事件和行业线索。", "source": "覆盖率摘要"},
                    ],
                    "theme_news": [
                        {"title": "AI服务器资本开支延续，先进封装设备链情绪回暖", "source": "财联社", "date": "2026-03-25"},
                    ],
                },
            },
            "taxonomy_summary": "主暴露属于半导体。",
        },
        "alternatives": [],
        "notes": [],
    }
    rendered = render_editor_homepage(build_etf_pick_editor_packet(payload))
    assert "主题级新闻：" in rendered
    assert "先进封装设备链情绪回暖" in rendered
    assert "当前可前置的一手情报有限" not in rendered


def test_render_editor_homepage_preserves_markdown_links_and_bolds_key_action_data() -> None:
    payload = {
        "generated_at": "2026-03-26 10:00:00",
        "day_theme": {"label": "创新药 / BD 出海"},
        "regime": {"current_regime": "deflation"},
        "selection_context": {"delivery_observe_only": True},
        "winner": {
            "name": "港股通创新药ETF",
            "symbol": "159570",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "action": {
                "direction": "观察为主",
                "entry": "等 MA20 / MA60 向上拐头后再看",
                "position": "首次建仓 ≤3%",
                "stop": "跌破 1.420 重新评估",
                "stop_ref": 1.420,
                "target_ref": 1.784,
                "horizon": {"label": "观察期", "fit_reason": "先看确认，不急着把主题逻辑直接翻译成交易动作。"},
            },
            "dimensions": _sample_dimensions(),
            "taxonomy_summary": "主暴露属于创新药。",
            "catalyst_web_review": {
                "key_evidence": [
                    "2026-03-13 · 财联社：[科创成长层迎首批“毕业生”](https://www.cls.cn/detail/2312134)",
                ]
            },
        },
        "alternatives": [],
        "notes": [],
    }

    rendered = render_editor_homepage(build_etf_pick_editor_packet(payload))

    assert "[科创成长层迎首批“毕业生”](https://www.cls.cn/detail/2312134)" in rendered
    assert "**首次建仓 ≤3%**" not in rendered
    assert "**1.420**" in rendered
    assert "**1.784**" in rendered
    assert "观察稿阶段先记触发、失效和第一次兑现框架" in rendered


def test_render_editor_homepage_preserves_event_digest_links_for_key_news() -> None:
    subject = {
        "name": "贵州茅台",
        "symbol": "600519",
        "asset_type": "cn_stock",
        "trade_state": "观察为主",
        "action": {"direction": "观察为主"},
        "dimensions": {
            **_sample_dimensions(),
            "catalyst": {
                "score": 38,
                "summary": "公司级执行事件已开始改写估值/资金偏好。",
                "evidence": [
                    {
                        "layer": "公告",
                        "title": "关于举办投资者关系活动的公告",
                        "source": "CNINFO",
                        "configured_source": "CNINFO::direct",
                        "source_note": "official_direct",
                        "link": "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600519",
                        "date": "2026-03-31",
                    }
                ],
            },
        },
        "taxonomy_summary": "主暴露属于白酒。",
    }
    payload = {
        "generated_at": "2026-03-31 10:00:00",
        "day_theme": {"label": "白酒"},
        "regime": {"current_regime": "recovery"},
        "selection_context": {"delivery_observe_only": True},
        "top": [subject],
        "watch_positive": [subject],
        "coverage_analyses": [subject],
        "alternatives": [],
        "notes": [],
    }

    rendered = render_editor_homepage(build_stock_pick_editor_packet(payload))

    assert "[关于举办投资者关系活动的公告](https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600519)" in rendered


def test_render_editor_homepage_ignores_placeholder_buy_range_and_uses_numeric_watch_levels() -> None:
    payload = {
        "generated_at": "2026-03-26 10:00:00",
        "day_theme": {"label": "创新药 / BD 出海"},
        "regime": {"current_regime": "deflation"},
        "selection_context": {"delivery_observe_only": True},
        "winner": {
            "name": "港股通创新药ETF",
            "symbol": "159570",
            "asset_type": "cn_etf",
            "trade_state": "观察为主",
            "action": {
                "direction": "观察为主",
                "entry": "等 MA20 / MA60 向上拐头后再看",
                "position": "暂不出手",
                "stop": "跌破 1.420 或主线/催化失效时重新评估",
                "buy_range": "暂不设，先等右侧确认",
                "trim_range": "1.730 - 1.838",
                "stop_ref": 1.420,
                "target_ref": 1.784,
                "horizon": {"label": "观察期", "fit_reason": "继续观察比仓促下手更稳。"},
            },
            "dimensions": _sample_dimensions(),
            "taxonomy_summary": "主暴露属于创新药。",
        },
        "alternatives": [],
        "notes": [],
    }

    rendered = render_editor_homepage(build_etf_pick_editor_packet(payload))

    assert "**1.420**" in rendered
    assert "**1.784**" in rendered
    assert "暂不设，先等右侧确认" not in rendered


def test_build_stock_pick_editor_packet_has_theme_homepage() -> None:
    packet = build_stock_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "day_theme": {"label": "黄金 / 有色资源"},
            "regime": {"current_regime": "stagflation"},
            "market_label": "A股",
            "top": [
                {
                    "name": "中金黄金",
                    "symbol": "600489",
                    "asset_type": "cn_stock",
                    "trade_state": "观察为主",
                    "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
                    "dimensions": _sample_dimensions(),
                    "metadata": {"sector": "有色"},
                }
            ],
            "watch_positive": [],
        }
    )
    assert packet["packet_version"] == "editor-v2"
    assert packet["homepage"]["theme_lines"]
    assert packet["homepage"]["total_judgment"].startswith("今天先按观察稿处理")
    assert "本页重点看 `中金黄金 (600489)`" in packet["homepage"]["total_judgment"]
    assert packet["homepage"]["news_lines"]


def test_build_stock_pick_editor_packet_prefers_existing_subject_theme_playbook() -> None:
    packet = build_stock_pick_editor_packet(
        {
            "generated_at": "2026-04-05 10:00:00",
            "day_theme": {"label": "背景宏观主导"},
            "regime": {"current_regime": "recovery"},
            "market_label": "A股",
            "top": [
                {
                    "name": "阳光电源",
                    "symbol": "300274",
                    "asset_type": "cn_stock",
                    "trade_state": "观察为主",
                    "action": {"direction": "观察为主"},
                    "dimensions": _sample_dimensions(),
                    "metadata": {
                        "sector": "电力设备",
                        "industry_framework_label": "光伏主链",
                        "chain_nodes": ["光伏主链", "储能", "电网设备"],
                    },
                    "theme_playbook": {
                        "key": "solar_mainchain",
                        "label": "光伏主链",
                        "playbook_level": "theme",
                        "hard_sector_label": "电力设备 / 新能源设备",
                    },
                }
            ],
            "watch_positive": [],
        }
    )

    assert packet["theme_playbook"]["key"] == "solar_mainchain"
    assert packet["theme_playbook"]["label"] == "光伏主链"
    assert any("光伏主链" in line for line in packet["homepage"]["theme_lines"])
    assert not any("电网设备当前更像在交易" in line for line in packet["homepage"]["theme_lines"])


def test_build_stock_pick_editor_packet_separates_market_day_theme_from_subject_theme() -> None:
    packet = build_stock_pick_editor_packet(
        {
            "generated_at": "2026-04-22 10:00:00",
            "day_theme": {"label": "硬科技 / AI硬件链"},
            "regime": {"current_regime": "recovery"},
            "market_label": "A股",
            "top": [
                {
                    "name": "紫金矿业",
                    "symbol": "601899",
                    "asset_type": "cn_stock",
                    "trade_state": "看好但暂不推荐",
                    "action": {"direction": "小仓试仓", "entry": "等回踩确认", "position": "首次建仓 2%-5%"},
                    "rating": {"rank": 2},
                    "dimensions": {
                        **_sample_dimensions(),
                        "technical": {"score": 48, "max_score": 100, "summary": "技术修复中。", "factors": []},
                        "fundamental": {"score": 78, "max_score": 100, "summary": "基本面仍有支撑。", "factors": []},
                        "catalyst": {"score": 64, "max_score": 100, "summary": "催化继续跟踪。", "factors": []},
                        "relative_strength": {"score": 72, "max_score": 100, "summary": "相对强弱占优。", "factors": []},
                    },
                    "metadata": {"sector": "有色"},
                    "theme_playbook": {
                        "key": "gold_nonferrous",
                        "label": "黄金 / 有色资源",
                        "playbook_level": "theme",
                        "hard_sector_label": "材料",
                    },
                }
            ],
            "watch_positive": [],
        }
    )

    summary = packet["homepage"]["total_judgment"]
    assert "本页重点看 `紫金矿业 (601899)`" in summary
    assert "今天这份个股推荐稿更适合按 `A股` 范围理解" in summary
    assert "当前市场主线背景偏 `硬科技 / AI硬件链`" in summary
    assert "但这页真正先看 `黄金 / 有色资源` 这条线里谁已经先走到可执行边界。" in summary
    assert "当前主线偏 `硬科技 / AI硬件链`，已经有少数标的从方向判断走到可执行边界。" not in summary


def test_build_stock_pick_editor_packet_resistance_level_ignores_ma_window_number() -> None:
    packet = build_stock_pick_editor_packet(
        {
            "generated_at": "2026-04-24 10:00:00",
            "day_theme": {"label": "硬科技 / AI硬件链"},
            "regime": {"current_regime": "recovery"},
            "market_label": "A股",
            "top": [
                {
                    "name": "宝丰能源",
                    "symbol": "600989",
                    "asset_type": "cn_stock",
                    "trade_state": "看好但暂不推荐",
                    "action": {
                        "direction": "做多",
                        "entry": "先等 MACD 再次转强或站回 MA20",
                        "position": "首次建仓 ≤3%",
                        "stop": "跌破 26.478 或主线/催化失效时重新评估",
                        "buy_range": "26.951 - 28.348",
                        "stop_ref": 26.4776,
                        "target_ref": 36.49,
                    },
                    "dimensions": {
                        **_sample_dimensions(),
                        "technical": {
                            "score": 38,
                            "max_score": 100,
                            "summary": "技术面仍需确认。",
                            "factors": [
                                {
                                    "factor_id": "j1_resistance_zone",
                                    "name": "压力位",
                                    "signal": "上方存在近端压力：MA20 29.033（上方 0.9%）",
                                    "detail": "优先看 MA20、斐波那契 0.786、近20/60日高点和摆动前高；上方压制越近，反弹越容易先进入承压消化。",
                                }
                            ],
                        },
                    },
                    "metadata": {"sector": "煤炭"},
                    "theme_playbook": {
                        "key": "energy_resources",
                        "label": "能源 / 资源",
                        "playbook_level": "theme",
                    },
                }
            ],
            "watch_positive": [],
        }
    )

    action_lines = "\n".join(packet["homepage"]["action_lines"])
    assert "`29.033`" in action_lines
    assert "`20.000`" not in action_lines


def test_build_stock_pick_editor_packet_surfaces_hard_exclusion_before_watch_trigger() -> None:
    packet = build_stock_pick_editor_packet(
        {
            "generated_at": "2026-04-24 10:00:00",
            "day_theme": {"label": "硬科技 / AI硬件链"},
            "regime": {"current_regime": "recovery"},
            "market_label": "A股",
            "top": [
                {
                    "name": "中际旭创",
                    "symbol": "300308",
                    "asset_type": "cn_stock",
                    "trade_state": "观察为主",
                    "excluded": True,
                    "exclusion_reasons": ["个股估值处于极高区间"],
                    "action": {
                        "direction": "回避",
                        "entry": "等 RSI 回落到 60 附近且 MACD 不死叉",
                        "position": "首次建仓 ≤3%",
                        "stop_ref": 825.240,
                        "target_ref": 1004.640,
                    },
                    "dimensions": _sample_dimensions(),
                    "metadata": {"sector": "科技", "industry": "通信设备", "chain_nodes": ["光模块", "CPO"]},
                    "theme_playbook": {
                        "key": "ai_compute",
                        "label": "AI算力",
                        "playbook_level": "theme",
                    },
                }
            ],
            "watch_positive": [],
        }
    )

    action_lines = packet["homepage"]["action_lines"]
    assert action_lines[0].startswith("执行卡：已触发硬排除")
    assert "不能按正式推荐执行" in action_lines[0]


def test_build_stock_pick_editor_packet_prefers_top_subject_when_observe_only() -> None:
    packet = build_stock_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "day_theme": {"label": "背景宏观主导"},
            "regime": {"current_regime": "deflation"},
            "market_label": "A股",
            "top": [
                {
                    "name": "恒瑞医药",
                    "symbol": "600276",
                    "asset_type": "cn_stock",
                    "trade_state": "观察为主",
                    "action": {"direction": "观察为主"},
                    "dimensions": _sample_dimensions(),
                    "metadata": {"sector": "医药"},
                }
            ],
            "watch_positive": [
                {
                    "name": "紫金矿业",
                    "symbol": "601899",
                    "asset_type": "cn_stock",
                    "trade_state": "观察为主",
                    "action": {"direction": "观察为主"},
                    "dimensions": _sample_dimensions(),
                    "metadata": {"sector": "有色"},
                }
            ],
        }
    )
    assert packet["homepage"]["total_judgment"].startswith("今天先按观察稿处理")
    assert "本页重点看 `恒瑞医药 (600276)`" in packet["homepage"]["total_judgment"]


def test_build_stock_pick_editor_packet_prefers_top_subject_even_when_watch_list_exists() -> None:
    packet = build_stock_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "day_theme": {"label": "背景宏观主导"},
            "regime": {"current_regime": "recovery"},
            "market_label": "A股",
            "top": [
                {
                    "name": "贵州茅台",
                    "symbol": "600519",
                    "asset_type": "cn_stock",
                    "trade_state": "看好但暂不推荐",
                    "action": {"direction": "看好但暂不推荐"},
                    "dimensions": _sample_dimensions(),
                    "metadata": {"sector": "白酒"},
                }
            ],
            "watch_positive": [
                {
                    "name": "紫金矿业",
                    "symbol": "601899",
                    "asset_type": "cn_stock",
                    "trade_state": "看好但暂不推荐",
                    "action": {"direction": "看好但暂不推荐"},
                    "dimensions": _sample_dimensions(),
                    "metadata": {"sector": "有色"},
                }
            ],
        }
    )
    assert "本页重点看 `贵州茅台 (600519)`" in packet["homepage"]["total_judgment"]


def test_build_stock_pick_editor_packet_uses_ranked_top_subject_when_watch_list_is_empty() -> None:
    packet = build_stock_pick_editor_packet(
        {
            "generated_at": "2026-03-28 10:00:00",
            "day_theme": {"label": "背景宏观主导"},
            "regime": {"current_regime": "recovery"},
            "market_label": "A股",
            "top": [
                {
                    "name": "贵州茅台",
                    "symbol": "600519",
                    "asset_type": "cn_stock",
                    "trade_state": "观察为主",
                    "action": {"direction": "观察为主"},
                    "rating": {"rank": 1},
                    "dimensions": {
                        **_sample_dimensions(),
                        "fundamental": {"score": 47, "max_score": 100, "summary": "基本面未形成强支撑。", "factors": []},
                        "relative_strength": {"score": 8, "max_score": 100, "summary": "相对强弱偏弱。", "factors": []},
                    },
                    "metadata": {"sector": "白酒"},
                },
                {
                    "name": "紫金矿业",
                    "symbol": "601899",
                    "asset_type": "cn_stock",
                    "trade_state": "看好但暂不推荐",
                    "action": {"direction": "看好但暂不推荐", "entry": "先观察"},
                    "rating": {"rank": 2},
                    "dimensions": {
                        **_sample_dimensions(),
                        "fundamental": {"score": 84, "max_score": 100, "summary": "基本面更强。", "factors": []},
                        "relative_strength": {"score": 65, "max_score": 100, "summary": "相对强弱更好。", "factors": []},
                    },
                    "metadata": {"sector": "有色"},
                },
            ],
            "watch_positive": [],
        }
    )
    assert packet["homepage"]["total_judgment"].startswith("今天先按观察稿处理")
    assert "本页重点看 `紫金矿业 (601899)`" in packet["homepage"]["total_judgment"]
    assert "今天这份个股观察稿更适合按 `A股` 范围理解" in packet["homepage"]["total_judgment"]


def test_build_stock_pick_editor_packet_surfaces_strategy_background_confidence(monkeypatch) -> None:
    class _FakeStrategyRepo:
        def summarize_background_confidence(self, symbol, lookback=8):
            assert symbol == "601899"
            assert lookback == 8
            return {
                "symbol": "601899",
                "status": "stable",
                "label": "稳定",
                "summary": "这类 setup 过去验证仍稳定。",
                "reason": "最近 `4` 个可验证样本命中率 `75%`，成本后方向收益 `+2.2%`，未见明显 fixture 降级。",
            }

    monkeypatch.setattr("src.output.editor_payload.StrategyRepository", lambda: _FakeStrategyRepo())

    packet = build_stock_pick_editor_packet(
        {
            "generated_at": "2026-03-28 10:00:00",
            "day_theme": {"label": "背景宏观主导"},
            "regime": {"current_regime": "recovery"},
            "market_label": "A股",
            "top": [
                {
                    "name": "紫金矿业",
                    "symbol": "601899",
                    "asset_type": "cn_stock",
                    "trade_state": "看好但暂不推荐",
                    "action": {"direction": "看好但暂不推荐", "entry": "先观察"},
                    "rating": {"rank": 2},
                    "dimensions": {
                        **_sample_dimensions(),
                        "fundamental": {"score": 84, "max_score": 100, "summary": "基本面更强。", "factors": []},
                        "relative_strength": {"score": 65, "max_score": 100, "summary": "相对强弱更好。", "factors": []},
                    },
                    "metadata": {"sector": "有色"},
                },
            ],
            "watch_positive": [],
        }
    )

    assert packet["homepage"]["micro_lines"][0].startswith("策略后台置信度：`稳定`。")
    assert "当前只当辅助加分，不单独替代基本面和事件判断" in packet["homepage"]["micro_lines"][0]


def test_build_stock_analysis_editor_packet_surfaces_strategy_confidence_upgrade_guard_in_action_lines(monkeypatch) -> None:
    class _FakeStrategyRepo:
        def summarize_background_confidence(self, symbol, lookback=8):
            assert symbol == "600519"
            assert lookback == 8
            return {
                "symbol": "600519",
                "status": "watch",
                "label": "观察",
                "summary": "这类 setup 当前还在观察。",
                "reason": "最近只有 `2` 个可验证样本，暂时不够把它当成稳定策略，只能做辅助说明。",
            }

    monkeypatch.setattr("src.output.editor_payload.StrategyRepository", lambda: _FakeStrategyRepo())

    packet = build_stock_analysis_editor_packet(
        {
            "name": "贵州茅台",
            "symbol": "600519",
            "asset_type": "cn_stock",
            "day_theme": {"label": "消费分化"},
            "regime": {"current_regime": "recovery"},
            "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        }
    )

    assert any("不单靠它把观察稿升级成动作" in line for line in packet["homepage"]["action_lines"])


def test_build_stock_pick_editor_packet_uses_strategy_confidence_as_tiebreak(monkeypatch) -> None:
    class _FakeStrategyRepo:
        def summarize_background_confidence(self, symbol, lookback=8):
            assert lookback == 8
            if symbol == "600519":
                return {
                    "symbol": "600519",
                    "status": "degraded",
                    "label": "退化",
                    "reason": "最近验证开始走弱。",
                }
            if symbol == "601899":
                return {
                    "symbol": "601899",
                    "status": "stable",
                    "label": "稳定",
                    "reason": "最近验证仍稳定。",
                }
            return {}

    monkeypatch.setattr("src.output.editor_payload.StrategyRepository", lambda: _FakeStrategyRepo())

    packet = build_stock_pick_editor_packet(
        {
            "generated_at": "2026-03-28 10:00:00",
            "day_theme": {"label": "背景宏观主导"},
            "regime": {"current_regime": "recovery"},
            "market_label": "A股",
            "top": [
                {
                    "name": "贵州茅台",
                    "symbol": "600519",
                    "asset_type": "cn_stock",
                    "trade_state": "看好但暂不推荐",
                    "action": {"direction": "看好但暂不推荐", "entry": "先观察"},
                    "rating": {"rank": 2},
                    "dimensions": {
                        **_sample_dimensions(),
                        "fundamental": {"score": 84, "max_score": 100, "summary": "基本面更强。", "factors": []},
                        "relative_strength": {"score": 65, "max_score": 100, "summary": "相对强弱更好。", "factors": []},
                    },
                    "metadata": {"sector": "白酒"},
                },
                {
                    "name": "紫金矿业",
                    "symbol": "601899",
                    "asset_type": "cn_stock",
                    "trade_state": "看好但暂不推荐",
                    "action": {"direction": "看好但暂不推荐", "entry": "先观察"},
                    "rating": {"rank": 2},
                    "dimensions": {
                        **_sample_dimensions(),
                        "fundamental": {"score": 84, "max_score": 100, "summary": "基本面更强。", "factors": []},
                        "relative_strength": {"score": 65, "max_score": 100, "summary": "相对强弱更好。", "factors": []},
                    },
                    "metadata": {"sector": "有色"},
                },
            ],
            "watch_positive": [],
        }
    )

    assert "本页重点看 `紫金矿业 (601899)`" in packet["homepage"]["total_judgment"]


def test_build_stock_pick_editor_packet_keeps_observe_threshold_when_strategy_confidence_is_watch(monkeypatch) -> None:
    class _FakeStrategyRepo:
        def summarize_background_confidence(self, symbol, lookback=8):
            assert symbol == "601899"
            assert lookback == 8
            return {
                "symbol": "601899",
                "status": "watch",
                "label": "观察",
                "summary": "这类 setup 当前还在观察。",
                "reason": "最近只有 `2` 个可验证样本，暂时不够把它当成稳定策略，只能做辅助说明。",
            }

    monkeypatch.setattr("src.output.editor_payload.StrategyRepository", lambda: _FakeStrategyRepo())

    packet = build_stock_pick_editor_packet(
        {
            "generated_at": "2026-03-29 10:00:00",
            "day_theme": {"label": "资源轮动"},
            "regime": {"current_regime": "recovery"},
            "market_label": "A股",
            "top": [
                {
                    "name": "紫金矿业",
                    "symbol": "601899",
                    "asset_type": "cn_stock",
                    "trade_state": "观察为主",
                    "action": {"direction": "小仓试仓", "entry": "等回踩确认", "position": "首次建仓 ≤3%"},
                    "rating": {"rank": 1},
                    "dimensions": {
                        **_sample_dimensions(),
                        "technical": {"score": 48, "max_score": 100, "summary": "技术修复中。", "factors": []},
                        "fundamental": {"score": 78, "max_score": 100, "summary": "基本面仍有支撑。", "factors": []},
                        "catalyst": {"score": 64, "max_score": 100, "summary": "催化还在跟。", "factors": []},
                        "relative_strength": {"score": 72, "max_score": 100, "summary": "相对强弱占优。", "factors": []},
                    },
                    "metadata": {"sector": "有色"},
                },
            ],
            "watch_positive": [],
        }
    )

    assert "今天这份个股观察稿更适合按 `A股` 范围理解" in packet["homepage"]["total_judgment"]
    assert any("不单靠它把观察稿升级成动作" in line for line in packet["homepage"]["action_lines"])


def test_action_lines_do_not_repeat_watch_level_prefix() -> None:
    lines = _action_lines(
        {
            "trade_state": "观察为主",
            "action": {
                "direction": "观察为主",
                "stop": "跌破 29.872 或主线/催化失效时重新评估",
                "stop_ref": 29.872,
                "target_ref": 44.940,
            },
        },
        observe_only=True,
    )
    assert any("下沿先看 `29.872` 上方能不能稳住；上沿先看 `44.940` 附近能不能放量突破" in line for line in lines)
    assert not any("关键位先看 下沿先看" in line for line in lines)


def test_theme_lines_normalize_stage_pattern_machine_phrasing() -> None:
    lines = _theme_lines(
        {
            "hard_sector_label": "材料",
            "theme_family": "周期 / 宏观",
            "stage_pattern": ["更常处在避险升温后的高位震荡或全球需求修复下的顺周期扩散阶段"],
        },
        {"name": "紫金矿业", "symbol": "601899"},
    )
    assert any("常见阶段更像 **避险升温后的高位震荡或全球需求修复下的顺周期扩散阶段**。" == line for line in lines)


def test_build_stock_pick_editor_packet_surfaces_candidate_conflict_in_micro_lines() -> None:
    packet = build_stock_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "day_theme": {"label": "消费修复"},
            "regime": {"current_regime": "recovery"},
            "market_label": "A股",
            "top": [
                {
                    "name": "消费ETF",
                    "symbol": "159928",
                    "asset_type": "cn_etf",
                    "trade_state": "观察为主",
                    "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
                    "dimensions": _sample_dimensions(),
                    "metadata": {"sector": "可选消费"},
                    "notes": ["白酒批价回暖和家电以旧换新一起走强。"],
                }
            ],
            "watch_positive": [],
        }
    )
    assert any("还在打架" in line for line in packet["homepage"]["micro_lines"])


def test_build_etf_pick_editor_packet_surfaces_candidate_conflict_in_micro_lines() -> None:
    packet = build_etf_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "day_theme": {"label": "消费修复"},
            "regime": {"current_regime": "recovery"},
            "selection_context": {"delivery_observe_only": True},
            "winner": {
                "name": "消费ETF",
                "symbol": "159928",
                "asset_type": "cn_etf",
                "trade_state": "观察为主",
                "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
                "dimensions": _sample_dimensions(),
                "metadata": {"sector": "可选消费"},
                "notes": ["白酒批价回暖和家电以旧换新一起走强。"],
            },
            "alternatives": [],
        }
    )
    assert packet["homepage"]["micro_lines"][0].startswith("本页重点分析对象是")
    assert any("还在打架" in line for line in packet["homepage"]["micro_lines"])


def test_build_etf_pick_editor_packet_surfaces_strategy_background_confidence(monkeypatch) -> None:
    class _FakeStrategyRepo:
        def summarize_background_confidence(self, symbol, lookback=8):
            assert symbol == "159570"
            assert lookback == 8
            return {
                "symbol": "159570",
                "status": "stable",
                "label": "稳定",
                "summary": "这类 setup 过去验证仍稳定。",
                "reason": "最近 `4` 个可验证样本命中率 `75%`，成本后方向收益 `+2.2%`，未见明显 fixture 降级。",
            }

    monkeypatch.setattr("src.output.editor_payload.StrategyRepository", lambda: _FakeStrategyRepo())

    packet = build_etf_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "day_theme": {"label": "消费修复"},
            "regime": {"current_regime": "recovery"},
            "selection_context": {"delivery_observe_only": True},
            "winner": {
                "name": "港股通创新药ETF",
                "symbol": "159570",
                "asset_type": "cn_etf",
                "trade_state": "观察为主",
                "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
                "dimensions": _sample_dimensions(),
                "metadata": {"sector": "医药"},
                "notes": ["当前仍在主题确认中。"],
            },
            "alternatives": [],
        }
    )
    assert any(line.startswith("策略后台置信度：`稳定`。") for line in packet["homepage"]["micro_lines"])
    assert any("当前只当辅助加分，不单独替代基本面和事件判断" in line for line in packet["homepage"]["micro_lines"])


def test_build_etf_pick_editor_packet_surfaces_portfolio_overlap_in_micro_lines() -> None:
    packet = build_etf_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "day_theme": {"label": "电网设备"},
            "regime": {"current_regime": "recovery"},
            "selection_context": {"delivery_observe_only": True},
            "winner": {
                "name": "电网ETF",
                "symbol": "561380",
                "asset_type": "cn_etf",
                "trade_state": "观察为主",
                "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
                "dimensions": _sample_dimensions(),
                "metadata": {"sector": "电网"},
                "portfolio_overlap_summary": {
                    "overlap_label": "同一行业主线加码",
                    "summary_line": "这条建议和现有组合最重的行业 `电网` 同线，重复度较高，更像同一主线延伸，而不是完全新方向。",
                    "style_priority_hint": "如果只是同风格加码，优先级低于补新方向。",
                },
            },
            "alternatives": [],
        }
    )
    assert any("和现有持仓的关系上，这条更像 `同一行业主线加码`" in line for line in packet["homepage"]["micro_lines"])


def test_build_etf_pick_editor_packet_prefers_etf_index_identity_over_noisy_chain_nodes() -> None:
    packet = build_etf_pick_editor_packet(
        {
            "generated_at": "2026-04-17 18:21:51",
            "day_theme": {"label": "硬科技 / AI硬件链"},
            "regime": {"current_regime": "recovery"},
            "selection_context": {"delivery_observe_only": True},
            "winner": {
                "name": "汇添富中证主要消费ETF",
                "symbol": "159928",
                "asset_type": "cn_etf",
                "trade_state": "回调更优",
                "action": {"direction": "做多", "entry": "等回踩再看", "stop": "跌破支撑重评"},
                "dimensions": _sample_dimensions(),
                "metadata": {
                    "sector": "消费",
                    "industry_framework_label": "内需",
                    "chain_nodes": ["内需", "消费修复", "消费电子零部件及组装", "电子"],
                },
                "benchmark_name": "中证主要消费指数",
                "fund_profile": {
                    "overview": {
                        "业绩比较基准": "中证主要消费指数",
                        "跟踪标的": "中证主要消费指数",
                    }
                },
                "fund_sections": [
                    "## 基金画像",
                    "",
                    "| 项目 | 内容 |",
                    "| --- | --- |",
                    "| 业绩比较基准 | 中证主要消费指数 |",
                ],
            },
            "alternatives": [],
        }
    )

    assert packet["theme_playbook"]["key"] == "sector::consumer_discretionary"
    assert packet["theme_playbook"]["hard_sector_label"] in {"消费", "可选消费"}
    assert packet["theme_playbook"]["label"] != "信息技术"


def test_build_fund_pick_editor_packet_has_theme_homepage() -> None:
    packet = build_fund_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "day_theme": {"label": "黄金 / 有色资源"},
            "regime": {"current_regime": "stagflation"},
            "selection_context": {"delivery_observe_only": True},
            "winner": {
                "name": "黄金ETF联接",
                "symbol": "021740",
                "asset_type": "cn_fund",
                "trade_state": "观察为主",
                "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
                "dimensions": _sample_dimensions(),
            },
        }
    )
    assert packet["packet_version"] == "editor-v2"
    assert packet["homepage"]["theme_lines"]


def test_build_fund_pick_editor_packet_prefers_ai_taxonomy_over_market_beta() -> None:
    packet = build_fund_pick_editor_packet(
        {
            "generated_at": "2026-04-24 15:00:00",
            "day_theme": {"label": "硬科技 / AI硬件链"},
            "selection_context": {"delivery_observe_only": True},
            "winner": {
                "name": "南方创业板人工智能ETF联接A",
                "symbol": "024725",
                "asset_type": "cn_fund",
                "trade_state": "观察为主",
                "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
                "dimensions": _sample_dimensions(),
                "metadata": {
                    "sector": "科技",
                    "benchmark": "创业板人工智能指数收益率*95%+银行活期存款利率(税后)*5%",
                    "chain_nodes": ["AI算力", "软件服务", "成长股估值修复"],
                    "taxonomy": {
                        "theme_profile": {
                            "primary_chain": "AI/成长科技",
                            "theme_family": "泛科技",
                            "theme_role": "主题成长",
                            "evidence_keywords": ["人工智能", "AI算力"],
                        }
                    },
                },
            },
        }
    )
    theme_lines = "\n".join(packet["homepage"]["theme_lines"])
    assert packet["theme_playbook"]["key"] == "ai_computing"
    assert "AI算力" in theme_lines
    assert "宽基 / 市场Beta" not in theme_lines


def test_build_fund_pick_editor_packet_surfaces_strategy_background_confidence(monkeypatch) -> None:
    class _FakeStrategyRepo:
        def summarize_background_confidence(self, symbol, lookback=8):
            assert symbol == "021740"
            assert lookback == 8
            return {
                "symbol": "021740",
                "status": "watch",
                "label": "观察",
                "summary": "这类 setup 当前还在观察。",
                "reason": "最近只有 `2` 个可验证样本，暂时不够把它当成稳定策略，只能做辅助说明。",
            }

    monkeypatch.setattr("src.output.editor_payload.StrategyRepository", lambda: _FakeStrategyRepo())

    packet = build_fund_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "day_theme": {"label": "黄金 / 有色资源"},
            "regime": {"current_regime": "stagflation"},
            "selection_context": {"delivery_observe_only": True},
            "winner": {
                "name": "黄金ETF联接",
                "symbol": "021740",
                "asset_type": "cn_fund",
                "trade_state": "观察为主",
                "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
                "dimensions": _sample_dimensions(),
            },
        }
    )
    assert packet["homepage"]["micro_lines"][0].startswith("策略后台置信度：`观察`。")
    assert "这次信号只能做辅助说明，不单独升级动作" in packet["homepage"]["micro_lines"][0]


def test_build_fund_pick_editor_packet_surfaces_candidate_conflict_in_micro_lines() -> None:
    packet = build_fund_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "day_theme": {"label": "政策主线"},
            "regime": {"current_regime": "recovery"},
            "selection_context": {"delivery_observe_only": True},
            "winner": {
                "name": "政策精选混合",
                "symbol": "021741",
                "asset_type": "cn_fund",
                "trade_state": "观察为主",
                "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
                "dimensions": _sample_dimensions(),
                "metadata": {"sector": "工业"},
                "notes": ["一带一路、新质生产力和中特估一起走强。"],
            },
        }
    )
    assert any("还在打架" in line for line in packet["homepage"]["micro_lines"])


def test_observe_packets_frontload_no_signal_notice() -> None:
    etf_packet = build_etf_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "selection_context": {"delivery_observe_only": True},
            "winner": {
                "name": "港股通创新药ETF",
                "symbol": "159570",
                "asset_type": "cn_etf",
                "trade_state": "观察为主",
                "action": {"direction": "回避", "entry": "等确认", "stop": "跌破支撑"},
                "dimensions": _sample_dimensions(),
            },
        }
    )
    fund_packet = build_fund_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "selection_context": {"delivery_observe_only": True},
            "winner": {
                "name": "黄金ETF联接",
                "symbol": "021740",
                "asset_type": "cn_fund",
                "trade_state": "观察为主",
                "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
                "dimensions": _sample_dimensions(),
            },
        }
    )
    scan_packet = build_scan_editor_packet(
        {
            "name": "半导体ETF",
            "symbol": "512480",
            "asset_type": "cn_etf",
            "day_theme": {"label": "AI / 半导体催化"},
            "regime": {"current_regime": "deflation"},
            "action": {"direction": "回避", "entry": "等确认", "stop": "跌破支撑"},
            "narrative": {"judgment": {"state": "观察为主"}},
            "dimensions": _sample_dimensions(),
        }
    )
    for packet in (etf_packet, fund_packet, scan_packet):
        assert packet["homepage"]["total_judgment"].startswith("今天先按观察稿处理")


def test_etf_pick_editor_packet_keeps_recommendation_homepage_for_guanhang_pian_duo() -> None:
    packet = build_etf_pick_editor_packet(
        {
            "generated_at": "2026-03-26 10:00:00",
            "selection_context": {"delivery_observe_only": False},
            "winner": {
                "name": "化工ETF",
                "symbol": "159870",
                "asset_type": "cn_etf",
                "trade_state": "持有优于追高",
                "action": {
                    "direction": "观望偏多",
                    "entry": "等回踩再看",
                    "position": "首次建仓 ≤3%",
                    "stop": "跌破支撑",
                },
                "dimensions": _sample_dimensions(),
            },
        }
    )

    assert "今天先按观察稿处理" not in packet["homepage"]["total_judgment"]


def test_build_scan_editor_packet_surfaces_candidate_conflict_in_micro_lines() -> None:
    packet = build_scan_editor_packet(
        {
            "name": "政策主线ETF",
            "symbol": "560001",
            "asset_type": "cn_etf",
            "day_theme": {"label": "政策主线"},
            "regime": {"current_regime": "recovery"},
            "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
            "narrative": {"judgment": {"state": "观察为主"}},
            "dimensions": _sample_dimensions(),
            "metadata": {"sector": "工业"},
            "notes": ["一带一路、新质生产力和中特估一起走强。"],
        },
        bucket="观察稿",
    )
    assert any("还在打架" in line for line in packet["homepage"]["micro_lines"])


def test_build_scan_editor_packet_surfaces_strategy_background_confidence(monkeypatch) -> None:
    class _FakeStrategyRepo:
        def summarize_background_confidence(self, symbol, lookback=8):
            assert symbol == "600519"
            assert lookback == 8
            return {
                "symbol": "600519",
                "status": "watch",
                "label": "观察",
                "summary": "这类 setup 当前还在观察。",
                "reason": "最近只有 `2` 个可验证样本，暂时不够把它当成稳定策略，只能做辅助说明。",
            }

    monkeypatch.setattr("src.output.editor_payload.StrategyRepository", lambda: _FakeStrategyRepo())

    packet = build_scan_editor_packet(
        {
            "name": "贵州茅台",
            "symbol": "600519",
            "asset_type": "cn_stock",
            "day_theme": {"label": "消费分化"},
            "regime": {"current_regime": "recovery"},
            "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
        },
        bucket="观察稿",
    )

    assert packet["homepage"]["micro_lines"][0].startswith("策略后台置信度：`观察`。")
    assert "这次信号只能做辅助说明，不单独升级动作" in packet["homepage"]["micro_lines"][0]


def test_build_stock_pick_editor_packet_surfaces_portfolio_overlap_in_micro_lines() -> None:
    packet = build_stock_pick_editor_packet(
        {
            "generated_at": "2026-03-28 10:00:00",
            "day_theme": {"label": "电网设备"},
            "regime": {"current_regime": "recovery"},
            "market_label": "A股",
            "top": [
                {
                    "name": "国电南瑞",
                    "symbol": "600406",
                    "asset_type": "cn_stock",
                    "trade_state": "观察为主",
                    "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
                    "rating": {"rank": 1},
                    "dimensions": _sample_dimensions(),
                    "metadata": {"sector": "电网"},
                    "portfolio_overlap_summary": {
                        "overlap_label": "同一行业主线加码",
                        "summary_line": "这条建议和现有组合最重的行业 `电网` 同线，重复度较高，更像同一主线延伸，而不是完全新方向。",
                        "style_priority_hint": "如果只是同风格加码，优先级低于补新方向。",
                    },
                }
            ],
            "watch_positive": [],
        }
    )
    assert any("和现有持仓的关系上，这条更像 `同一行业主线加码`" in line for line in packet["homepage"]["micro_lines"])


def test_build_briefing_editor_packet_has_market_homepage() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-03-26 08:30:00",
            "day_theme": "AI算力",
            "regime": {"current_regime": "recovery"},
            "headline_lines": ["今天更像算力主线内部轮动，不是全面风险偏好回归。", "市场先看主线扩散。"],
            "core_event_lines": ["财联社：算力链订单继续验证，市场先交易龙头扩散。"],
            "action_lines": ["先看早段延续性，再决定是否升级风险偏好。"],
            "macro_items": ["M1-M2 剪刀差仍偏弱。"],
            "quality_lines": ["情绪代理只作辅助，不等于真实社媒抓取。"],
            "proxy_contract": {"market_flow": {"interpretation": "资金仍偏结构性，不是全面 risk-on。"}},
            "a_share_watch_candidates": [
                {
                    "name": "新易盛",
                    "symbol": "300502",
                    "asset_type": "cn_stock",
                    "dimensions": _sample_dimensions(),
                    "metadata": {"sector": "AI算力"},
                }
            ],
        }
    )
    assert packet["packet_version"] == "editor-v2"
    assert packet["homepage"]["macro_lines"]
    assert packet["homepage"]["theme_lines"]


def test_build_briefing_editor_packet_surfaces_strategy_background_confidence(monkeypatch) -> None:
    class _FakeStrategyRepo:
        def summarize_background_confidence(self, symbol, lookback=8):
            assert symbol == "300502"
            assert lookback == 8
            return {
                "symbol": "300502",
                "status": "degraded",
                "label": "退化",
                "summary": "这类 setup 过去有效，但最近退化。",
                "reason": "最近 `4` 个可验证样本命中率 `25%`，成本后方向收益 `-2.5%`，稳定性开始走弱。",
            }

    monkeypatch.setattr("src.output.editor_payload.StrategyRepository", lambda: _FakeStrategyRepo())

    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-03-26 08:30:00",
            "day_theme": "AI算力",
            "regime": {"current_regime": "recovery"},
            "headline_lines": ["今天更像算力主线内部轮动，不是全面风险偏好回归。", "市场先看主线扩散。"],
            "core_event_lines": ["财联社：算力链订单继续验证，市场先交易龙头扩散。"],
            "action_lines": ["先看早段延续性，再决定是否升级风险偏好。"],
            "macro_items": ["M1-M2 剪刀差仍偏弱。"],
            "quality_lines": ["情绪代理只作辅助，不等于真实社媒抓取。"],
            "proxy_contract": {"market_flow": {"interpretation": "资金仍偏结构性，不是全面 risk-on。"}},
            "a_share_watch_candidates": [
                {
                    "name": "新易盛",
                    "symbol": "300502",
                    "asset_type": "cn_stock",
                    "dimensions": _sample_dimensions(),
                    "metadata": {"sector": "AI算力"},
                }
            ],
        }
    )
    assert packet["homepage"]["micro_lines"][0].startswith("策略后台置信度：`退化`。")
    assert packet["homepage"]["action_lines"][0].startswith("策略后台置信度只作辅助约束")
    assert "不替代今天的宏观与主题判断" in packet["homepage"]["action_lines"][0]


def test_build_briefing_editor_packet_surfaces_candidate_conflict_in_micro_lines() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-03-26 08:30:00",
            "day_theme": "政策主线",
            "regime": {"current_regime": "recovery"},
            "headline_lines": ["今天更像政策主题内部轮动，不是单一细主题扩散。"],
            "core_event_lines": ["财联社：政策线索继续发酵，但内部仍在等更清晰的收敛方向。"],
            "action_lines": ["先看主线内部先往哪条线收敛。"],
            "macro_items": ["政策预期仍偏强。"],
            "a_share_watch_candidates": [
                {
                    "name": "政策主线ETF",
                    "symbol": "560001",
                    "asset_type": "cn_etf",
                    "trade_state": "观察为主",
                    "dimensions": _sample_dimensions(),
                    "metadata": {"sector": "工业"},
                    "notes": ["一带一路、新质生产力和中特估一起走强。"],
                }
            ],
        }
    )
    assert any("还在打架" in line for line in packet["homepage"]["micro_lines"])
    assert "财联社：" in packet["homepage"]["news_lines"][0]


def test_build_briefing_editor_packet_surfaces_portfolio_overlap_in_micro_lines() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-03-26 08:30:00",
            "day_theme": "电网设备",
            "regime": {"current_regime": "recovery"},
            "headline_lines": ["今天更像电网链内部轮动，不是全市场统一 risk-on。"],
            "core_event_lines": ["财联社：电网设备订单继续验证，但内部仍在等更清晰的扩散。"],
            "action_lines": ["先看观察池里谁先补齐确认。"],
            "macro_items": ["信用环境边际改善，但还没有变成全市场共振。"],
            "a_share_watch_candidates": [
                {
                    "name": "国电南瑞",
                    "symbol": "600406",
                    "asset_type": "cn_stock",
                    "trade_state": "观察为主",
                    "dimensions": _sample_dimensions(),
                    "metadata": {"sector": "电网"},
                    "portfolio_overlap_summary": {
                        "overlap_label": "同一行业主线加码",
                        "summary_line": "这条建议和现有组合最重的行业 `电网` 同线，重复度较高，更像同一主线延伸，而不是完全新方向。",
                        "style_priority_hint": "如果只是同风格加码，优先级低于补新方向。",
                    },
                }
            ],
        }
    )
    assert any("和现有持仓的关系上，这条更像 `同一行业主线加码`" in line for line in packet["homepage"]["micro_lines"])


def test_build_briefing_editor_packet_ignores_reuse_only_candidates_in_micro_lines() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-04-03 08:30:00",
            "day_theme": "背景宏观",
            "regime": {"current_regime": "recovery"},
            "headline_lines": ["今天更像先看主线，再决定是否升级风险暴露。", "市场先看主线扩散。"],
            "action_lines": ["先等强势方向和成交确认。"],
            "a_share_watch_meta": {"pool_size": 0, "complete_analysis_size": 0},
            "a_share_watch_candidates": [
                {
                    "name": "",
                    "symbol": "",
                    "briefing_reuse_only": True,
                    "market_event_rows": [
                        ["2026-04-03", "标准行业框架：宁德时代 属于 申万二级行业·电池", "申万行业框架", "高", "电池", "", "标准行业归因", "偏利多。"]
                    ],
                }
            ],
        }
    )

    joined = "\n".join(packet["homepage"]["micro_lines"])
    assert "现在相对更值得继续跟踪的是" not in joined
    assert "()" not in joined


def test_build_briefing_editor_packet_deduplicates_bare_title_only_news_line() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-03-26 08:30:00",
            "day_theme": "黄金避险",
            "regime": {"current_regime": "deflation"},
            "headline_lines": ["**黄金避险**"],
            "core_event_lines": [
                "**黄金避险**\n  → 地缘/波动率 -> 黄金相对收益 -> 风险资产仓位收缩。\n  → 重点看黄金是否持续强于宽基和科技，而不是只看 headlines。"
            ],
            "action_lines": ["先按防守优先处理。"],
        }
    )
    assert len(packet["homepage"]["news_lines"]) == 1
    assert "地缘/波动率" in packet["homepage"]["news_lines"][0]


def test_build_briefing_editor_packet_skips_background_summary_when_collecting_news() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-03-26 08:30:00",
            "day_theme": "电网/公用事业",
            "regime": {"current_regime": "recovery"},
            "headline_lines": ["背景框架: `温和复苏`；交易主线候选: `电网/公用事业`。"],
            "core_event_lines": [
                "**电网/公用事业**\n  → 政策/确定性偏好 -> 电力电网与公用事业相对收益 -> 风格防守但不完全 risk-off。"
            ],
            "action_lines": ["先看承接，再决定是否升级风险偏好。"],
        }
    )
    assert len(packet["homepage"]["news_lines"]) == 1
    assert not any("背景框架:" in line for line in packet["homepage"]["news_lines"])


def test_build_briefing_editor_packet_news_lines_surface_intelligence_tags() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-03-30 08:30:00",
            "day_theme": "电网/公用事业",
            "regime": {"current_regime": "recovery"},
            "news_report": {
                "items": [
                    {
                        "category": "china_market_domestic",
                        "title": "国家电网发布新一轮特高压招标",
                        "source": "CNINFO",
                        "published_at": "2026-03-30 07:20:00",
                        "link": "https://example.com/grid",
                        "freshness_bucket": "fresh",
                    }
                ]
            },
            "headline_lines": [],
            "core_event_lines": [],
            "action_lines": ["先看承接，再决定是否升级风险偏好。"],
        }
    )

    assert packet["homepage"]["news_lines"]
    assert any("新鲜情报" in line for line in packet["homepage"]["news_lines"])
    assert any("一手直连" in line for line in packet["homepage"]["news_lines"])
    assert any("传导：" in line for line in packet["homepage"]["news_lines"])


def test_build_briefing_editor_packet_prefers_raw_news_lines_over_internal_validation_lead() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-03-30 08:30:00",
            "day_theme": "黄金避险",
            "regime": {"current_regime": "deflation"},
            "news_report": {
                "items": [
                    {
                        "category": "china_market_domestic",
                        "title": "上海黄金交易所提示贵金属波动加大",
                        "source": "Reuters",
                        "published_at": "2026-03-30 07:35:00",
                        "link": "https://example.com/gold-alert",
                        "freshness_bucket": "fresh",
                    },
                    {
                        "category": "china_market_domestic",
                        "title": "全球避险需求抬升，黄金盘前走强",
                        "source": "Bloomberg",
                        "published_at": "2026-03-30 07:10:00",
                        "link": "https://example.com/gold-open",
                        "freshness_bucket": "fresh",
                    },
                ]
            },
            "headline_lines": ["主线校验: 价格 ✅ / 盘面 ✅ / 跨市场 ⚠️，通过 2/2 项（跨市场待补）。"],
            "core_event_lines": [],
            "action_lines": ["先看避险情绪能否扩散。"],
        }
    )

    news_lines = list(packet["homepage"]["news_lines"] or [])
    assert news_lines
    assert "上海黄金交易所提示贵金属波动加大" in news_lines[0]
    assert not news_lines[0].startswith("事件状态")


def test_build_briefing_editor_packet_falls_back_to_market_event_rows_when_raw_news_missing() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-03-30 08:30:00",
            "day_theme": "黄金避险",
            "regime": {"current_regime": "deflation"},
            "market_event_rows": [
                ["待定", "黄金盘前走强，避险需求回升", "—", "高", "黄金/防守"],
                ["待定", "有色板块跟随升温", "—", "中", "有色/资源"],
            ],
            "headline_lines": ["主线校验: 价格 ✅ / 盘面 ✅ / 跨市场 ⚠️，通过 2/2 项（跨市场待补）。"],
            "core_event_lines": [],
            "action_lines": ["先看避险情绪能否扩散。"],
        }
    )

    news_lines = list(packet["homepage"]["news_lines"] or [])
    assert news_lines
    assert "黄金盘前走强，避险需求回升" in news_lines[0]
    assert any(item.startswith("外部情报：本轮未拿到可点击外部情报") for item in news_lines)


def test_build_briefing_editor_packet_explicitly_discloses_missing_clickable_external_news() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-04-08 23:00:29",
            "day_theme": "宽基修复",
            "regime": {"current_regime": "recovery"},
            "news_report": {"items": [], "summary_lines": [], "lines": ["实时 RSS 暂不可用"]},
            "market_event_rows": [
                ["2026-04-08", "卖方共识升温：宁德时代 本月获 5 家券商金股推荐", "卖方共识专题", "高", "宁德时代", "", "卖方共识升温", "当前更像方向验证，不直接等于全市场主线确认。"],
            ],
            "headline_lines": ["主线校验: 价格 ✅ / 盘面 ✅ / 跨市场 ⚠️，通过 2/2 项（跨市场待补）。"],
            "core_event_lines": [],
            "action_lines": ["先看宽基和金融权重能否继续扩散。"],
        }
    )

    news_lines = list(packet["homepage"]["news_lines"] or [])
    assert news_lines
    assert "卖方共识升温：宁德时代" in news_lines[0]
    assert any(item.startswith("外部情报：本轮未拿到可点击外部情报") for item in news_lines)
    assert any(item.startswith("结构证据：") for item in news_lines)


def test_build_briefing_editor_packet_merges_market_event_rows_and_theme_tracking_rows() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-03-30 08:30:00",
            "day_theme": "黄金避险",
            "regime": {"current_regime": "deflation"},
            "market_event_rows": [
                ["2026-03-30", "黄金盘前走强，避险需求回升", "CNBC", "高", "黄金/防守", "https://example.com/gold"],
            ],
            "theme_tracking_rows": [
                ["高股息/红利", "防守配套", "说明", "防守底仓", "若风险偏好快速修复，红利方向会先跑输弹性资产。", True],
            ],
            "headline_lines": ["主线校验: 价格 ✅ / 盘面 ✅ / 跨市场 ⚠️，通过 2/2 项（跨市场待补）。"],
            "core_event_lines": [],
            "action_lines": ["先看避险情绪能否扩散。"],
        }
    )

    news_lines = list(packet["homepage"]["news_lines"] or [])
    joined = "\n".join(news_lines)
    assert "[黄金盘前走强，避险需求回升](https://example.com/gold)" in joined
    assert "高股息/红利" in joined


def test_build_briefing_editor_packet_prioritizes_market_event_rows_over_raw_news() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-04-01 08:30:00",
            "day_theme": "A股主线轮动",
            "regime": {"current_regime": "recovery"},
            "news_report": {
                "items": [
                    {
                        "title": "Global bond investors reassess conflict risks",
                        "source": "Bloomberg",
                        "published_at": "2026-04-01T07:00:00",
                        "link": "https://example.com/bloomberg",
                    }
                ]
            },
            "market_event_rows": [
                ["2026-04-01", "A股概念领涨：创新药（+7.80%）", "A股概念/盘面", "高", "创新药/医药", "", "医药催化", "偏利多，先看 `创新药/医药` 能否继续拿到价格与成交确认。"],
            ],
            "headline_lines": ["主线校验: 价格 ✅ / 盘面 ✅ / 跨市场 ⚠️，通过 2/2 项（跨市场待补）。"],
            "core_event_lines": [],
            "action_lines": ["先看创新药主线能否扩散。"],
        }
    )

    news_lines = list(packet["homepage"]["news_lines"] or [])
    assert news_lines
    assert news_lines[0].startswith("外部情报：")
    assert "https://example.com/bloomberg" in news_lines[0]
    joined = "\n".join(news_lines)
    assert "A股概念领涨：创新药" in joined
    assert "信号：`医药催化`" in joined
    assert "结论：偏利多" in joined
    assert "[Global bond investors reassess conflict risks](https://example.com/bloomberg)" in joined
    assert "结论：" in joined


def test_build_briefing_editor_packet_does_not_let_encoded_urls_pollute_ai_news_impact() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-04-27 11:46:41",
            "day_theme": "硬科技 / AI硬件链",
            "regime": {"current_regime": "recovery"},
            "headline_lines": ["前排主线集中在 `创新药/医药 / AI硬件链`。"],
            "core_event_lines": [
                "**国内创新药“大单品”竞相涌现 商业化进程全面提速 - 证券时报** (证券时报)\n  → 消息面 -> 风险偏好 -> 相关资产表现。",
                "**国务院重磅部署！事关AI、算力、6G、卫星互联网等 - 财联社** (财联社)\n  → 消息面 -> 风险偏好 -> 相关资产表现。",
            ],
            "action_lines": ["先看扩散。"],
        }
    )

    ai_line = next(line for line in packet["homepage"]["news_lines"] if "国务院重磅部署" in line)

    assert "信号：`AI硬件催化`" in ai_line
    assert "主要影响：`AI硬件链`" in ai_line
    assert "主要影响：`创新药/医药`" not in ai_line


def test_build_briefing_editor_packet_keeps_ai_hardware_theme_line_as_hardware() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-04-27 11:46:41",
            "day_theme": "硬科技 / AI硬件链",
            "regime": {"current_regime": "recovery"},
            "theme_tracking_rows": [
                ["AI硬件链", "模型/产品发布 + 资本开支验证", "说明", "中线配置", "若产品催化停留在标题级，板块容易冲高回落。", True],
            ],
            "headline_lines": ["今天先看 AI硬件链。"],
            "core_event_lines": [],
            "action_lines": ["先看扩散。"],
        }
    )

    joined = "\n".join(packet["homepage"]["news_lines"])

    assert "结构证据：AI硬件链" in joined
    assert "信号类型：`AI硬件催化`" in joined
    assert "主要影响：`AI硬件链`" in joined
    assert "信号类型：`AI应用催化`" not in joined


def test_build_briefing_editor_packet_keeps_one_linked_external_news_before_structured_rows() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-04-08 23:00:29",
            "day_theme": "宽基修复",
            "regime": {"current_regime": "recovery"},
            "news_report": {
                "items": [
                    {
                        "title": "美伊接受停火提议，市场风险偏好抬升",
                        "source": "财联社",
                        "published_at": "2026-04-08 07:25:00",
                        "link": "https://example.com/ceasefire",
                        "category": "geopolitics",
                    }
                ]
            },
            "market_event_rows": [
                ["2026-04-08", "卖方共识升温：宁德时代 本月获 5 家券商金股推荐", "卖方共识专题", "高", "宁德时代", "", "卖方共识升温", "券商月度金股名单抬升。"],
                ["2026-04-08", "宽基/核心资产", "", "", "", "", "", "指数与核心资产修复。"],
            ],
        }
    )

    news_lines = list(packet["homepage"]["news_lines"] or [])

    assert news_lines
    assert news_lines[0].startswith("外部情报：")
    assert "https://example.com/ceasefire" in news_lines[0]
    assert any(line.startswith("结构证据：") for line in news_lines)


def test_build_briefing_editor_packet_prioritizes_market_wide_geopolitics_over_theme_news() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-04-08 23:00:29",
            "day_theme": "宽基修复",
            "regime": {"current_regime": "recovery"},
            "news_report": {
                "items": [
                    {
                        "title": "智谱发布技术报告，Agent 场景渗透提速",
                        "source": "财联社",
                        "published_at": "2026-04-08 12:00:00",
                        "link": "https://example.com/ai",
                        "category": "topic_search",
                    },
                    {
                        "title": "美伊停火带动全球风险偏好修复",
                        "source": "财联社",
                        "published_at": "2026-04-08 07:25:00",
                        "link": "https://example.com/ceasefire",
                        "category": "geopolitics",
                    },
                ]
            },
        }
    )

    news_lines = list(packet["homepage"]["news_lines"] or [])
    assert news_lines
    assert "https://example.com/ceasefire" in news_lines[0]


def test_build_scan_editor_packet_prioritizes_interactive_ir_over_generic_framework_rows() -> None:
    packet = build_scan_editor_packet(
        {
            "name": "药明康德",
            "symbol": "603259",
            "asset_type": "cn_stock",
            "generated_at": "2026-04-03 10:00:00",
            "day_theme": {"label": "创新药"},
            "regime": {"current_regime": "recovery"},
            "action": {
                "direction": "观察为主",
                "entry": "等右侧确认后再看",
                "stop": "跌破支撑重评",
                "horizon": {"label": "观察期", "fit_reason": "先看管理层口径和渠道节奏。"},
            },
            "narrative": {"judgment": {"state": "中期逻辑未坏"}},
            "dimensions": _sample_dimensions(),
            "market_event_rows": [
                [
                    "2026-04-03",
                    "标准行业框架：药明康德 属于 申万二级行业·化学制药（-1.20%）",
                    "申万行业框架",
                    "低",
                    "化学制药",
                    "",
                    "行业框架承压",
                    "行业指数仍在回落。",
                ],
                [
                    "2026-04-03",
                    "互动易确认：公司回复海外订单进展",
                    "互动易/投资者关系",
                    "中",
                    "药明康德",
                    "",
                    "管理层口径确认",
                    "先按补充证据处理，不替代正式公告。",
                ],
            ],
        },
        bucket="观察稿",
    )

    news_lines = list(packet["homepage"]["news_lines"] or [])
    assert news_lines
    assert any(line.startswith("结构证据：") and "管理层口径确认" in line for line in news_lines)
    assert all("标准行业框架：药明康德 属于 申万二级行业·化学制药（-1.20%）" not in line for line in news_lines)


def test_build_briefing_editor_packet_summary_uses_growth_and_geopolitical_signals() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-04-02 08:30:00",
            "day_theme": "背景宏观",
            "regime": {"current_regime": "recovery"},
            "news_report": {
                "items": [
                    {
                        "title": "智谱AI财报炸场，Token价值暴涨",
                        "source": "InfoQ 官网",
                        "published_at": "2026-04-02T02:57:39",
                        "link": "https://example.com/zhipu",
                    },
                    {
                        "title": "伊朗总统：愿在诉求满足前提下结束战争",
                        "source": "财联社",
                        "published_at": "2026-04-01T23:43:26",
                        "link": "https://example.com/ceasefire",
                    },
                ]
            },
            "market_event_rows": [
                ["2026-04-01", "A股概念领涨：创新药（+7.80%）", "A股概念/盘面", "高", "创新药/医药", "", "医药催化", "偏利多，先看 `创新药/医药` 能否继续拿到价格与成交确认。"],
            ],
        }
    )

    summary = packet["homepage"]["total_judgment"]
    assert "强修复" in summary
    assert "创新药/医药" in summary
    assert "AI应用/算力" in summary or "AI软件/应用" in summary
    assert "中东缓和" in summary


def test_build_briefing_editor_packet_filters_workflow_lines_and_deduplicates_same_news_title() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-04-02 08:30:00",
            "day_theme": "AI算力",
            "regime": {"current_regime": "recovery"},
            "news_report": {
                "items": [
                    {
                        "title": "智谱AI财报炸场，Token价值暴涨",
                        "source": "InfoQ 官网",
                        "published_at": "2026-04-02T02:57:39",
                        "link": "https://example.com/zhipu",
                    }
                ]
            },
            "core_event_lines": [
                "2026-04-02T02:57:39 · InfoQ 官网 · AI应用催化 · 强度 高 · 新鲜情报 / 搜索回退：[智谱AI财报炸场，Token价值暴涨](https://example.com/zhipu)；结论：偏利多。"
            ],
            "headline_lines": [
                "下个交易日 09:00 · 检查 watchlist 最强/最弱方向、盘前风险点和 thesis 是否需要上调优先级。：A股盘前检查（信号：`主题/市场情报`；强弱：`高`；关注 `成长股、顺周期、港股科技`）"
            ],
        }
    )

    news_lines = list(packet["homepage"]["news_lines"] or [])
    assert len(news_lines) == 1
    assert "智谱AI财报炸场" in news_lines[0]
    assert not any("检查 watchlist" in line for line in news_lines)


def test_build_briefing_editor_packet_treats_ceasefire_as_risk_on_and_skips_workflow_market_rows() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-04-02 08:30:00",
            "day_theme": "背景宏观",
            "regime": {"current_regime": "recovery"},
            "news_report": {
                "items": [
                    {
                        "title": "伊朗总统：愿在诉求满足前提下结束战争",
                        "source": "财联社",
                        "published_at": "2026-04-01T23:43:26",
                        "link": "https://example.com/ceasefire",
                        "signal_type": "地缘扰动",
                    }
                ]
            },
            "market_event_rows": [
                ["下个交易日 09:00", "A股盘前检查", "检查 watchlist 最强/最弱方向、盘前风险点和 thesis 是否需要上调优先级。", "高", "成长股、顺周期、港股科技"],
                ["2026-04-01", "观察资产走强：港股创新药ETF (513120)（1日 +7.24% / 5日 +12.04%）", "观察池/跨市场", "高", "创新药/医药", "", "医药催化", "偏利多，先看 `创新药/医药` 能否继续拿到价格与成交确认。"],
            ],
        }
    )

    news_lines = list(packet["homepage"]["news_lines"] or [])
    joined = "\n".join(news_lines)
    assert "A股盘前检查" not in joined
    assert "观察资产走强：港股创新药ETF" in joined
    assert "地缘缓和" in joined


def test_build_briefing_editor_packet_keeps_stalled_ceasefire_attack_bearish() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-04-11 08:30:00",
            "day_theme": "宽基修复",
            "regime": {"current_regime": "recovery"},
            "news_report": {
                "items": [
                    {
                        "title": "美伊停火斡旋陷入僵局 中东最大铝生产商工厂遭袭受损严重",
                        "source": "财联社",
                        "published_at": "2026-04-11T07:25:00",
                        "link": "https://example.com/stalled-ceasefire",
                        "signal_type": "地缘缓和",
                    }
                ]
            },
        }
    )

    joined = "\n".join(packet["homepage"]["news_lines"] or [])
    assert "地缘扰动" in joined
    assert "地缘缓和" not in joined


def test_build_briefing_editor_packet_skips_close_review_workflow_rows() -> None:
    packet = build_briefing_editor_packet(
        {
            "generated_at": "2026-04-02 15:10:00",
            "day_theme": "AI算力",
            "regime": {"current_regime": "recovery"},
            "news_report": {
                "items": [
                    {
                        "title": "智谱AI财报炸场，Token价值暴涨",
                        "source": "InfoQ 官网",
                        "published_at": "2026-04-02T02:57:39",
                        "link": "https://example.com/zhipu",
                    }
                ]
            },
            "market_event_rows": [
                ["15:00", "A股收盘复核", "复核日内强弱是否延续，确认次日晨报需要上调的方向。", "高", "成长股、顺周期、港股科技"],
                ["2026-04-01", "观察资产走强：港股创新药ETF (513120)（1日 +7.24% / 5日 +12.04%）", "观察池/跨市场", "高", "创新药/医药", "", "医药催化", "偏利多，先看 `创新药/医药` 能否继续拿到价格与成交确认。"],
            ],
        }
    )

    joined = "\n".join(packet["homepage"]["news_lines"] or [])
    assert "A股收盘复核" not in joined
    assert "复核日内强弱" not in joined
    assert "观察资产走强：港股创新药ETF" in joined


def test_client_report_stock_analysis_prepends_homepage_v2() -> None:
    analysis = {
        "name": "农发种业",
        "symbol": "600313",
        "asset_type": "cn_stock",
        "generated_at": "2026-03-26 10:00:00",
        "day_theme": {"label": "中国政策 / 内需确定性"},
        "regime": {"current_regime": "recovery"},
        "visuals": {},
        "rating": {"label": "无信号", "stars": "—"},
        "conclusion": "当前更适合观察。",
        "narrative": {
            "headline": "逻辑没坏，但买点还没到。",
            "positives": ["底层逻辑仍在。"],
            "cautions": ["技术确认还不够。"],
            "contradiction": "逻辑没坏，但价格和确认还没跟上。",
            "judgment": {
                "direction": "观察为主",
                "state": "持有优于追高",
                "cycle": "观察期",
                "odds": "一般",
            },
            "phase": {"label": "修复观察", "body": "更像下跌后的修复观察阶段。"},
            "watch_points": ["先看 MA20 是否重新走平。", "再看相对强弱能否改善。"],
            "drivers": {
                "macro": "中期背景不逆风，但也不是全面顺风。",
                "flow": "资金还没有形成足够强的右侧承接。",
                "relative": "相对强弱修复有限，还不能证明它重新跑赢。",
                "technical": "技术结构仍处在观察区。",
            },
            "scenarios": {
                "base": "继续观察，等待右侧确认。",
                "bull": "技术和相对强弱一起修复后，才有升级空间。",
                "bear": "如果关键位失守，会重新回到弱势整理。",
            },
            "playbook": {
                "allocation": "先按观察仓理解。",
                "trend": "等右侧确认后再跟。",
            },
            "risk_points": {
                "fundamental": "当前没有看到基本面快速恶化。",
                "valuation": "位置一般，赔率不高。",
                "crowding": "拥挤度暂时不算最高。",
                "external": "外部变量还会反复扰动节奏。",
            },
            "summary_lines": ["逻辑没坏，但还缺确认。", "今天更适合继续跟，不适合直接抬仓位。"],
        },
        "action": {
            "direction": "观察为主",
            "entry": "等 MA20 重新走平后再看",
            "stop": "跌破关键支撑重评",
            "target": "先看前高",
            "position": "暂不出手",
            "scaling_plan": "确认后再看",
            "timeframe": "观察期",
            "horizon": {"label": "观察期", "fit_reason": "当前更适合继续跟，不适合直接抬仓位。"},
        },
        "dimensions": _sample_dimensions(),
        "hard_checks": [],
        "risks": [],
        "notes": [],
    }
    rendered = ClientReportRenderer().render_stock_analysis_detailed(analysis)
    assert "## 首页判断" in rendered
    assert "### 宏观面" in rendered
    assert "### 板块 / 主题认知" in rendered


def test_client_report_etf_pick_prepends_homepage_v2() -> None:
    payload = {
        "generated_at": "2026-03-26 10:00:00",
        "day_theme": {"label": "能源冲击 + 地缘风险"},
        "regime": {"current_regime": "stagflation"},
        "selection_context": {
            "delivery_observe_only": True,
            "coverage_lines": ["结构化事件覆盖 67%（2/3）", "高置信直接新闻覆盖 0%（0/3）"],
            "coverage_note": "本轮新闻/事件覆盖基本正常。",
            "coverage_total": 3,
            "delivery_tier_label": "观察优先稿",
        },
        "winner": {
            "name": "有色金属ETF",
            "symbol": "512400",
            "asset_type": "cn_etf",
            "generated_at": "2026-03-26 10:00:00",
            "trade_state": "观察为主",
            "positives": ["方向没坏。"],
            "dimension_rows": [["技术面", "32/100", "技术结构还没确认。"]],
            "action": {
                "direction": "观察为主",
                "entry": "等回踩再看",
                "stop": "跌破支撑重评",
                "position": "暂不出手",
                "horizon": {"label": "观察期", "fit_reason": "当前更适合继续跟，不适合直接抬仓位。"},
            },
            "dimensions": _sample_dimensions(),
            "positioning_lines": [],
            "evidence": [{"title": "资源方向仍在观察名单里。", "source": "测试源"}],
            "narrative": {"playbook": {"trend": "回踩确认后再跟。"}},
            "taxonomy_rows": [["产品形态", "ETF"]],
            "taxonomy_summary": "主暴露属于资源。",
            "proxy_signals": {
                "social_sentiment": {
                    "aggregate": {
                        "interpretation": "情绪指数 61，热度偏高，需防拥挤交易。",
                        "limitations": ["这是价格和量能行为推导出的情绪代理，不是真实社媒抓取。"],
                    }
                }
            },
        },
        "alternatives": [],
        "notes": [],
    }
    rendered = ClientReportRenderer().render_etf_pick(payload)
    assert "## 首页判断" in rendered
    assert "### 宏观面" in rendered
    assert "### 板块 / 主题认知" in rendered


def test_build_scan_editor_packet_uses_thesis_first_v2_for_semiconductor() -> None:
    analysis = {
        "name": "半导体ETF",
        "symbol": "512480",
        "asset_type": "cn_etf",
        "day_theme": {"label": "AI算力"},
        "regime": {"current_regime": "stagflation"},
        "trade_state": "观察为主",
        "action": {
            "direction": "观察为主",
            "entry": "等 MA20 重新走平后再看",
            "stop": "跌破关键支撑重评",
            "horizon": {"label": "观察期", "fit_reason": "当前更适合继续跟，不适合直接抬仓位。"},
        },
        "taxonomy_summary": "主暴露属于半导体。",
        "dimensions": _sample_dimensions(),
        "proxy_signals": {
            "social_sentiment": {
                "aggregate": {
                    "interpretation": "情绪指数 61，热度偏高，需防拥挤交易。",
                    "limitations": ["这是价格和量能行为推导出的情绪代理，不是真实社媒抓取。"],
                }
            }
        },
        "evidence": [{"title": "AI服务器资本开支延续，先进封装设备链情绪回暖", "source": "财联社", "date": "2026-03-25"}],
        "narrative": {"judgment": {"state": "观察为主"}},
    }
    packet = build_scan_editor_packet(analysis, bucket="观察稿")
    assert packet["packet_version"] == "editor-v2"
    assert packet["homepage"]["version"] == "thesis-first-v2"
    assert packet["homepage"]["total_judgment"].startswith("今天先按观察稿处理")
    assert "本页重点看 `半导体ETF (512480)`" in packet["homepage"]["total_judgment"]
    assert packet["homepage"]["news_lines"]
    rendered = render_editor_homepage(packet)
    assert "## 首页判断" in rendered
    assert "### 板块 / 主题认知" in rendered
    assert "半导体" in rendered


def test_build_scan_editor_packet_softens_observe_watch_levels_on_homepage() -> None:
    analysis = {
        "name": "半导体ETF",
        "symbol": "512480",
        "asset_type": "cn_etf",
        "generated_at": "2026-04-11 09:30:00",
        "day_theme": {"label": "硬科技 / AI硬件链"},
        "regime": {"current_regime": "recovery"},
        "trade_state": "观察为主",
        "action": {
            "direction": "回避",
            "entry": "等 MACD/OBV 重新同步后再看",
            "buy_range": "0.870-0.874",
            "trim_range": "0.966-1.026",
            "stop": "跌破关键支撑重评",
        },
        "dimensions": _sample_dimensions(),
    }

    packet = build_scan_editor_packet(analysis, bucket="观察稿")
    action_lines = "\n".join(packet["homepage"]["action_lines"] or [])

    assert "关键支撑承接" in action_lines
    assert "0.870-0.874" not in action_lines
    assert "0.966-1.026" not in action_lines


def test_build_scan_editor_packet_keeps_rich_subject_snapshot() -> None:
    analysis = {
        "name": "半导体ETF",
        "symbol": "512480",
        "asset_type": "cn_etf",
        "generated_at": "2026-04-11 09:30:00",
        "day_theme": {"label": "背景宏观主导"},
        "regime": {"current_regime": "recovery"},
        "trade_state": "观察为主",
        "action": {"direction": "观察为主", "entry": "等确认", "stop": "跌破支撑"},
        "dimensions": _sample_dimensions(),
        "metadata": {"chain_nodes": ["半导体", "AI算力"]},
        "notes": ["相对强弱继续改善。"],
    }

    packet = build_scan_editor_packet(analysis, bucket="观察稿")

    assert packet["subject"]["dimensions"]["technical"]["score"] == 32
    assert packet["subject"]["day_theme"]["label"] == "背景宏观主导"
    assert packet["subject"]["metadata"]["chain_nodes"] == ["半导体", "AI算力"]


def test_build_scan_editor_packet_uses_prior_editor_payload_when_thesis_missing(monkeypatch) -> None:
    class _EmptyThesisRepo:
        def get(self, symbol):
            assert symbol == "159516"
            return {}

    monkeypatch.setattr("src.output.editor_payload.ThesisRepository", lambda: _EmptyThesisRepo())
    monkeypatch.setattr(
        "src.output.editor_payload._load_previous_editor_payload_context",
        lambda symbol, report_type="", generated_at=None: {
            "reviewed_at": "2026-04-11 23:53:35",
            "event_digest": {
                "status": "已消化",
                "lead_layer": "行业主题事件",
                "lead_detail": "主题事件：成分权重结构；结论：先按指数暴露理解。",
                "lead_title": "指数成分权重：前十权重合计 65.1%",
                "impact_summary": "景气 / 资金偏好",
                "thesis_scope": "结构基线",
            },
        },
    )

    packet = build_scan_editor_packet(
        {
            "name": "国泰中证半导体材料设备主题ETF",
            "symbol": "159516",
            "asset_type": "cn_etf",
            "generated_at": "2026-04-12 10:00:00",
            "history": _sample_history(),
            "action": {"direction": "观察为主", "entry": "等右侧确认"},
            "narrative": {"judgment": {"state": "等右侧确认"}},
            "dimensions": _sample_dimensions(),
        },
        bucket="观察为主",
    )

    assert packet["event_digest"]["previous_reviewed_at"] == "2026-04-11 23:53:35"
    assert packet["what_changed"]["conclusion_label"] != "首次跟踪"
    assert "事件边界是" in packet["what_changed"]["previous_view"]
    assert not any("首次跟踪" in line for line in packet["homepage"]["news_lines"])


def test_load_previous_editor_payload_context_prefers_prior_day_over_same_day_rerun(tmp_path, monkeypatch) -> None:
    def _write_payload(path, generated_at: str, *, lead_title: str) -> None:
        path.write_text(
            json.dumps(
                {
                    "subject": {"symbol": "159516", "generated_at": generated_at},
                    "event_digest": {"lead_title": lead_title, "status": "已消化"},
                    "what_changed": {"conclusion_label": "维持"},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    _write_payload(
        tmp_path / "scan_159516_2026-04-11_editor_payload.json",
        "2026-04-11 23:53:35",
        lead_title="前一日结构基线",
    )
    _write_payload(
        tmp_path / "scan_159516_2026-04-12_editor_payload.json",
        "2026-04-12 00:10:23",
        lead_title="同日更早重刷",
    )
    monkeypatch.setattr(
        "src.output.editor_payload._report_editor_payload_dirs",
        lambda report_type: [tmp_path],
    )

    context = _load_previous_editor_payload_context(
        "159516",
        report_type="scan",
        generated_at="2026-04-12 10:00:00",
    )

    assert context["reviewed_at"] == "2026-04-11 23:53:35"
    assert context["event_digest"]["lead_title"] == "前一日结构基线"


def test_build_scan_editor_packet_flags_lagging_relative_strength_when_catalyst_is_zero() -> None:
    dimensions = _sample_dimensions()
    dimensions["catalyst"]["score"] = 0
    dimensions["catalyst"]["summary"] = "催化不足，现在处在静态博弈。"
    dimensions["relative_strength"]["score"] = 75
    dimensions["relative_strength"]["summary"] = "相对强弱有改善，但行业宽度/龙头确认仍缺失，先按低置信代理理解。"

    packet = build_scan_editor_packet(
        {
            "name": "国泰中证半导体材料设备主题ETF",
            "symbol": "159516",
            "asset_type": "cn_etf",
            "generated_at": "2026-04-12 10:00:00",
            "history": _sample_history(),
            "action": {"direction": "观察为主", "entry": "等右侧确认"},
            "narrative": {"judgment": {"state": "等右侧确认"}},
            "dimensions": dimensions,
        },
        bucket="观察为主",
    )

    joined = "\n".join(packet["homepage"]["micro_lines"])
    assert "主线惯性" in joined or "滞后结果" in joined
    assert "别把这个高分直接读成新一轮确认" in joined


def test_build_scan_editor_packet_reconciles_index_strength_and_product_weakness() -> None:
    packet = build_scan_editor_packet(
        {
            "name": "国泰中证半导体材料设备主题ETF",
            "symbol": "159516",
            "asset_type": "cn_etf",
            "generated_at": "2026-04-12 10:00:00",
            "history": _sample_history(),
            "action": {"direction": "观察为主", "entry": "等右侧确认"},
            "narrative": {"judgment": {"state": "等右侧确认"}},
            "dimensions": _sample_dimensions(),
            "metadata": {
                "index_technical_snapshot": {"trend_label": "修复中", "momentum_label": "动能偏强"},
                "fund_factor_trend_label": "趋势偏弱",
                "fund_factor_momentum_label": "动能偏弱",
            },
        },
        bucket="观察为主",
    )

    joined = "\n".join(packet["homepage"]["micro_lines"])
    assert "赛道背景先看指数" in joined
    assert "产品层修复为准" in joined


def test_build_scan_editor_packet_marks_stale_market_snapshot_in_sentiment_and_macro() -> None:
    history = _sample_history()
    history["date"] = pd.date_range("2026-01-02", periods=len(history), freq="B")

    packet = build_scan_editor_packet(
        {
            "name": "国泰中证半导体材料设备主题ETF",
            "symbol": "159516",
            "asset_type": "cn_etf",
            "generated_at": "2026-06-30 10:00:00",
            "history": history,
            "action": {"direction": "观察为主", "entry": "等右侧确认"},
            "narrative": {"judgment": {"state": "等右侧确认"}},
            "regime": {"current_regime": "recovery"},
            "dimensions": _sample_dimensions(),
        },
        bucket="观察为主",
    )

    assert any("当前仍使用 `" in line for line in packet["homepage"]["sentiment_lines"])
    assert any("宏观月度因子这轮没有新增月频更新" in line for line in packet["homepage"]["macro_lines"])
