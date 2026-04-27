from src.output.event_digest import (
    _instrument_proxy_topic_terms,
    build_event_digest,
    event_digest_action_line,
    event_digest_homepage_lines,
    effective_intelligence_link,
    render_event_digest_section,
    sort_event_items,
    summarize_event_digest_contract,
)


def test_build_event_digest_structures_earnings_axes_and_scope() -> None:
    digest = build_event_digest(
        {
            "name": "新易盛",
            "symbol": "300502",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "龙头公告/业绩",
                            "title": "新易盛一季报：净利润同比增长 82%，上修全年利润指引",
                            "source": "公司公告",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    contract = summarize_event_digest_contract(digest)

    assert digest["status"] == "已消化"
    assert digest["lead_layer"] == "财报"
    assert digest["lead_detail"] == "财报摘要：盈利/指引上修"
    assert "盈利" in digest["impact_axes"]
    assert any(axis in digest["impact_axes"] for axis in ("估值", "景气"))
    assert digest["thesis_scope"] == "thesis变化"
    assert contract["impact_summary"].startswith("盈利")
    assert contract["thesis_scope"] == "thesis变化"
    assert "盈利" in digest["changed_what"]


def test_build_event_digest_treats_etf_top_holding_disclosure_calendar_as_earnings() -> None:
    digest = build_event_digest(
        {
            "name": "通信ETF",
            "symbol": "515880",
            "asset_type": "cn_etf",
            "generated_at": "2026-04-23 15:30:00",
            "market_event_rows": [
                [
                    "2026-04-24",
                    "核心持仓财报窗口：新易盛(300502) 预约披露 2025年年报，持仓占净值约 15.09%",
                    "核心持仓财报日历",
                    "高",
                    "新易盛",
                    "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=300502",
                    "核心持仓财报窗口",
                    "这是 ETF 前排权重股的确定披露日历，会影响持仓兑现预期和短线波动。",
                ],
                [
                    "2026-04-29",
                    "核心持仓财报窗口：工业富联(601138) 预约披露 2026年一季报，持仓占净值约 10.48%",
                    "核心持仓财报日历",
                    "高",
                    "工业富联",
                    "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=601138",
                    "核心持仓财报窗口",
                    "这是 ETF 前排权重股的确定披露日历，会影响持仓兑现预期和短线波动。",
                ],
            ],
        }
    )

    assert digest["lead_layer"] == "财报"
    assert digest["lead_detail"] == "财报日历：待披露窗口"
    assert "新易盛" in digest["lead_title"]
    assert digest["latest_signal_at"] == "2026-04-24"
    assert "盈利" in digest["impact_axes"]
    assert digest["thesis_scope"] == "待确认"
    assert "不是财报结果" in digest["importance_reason"]


def test_instrument_proxy_topic_terms_include_taxonomy_profile_evidence_terms() -> None:
    terms = _instrument_proxy_topic_terms(
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
        }
    )

    assert "光模块" in terms
    assert "CPO" in terms
    assert "800G" in terms


def test_build_event_digest_recognizes_margin_improvement_as_direct_financial_change() -> None:
    digest = build_event_digest(
        {
            "name": "利润率样本",
            "symbol": "300001",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "龙头公告/业绩",
                            "title": "公司一季报：毛利率提升 6 个点，费用率明显下降",
                            "source": "公司公告",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "财报"
    assert digest["lead_detail"] == "财报摘要：毛利率/费用率改善"
    assert digest["importance"] == "high"
    assert digest["impact_summary"] == "盈利 / 估值"
    assert digest["thesis_scope"] == "thesis变化"
    assert "利润率改善和经营杠杆层" in digest["changed_what"]
    assert "费用纪律" in digest["next_step"]
    assert "利润率和费用率改善" in digest["importance_reason"]


def test_build_event_digest_treats_cashflow_improvement_as_pending_confirmation() -> None:
    digest = build_event_digest(
        {
            "name": "现金流样本",
            "symbol": "300002",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "龙头公告/业绩",
                            "title": "公司中报：经营现金流转正，合同负债与预收款同步提升",
                            "source": "公司公告",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "财报"
    assert digest["lead_detail"] == "财报摘要：现金流/合同负债改善"
    assert digest["importance"] == "medium"
    assert digest["impact_summary"] == "盈利 / 景气"
    assert digest["thesis_scope"] == "待确认"
    assert "回款质量、订单前瞻和兑现质量层" in digest["changed_what"]
    assert "回款持续性" in digest["next_step"]
    assert "前瞻验证" in digest["importance_reason"]


def test_build_event_digest_recognizes_inventory_impairment_pressure() -> None:
    digest = build_event_digest(
        {
            "name": "减值样本",
            "symbol": "300003",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "龙头公告/业绩",
                            "title": "公司年报：计提大额资产减值，存货跌价准备明显提升",
                            "source": "公司公告",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "财报"
    assert digest["lead_detail"] == "财报摘要：存货/减值压力"
    assert digest["importance"] == "high"
    assert digest["impact_summary"] == "盈利 / 景气"
    assert digest["thesis_scope"] == "thesis变化"
    assert "库存去化、资产质量和需求承压层" in digest["changed_what"]
    assert "去库节奏" in digest["next_step"]
    assert "库存和减值压力" in digest["importance_reason"]


def test_sort_event_items_prioritizes_direct_policy_execution_over_theme_heat() -> None:
    ranked = sort_event_items(
        [
            {
                "layer": "主题级关键新闻",
                "title": "AI 算力链热度继续扩散，板块情绪升温",
                "source": "财联社",
                "date": "2026-03-29",
            },
            {
                "layer": "政策催化",
                "title": "财政部下达设备更新补贴资金，首批名单同步公布",
                "source": "财政部",
                "date": "2026-03-29",
            },
        ],
        as_of="2026-03-29 10:00:00",
    )

    assert ranked[0]["layer"] == "政策"
    assert ranked[0]["lead_detail"] == "政策影响层：财政支持/名单落地"
    assert ranked[0]["thesis_scope"] == "thesis变化"


def test_sort_event_items_prioritizes_margin_improvement_over_cashflow_quality() -> None:
    ranked = sort_event_items(
        [
            {
                "layer": "龙头公告/业绩",
                "title": "公司中报：经营现金流转正，合同负债与预收款同步提升",
                "source": "公司公告",
                "date": "2026-03-28",
            },
            {
                "layer": "龙头公告/业绩",
                "title": "公司一季报：毛利率提升 6 个点，费用率明显下降",
                "source": "公司公告",
                "date": "2026-03-28",
            },
        ],
        as_of="2026-03-29 10:00:00",
    )

    assert ranked[0]["lead_detail"] == "财报摘要：毛利率/费用率改善"
    assert ranked[0]["importance"] == "high"
    assert ranked[1]["lead_detail"] == "财报摘要：现金流/合同负债改善"
    assert ranked[1]["importance"] == "medium"


def test_build_event_digest_recognizes_theme_heat_as_observation_only() -> None:
    digest = build_event_digest(
        {
            "name": "AI算力链",
            "symbol": "theme",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "theme_news": [
                        {
                            "layer": "主题级关键新闻",
                            "title": "AI 算力链热度继续扩散，板块情绪升温，讨论度快速发酵",
                            "source": "财联社",
                            "date": "2026-03-29",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "行业主题事件"
    assert digest["lead_detail"] == "主题事件：情绪热度"
    assert digest["importance"] == "medium"
    assert digest["impact_summary"] == "资金偏好"
    assert digest["thesis_scope"] == "待确认"
    assert "市场关注度和资金偏好层" in digest["changed_what"]
    assert "热度是否转成景气" in digest["next_step"]
    assert "抬升 `资金偏好`" in digest["importance_reason"]


def test_build_event_digest_keeps_directional_etf_peer_news_as_theme_event() -> None:
    digest = build_event_digest(
        {
            "name": "国泰中证半导体材料设备主题ETF",
            "symbol": "159516",
            "generated_at": "2026-04-11 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "产品/跟踪方向催化",
                            "title": "半导体设备ETF易方达（159558）盘中获1800万份净申购，半导体设备龙头业绩炸裂",
                            "source": "财联社",
                            "date": "2026-04-10",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "行业主题事件"
    assert digest["status"] == "已消化"
    assert digest["thesis_scope"] == "待确认"


def test_build_event_digest_filters_etf_proxy_news_without_direct_coverage() -> None:
    payload = {
        "asset_type": "cn_etf",
        "name": "国泰中证半导体材料设备主题ETF",
        "symbol": "159516",
        "generated_at": "2026-04-11 10:00:00",
        "metadata": {
            "tracked_index_name": "中证半导体材料设备主题指数",
            "index_framework_label": "中证半导体材料设备主题指数",
        },
        "dimensions": {
            "catalyst": {
                "coverage": {
                    "directional_catalyst_hit": False,
                    "direct_news_count": 0,
                },
                "theme_news": [
                    {
                        "layer": "主题级关键新闻",
                        "title": "北方华创股价连续上涨，嘉实基金旗下11只基金重仓持有",
                        "source": "财联社",
                        "date": "2026-04-10",
                    }
                ],
            }
        },
        "news_report": {
            "items": [
                {
                    "title": "半导体设备ETF易方达（159558）盘中获1800万份净申购，半导体设备龙头业绩炸裂",
                    "source": "财联社",
                    "published_at": "2026-04-10T11:20:00",
                },
                {
                    "title": "【早报】消费领域多个利好来袭，多家A股公司被证监会立案",
                    "source": "财联社",
                    "published_at": "2026-04-10T08:00:00",
                },
            ]
        },
        "market_event_rows": [
            [
                "2026-04-11",
                "跟踪指数/行业框架：中证半导体材料设备主题指数对应申万二级行业半导体（+1.96%）",
                "行业/指数框架",
                "中",
                "景气 / 资金偏好",
                "",
                "行业框架确认",
                "只作为跟踪指数背景，不升级成产品直连催化。",
            ]
        ],
    }

    digest = build_event_digest(payload)
    homepage_lines = event_digest_homepage_lines(digest)
    item_titles = [str(item.get("title") or "") for item in list(digest.get("items") or [])]
    combined = "\n".join([digest.get("lead_title", ""), *homepage_lines, *item_titles])

    assert digest["lead_title"].startswith("跟踪指数/行业框架")
    assert "159558" not in combined
    assert "北方华创股价连续上涨" not in combined
    assert "消费领域多个利好" not in combined
    assert "财报摘要" not in combined


def test_build_event_digest_recognizes_overseas_mapping_as_pending_confirmation() -> None:
    digest = build_event_digest(
        {
            "name": "苹果链",
            "symbol": "theme",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "theme_news": [
                        {
                            "layer": "海外映射",
                            "title": "英伟达新品发布带动光模块链映射，A股相关产业链同步走强",
                            "source": "财联社",
                            "date": "2026-03-29",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "行业主题事件"
    assert digest["lead_detail"] == "主题事件：海外映射/链式催化"
    assert digest["importance"] == "medium"
    assert digest["impact_summary"] == "景气 / 资金偏好"
    assert digest["thesis_scope"] == "待确认"
    assert "海外事件外溢和本土映射层" in digest["changed_what"]
    assert "国内链条直连证据" in digest["next_step"]
    assert "国内链条直连证据" in digest["importance_reason"]


def test_build_event_digest_recognizes_theme_price_validation_as_thesis_change() -> None:
    digest = build_event_digest(
        {
            "name": "锂电链",
            "symbol": "theme",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "theme_news": [
                        {
                            "layer": "主题级关键新闻",
                            "title": "锂电材料涨价与排产同步上修，产业链出货延续强势",
                            "source": "上海证券报",
                            "date": "2026-03-29",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "行业主题事件"
    assert digest["lead_detail"] == "主题事件：价格/排产验证"
    assert digest["importance"] == "high"
    assert digest["impact_summary"] == "盈利 / 景气"
    assert digest["thesis_scope"] == "thesis变化"
    assert "供需、价格和排产验证层" in digest["changed_what"]
    assert "涨价传导" in digest["next_step"]
    assert "主题景气和价格验证" in digest["importance_reason"]


def test_build_event_digest_consumes_briefing_market_event_rows_before_generic_news() -> None:
    digest = build_event_digest(
        {
            "name": "今日晨报",
            "symbol": "briefing",
            "generated_at": "2026-04-01 10:00:00",
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
                ["2026-04-01", "A股热股前排：新易盛（+12.50%）", "A股热度/个股", "高", "AI算力/光模块", "", "海外科技映射", "偏利多，先看 `AI算力/光模块` 能否继续拿到价格与成交确认。"],
            ],
        }
    )

    assert digest["lead_title"] == "A股热股前排：新易盛（+12.50%）"
    assert digest["lead_detail"] == "主题事件：海外科技映射；结论：偏利多，先看 `AI算力/光模块` 能否继续拿到价格与成交确认。"


def test_build_event_digest_treats_ceasefire_risk_repair_as_non_negative() -> None:
    digest = build_event_digest(
        {
            "name": "A股市场",
            "symbol": "market",
            "generated_at": "2026-04-11 02:00:00",
            "headline_lines": [
                "**美伊停火利好A股_新浪新闻 - 手机新浪网** (手机新浪网)\n"
                "  → 消息面 -> 风险偏好 -> 相关资产表现。\n"
                "  → 先把它当成新增情报线索，再结合盘面和后文验证点确认。"
            ],
        }
    )

    assert digest["lead_detail"] == "主题事件：地缘缓和/风险偏好修复"
    assert digest["thesis_scope"] == "待确认"
    assert digest["signal_conclusion"].startswith("中性偏多")


def test_build_event_digest_does_not_mark_risk_appetite_repair_headline_as_bearish() -> None:
    digest = build_event_digest(
        {
            "name": "A股市场",
            "symbol": "market",
            "generated_at": "2026-04-11 02:00:00",
            "headline_lines": [
                "**A股放量大涨，机构眼中的后市机会在哪？ - 新浪财经** (新浪财经)\n"
                "  → 消息面 -> 风险偏好 -> 相关资产表现。\n"
                "  → 先把它当成新增情报线索，再结合盘面和后文验证点确认。"
            ],
        }
    )

    assert digest["lead_detail"] == "主题事件：市场修复/风险偏好回暖"
    assert "偏利空" not in digest["signal_conclusion"]


def test_build_event_digest_does_not_treat_stalled_ceasefire_attack_as_risk_on() -> None:
    digest = build_event_digest(
        {
            "name": "A股市场",
            "symbol": "market",
            "generated_at": "2026-04-11 02:00:00",
            "headline_lines": [
                "**美伊停火斡旋陷入僵局 中东最大铝生产商工厂遭袭受损严重** (财联社)\n"
                "  → 消息面 -> 风险偏好 -> 相关资产表现。"
            ],
        }
    )

    assert digest["lead_detail"] == "主题事件：地缘扰动/风险偏好承压"
    assert digest["signal_conclusion"].startswith("偏利空")


def test_build_event_digest_skips_workflow_market_event_rows_and_headline_text() -> None:
    digest = build_event_digest(
        {
            "name": "今日晨报",
            "symbol": "briefing",
            "generated_at": "2026-04-02 15:10:00",
            "market_event_rows": [
                ["15:00", "A股收盘复核", "复核日内强弱是否延续，确认次日晨报需要上调的方向。", "高", "成长股、顺周期、港股科技"],
                ["2026-04-01", "观察资产走强：港股创新药ETF (513120)（1日 +7.24% / 5日 +12.04%）", "观察池/跨市场", "高", "创新药/医药", "", "医药催化", "偏利多，先看 `创新药/医药` 能否继续拿到价格与成交确认。"],
            ],
            "headline_lines": [
                "下个交易日 09:00 · 检查 watchlist 最强/最弱方向、盘前风险点和 thesis 是否需要上调优先级。：A股盘前检查（信号：`主题/市场情报`；强弱：`高`）"
            ],
        }
    )

    assert digest["lead_title"].startswith("观察资产走强：港股创新药ETF")
    assert "A股收盘复核" not in "\n".join(render_event_digest_section(digest))


def test_build_event_digest_ignores_empty_intelligence_placeholder_when_market_event_rows_exist() -> None:
    digest = build_event_digest(
        {
            "name": "有色金属ETF",
            "symbol": "512400",
            "generated_at": "2026-04-01 21:47:47",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "title": "新增情报 0 条",
                            "source": "研报/新闻密度",
                            "lead_detail": "信息环境：新闻/舆情脉冲",
                        }
                    ]
                }
            },
            "market_event_rows": [
                ["2026-04-01", "A股行业走强：有色（+2.65%）；领涨 紫金矿业", "A股行业/盘面", "高", "有色", "", "主线增强", "偏利多，先看 `有色` 能否继续扩散。"],
            ],
        }
    )

    assert digest["lead_title"] == "A股行业走强：有色（+2.65%）；领涨 紫金矿业"
    assert digest["lead_detail"] == "主题事件：主线增强；结论：偏利多，先看 `有色` 能否继续扩散。"


def test_sort_event_items_prioritizes_price_validation_over_theme_heat() -> None:
    ranked = sort_event_items(
        [
            {
                "layer": "主题级关键新闻",
                "title": "AI 算力链热度继续扩散，板块情绪升温，讨论度快速发酵",
                "source": "财联社",
                "date": "2026-03-29",
            },
            {
                "layer": "主题级关键新闻",
                "title": "锂电材料涨价与排产同步上修，产业链出货延续强势",
                "source": "上海证券报",
                "date": "2026-03-29",
            },
        ],
        as_of="2026-03-29 10:00:00",
    )

    assert ranked[0]["lead_detail"] == "主题事件：价格/排产验证"
    assert ranked[0]["importance"] == "high"
    assert ranked[0]["thesis_scope"] == "thesis变化"
    assert ranked[1]["lead_detail"] == "主题事件：情绪热度"
    assert ranked[1]["importance"] == "medium"


def test_render_event_digest_section_and_action_line_surface_deep_fields() -> None:
    digest = build_event_digest(
        {
            "name": "国电南瑞",
            "symbol": "600406",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "国电南瑞公告：中标国家电网特高压订单",
                            "source": "公司公告",
                            "date": "2026-03-28",
                        },
                        {
                            "layer": "公告",
                            "title": "国电南瑞投资者关系活动记录表更新订单节奏",
                            "source": "CNINFO",
                            "configured_source": "CNINFO::direct",
                            "source_note": "official_direct",
                            "date": "2026-03-29",
                        }
                    ]
                }
            },
        }
    )

    lines = render_event_digest_section(digest)
    action_line = event_digest_action_line(digest, observe_only=True)

    assert lines.count("") >= 3
    assert any(line.startswith("- 信号判断：") for line in lines)
    assert any("事件细分：`公告类型：中标/订单`" in line for line in lines)
    assert any("信号类型：`公告类型：中标/订单`" in line for line in lines)
    assert any("结论：偏利多" in line for line in lines)
    assert any("情报属性：`首次跟踪 / 新鲜情报 / 一手直连 / 结构化披露`" in line for line in lines)
    assert any("来源层级：`官方直连 / 结构化披露`" in line for line in lines)
    assert any("最新情报时点" in line and "2026-03-29" in line for line in lines)
    assert any("影响层与性质" in line and "盈利" in line and "thesis变化" in line for line in lines)
    assert any("优先级判断" in line and "公司级执行事件" in line for line in lines)
    assert any(line.startswith("- 相关情报补充：") for line in lines)
    assert any(line.startswith("  信号类型：") for line in lines)
    assert any(line.startswith("  来源：") for line in lines)
    assert "改写 `" in action_line and "盈利" in action_line
    assert "动作上先等" in action_line or "动作上先按" in action_line


def test_build_event_digest_exposes_signal_fields_and_homepage_conclusion_for_non_briefing_items() -> None:
    digest = build_event_digest(
        {
            "name": "国电南瑞",
            "symbol": "600406",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "国电南瑞公告：中标国家电网特高压订单",
                            "source": "公司公告",
                            "date": "2026-03-28",
                        },
                        {
                            "layer": "主题级关键新闻",
                            "title": "特高压投资节奏提速，电网设备景气延续",
                            "source": "证券时报",
                            "date": "2026-03-29",
                        },
                    ]
                }
            },
        }
    )

    lines = event_digest_homepage_lines(digest)

    assert digest["signal_type"] == "公告类型：中标/订单"
    assert digest["signal_strength"] == "强"
    assert "偏利多" in digest["signal_conclusion"]
    assert any("信号：`公告类型：中标/订单`" in line for line in lines)
    assert any("结论：偏利多" in line for line in lines)


def test_build_event_digest_recognizes_product_launch_as_product_announcement() -> None:
    digest = build_event_digest(
        {
            "name": "新易盛",
            "symbol": "300502",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "新易盛公告：800G 光模块新品发布",
                            "source": "证券时报",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "公告"
    assert digest["lead_detail"] == "公告类型：产品/新品"
    assert digest["importance"] == "medium"
    assert digest["impact_summary"] == "景气 / 资金偏好"
    assert digest["thesis_scope"] == "待确认"
    assert "产品验证" in digest["changed_what"]
    assert "新品验证" in digest["next_step"]


def test_build_event_digest_treats_policy_rule_detail_as_pending_confirmation() -> None:
    digest = build_event_digest(
        {
            "name": "电网ETF",
            "symbol": "561380",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "政策催化",
                            "title": "国家能源局发布电网投资配套细则，明确重点项目口径",
                            "source": "国家能源局",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "政策"
    assert digest["lead_detail"] == "政策影响层：配套细则"
    assert digest["importance"] == "medium"
    assert digest["thesis_scope"] == "待确认"
    assert "执行框架层" in digest["changed_what"]
    assert "覆盖范围" in digest["next_step"]


def test_build_event_digest_treats_price_mechanism_policy_as_pending_confirmation() -> None:
    digest = build_event_digest(
        {
            "name": "储能样本",
            "symbol": "300888",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "政策催化",
                            "title": "发改委明确容量电价机制与储能收益口径",
                            "source": "发改委",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "政策"
    assert digest["lead_detail"] == "政策影响层：价格机制/收费调整"
    assert digest["importance"] == "medium"
    assert digest["impact_summary"] == "盈利 / 估值"
    assert digest["thesis_scope"] == "待确认"
    assert "价格传导" in digest["changed_what"]
    assert "执行口径" in digest["next_step"]
    assert "传导滞后" in digest["importance_reason"]


def test_sort_event_items_prioritizes_fiscal_support_over_price_mechanism() -> None:
    ranked = sort_event_items(
        [
            {
                "layer": "政策催化",
                "title": "发改委明确容量电价机制与储能收益口径",
                "source": "发改委",
                "date": "2026-03-28",
            },
            {
                "layer": "政策催化",
                "title": "财政部下达设备更新专项资金，首批目录名单同步公布",
                "source": "财政部",
                "date": "2026-03-29",
            },
        ],
        as_of="2026-03-29 10:00:00",
    )

    assert ranked[0]["lead_detail"] == "政策影响层：财政支持/名单落地"
    assert ranked[0]["importance_score"] > ranked[1]["importance_score"]
    assert ranked[1]["lead_detail"] == "政策影响层：价格机制/收费调整"
    assert ranked[1]["thesis_scope"] == "待确认"


def test_build_event_digest_treats_buyback_style_earnings_as_pending_confirmation() -> None:
    digest = build_event_digest(
        {
            "name": "红利样本",
            "symbol": "600000",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "龙头公告/业绩",
                            "title": "公司年报：拟回购股份并提升分红比例",
                            "source": "公司公告",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "财报"
    assert digest["lead_detail"] == "财报摘要：回购/分红"
    assert digest["importance"] == "medium"
    assert digest["impact_summary"] == "估值 / 资金偏好"
    assert digest["thesis_scope"] == "待确认"
    assert "放到前排跟踪" in digest["importance_reason"]
    assert "股东回报和估值支撑层" in digest["changed_what"]
    assert "回购执行" in digest["next_step"]


def test_build_event_digest_treats_capex_style_earnings_as_pending_confirmation() -> None:
    digest = build_event_digest(
        {
            "name": "扩产样本",
            "symbol": "300001",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "龙头公告/业绩",
                            "title": "公司年报：资本开支大幅提升并启动新产能扩建",
                            "source": "公司公告",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "财报"
    assert digest["lead_detail"] == "财报摘要：资本开支/扩产"
    assert digest["importance"] == "medium"
    assert digest["impact_summary"] == "盈利 / 景气"
    assert digest["thesis_scope"] == "待确认"
    assert "未来兑现" in digest["importance_reason"]
    assert "产能投入和未来兑现层" in digest["changed_what"]
    assert "扩产节奏" in digest["next_step"]


def test_build_event_digest_filters_soft_diagnostic_factor_rows() -> None:
    digest = build_event_digest(
        {
            "name": "农发种业",
            "symbol": "600313",
            "generated_at": "2026-04-01 10:00:00",
            "dimensions": {
                "catalyst": {
                    "factors": [
                        {
                            "name": "前瞻催化",
                            "signal": "未来 14 日前瞻催化窗口暂不突出",
                            "detail": "情报覆盖偏窄",
                        },
                        {
                            "name": "结构化事件",
                            "signal": "农发种业 披露现金分红预案",
                            "detail": "董事会预案，现金分红 0.10 元",
                        },
                    ]
                }
            },
        }
    )

    assert digest["lead_title"] == "农发种业 披露现金分红预案"
    assert "前瞻催化窗口暂不突出" not in digest["lead_title"]
    section = "\n".join(render_event_digest_section(digest))
    assert "前瞻催化窗口暂不突出" not in section
    assert "情报覆盖偏窄" not in section


def test_build_event_digest_softens_old_news_lead_to_history_baseline() -> None:
    digest = build_event_digest(
        {
            "name": "农发种业",
            "symbol": "600313",
            "generated_at": "2026-04-01 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "农发种业披露现金分红预案",
                            "source": "公司公告",
                            "date": "2025-08-23",
                        }
                    ]
                }
            },
        }
    )

    assert "旧闻回放" in digest["intelligence_attributes"]
    assert digest["thesis_scope"] == "历史基线"
    assert "历史基线" in digest["changed_what"]
    assert "不直接当成本轮新增催化" in digest["changed_what"]
    assert "先把它当历史基线看" in digest["importance_reason"]
    assert "继续找新增公司/产品级直连情报和价格确认" in digest["next_step"]


def test_build_event_digest_history_baseline_overrides_explicit_positive_signal() -> None:
    digest = build_event_digest(
        {
            "name": "农发种业",
            "symbol": "600313",
            "generated_at": "2026-04-02 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "农发种业披露现金分红预案",
                            "source": "公司公告",
                            "date": "2025-08-23",
                            "signal_strength": "强",
                            "signal_conclusion": "偏利多，现金回报预期改善。",
                        }
                    ]
                }
            },
        }
    )

    homepage_lines = event_digest_homepage_lines(digest, [])

    assert digest["thesis_scope"] == "历史基线"
    assert digest["signal_strength"] == "中"
    assert digest["signal_conclusion"] == "中性，当前更多是历史基线，不把它直接当成新增催化。"
    assert any("当前先放在背景参考位的是" in line for line in homepage_lines)


def test_build_event_digest_treats_financing_announcement_as_pending_confirmation() -> None:
    digest = build_event_digest(
        {
            "name": "融资样本",
            "symbol": "300888",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "公司公告：拟定增募资并发行可转债",
                            "source": "公司公告",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "公告"
    assert digest["lead_detail"] == "公告类型：融资/定增"
    assert digest["importance"] == "medium"
    assert digest["impact_summary"] == "估值 / 资金偏好"
    assert digest["thesis_scope"] == "待确认"
    assert "资本结构" in digest["changed_what"]
    assert "发行条款" in digest["next_step"]
    assert "摊薄影响" in digest["importance_reason"]


def test_build_event_digest_treats_restructuring_announcement_as_pending_confirmation() -> None:
    digest = build_event_digest(
        {
            "name": "重组样本",
            "symbol": "600999",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "公司公告：拟收购上游资产并进行重大资产重组",
                            "source": "公司公告",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "公告"
    assert digest["lead_detail"] == "公告类型：并购/重组"
    assert digest["importance"] == "high"
    assert digest["impact_summary"] == "盈利 / 估值"
    assert digest["thesis_scope"] == "待确认"
    assert "外延整合" in digest["changed_what"]
    assert "审批进度" in digest["next_step"]
    assert "交易对价" in digest["importance_reason"]


def test_build_event_digest_treats_share_overhang_announcement_as_pending_confirmation() -> None:
    digest = build_event_digest(
        {
            "name": "减持样本",
            "symbol": "600998",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "控股股东公告：减持计划披露，解禁股份将陆续上市流通",
                            "source": "公司公告",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "公告"
    assert digest["lead_detail"] == "公告类型：减持/解禁"
    assert digest["importance"] == "high"
    assert digest["impact_summary"] == "估值 / 资金偏好"
    assert digest["thesis_scope"] == "待确认"
    assert "筹码供给" in digest["changed_what"]
    assert "减持规模" in digest["next_step"]
    assert "筹码压力" in digest["importance_reason"]


def test_sort_event_items_prioritizes_direct_order_over_product_launch() -> None:
    ranked = sort_event_items(
        [
            {
                "layer": "结构化事件",
                "title": "新易盛公告：800G 光模块新品发布",
                "source": "证券时报",
                "date": "2026-03-28",
            },
            {
                "layer": "结构化事件",
                "title": "国电南瑞公告：中标国家电网特高压订单",
                "source": "公司公告",
                "date": "2026-03-28",
            },
        ],
        as_of="2026-03-29 10:00:00",
    )

    assert ranked[0]["lead_detail"] == "公告类型：中标/订单"
    assert ranked[0]["importance"] == "high"
    assert ranked[1]["lead_detail"] == "公告类型：产品/新品"
    assert ranked[1]["importance"] == "medium"


def test_sort_event_items_prioritizes_direct_order_over_restructuring() -> None:
    ranked = sort_event_items(
        [
            {
                "layer": "结构化事件",
                "title": "公司公告：拟收购上游资产并进行重大资产重组",
                "source": "公司公告",
                "date": "2026-03-28",
            },
            {
                "layer": "结构化事件",
                "title": "国电南瑞公告：中标国家电网特高压订单",
                "source": "公司公告",
                "date": "2026-03-28",
            },
        ],
        as_of="2026-03-29 10:00:00",
    )

    assert ranked[0]["lead_detail"] == "公告类型：中标/订单"
    assert ranked[1]["lead_detail"] == "公告类型：并购/重组"
    assert ranked[1]["thesis_scope"] == "待确认"


def test_sort_event_items_prioritizes_first_party_fresh_intelligence_over_stale_media() -> None:
    ranked = sort_event_items(
        [
            {
                "layer": "主题级关键新闻",
                "title": "光模块链景气继续改善，市场情绪再度升温",
                "source": "财联社",
                "date": "2026-03-24",
                "freshness_bucket": "stale",
                "age_days": 6,
            },
            {
                "layer": "龙头公告/业绩",
                "title": "公司公告：中标海外数据中心项目，订单金额超预期",
                "source": "巨潮资讯",
                "date": "2026-03-29",
                "freshness_bucket": "fresh",
                "age_days": 1,
            },
        ],
        as_of="2026-03-30 10:00:00",
    )

    assert ranked[0]["source"] == "巨潮资讯"
    assert ranked[0]["source_directness_rank"] == 2
    assert ranked[0]["freshness_rank"] == 2


def test_sort_event_items_prioritizes_fresher_media_over_stale_first_party_with_same_layer() -> None:
    ranked = sort_event_items(
        [
            {
                "layer": "主题级关键新闻",
                "title": "行业主题旧闻回放：讨论热度一度升温",
                "source": "CNINFO",
                "date": "2026-03-05",
                "freshness_bucket": "stale",
                "age_days": 25,
            },
            {
                "layer": "主题级关键新闻",
                "title": "板块资金热度继续升温，龙头活跃度上行",
                "source": "财联社",
                "date": "2026-03-29",
                "freshness_bucket": "fresh",
                "age_days": 1,
            },
        ],
        as_of="2026-03-30 10:00:00",
    )

    assert ranked[0]["source"] == "财联社"
    assert ranked[0]["freshness_rank"] == 2
    assert ranked[1]["source"] == "CNINFO"


def test_sort_event_items_prioritizes_interactive_ir_over_generic_framework_rows() -> None:
    ranked = sort_event_items(
        [
            {
                "layer": "行业主题事件",
                "title": "标准行业框架：药明康德 属于 申万二级行业·化学制药（-1.20%）",
                "source": "申万行业框架",
                "date": "2026-04-03",
                "freshness_bucket": "fresh",
                "age_days": 0,
            },
            {
                "layer": "行业主题事件",
                "title": "互动易确认：公司回复海外订单进展",
                "source": "互动易/投资者关系",
                "date": "2026-04-03",
                "freshness_bucket": "fresh",
                "age_days": 0,
            },
        ],
        as_of="2026-04-03 10:00:00",
    )

    assert ranked[0]["title"] == "互动易确认：公司回复海外订单进展"
    assert ranked[0]["source_directness_rank"] == 2
    assert ranked[0]["importance_score"] >= ranked[1]["importance_score"]


def test_build_event_digest_prefers_fresh_first_party_lead_item_over_theme_media() -> None:
    digest = build_event_digest(
        {
            "generated_at": "2026-03-30 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "主题级关键新闻",
                            "title": "板块热度继续发酵",
                            "source": "财联社",
                            "date": "2026-03-24",
                            "freshness_bucket": "stale",
                            "age_days": 6,
                        },
                        {
                            "layer": "公告",
                            "title": "公司公告：签下国家电网订单",
                            "source": "CNINFO",
                            "date": "2026-03-29",
                            "freshness_bucket": "fresh",
                            "age_days": 1,
                        },
                    ],
                }
            },
        }
    )

    assert digest["lead_title"].startswith("公司公告：签下国家电网订单")
    assert digest["lead_layer"] == "公告"
    assert digest["lead_detail"] == "公告类型：中标/订单"


def test_sort_event_items_prioritizes_fresh_first_party_over_stale_search_fallback() -> None:
    ranked = sort_event_items(
        [
            {
                "layer": "主题级关键新闻",
                "title": "板块旧闻回放：讨论热度继续被提及",
                "source": "财联社",
                "configured_source": "topic_search",
                "category": "topic_search",
                "date": "2026-03-20",
                "freshness_bucket": "stale",
                "age_days": 10,
            },
            {
                "layer": "结构化事件",
                "title": "公司公告：签下国家电网订单",
                "source": "CNINFO",
                "configured_source": "CNINFO",
                "category": "structured_disclosure",
                "date": "2026-03-29",
                "freshness_bucket": "fresh",
                "age_days": 1,
            },
        ],
        as_of="2026-03-30 10:00:00",
    )

    assert ranked[0]["title"] == "公司公告：签下国家电网订单"
    assert ranked[0]["source_tier_rank"] == 3
    assert ranked[0]["freshness_rank"] == 2
    assert ranked[1]["title"].startswith("板块旧闻回放")
    assert ranked[1]["source_tier_rank"] == 0


def test_build_event_digest_filters_stock_news_from_unrelated_market_theme() -> None:
    digest = build_event_digest(
        {
            "asset_type": "cn_stock",
            "name": "紫金矿业",
            "symbol": "601899",
            "generated_at": "2026-04-23 14:26:09",
            "day_theme": {"label": "硬科技 / AI硬件链"},
            "metadata": {
                "sector": "有色",
                "industry": "铜",
                "chain_nodes": ["工业金属", "铜"],
            },
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "前瞻催化",
                            "title": "紫金矿业 已于 2026-04-22 披露 2026年一季报",
                            "source": "Tushare disclosure_date",
                            "date": "2026-04-22",
                        },
                        {
                            "title": "国务院重磅部署！事关AI、算力、6G、卫星互联网等 - 财联社",
                            "source": "财联社",
                            "published_at": "2026-04-21T09:09:05",
                            "category": "ai",
                            "link": "https://example.com/ai-policy",
                        },
                    ]
                }
            },
            "news_report": {
                "items": [
                    {
                        "title": "国内创新药“大单品”竞相涌现 商业化进程全面提速 - 证券时报",
                        "source": "证券时报",
                        "published_at": "2026-04-21T22:56:00",
                        "category": "biotech",
                        "link": "https://example.com/biotech",
                    }
                ]
            },
        },
        theme_playbook={"label": "黄金 / 有色资源"},
    )
    joined = "\n".join(
        [
            digest.get("lead_title", ""),
            *event_digest_homepage_lines(digest, []),
            *render_event_digest_section(digest),
            *[str(item.get("title", "")) for item in digest.get("items", [])],
        ]
    )

    assert digest["lead_title"].startswith("紫金矿业 已于 2026-04-22")
    assert "创新药" not in joined
    assert "AI、算力" not in joined


def test_build_event_digest_keeps_pending_review_high_priority() -> None:
    digest = build_event_digest(
        {
            "name": "新易盛",
            "symbol": "300502",
            "generated_at": "2026-03-29 10:00:00",
            "catalyst_web_review": {"completed": False},
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "新易盛公告：800G 光模块新品发布",
                            "source": "证券时报",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    homepage_lines = build_event_digest(
        {
            "name": "tmp",
            "symbol": "tmp",
            "generated_at": "2026-03-29 10:00:00",
            "catalyst_web_review": {"completed": False},
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "新易盛公告：800G 光模块新品发布",
                            "source": "证券时报",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    assert digest["status"] == "待复核"
    assert digest["importance"] == "high"
    assert "必须前置复核" in digest["importance_reason"]
    assert "前置理由：" in event_digest_action_line(digest)


def test_event_digest_homepage_lines_keep_fallback_and_add_priority_clause() -> None:
    digest = build_event_digest(
        {
            "name": "新易盛",
            "symbol": "300502",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "新易盛公告：800G 光模块新品发布",
                            "source": "证券时报",
                            "date": "2026-03-28",
                        }
                    ]
                }
            },
        }
    )

    from src.output.event_digest import event_digest_homepage_lines

    lines = event_digest_homepage_lines(digest, ["主题级新闻：AI服务器资本开支持续"])

    assert any("情报属性：首次跟踪 / 新鲜情报 / 结构化披露 / 媒体直连" in line for line in lines)
    assert any("来源层级：`结构化披露`" in line for line in lines)
    assert any("先观察，因为" in line for line in lines)
    assert any("主题级新闻：AI服务器资本开支持续" in line for line in lines)


def test_event_digest_homepage_lines_use_soft_client_safe_fallback_when_lead_title_missing() -> None:
    digest = {
        "status": "待补充",
        "changed_what": "当前更多只是把研究焦点抬到 `主题事件：主题热度/映射` 这层，更直接改的是 `景气 / 资金偏好` 这层。",
        "lead_layer": "行业主题事件",
        "lead_title": "",
        "lead_detail": "",
        "history_note": "这是首次跟踪，当前先建立情报基线。",
        "items": [],
    }

    lines = event_digest_homepage_lines(digest, [])

    assert any("当前可前置的外部情报仍偏少" in line for line in lines)
    assert not any("当前更像 `行业主题事件` 层的补证据阶段" in line for line in lines)


def test_event_digest_homepage_and_action_lines_compact_long_theme_detail() -> None:
    digest = {
        "status": "已消化",
        "lead_layer": "行业主题事件",
        "lead_detail": "主题事件：成分权重结构；结论：指数主链已明确科创半导体材料设备的核心成分和权重结构，推荐理由优先按标准指数暴露理解。",
        "lead_title": "指数成分权重：前十权重合计 74.3%",
        "impact_summary": "景气 / 资金偏好",
        "thesis_scope": "一次性噪音",
        "signal_type": "成分权重结构",
        "signal_strength": "强",
        "signal_conclusion": "指数主链已明确科创半导体材料设备的核心成分和权重结构。",
        "changed_what": "主题事件：成分权重结构；结论：指数主链已明确科创半导体材料设备的核心成分和权重结构。这条线目前更多像一次性噪音，更直接改的是景气 / 资金偏好这层。",
        "importance_reason": "先不升级优先级，因为这条线更像一次性噪音，只把它当景气 / 资金偏好的辅助线索。",
        "next_step": "继续盯价格确认和后续证据。",
    }

    homepage_lines = event_digest_homepage_lines(digest, [])
    action_line = event_digest_action_line(digest, observe_only=True)

    assert any("`成分权重结构`" in line for line in homepage_lines)
    assert not any("主题事件：成分权重结构；结论：" in line for line in homepage_lines)
    assert any("前十权重合计 74.3%" in line for line in homepage_lines)
    assert not any("指数成分权重：" in line for line in homepage_lines)
    assert "`成分权重结构`" in action_line
    assert "主题事件：成分权重结构；结论：" not in action_line


def test_event_digest_homepage_lines_prioritize_index_weight_over_weekly_monthly_structure() -> None:
    digest = build_event_digest(
        {
            "name": "华夏上证科创板半导体材料设备主题ETF",
            "symbol": "588170",
            "generated_at": "2026-04-02 21:41:51",
            "market_event_rows": [
                [
                    "2026-04-02",
                    "指数月线：科创半导体材料设备 缺失",
                    "指数月线",
                    "低",
                    "科创半导体材料设备",
                    "",
                    "月线结构",
                    "先按 `科创半导体材料设备` 的月线结构理解，不把单日波动误判成趋势。 月线缺失。",
                ],
                [
                    "2026-04-02",
                    "指数周线：科创半导体材料设备 缺失",
                    "指数周线",
                    "低",
                    "科创半导体材料设备",
                    "",
                    "周线结构",
                    "先按 `科创半导体材料设备` 的周线结构理解，不把单日波动误判成趋势。 周线缺失。",
                ],
                [
                    "2026-04-02",
                    "指数成分权重：前十权重合计 74.3%；核心成分 688072 10.8%、688120 10.7%、688012 10.0%",
                    "指数成分/权重",
                    "高",
                    "科创半导体材料设备",
                    "",
                    "成分权重结构",
                    "指数主链已明确 `科创半导体材料设备` 的核心成分和权重结构，推荐理由优先按标准指数暴露理解。",
                ],
            ],
        }
    )

    homepage_lines = event_digest_homepage_lines(digest, [])

    assert digest["thesis_scope"] == "结构基线"
    assert "一次性噪音" not in digest["changed_what"]
    assert "新增催化" in digest["importance_reason"]
    assert "产品画像" in digest["importance_reason"]
    assert homepage_lines
    assert "成分权重结构" in homepage_lines[1]
    assert "月线结构" not in homepage_lines[1]
    assert "周线结构" not in homepage_lines[1]


def test_event_digest_intelligence_attributes_surface_search_fallback_and_since_last_review() -> None:
    digest = build_event_digest(
        {
            "name": "半导体ETF",
            "symbol": "512480",
            "generated_at": "2026-03-31 10:00:00",
            "dimensions": {
                "catalyst": {
                    "theme_news": [
                        {
                            "layer": "主题级关键新闻",
                            "title": "AI服务器资本开支延续，先进封装设备链情绪回暖",
                            "source": "Reuters",
                            "configured_source": "Reuters",
                            "category": "topic_search",
                            "date": "2026-03-29",
                        }
                    ]
                }
            },
        },
        previous_reviewed_at="2026-03-30 09:00:00",
    )

    assert digest["intelligence_attributes"] == ["旧闻回放", "媒体直连", "搜索回退", "主题级情报"]
    homepage_lines = event_digest_homepage_lines(digest, [])
    assert any("自上次复查（`2026-03-30 09:00:00`）以来" in line for line in homepage_lines)
    assert any("情报属性：旧闻回放 / 媒体直连 / 搜索回退 / 主题级情报" in line for line in homepage_lines)
    lines = render_event_digest_section(digest)
    assert any("情报属性：`旧闻回放 / 媒体直连 / 搜索回退 / 主题级情报`" in line for line in lines)


def test_event_digest_labels_official_site_search_without_promoting_to_direct() -> None:
    digest = build_event_digest(
        {
            "name": "贵州茅台",
            "symbol": "600519",
            "generated_at": "2026-03-31 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "贵州茅台投资者关系活动记录表更新经营情况",
                            "source": "Investor Relations",
                            "configured_source": "Investor Relations::search",
                            "source_note": "official_site_search",
                            "category": "stock_live_intelligence",
                            "published_at": "2026-03-31T09:30:00",
                            "link": "https://ir.kweichowmoutai.com/cmscontent/123.html",
                        }
                    ]
                }
            },
        }
    )

    assert digest["intelligence_attributes"] == ["首次跟踪", "新鲜情报", "结构化披露", "官网/IR回退", "搜索回退"]
    homepage_lines = event_digest_homepage_lines(digest, [])
    assert any("官网/IR回退" in line for line in homepage_lines)
    assert any("来源层级：`结构化披露`" in line for line in homepage_lines)


def test_build_event_digest_marks_no_new_intelligence_since_last_review() -> None:
    digest = build_event_digest(
        {
            "name": "半导体ETF",
            "symbol": "512480",
            "generated_at": "2026-03-29 10:00:00",
            "dimensions": {
                "catalyst": {
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
                }
            },
        },
        previous_reviewed_at="2026-03-26 09:00:00",
    )

    assert "自上次复查" in digest["history_note"]
    assert "暂无新增高置信情报" in digest["history_note"]
    assert "旧闻回放" in event_digest_action_line(digest)
    lines = render_event_digest_section(digest)
    assert any("上次复查时间：2026-03-26 09:00:00" in line for line in lines)


def test_event_digest_marks_first_tracking_when_no_previous_review() -> None:
    digest = build_event_digest(
        {
            "name": "农发种业",
            "symbol": "600313",
            "generated_at": "2026-03-31 10:00:00",
            "dimensions": {
                "catalyst": {
                    "coverage": {
                        "diagnosis": "confirmed_live",
                        "latest_news_at": "2026-03-31",
                    },
                    "evidence": [
                        {
                            "layer": "财报",
                            "signal": "年报显示毛利率改善",
                            "source": "CNINFO",
                            "date": "2026-03-31",
                        }
                    ],
                }
            },
        }
    )

    homepage_lines = event_digest_homepage_lines(digest, [])
    assert any("首次跟踪" in line for line in homepage_lines)
    lines = render_event_digest_section(digest)
    assert any("首次跟踪" in line for line in lines)


def test_build_event_digest_ignores_placeholder_missed_items_and_prefers_real_structured_signal() -> None:
    digest = build_event_digest(
        {
            "name": "农发种业",
            "symbol": "600313",
            "generated_at": "2026-03-31 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "农发种业 披露现金分红预案",
                            "source": "结构化事件",
                            "published_at": "2026-03-31T09:00:00",
                        },
                        {
                            "layer": "海外映射",
                            "title": "未命中直接海外映射",
                            "source": "海外映射",
                        },
                        {
                            "layer": "龙头公告/业绩",
                            "title": "未命中直接龙头公告",
                            "source": "龙头公告/业绩",
                        },
                        {
                            "layer": "前瞻催化",
                            "title": "未来 14 日未命中直接催化事件",
                            "source": "前瞻催化",
                        },
                    ]
                }
            },
        }
    )

    titles = [str(item.get("title", "")) for item in digest["items"]]
    assert digest["lead_title"] == "农发种业 披露现金分红预案"
    assert "未命中直接海外映射" not in titles
    assert "未命中直接龙头公告" not in titles
    assert "未来 14 日未命中直接催化事件" not in titles


def test_build_event_digest_does_not_surface_placeholder_lead_titles_in_homepage_or_section() -> None:
    digest = build_event_digest(
        {
            "name": "A500ETF华泰柏瑞",
            "symbol": "563360",
            "generated_at": "2026-03-31 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "未命中明确结构化公司事件",
                            "source": "结构化事件",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_title"] == ""
    homepage_lines = event_digest_homepage_lines(digest, [])
    assert not any("未命中明确结构化公司事件" in line for line in homepage_lines)
    lines = render_event_digest_section(digest)
    assert not any("当前前置事件" in line for line in lines)
    assert not any("未命中明确结构化公司事件" in line for line in lines)


def test_build_event_digest_recognizes_ir_record_as_pending_observation() -> None:
    digest = build_event_digest(
        {
            "name": "贵州茅台",
            "symbol": "600519",
            "generated_at": "2026-03-31 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "公告",
                            "title": "贵州茅台投资者关系活动记录表：管理层更新渠道和动销口径",
                            "source": "Investor Relations",
                            "configured_source": "Investor Relations::search",
                            "source_note": "official_site_search",
                            "published_at": "2026-03-31T09:30:00",
                            "link": "https://ir.kweichowmoutai.com/cmscontent/123.html",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "公告"
    assert digest["lead_detail"] == "公告类型：投资者关系/路演纪要"
    assert digest["impact_summary"] == "景气 / 资金偏好"
    assert digest["thesis_scope"] == "待确认"
    assert "管理层表述、经营口径和景气预期层" in digest["changed_what"]
    assert "订单、财报、价格" in digest["next_step"]
    assert "还要看订单、财报和价格确认是否跟上" in digest["importance_reason"]


def test_build_event_digest_uses_official_direct_note_to_classify_generic_announcement() -> None:
    digest = build_event_digest(
        {
            "name": "贵州茅台",
            "symbol": "600519",
            "generated_at": "2026-03-31 10:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "关于举办投资者关系活动的公告",
                            "source": "CNINFO",
                            "configured_source": "CNINFO::direct",
                            "source_note": "official_direct",
                            "note": "投资者关系/路演纪要",
                            "date": "2026-03-31",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_detail"] == "公告类型：投资者关系/路演纪要"


def test_build_event_digest_uses_tushare_irm_note_to_classify_interactive_qa() -> None:
    digest = build_event_digest(
        {
            "name": "汇洲智能",
            "symbol": "002122",
            "generated_at": "2026-04-02 16:00:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "汇洲智能互动平台问答：公司是否和幻方量化有合作；回复称经核查无合作关系…",
                            "source": "Tushare",
                            "configured_source": "Tushare::irm_qa_sz",
                            "source_note": "structured_disclosure",
                            "note": "投资者关系/路演纪要",
                            "date": "2026-04-02",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_detail"] == "公告类型：投资者关系/路演纪要"


def test_build_event_digest_keeps_leader_ir_product_solution_out_of_policy_layer() -> None:
    digest = build_event_digest(
        {
            "name": "中际旭创",
            "symbol": "300308",
            "generated_at": "2026-04-27 12:30:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "龙头公告/业绩",
                            "title": "中际旭创互动平台问答：公司1.6T系列产品中，使用自研硅光方案",
                            "source": "Tushare",
                            "link": "https://irm.cninfo.com.cn/",
                            "date": "2026-04-26",
                        }
                    ]
                }
            },
        }
    )

    assert digest["lead_layer"] == "公告"
    assert digest["lead_detail"] == "公告类型：投资者关系/路演纪要"
    assert digest["signal_type"] == "公告类型：投资者关系/路演纪要"


def test_event_digest_homepage_and_section_preserve_lead_markdown_link() -> None:
    digest = build_event_digest(
        {
            "name": "贵州茅台",
            "symbol": "600519",
            "generated_at": "2026-03-31 10:00:00",
            "dimensions": {
                "catalyst": {
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
                    ]
                }
            },
        }
    )

    homepage_lines = event_digest_homepage_lines(digest, [])
    assert any("](https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600519)" in line for line in homepage_lines)
    section_lines = render_event_digest_section(digest)
    assert any("](https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600519)" in line for line in section_lines)
    assert "管理层表述、经营口径和景气预期层" in digest["changed_what"]


def test_event_digest_surfaces_related_intelligence_items_with_signal_labels() -> None:
    digest = build_event_digest(
        {
            "name": "贵州茅台",
            "symbol": "600519",
            "generated_at": "2026-03-31 10:00:00",
            "dimensions": {
                "catalyst": {
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
                    ]
                }
            },
        }
    )

    homepage_lines = event_digest_homepage_lines(digest, [])
    assert any("相关情报补充" in line for line in homepage_lines)
    assert any("投资者关系活动记录表" in line for line in homepage_lines)
    section_lines = render_event_digest_section(digest)
    assert any("相关情报补充" in line for line in section_lines)
    assert any(line.startswith("  信号类型：") for line in section_lines)
    assert any(line.startswith("  来源：") for line in section_lines)


def test_event_digest_related_items_soften_old_news_to_history_baseline() -> None:
    digest = build_event_digest(
        {
            "name": "紫金矿业",
            "symbol": "601899",
            "generated_at": "2026-04-02 22:18:38",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "layer": "结构化事件",
                            "title": "紫金矿业 披露现金分红预案（每10股派现 0.38 元）",
                            "source": "Tushare dividend",
                            "date": "2026-03-21",
                        },
                        {
                            "layer": "公告",
                            "title": "紫金矿业 已于 2026-03-21 披露 2025年年报",
                            "source": "Tushare disclosure_date",
                            "date": "2026-03-21",
                            "signal_strength": "强",
                            "signal_conclusion": "偏利多，已开始改写 `盈利 / 估值` 这层。",
                        },
                    ]
                }
            },
        }
    )

    homepage_lines = event_digest_homepage_lines(digest, [])
    section_lines = render_event_digest_section(digest)

    assert any("相关情报补充" in line and "信号强弱：`中`" in line and "结论：中性，当前更多是历史基线，不把它直接当成新增催化。" in line for line in homepage_lines)
    assert any("相关情报补充" in line for line in section_lines)
    assert any("强弱：`中`" in line for line in section_lines)
    assert not any(line.startswith("  ") and "当前更像 `历史基线`" in line for line in section_lines)
    assert not any(line.startswith("  ") and "先把它当历史基线看" in line for line in section_lines)


def test_render_event_digest_section_keeps_raw_contract_in_humanized_theme_detail() -> None:
    digest = {
        "status": "已消化",
        "lead_layer": "行业主题事件",
        "lead_detail": "主题事件：成分权重结构；结论：指数主链已明确科创半导体材料设备的核心成分和权重结构，推荐理由优先按标准指数暴露理解。",
        "importance_label": "中",
        "signal_type": "成分权重结构",
        "signal_strength": "强",
        "signal_conclusion": "指数主链已明确科创半导体材料设备的核心成分和权重结构。",
        "impact_summary": "景气 / 资金偏好",
        "thesis_scope": "一次性噪音",
        "importance_reason": "先不升级优先级，因为这条线更像一次性噪音，只把它当景气 / 资金偏好的辅助线索。",
        "changed_what": "主题事件：成分权重结构；结论：指数主链已明确科创半导体材料设备的核心成分和权重结构。这条线目前更多像一次性噪音，更直接改的是景气 / 资金偏好这层。",
    }

    lines = render_event_digest_section(digest)
    detail_line = next(line for line in lines if "事件细分：" in line)

    assert "先按 `成分权重结构` 理解" in detail_line
    assert digest["lead_detail"] in detail_line


def test_build_event_digest_collects_news_report_items_alongside_direct_and_theme_intelligence() -> None:
    digest = build_event_digest(
        {
            "name": "贵州茅台",
            "symbol": "600519",
            "generated_at": "2026-03-31 10:00:00",
            "dimensions": {
                "catalyst": {
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
                    "theme_news": [
                        {
                            "title": "白酒板块情绪回暖，龙头估值修复预期抬升",
                            "source": "财联社",
                            "date": "2026-03-31",
                        }
                    ],
                }
            },
            "news_report": {
                "items": [
                    {
                        "title": "机构调研聚焦高端白酒库存与动销节奏",
                        "source": "证券时报",
                        "date": "2026-03-31",
                        "link": "https://example.com/ir",
                    }
                ]
            },
        }
    )

    titles = [str(item.get("title", "")) for item in digest.get("items", [])]
    assert "关于举办投资者关系活动的公告" in titles
    assert "白酒板块情绪回暖，龙头估值修复预期抬升" in titles
    assert "机构调研聚焦高端白酒库存与动销节奏" in titles


def test_build_event_digest_filters_miss_diagnostic_factor_rows_when_using_factor_fallback() -> None:
    digest = build_event_digest(
        {
            "name": "农发种业",
            "symbol": "600313",
            "generated_at": "2026-04-01 00:20:00",
            "dimensions": {
                "catalyst": {
                    "evidence": [],
                    "theme_news": [],
                    "factors": [
                        {
                            "name": "政策催化",
                            "signal": "近 7 日未命中直接政策催化",
                            "detail": "政策原文和一级媒体优先",
                        },
                        {
                            "name": "龙头公告/业绩",
                            "signal": "未命中直接龙头公告",
                            "detail": "优先看订单、扩产、回购、并购或超预期业绩",
                        },
                        {
                            "name": "结构化事件",
                            "signal": "农发种业 披露现金分红预案",
                            "detail": "事件距今 221 天，已超出结构化事件有效窗口，不再作为当前催化加分。",
                        },
                        {
                            "name": "研报/新闻密度",
                            "signal": "个股新鲜情报 0 条（主题/行业情报 0 条）",
                            "detail": "这里优先统计近 3 日新增情报，不把旧闻回放直接算成新催化。",
                        },
                        {
                            "name": "研报/新闻密度",
                            "signal": "新增情报 0 条",
                            "detail": "这里优先统计近 3 日新增情报，不把旧闻回放直接算成新催化。",
                        },
                    ],
                }
            },
        }
    )

    titles = [str(item.get("title", "")) for item in digest.get("items", [])]
    assert "农发种业 披露现金分红预案" in titles
    assert not any("未命中直接" in title for title in titles)
    assert not any("新鲜情报 0 条" in title for title in titles)
    assert "新增情报 0 条" not in titles


def test_effective_intelligence_link_recognizes_tushare_prefixed_structured_source_names() -> None:
    assert (
        effective_intelligence_link(
            {
                "source": "Tushare dividend",
                "configured_source": "Tushare dividend",
                "source_note": "",
                "link": "",
            },
            symbol="601899",
        )
        == "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=601899"
    )


def test_effective_intelligence_link_uses_exchange_homepage_for_tushare_irm_items() -> None:
    assert (
        effective_intelligence_link(
            {
                "source": "Tushare",
                "configured_source": "Tushare::irm_qa_sz",
                "source_note": "structured_disclosure",
                "title": "汇洲智能互动平台问答：公司是否和幻方量化有合作",
                "link": "",
            },
            symbol="002122",
        )
        == "https://irm.cninfo.com.cn/"
    )


def test_effective_intelligence_link_falls_back_for_structured_lead_items_without_original_source_note() -> None:
    assert (
        effective_intelligence_link(
            {
                "source": "结构化事件",
                "configured_source": "",
                "source_note": "",
                "title": "农发种业 披露现金分红预案",
                "lead_detail": "公告类型：分红/回报",
                "link": "",
            },
            symbol="600313",
        )
        == "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=600313"
    )


def test_build_event_digest_synthesizes_official_disclosure_query_title_when_lead_title_is_placeholder() -> None:
    digest = build_event_digest(
        {
            "name": "A500ETF华泰柏瑞",
            "symbol": "563360",
            "dimensions": {
                "catalyst": {
                    "evidence": [
                        {
                            "title": "未命中明确结构化公司事件",
                            "source": "结构化事件",
                            "lead_detail": "公告类型：一般公告",
                        }
                    ]
                }
            },
        }
    )
    assert digest["lead_title"] == "A500ETF华泰柏瑞 官方披露查询"
    assert digest["lead_link"] == "https://www.cninfo.com.cn/new/disclosure/detail?stockCode=563360"


def test_sort_event_items_prefers_higher_source_lane_when_scores_are_similar() -> None:
    ranked = sort_event_items(
        [
            {
                "layer": "结构化事件",
                "title": "关于举办投资者关系活动的公告",
                "source": "CNINFO",
                "configured_source": "CNINFO::direct",
                "source_note": "official_direct",
                "note": "投资者关系/路演纪要",
                "date": "2026-03-31",
                "freshness_bucket": "fresh",
                "age_days": 0,
            },
            {
                "layer": "结构化事件",
                "title": "公司交流纪要更新经营情况",
                "source": "财联社",
                "configured_source": "财联社",
                "date": "2026-03-31",
                "freshness_bucket": "fresh",
                "age_days": 0,
            },
        ],
        as_of="2026-03-31 10:00:00",
    )

    assert ranked[0]["title"] == "关于举办投资者关系活动的公告"
    assert ranked[0]["source_lane_rank"] > ranked[1]["source_lane_rank"]
