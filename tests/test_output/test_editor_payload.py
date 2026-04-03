import pandas as pd

from src.output.client_report import ClientReportRenderer
from src.output.editor_payload import (
    _action_lines,
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
    assert any("信号强弱" in line or "结论：" in line for line in lines[:1])
    assert any("https://example.com/semiconductor" in line for line in lines)
    assert any("财联社" in line for line in lines)


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
    assert "当时的优先级判断是：" in packet["what_changed"]["previous_view"]
    assert "事件状态从 `待补充` 升到 `已消化`" in packet["what_changed"]["change_summary"]
    assert "当前更该前置的是" in packet["what_changed"]["change_summary"]
    assert "更直接影响 `" in packet["what_changed"]["current_event_understanding"]
    assert "当前更像 `" in packet["what_changed"]["current_event_understanding"]
    assert "优先级判断是：" in packet["what_changed"]["current_event_understanding"]
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
    assert "硬分类" in homepage["theme_lines"][0]
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
    assert lines[0] == "空仓先别急着直接找买点，更合理的是先看 `顶背离/假突破消化、MACD/OBV 重新同步` 这类确认。"
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

    assert any("不先给精确仓位、止损和目标模板" in line for line in lines)
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
    assert "**首次建仓 ≤3%**" in rendered
    assert "**1.420**" in rendered
    assert "**1.784**" in rendered


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
    assert packet["homepage"]["total_judgment"].startswith("今天没有有效动作信号")
    assert "本页重点看 `中金黄金 (600489)`" in packet["homepage"]["total_judgment"]
    assert packet["homepage"]["news_lines"]


def test_build_stock_pick_editor_packet_prefers_watch_subject_when_observe_only() -> None:
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
    assert packet["homepage"]["total_judgment"].startswith("今天没有有效动作信号")
    assert "本页重点看 `紫金矿业 (601899)`" in packet["homepage"]["total_judgment"]


def test_build_stock_pick_editor_packet_prefers_watch_subject_even_when_top_is_non_observe() -> None:
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
    assert "本页重点看 `紫金矿业 (601899)`" in packet["homepage"]["total_judgment"]


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
    assert packet["homepage"]["total_judgment"].startswith("今天没有有效动作信号")
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
        assert packet["homepage"]["total_judgment"].startswith("今天没有有效动作信号")


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
    assert "A股概念领涨：创新药" in news_lines[0]
    assert "信号：`医药催化`" in news_lines[0]
    assert "结论：偏利多" in news_lines[0]
    joined = "\n".join(news_lines)
    assert "[Global bond investors reassess conflict risks](https://example.com/bloomberg)" in joined
    assert "结论：" in joined


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
    assert "AI应用/算力" in summary
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
    assert packet["homepage"]["total_judgment"].startswith("今天没有有效动作信号")
    assert "本页重点看 `半导体ETF (512480)`" in packet["homepage"]["total_judgment"]
    assert packet["homepage"]["news_lines"]
    rendered = render_editor_homepage(packet)
    assert "## 首页判断" in rendered
    assert "### 板块 / 主题认知" in rendered
    assert "半导体" in rendered
