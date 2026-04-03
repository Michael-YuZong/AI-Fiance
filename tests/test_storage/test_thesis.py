"""Tests for thesis repository."""

from __future__ import annotations

from pathlib import Path

from src.storage.thesis import (
    ThesisRepository,
    build_review_queue_action_items,
    build_thesis_state_transition,
    build_thesis_review_queue,
    compare_event_digest_snapshots,
    lookup_latest_symbol_reports,
    summarize_review_queue_action_lines,
    summarize_review_queue_followup_lines,
    summarize_review_queue_history_lines,
    summarize_review_queue_summary_line,
    summarize_review_queue_transitions,
    summarize_thesis_event_memory,
    summarize_thesis_review_priority,
    summarize_thesis_state_snapshot,
)


def test_thesis_repository_upsert_and_delete(tmp_path: Path):
    repo = ThesisRepository(thesis_path=tmp_path / "thesis.json")
    record = repo.upsert(
        symbol="561380",
        core_assumption="电网投资提升",
        validation_metric="投资完成额同比 > 10%",
        stop_condition="估值过高且增速下滑",
        holding_period="6-12个月",
    )
    assert record["symbol"] == "561380"
    assert repo.get("561380")["core_assumption"] == "电网投资提升"
    assert len(repo.list_all()) == 1
    assert repo.delete("561380") is True
    assert repo.get("561380") is None


def test_compare_event_digest_snapshots_detects_status_upgrade() -> None:
    delta = compare_event_digest_snapshots(
        {
            "status": "待补充",
            "lead_layer": "行业主题事件",
            "lead_detail": "主题事件：主题热度/映射",
            "impact_summary": "资金偏好 / 景气",
            "thesis_scope": "待确认",
            "changed_what": "先按主题跟踪。",
        },
        {
            "status": "已消化",
            "lead_layer": "公告",
            "lead_detail": "公告类型：一般公告",
            "impact_summary": "盈利 / 估值",
            "thesis_scope": "thesis变化",
            "changed_what": "已经下沉到公司级执行。",
        },
    )

    assert delta["change_type"] == "status_up"
    assert "待补充" in delta["summary"]
    assert "已消化" in delta["summary"]
    assert "公告类型：一般公告" in delta["summary"]
    assert "盈利 / 估值" in delta["summary"]
    assert delta["current_understanding"].startswith("更该前置的是 `公告类型：一般公告`")


def test_compare_event_digest_snapshots_detects_thesis_scope_shift() -> None:
    delta = compare_event_digest_snapshots(
        {
            "status": "已消化",
            "lead_layer": "政策",
            "lead_detail": "政策影响层：方向表态",
            "impact_summary": "资金偏好 / 景气",
            "thesis_scope": "待确认",
            "changed_what": "先按方向线索理解。",
        },
        {
            "status": "已消化",
            "lead_layer": "政策",
            "lead_detail": "政策影响层：直接执行",
            "impact_summary": "盈利 / 景气",
            "thesis_scope": "thesis变化",
            "changed_what": "已经下沉到执行层。",
        },
    )

    assert delta["change_type"] == "event_detail_retyped"
    assert "传导环节" in delta["summary"]
    assert "政策影响层：直接执行" in delta["summary"]
    assert "盈利 / 景气" in delta["summary"]


def test_compare_event_digest_snapshots_surfaces_theme_focus_shift() -> None:
    delta = compare_event_digest_snapshots(
        {
            "status": "已消化",
            "lead_layer": "行业主题事件",
            "lead_detail": "主题事件：情绪热度",
            "impact_summary": "资金偏好",
            "thesis_scope": "待确认",
            "changed_what": "先按热度扩散跟踪。",
        },
        {
            "status": "已消化",
            "lead_layer": "行业主题事件",
            "lead_detail": "主题事件：价格/排产验证",
            "impact_summary": "盈利 / 景气",
            "thesis_scope": "thesis变化",
            "changed_what": "已经下沉到供需和价格验证层。",
        },
    )

    assert delta["change_type"] == "event_detail_retyped"
    assert "市场关注度和资金偏好" in delta["summary"]
    assert "供需、价格和排产验证" in delta["summary"]


def test_compare_event_digest_snapshots_detects_priority_reason_shift() -> None:
    delta = compare_event_digest_snapshots(
        {
            "status": "已消化",
            "lead_layer": "公告",
            "lead_detail": "公告类型：产品/新品",
            "impact_summary": "景气 / 资金偏好",
            "thesis_scope": "待确认",
            "importance_reason": "保留前排观察，因为新品已开始影响 `景气 / 资金偏好`，但还要看产品验证和放量兑现。",
        },
        {
            "status": "已消化",
            "lead_layer": "公告",
            "lead_detail": "公告类型：产品/新品",
            "impact_summary": "景气 / 资金偏好",
            "thesis_scope": "待确认",
            "importance_reason": "先不升级优先级，因为 `公告类型：产品/新品` 更像一次性噪音，只把它当 `景气 / 资金偏好` 的辅助线索。",
        },
    )

    assert delta["change_type"] == "priority_reason_updated"
    assert "优先级判断已经更新" in delta["summary"]


def test_compare_event_digest_snapshots_detects_importance_shift() -> None:
    delta = compare_event_digest_snapshots(
        {
            "status": "已消化",
            "lead_layer": "公告",
            "lead_detail": "公告类型：产品/新品",
            "importance": "medium",
            "impact_summary": "景气 / 资金偏好",
            "thesis_scope": "待确认",
            "importance_reason": "保留前排观察，因为新品已开始影响 `景气 / 资金偏好`，但还要看产品验证和放量兑现。",
        },
        {
            "status": "已消化",
            "lead_layer": "公告",
            "lead_detail": "公告类型：产品/新品",
            "importance": "high",
            "impact_summary": "景气 / 资金偏好",
            "thesis_scope": "待确认",
            "importance_reason": "优先前置，因为新品验证开始真正改写 `景气 / 资金偏好`。",
        },
    )

    assert delta["change_type"] == "importance_changed"
    assert "事件优先级已从 `中` 调整到 `高`" in delta["summary"]


def test_build_thesis_state_transition_upgrades_on_status_up() -> None:
    transition = build_thesis_state_transition(
        {
            "event_digest_snapshot": {
                "status": "待补充",
                "lead_layer": "行业主题事件",
                "lead_title": "AI 算力链热度扩散",
            }
        },
        {
            "status": "已消化",
            "lead_layer": "公告",
            "lead_title": "公司发布 800G 光模块新品公告",
            "changed_what": "已经下沉到公司级执行。",
        },
        {"change_type": "status_up", "summary": "事件状态从 `待补充` 升到 `已消化`。"},
        source="scan",
        recorded_at="2026-03-29 10:00:00",
    )

    assert transition["state"] == "升级"
    assert transition["trigger"] == "事件完成消化"
    assert transition["change_type"] == "status_up"


def test_build_thesis_state_transition_upgrades_on_importance_change() -> None:
    transition = build_thesis_state_transition(
        {
            "thesis_state_snapshot": {
                "state": "维持",
                "trigger": "事件延续确认",
                "summary": "当前 thesis 仍以延续确认处理，先沿用上次研究结论。",
            },
            "event_digest_snapshot": {
                "status": "已消化",
                "lead_layer": "公告",
                "lead_detail": "公告类型：产品/新品",
                "importance": "medium",
            },
        },
        {
            "status": "已消化",
            "lead_layer": "公告",
            "lead_detail": "公告类型：产品/新品",
            "importance": "high",
        },
        {
            "change_type": "importance_changed",
            "summary": "事件优先级已从 `中` 调整到 `高`，说明研究前置顺序已经变化。",
        },
        source="scan",
        recorded_at="2026-03-29 10:00:00",
    )

    assert transition["state"] == "升级"
    assert transition["trigger"] == "事件优先级上调"
    assert "事件优先级已从 `中` 调整到 `高`" in transition["summary"]


def test_build_thesis_state_transition_upgrades_when_scope_turns_into_thesis_change() -> None:
    transition = build_thesis_state_transition(
        {
            "event_digest_snapshot": {
                "status": "已消化",
                "lead_layer": "政策",
                "lead_detail": "政策影响层：方向表态",
                "impact_summary": "资金偏好 / 景气",
                "thesis_scope": "待确认",
            }
        },
        {
            "status": "已消化",
            "lead_layer": "政策",
            "lead_detail": "政策影响层：直接执行",
            "impact_summary": "盈利 / 景气",
            "thesis_scope": "thesis变化",
            "changed_what": "已经下沉到执行层。",
        },
        {"change_type": "thesis_scope_changed", "summary": "这条事件对 thesis 的影响边界已从 `待确认` 调整到 `thesis变化`。"},
        source="research",
        recorded_at="2026-03-29 12:00:00",
    )

    assert transition["state"] == "升级"
    assert transition["trigger"] == "事件升级为 thesis 变化"


def test_build_thesis_state_transition_weakens_when_negative_event_turns_into_thesis_change() -> None:
    transition = build_thesis_state_transition(
        {
            "event_digest_snapshot": {
                "status": "已消化",
                "lead_layer": "财报",
                "lead_detail": "财报摘要：现金流/合同负债改善",
                "impact_summary": "盈利 / 景气",
                "thesis_scope": "待确认",
            }
        },
        {
            "status": "已消化",
            "lead_layer": "财报",
            "lead_detail": "财报摘要：存货/减值压力",
            "impact_summary": "盈利 / 景气",
            "thesis_scope": "thesis变化",
            "changed_what": "已经下沉到去库与资产质量压力层。",
        },
        {"change_type": "thesis_scope_changed", "summary": "这条事件对 thesis 的影响边界已从 `待确认` 调整到 `thesis变化`。"},
        source="research",
        recorded_at="2026-03-29 12:30:00",
    )

    assert transition["state"] == "削弱"
    assert transition["trigger"] == "负面事件升级为 thesis 变化"


def test_build_thesis_state_transition_upgrades_when_theme_moves_from_heat_to_validation() -> None:
    transition = build_thesis_state_transition(
        {
            "event_digest_snapshot": {
                "status": "已消化",
                "lead_layer": "行业主题事件",
                "lead_detail": "主题事件：情绪热度",
                "impact_summary": "资金偏好",
                "thesis_scope": "待确认",
            }
        },
        {
            "status": "已消化",
            "lead_layer": "行业主题事件",
            "lead_detail": "主题事件：价格/排产验证",
            "impact_summary": "盈利 / 景气",
            "thesis_scope": "thesis变化",
        },
        {
            "change_type": "event_detail_retyped",
            "summary": "当前前置事件仍在 `行业主题事件` 层，但已从 `主题事件：情绪热度` 切到 `主题事件：价格/排产验证`。",
        },
        source="scan",
        recorded_at="2026-03-29 13:00:00",
    )

    assert transition["state"] == "升级"
    assert transition["trigger"] == "主题从热度切到景气验证"


def test_build_thesis_state_transition_revokes_when_direct_event_falls_back_to_news() -> None:
    transition = build_thesis_state_transition(
        {
            "event_digest_snapshot": {
                "status": "已消化",
                "lead_layer": "公告",
                "lead_title": "公司中标公告",
            },
            "thesis_state_snapshot": {"state": "维持", "trigger": "事件延续确认", "summary": "原 thesis 仍成立。"},
        },
        {
            "status": "待补充",
            "lead_layer": "新闻",
            "lead_title": "板块讨论热度升温",
            "changed_what": "只剩情绪讨论。",
        },
        {"change_type": "status_down", "summary": "事件状态从 `已消化` 退到 `待补充`。"},
        source="research",
        recorded_at="2026-03-29 11:00:00",
    )

    assert transition["state"] == "撤销"
    assert transition["trigger"] == "高质量事件支点消失"


def test_thesis_repository_upsert_preserves_event_digest_memory(tmp_path: Path) -> None:
    repo = ThesisRepository(thesis_path=tmp_path / "thesis.json")
    repo.upsert(
        symbol="561380",
        core_assumption="电网投资提升",
        validation_metric="投资完成额同比 > 10%",
        stop_condition="估值过高且增速下滑",
        holding_period="6-12个月",
    )
    repo.record_event_digest(
        "561380",
        {
            "status": "待补充",
            "lead_layer": "行业主题事件",
            "lead_title": "特高压投资节奏跟踪",
            "changed_what": "先按产业链热度跟踪。",
            "next_step": "补公司级事件。",
        },
        source="scan",
        recorded_at="2026-03-29 10:00:00",
    )

    record = repo.upsert(
        symbol="561380",
        core_assumption="电网投资主线未坏",
        validation_metric="投资完成额同比 > 10%",
        stop_condition="估值过高且增速下滑",
        holding_period="6-12个月",
    )

    assert record["core_assumption"] == "电网投资主线未坏"
    assert record["event_digest_snapshot"]["status"] == "待补充"
    assert record["event_digest_snapshot"]["lead_layer"] == "行业主题事件"
    assert len(record["event_digest_ledger"]) == 1


def test_summarize_thesis_event_memory_builds_monitor_label() -> None:
    summary = summarize_thesis_event_memory(
        {
            "event_digest_snapshot": {
                "status": "待复核",
                "lead_layer": "政策",
                "importance": "high",
                "lead_title": "政策细则待补",
                "importance_reason": "必须前置复核，因为政策细则可能改写景气。",
                "changed_what": "旧结论先退回复核。",
                "recorded_at": "2026-03-29 10:00:00",
            },
            "event_digest_ledger": [{"delta": {"summary": "事件状态回退。"}}],
        }
    )

    assert summary["monitor_label"] == "事件待复核"
    assert summary["importance_label"] == "高"
    assert "必须前置复核" in summary["importance_reason"]
    assert summary["ledger_size"] == 1
    assert summary["updated_at"] == "2026-03-29 10:00:00"


def test_thesis_repository_event_digest_snapshot_defaults_contract_version(tmp_path: Path) -> None:
    repo = ThesisRepository(thesis_path=tmp_path / "thesis.json")
    repo.upsert(
        symbol="561380",
        core_assumption="电网投资提升",
        validation_metric="投资完成额同比 > 10%",
        stop_condition="估值过高且增速下滑",
        holding_period="6-12个月",
    )

    result = repo.record_event_digest(
        "561380",
        {
            "status": "待复核",
            "lead_layer": "政策",
            "lead_title": "政策细则待补",
            "changed_what": "旧结论先退回复核。",
        },
        source="research",
        recorded_at="2026-03-29 12:00:00",
    )

    assert result["snapshot"]["contract_version"] == "event_digest.v1"
    assert repo.get("561380")["event_digest_snapshot"]["contract_version"] == "event_digest.v1"


def test_thesis_repository_record_event_digest_persists_thesis_state_snapshot(tmp_path: Path) -> None:
    repo = ThesisRepository(thesis_path=tmp_path / "thesis.json")
    repo.upsert(
        symbol="561380",
        core_assumption="电网投资提升",
        validation_metric="投资完成额同比 > 10%",
        stop_condition="估值过高且增速下滑",
        holding_period="6-12个月",
    )
    repo.record_event_digest(
        "561380",
        {
            "status": "待补充",
            "lead_layer": "行业主题事件",
            "lead_title": "特高压投资节奏跟踪",
            "changed_what": "先按产业链热度跟踪。",
        },
        source="scan",
        recorded_at="2026-03-28 10:00:00",
    )

    result = repo.record_event_digest(
        "561380",
        {
            "status": "已消化",
            "lead_layer": "公告",
            "lead_title": "国电南瑞中标国家电网项目",
            "changed_what": "已经下沉到公司级执行。",
        },
        source="research",
        recorded_at="2026-03-29 10:00:00",
    )

    state = summarize_thesis_state_snapshot(repo.get("561380"))

    assert result["state_transition"]["state"] == "升级"
    assert state["state"] == "升级"
    assert len(repo.get("561380")["thesis_state_ledger"]) >= 1


def test_summarize_thesis_review_priority_upgrades_pending_review_with_loss() -> None:
    summary = summarize_thesis_review_priority(
        {
            "event_digest_snapshot": {
                "status": "待复核",
                "lead_layer": "政策",
            }
        },
        weight=0.22,
        pnl=-0.09,
    )

    assert summary["priority"] == "高"
    assert "事件边界待复核" in summary["summary"]
    assert "浮亏已扩大" in summary["summary"]


def test_summarize_thesis_review_priority_uses_thesis_state_snapshot() -> None:
    summary = summarize_thesis_review_priority(
        {
            "thesis_state_snapshot": {
                "state": "削弱",
                "trigger": "事件退回待补充",
                "summary": "原 thesis 仍有方向价值，但确定性必须先下调。",
            },
            "event_digest_snapshot": {
                "status": "待补充",
                "lead_layer": "行业主题事件",
            },
        },
        weight=0.18,
        pnl=-0.02,
    )

    assert summary["priority"] == "中"
    assert summary["thesis_state"] == "削弱"
    assert "thesis 已削弱" in summary["summary"]


def test_build_thesis_review_queue_sorts_missing_and_pending_review_first() -> None:
    queue = build_thesis_review_queue(
        [
            {"symbol": "000001", "record": None, "weight": 0.18, "pnl": -0.01},
            {
                "symbol": "561380",
                "record": {"event_digest_snapshot": {"status": "待复核", "lead_layer": "政策"}},
                "weight": 0.28,
                "pnl": -0.09,
            },
            {
                "symbol": "512480",
                "record": {"event_digest_snapshot": {"status": "已消化", "lead_layer": "财报"}},
                "weight": 0.08,
                "pnl": 0.03,
            },
        ]
    )

    assert [item["symbol"] for item in queue[:2]] == ["561380", "000001"]
    assert queue[0]["priority"] == "高"
    assert queue[0]["event_layer"] == "政策"
    assert queue[1]["summary"] == "还没有绑定 thesis，无法持续复查原始判断。"


def test_thesis_repository_record_review_queue_tracks_transitions_and_stale_entries(tmp_path: Path) -> None:
    repo = ThesisRepository(
        thesis_path=tmp_path / "thesis.json",
        review_queue_path=tmp_path / "thesis_review_queue.json",
    )

    day1 = repo.record_review_queue(
        [
            {
                "symbol": "561380",
                "priority": "高",
                "score": 92,
                "summary": "事件边界待复核，仓位较重",
                "event_detail": "政策影响层：配套细则",
                "impact_summary": "盈利 / 景气",
                "thesis_scope": "待确认",
                "event_monitor_label": "事件待复核",
                "has_thesis": True,
            }
        ],
        source="briefing_daily",
        as_of="2026-03-29 07:30:00",
    )
    assert [item["symbol"] for item in day1["new_entries"]] == ["561380"]
    assert day1["active"][0]["active_days"] == 1
    assert day1["active"][0]["recommended_action"] == "重跑 scan"
    assert day1["active"][0]["event_detail"] == "政策影响层：配套细则"
    assert day1["active"][0]["impact_summary"] == "盈利 / 景气"
    assert day1["active"][0]["thesis_scope"] == "待确认"

    day2 = repo.record_review_queue(
        [
            {
                "symbol": "561380",
                "priority": "高",
                "score": 92,
                "summary": "事件边界待复核，仓位较重",
                "event_detail": "政策影响层：配套细则",
                "impact_summary": "盈利 / 景气",
                "thesis_scope": "待确认",
                "event_monitor_label": "事件待复核",
                "has_thesis": True,
            }
        ],
        source="briefing_daily",
        as_of="2026-03-30 07:30:00",
    )
    assert day2["new_entries"] == []
    assert day2["active"][0]["active_days"] == 2

    day3 = repo.record_review_queue(
        [
            {
                "symbol": "561380",
                "priority": "高",
                "score": 92,
                "summary": "事件边界待复核，仓位较重",
                "event_detail": "政策影响层：配套细则",
                "impact_summary": "盈利 / 景气",
                "thesis_scope": "待确认",
                "event_monitor_label": "事件待复核",
                "has_thesis": True,
            }
        ],
        source="briefing_daily",
        as_of="2026-03-31 07:30:00",
    )
    assert [item["symbol"] for item in day3["stale_high_priority"]] == ["561380"]
    assert day3["stale_high_priority"][0]["active_days"] == 3

    day4 = repo.record_review_queue(
        [],
        source="briefing_daily",
        as_of="2026-04-01 07:30:00",
    )
    assert [item["symbol"] for item in day4["resolved_entries"]] == ["561380"]
    assert repo.load_review_queue()["active"] == []


def test_summarize_review_queue_transitions_renders_review_actions() -> None:
    lines = summarize_review_queue_transitions(
        {
            "new_entries": [
                {
                    "symbol": "561380",
                    "priority": "高",
                    "recommended_action": "重跑 scan",
                    "event_detail": "政策影响层：配套细则",
                    "thesis_state_trigger": "事件边界待复核",
                    "thesis_state_summary": "当前事件边界已退回待复核，旧 thesis 先不要按已验证继续沿用。",
                    "event_importance_label": "高",
                    "impact_summary": "盈利 / 景气",
                    "thesis_scope": "待确认",
                },
            ],
            "resolved_entries": [
                {"symbol": "000001", "priority": "高", "recommended_action": "补 thesis", "event_detail": "公告类型：回购/增持"},
            ],
            "stale_high_priority": [
                {
                    "symbol": "512480",
                    "active_days": 3,
                    "recommended_action": "复查 thesis",
                    "impact_summary": "盈利 / 估值",
                }
            ],
        }
    )

    assert any("今日新进复查队列" in line and "重跑 scan" in line for line in lines)
    assert any(
        "今日新进复查队列" in line
        and "政策影响层：配套细则" in line
        and "事件边界待复核" in line
        and "当前事件边界已退回待复核" in line
        and "事件优先级 高" in line
        and "盈利 / 景气" in line
        for line in lines
    )
    assert any("今日移出复查队列" in line and "补 thesis" in line for line in lines)
    assert any("连续高优先级未处理" in line and "已连续 3 天高优先级" in line and "盈利 / 估值" in line for line in lines)


def test_summarize_review_queue_action_lines_builds_followup_commands() -> None:
    lines = summarize_review_queue_action_lines(
        [
            {
                "symbol": "561380",
                "priority": "高",
                "summary": "事件边界待复核，仓位较重",
                "event_detail": "政策影响层：配套细则",
                "event_importance_label": "高",
                "event_importance_reason": "必须前置复核，因为政策细则可能改写盈利 / 景气。",
                "impact_summary": "盈利 / 景气",
                "thesis_scope": "待确认",
                "event_monitor_label": "事件待复核",
                "has_thesis": True,
            },
            {
                "symbol": "000001",
                "priority": "高",
                "summary": "还没有绑定 thesis，无法持续复查原始判断。",
                "event_monitor_label": "",
                "has_thesis": False,
            },
        ]
    )

    assert "研究动作 1: 561380（高）先重跑 scan" in lines[0]
    assert "政策影响层：配套细则" in lines[0]
    assert "事件优先级 高" in lines[0]
    assert "盈利 / 景气" in lines[0]
    assert "待确认" in lines[0]
    assert "必须前置复核" in lines[0]
    assert "`python -m src.commands.scan 561380`" in lines[0]
    assert "研究动作 2: 000001（高）先补 thesis" in lines[1]
    assert "`python -m src.commands.research 000001 现在为什么值得继续跟踪`" in lines[1]


def test_summarize_review_queue_action_lines_upgrades_to_refresh_final_when_followup_is_stale() -> None:
    lines = summarize_review_queue_action_lines(
        [
            {
                "symbol": "561380",
                "priority": "高",
                "summary": "事件边界待复核，仓位较重",
                "event_detail": "政策影响层：配套细则",
                "report_followup": {
                    "status": "待更新正式稿",
                    "reason": "最近复查已完成，但当前正式稿仍停留在旧版本；下一步应补新的 final / client-final。",
                    "reports": [
                        {
                            "report_type": "scan",
                            "generated_at": "2026-03-29 09:00:00",
                            "markdown": "reports/scans/etfs/final/scan_561380_2026-03-29_client_final.md",
                        }
                    ],
                },
            }
        ]
    )

    assert "研究动作 1: 561380（高）先补正式稿" in lines[0]
    assert "最近正式稿状态 待更新正式稿" in lines[0]
    assert "`python -m src.commands.scan 561380 --client-final`" in lines[0]


def test_summarize_review_queue_followup_lines_surfaces_stale_final_refresh() -> None:
    lines = summarize_review_queue_followup_lines(
        [
            {
                "symbol": "561380",
                "report_followup": {
                    "status": "待更新正式稿",
                    "reason": "最近复查已完成，但当前正式稿仍停留在旧版本；下一步应补新的 final / client-final。",
                    "reports": [
                        {
                            "report_type": "scan",
                            "generated_at": "2026-03-29 09:00:00",
                            "markdown": "reports/scans/etfs/final/scan_561380_2026-03-29_client_final.md",
                        }
                    ],
                },
            }
        ]
    )

    assert "正式稿跟进: 561380 当前是 `待更新正式稿`" in lines[0]
    assert "`python -m src.commands.scan 561380 --client-final`" in lines[0]


def test_build_review_queue_action_items_keeps_command_and_reason() -> None:
    items = build_review_queue_action_items(
        [
            {
                "symbol": "561380",
                "priority": "高",
                "summary": "事件边界待复核，仓位较重",
                "thesis_state_trigger": "事件边界待复核",
                "thesis_state_summary": "当前事件边界已退回待复核，旧 thesis 先不要按已验证继续沿用。",
                "event_detail": "政策影响层：配套细则",
                "event_importance_label": "高",
                "event_importance_reason": "必须前置复核，因为政策细则可能改写盈利 / 景气。",
                "impact_summary": "盈利 / 景气",
                "thesis_scope": "待确认",
                "event_monitor_label": "事件待复核",
                "has_thesis": True,
            }
        ],
        limit=1,
    )

    assert items == [
        {
            "symbol": "561380",
            "priority": "高",
            "recommended_action": "重跑 scan",
            "command": "python -m src.commands.scan 561380",
            "detail": "政策影响层：配套细则；事件边界待复核；当前事件边界已退回待复核，旧 thesis 先不要按已验证继续沿用。；事件优先级 高；盈利 / 景气；待确认；必须前置复核，因为政策细则可能改写盈利 / 景气。；事件边界待复核，仓位较重；事件待复核",
        }
    ]


def test_summarize_review_queue_summary_line_surfaces_event_depth() -> None:
    line = summarize_review_queue_summary_line(
        [
            {
                "symbol": "561380",
                "priority": "高",
                "thesis_state": "待复核",
                "thesis_state_trigger": "事件边界待复核",
                "thesis_state_summary": "当前事件边界已退回待复核，旧 thesis 先不要按已验证继续沿用。",
                "thesis_scope": "待确认",
                "event_detail": "政策影响层：配套细则",
                "event_importance_label": "高",
                "impact_summary": "盈利 / 景气",
                "summary": "事件边界待复核，仓位较重",
                "event_monitor_label": "事件待复核",
            }
        ]
    )

    assert line.startswith("优先复查 thesis:")
    assert "事件边界待复核" in line
    assert "当前事件边界已退回待复核" in line
    assert "政策影响层：配套细则" in line
    assert "事件优先级 高" in line
    assert "盈利 / 景气" in line
    assert "待确认" in line


def test_lookup_latest_symbol_reports_reads_latest_manifest_refs(tmp_path: Path) -> None:
    reviews_root = tmp_path / "reports" / "reviews"
    reviews_root.mkdir(parents=True)
    older = reviews_root / "scan_561380_older__release_manifest.json"
    older.write_text(
        (
            '{"report_type":"scan","generated_at":"2026-03-28 08:00:00",'
            '"markdown":"reports/scans/final/scan_561380_2026-03-28_client_final.md",'
            '"artifacts":{"symbol":"561380"}}'
        ),
        encoding="utf-8",
    )
    newer = reviews_root / "scan_561380_newer__release_manifest.json"
    newer.write_text(
        (
            '{"report_type":"scan","generated_at":"2026-03-29 08:00:00",'
            '"markdown":"reports/scans/final/scan_561380_2026-03-29_client_final.md",'
            '"artifacts":{"symbol":"561380"}}'
        ),
        encoding="utf-8",
    )

    refs = lookup_latest_symbol_reports(["561380"], reviews_root=reviews_root)

    assert refs["561380"][0]["generated_at"] == "2026-03-29 08:00:00"
    assert refs["561380"][0]["markdown"].endswith("scan_561380_2026-03-29_client_final.md")


def test_record_review_queue_persists_report_followup_and_last_run(tmp_path: Path, monkeypatch) -> None:
    repo = ThesisRepository(
        thesis_path=tmp_path / "thesis.json",
        review_queue_path=tmp_path / "thesis_review_queue.json",
    )
    monkeypatch.setattr(
        "src.storage.thesis.lookup_latest_symbol_reports",
        lambda symbols, reviews_root=None, limit=2: {
            "561380": [
                {
                    "report_type": "scan",
                    "generated_at": "2026-03-29 09:00:00",
                    "markdown": "reports/scans/final/scan_561380_2026-03-29_client_final.md",
                    "manifest": "reports/reviews/scan_561380__release_manifest.json",
                }
            ]
        },
    )

    repo.record_review_queue(
        [
            {
                "symbol": "561380",
                "priority": "高",
                "score": 92,
                "summary": "事件边界待复核，仓位较重",
                "thesis_state_trigger": "事件边界待复核",
                "thesis_state_summary": "当前事件边界已退回待复核，旧 thesis 先不要按已验证继续沿用。",
                "event_detail": "政策影响层：配套细则",
                "event_importance_label": "高",
                "event_importance_reason": "必须前置复核，因为政策细则可能改写盈利 / 景气。",
                "impact_summary": "盈利 / 景气",
                "thesis_scope": "待确认",
                "event_monitor_label": "事件待复核",
                "has_thesis": True,
            }
        ],
        source="briefing_daily",
        as_of="2026-03-29 10:00:00",
    )
    repo.record_review_run(
        "561380",
        action="重跑 scan",
        status="completed",
        artifact_path="reports/scan_561380_2026-03-29.md",
        summary="事件状态已从待复核回到已消化。",
        recorded_at="2026-03-29 10:30:00",
    )

    payload = repo.load_review_queue()["history"]["561380"]
    lines = summarize_review_queue_history_lines(payload)

    assert payload["report_followup"]["status"] == "待更新正式稿"
    assert payload["report_followup"]["needs_refresh"] is True
    assert payload["last_run"]["action"] == "重跑 scan"
    assert any("最近复查焦点: 政策影响层：配套细则；事件边界待复核；事件优先级 高；盈利 / 景气；待确认" in line for line in lines)
    assert any("最近状态触发: 事件边界待复核" in line for line in lines)
    assert any("最近状态解释: 当前事件边界已退回待复核" in line for line in lines)
    assert any("最近优先级判断: 必须前置复核" in line for line in lines)
    assert any("最近正式稿状态: 待更新正式稿" in line for line in lines)
    assert any("最近复查动作: 重跑 scan" in line for line in lines)
    assert any("最近复查产物" in line for line in lines)


def test_record_review_run_marks_followup_as_review_only_when_no_final_exists(tmp_path: Path, monkeypatch) -> None:
    repo = ThesisRepository(
        thesis_path=tmp_path / "thesis.json",
        review_queue_path=tmp_path / "thesis_review_queue.json",
    )
    monkeypatch.setattr(
        "src.storage.thesis.lookup_latest_symbol_reports",
        lambda symbols, reviews_root=None, limit=2: {},
    )

    repo.record_review_queue(
        [
            {
                "symbol": "600313",
                "priority": "高",
                "score": 90,
                "summary": "财报边界待补充，先别直接沿用旧 thesis",
                "thesis_state_trigger": "事件边界待复核",
                "thesis_state_summary": "财报摘要还没补齐，旧 thesis 先退回复核。",
                "event_detail": "财报摘要：现金流/合同负债改善",
                "event_importance_label": "高",
                "event_importance_reason": "必须前置复核，因为现金流改善可能改写盈利 / 景气判断。",
                "impact_summary": "盈利 / 景气",
                "thesis_scope": "待确认",
                "event_monitor_label": "事件待复核",
                "has_thesis": True,
            }
        ],
        source="briefing_daily",
        as_of="2026-03-29 11:00:00",
    )
    repo.record_review_run(
        "600313",
        action="补 thesis review",
        status="completed",
        artifact_path="reports/research/internal/research_600313_review_2026-03-29_110000.md",
        summary="复查已完成，但还没有新的正式稿。",
        recorded_at="2026-03-29 11:30:00",
    )

    payload = repo.load_review_queue()["history"]["600313"]
    assert payload["report_followup"]["status"] == "已有复查稿，暂无正式稿"
    assert payload["report_followup"]["needs_refresh"] is True
