"""Tests for research command helpers."""

from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

import pandas as pd

from src.commands.research import (
    ResearchIntent,
    _asset_trade_plan_payload,
    _asset_next_step_lines,
    _asset_scenario_lines,
    _classify_question,
    _empty_portfolio_risk_payload,
    _fast_market_overview,
    _flow_and_sentiment_payload,
    _market_diagnosis_payload,
    _market_takeaway,
    _policy_payload,
    _pick_evidence_lines,
    _portfolio_risk_payload,
    _pulse_stats,
    _current_asset_event_digest_payload,
    _render_research_markdown,
    _snapshot_bias,
    _thesis_event_memory_payload,
    _trade_plan_focus,
    run_research_review,
)
from src.processors.risk import RiskAnalyzer
from src.storage.thesis import ThesisRepository


def test_classify_question_prefers_portfolio_risk_when_holdings_exist() -> None:
    intent = _classify_question("如果美股跌20%我的组合会怎样", ["QQQM"], has_holdings=True)

    assert intent.kind == "portfolio_risk"
    assert intent.needs_risk is True
    assert intent.needs_regime is True


def test_classify_question_keeps_symbol_risk_as_asset_thesis() -> None:
    intent = _classify_question("561380 风险大吗", ["561380"], has_holdings=True)

    assert intent.kind == "asset_thesis"
    assert intent.needs_risk is True


def test_classify_question_keeps_symbol_position_question_as_asset_thesis() -> None:
    intent = _classify_question("561380 现在适合上多少仓位，做得进去吗", ["561380"], has_holdings=False)

    assert intent.kind == "asset_thesis"


def test_trade_plan_focus_detects_position_and_execution_questions() -> None:
    focus = _trade_plan_focus("561380 现在适合上多少仓位，做得进去吗")

    assert focus["needs_trade_plan"] is True
    assert focus["ask_position"] is True
    assert focus["ask_execution"] is True


def test_classify_question_marks_market_diagnosis_without_symbol() -> None:
    intent = _classify_question("为什么最近市场有点别扭", [], has_holdings=False)

    assert intent.kind == "market_diagnosis"
    assert intent.needs_flow is True
    assert intent.needs_regime is True


def test_classify_question_marks_policy_impact_without_symbol() -> None:
    intent = _classify_question("电网政策接下来怎么看", [], has_holdings=False)

    assert intent.kind == "policy_impact"
    assert intent.needs_regime is True


def test_classify_question_keeps_policy_impact_even_when_alias_resolves_symbol() -> None:
    intent = _classify_question("电网政策接下来怎么看", ["561380"], has_holdings=False)

    assert intent.kind == "policy_impact"


def test_classify_question_marks_portfolio_risk_even_without_holdings() -> None:
    intent = _classify_question("我的组合现在风险大吗", [], has_holdings=False)

    assert intent.kind == "portfolio_risk"
    assert intent.needs_risk is False


def test_snapshot_bias_identifies_strong_trend() -> None:
    payload = _snapshot_bias(
        {"return_20d": 0.12},
        {
            "ma_system": {"signal": "bullish"},
            "macd": {"signal": "bullish"},
            "rsi": {"RSI": 66.0},
            "volume": {"vol_ratio": 1.2},
        },
    )

    assert payload["bias"] == "偏强"
    assert "顺势跟踪" in payload["answer"]


def test_asset_scenario_lines_expose_probability_frame() -> None:
    lines = _asset_scenario_lines(
        "561380",
        {"return_20d": 0.12},
        {
            "rsi": {"RSI": 66.0},
            "volume": {"vol_ratio": 1.0},
            "ma_system": {"signal": "bullish"},
        },
        "偏强",
    )

    assert any("主场景" in item for item in lines)
    assert any("次场景" in item for item in lines)


def test_render_research_markdown_has_structured_sections() -> None:
    markdown = _render_research_markdown(
        question="561380 现在还能不能买",
        intent=ResearchIntent("asset_thesis", "标的研究 / 交易问题", True, False, True),
        symbols=["561380"],
        direct_answer_lines=["561380 当前更像回踩确认，而不是无条件追高。"],
        proxy_contract={
            "market_flow": {
                "interpretation": "黄金相对成长更抗跌，市场风格偏防守。",
                "confidence_label": "中",
                "coverage_summary": "科技/黄金/国内/海外代理样本",
            },
            "social_sentiment": {"covered": 1, "total": 1, "confidence_labels": {"中": 1}},
        },
        evidence_lines=["[宏观] 当前 macro regime 为 recovery。", "[行情/技术] 561380 近20日 +8.0%。"],
        provenance_lines=["[行情时点] 561380 日线 as_of `2026-03-13`，来源 `本地历史日线链路`。"],
        risk_lines=["RSI 已偏热。"],
        action_lines=["先跑 561380 对应的 scan。"],
    )

    assert "## 一句话回答" in markdown
    assert "## 代理信号与限制" in markdown
    assert "## 证据" in markdown
    assert "## 证据时点与来源" in markdown
    assert "## 风险与不确定性" in markdown
    assert "## 下一步" in markdown
    assert "类型: 标的研究 / 交易问题" in markdown


def test_render_research_markdown_includes_thesis_memory_section() -> None:
    markdown = _render_research_markdown(
        question="561380 现在为什么还值得看",
        intent=ResearchIntent("asset_thesis", "标的研究 / 交易问题", True, False, False),
        symbols=["561380"],
        direct_answer_lines=["当前更像趋势未坏但还要看兑现。"],
        proxy_contract={},
        evidence_lines=["[事件消化] 当前 `已消化` / `公告`：判断已下沉到公司级执行。"],
        provenance_lines=["[事件消化] 当前事件快照 as_of `2026-03-29`。"],
        risk_lines=["旧催化不能直接当作已经验证。"],
        action_lines=["先看公司级兑现。"],
        thesis_memory_lines=["上次为什么看：`电网投资提升`。", "这次什么变了：事件状态从 `待补充` 升到 `已消化`。"],
    )

    assert "## 研究记忆 / Thesis Ledger" in markdown
    assert "这次什么变了" in markdown


def test_thesis_event_memory_payload_records_delta(tmp_path: Path) -> None:
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
            "lead_detail": "主题事件：景气/价格验证",
            "importance": "medium",
            "lead_title": "特高压投资节奏跟踪",
            "impact_summary": "景气 / 资金偏好",
            "thesis_scope": "待确认",
            "importance_reason": "先观察，因为产业链景气验证还没下沉成公司级兑现。",
            "changed_what": "先按产业链热度跟踪。",
            "next_step": "补公司级事件。",
        },
        source="scan",
        recorded_at="2026-03-28 10:00:00",
    )

    payload = _thesis_event_memory_payload(
        symbol="561380",
        thesis_repo=repo,
        current_event_digest={
            "status": "已消化",
            "lead_layer": "公告",
            "lead_detail": "公告类型：中标/订单",
            "importance": "high",
            "lead_title": "国电南瑞中标国家电网项目",
            "impact_summary": "盈利 / 景气",
            "thesis_scope": "thesis变化",
            "importance_reason": "优先前置，因为公司级执行事件已开始改写盈利 / 景气。",
            "changed_what": "已经下沉到公司级执行。",
            "next_step": "继续盯兑现节奏。",
        },
        source="research",
        recorded_at="2026-03-29 10:00:00",
    )

    assert any("上次为什么看" in item for item in payload["memory_lines"])
    assert any("这次什么变了" in item for item in payload["memory_lines"])
    assert any("上次事件细分" in item and "事件优先级 `medium`" in item and "优先级判断" in item for item in payload["memory_lines"])
    assert any("这次事件细分" in item and "盈利 / 景气" in item and "thesis变化" in item for item in payload["memory_lines"])
    assert any("这次事件细分" in item and "事件优先级 `high`" in item and "优先级判断" in item for item in payload["memory_lines"])
    assert any("thesis 状态" in item and "升级" in item and "事件完成消化" in item for item in payload["memory_lines"])
    assert any("thesis ledger" in item for item in payload["provenance_lines"])
    assert repo.get("561380")["event_digest_snapshot"]["status"] == "已消化"


def test_current_asset_event_digest_payload_surfaces_event_depth(monkeypatch) -> None:
    monkeypatch.setattr("src.commands.research.detect_asset_type", lambda symbol, config: "cn_etf")
    monkeypatch.setattr("src.commands.research.build_market_context", lambda config, relevant_asset_types=None: {})
    monkeypatch.setattr("src.commands.research.analyze_opportunity", lambda symbol, asset_type, config, context=None, today_mode=False: {"generated_at": "2026-03-29 10:00:00"})
    monkeypatch.setattr("src.commands.research.build_event_digest", lambda analysis: analysis)
    monkeypatch.setattr(
        "src.commands.research.summarize_event_digest_contract",
        lambda payload: {
            "status": "已消化",
            "lead_layer": "公告",
            "lead_detail": "公告类型：中标/订单",
            "importance": "high",
            "impact_summary": "盈利 / 景气",
            "thesis_scope": "thesis变化",
            "importance_reason": "优先前置，因为公司级执行事件已开始改写盈利 / 景气。",
            "changed_what": "已经下沉到公司级执行。",
        },
    )

    payload = _current_asset_event_digest_payload("561380", {"technical": {}})

    assert payload["contract"]["lead_detail"] == "公告类型：中标/订单"
    assert any("事件细分 `公告类型：中标/订单`" in item for item in payload["evidence_lines"])
    assert any("当前优先级 `high`" in item for item in payload["evidence_lines"])
    assert any("更直接影响 `盈利 / 景气`" in item for item in payload["evidence_lines"])
    assert any("当前更像 `thesis变化`" in item for item in payload["evidence_lines"])
    assert any("优先级判断：优先前置" in item for item in payload["evidence_lines"])


def test_run_research_review_persists_internal_artifact_and_summary(tmp_path: Path, monkeypatch) -> None:
    repo = ThesisRepository(thesis_path=tmp_path / "thesis.json")
    repo.upsert(
        symbol="561380",
        core_assumption="电网投资提升",
        validation_metric="投资完成额同比 > 10%",
        stop_condition="估值过高且增速下滑",
        holding_period="6-12个月",
    )
    monkeypatch.setattr(
        "src.commands.research.load_config",
        lambda _path=None: {"technical": {}},
    )
    monkeypatch.setattr(
        "src.commands.research._symbol_snapshot",
        lambda symbol, config: {
            "symbol": symbol,
            "answer": "当前更像趋势未坏但还要看兑现。",
            "risks": ["短线波动仍在。"],
            "scenario_lines": ["[场景概率] 主场景仍是震荡上行。"],
            "provenance_lines": ["[行情时点] 561380 日线 as_of `2026-03-29`。"],
            "evidence_lines": ["561380: 最新价 2.230，近20日 +6.00%。"],
        },
    )
    monkeypatch.setattr(
        "src.commands.research._current_asset_event_digest_payload",
        lambda symbol, config: {
            "contract": {
                "status": "已消化",
                "lead_layer": "公告",
                "lead_title": "国电南瑞中标国家电网项目",
                "changed_what": "已经下沉到公司级执行。",
                "next_step": "继续盯兑现节奏。",
            },
            "evidence_lines": ["[事件消化] 当前 `已消化` / `公告`：已经下沉到公司级执行。"],
            "provenance_lines": ["[事件消化] 当前事件快照 as_of `2026-03-29`。"],
            "risk_lines": [],
        },
    )
    monkeypatch.setattr(
        "src.commands.research.resolve_project_path",
        lambda path="": tmp_path / str(path),
    )

    payload = run_research_review(
        "561380",
        question="这次什么变了",
        thesis_repo=repo,
        recorded_at="2026-03-29 10:00:00",
    )

    assert "这次什么变了" in payload["markdown"]
    assert payload["artifact_path"].endswith("research_561380_review_2026-03-29_100000.md")
    assert Path(payload["artifact_path"]).exists()
    assert "这次先记住当前是" in payload["summary"]
    assert repo.get("561380")["thesis_state_snapshot"]["state"] == "升级"


def test_market_takeaway_marks_split_market_and_proxy_risk() -> None:
    takeaway = _market_takeaway(
        day_theme_label="背景宏观主导",
        breadth={"up_count": 2300, "down_count": 2600},
        flow_report={"risk_bias": "neutral", "domestic_bias": "neutral", "lines": ["成长与黄金的相对强弱接近。"], "method": "proxy"},
        pulse_stats={"zt_count": 42, "dt_count": 26, "strong_count": 71},
    )

    assert takeaway["state"] == "split_market"
    assert "分歧市" in takeaway["answer_lines"][0]
    assert any("代理" in item for item in takeaway["risk_lines"])


def test_run_research_review_surfaces_outdated_final_status(tmp_path, monkeypatch) -> None:
    repo = ThesisRepository(
        thesis_path=tmp_path / "thesis.json",
        review_queue_path=tmp_path / "thesis_review_queue.json",
    )
    repo.upsert(
        symbol="561380",
        core_assumption="电网投资提升",
        validation_metric="投资完成额同比 > 10%",
        stop_condition="估值过高且增速下滑",
        holding_period="6-12个月",
    )
    monkeypatch.setattr(
        "src.storage.thesis.lookup_latest_symbol_reports",
        lambda symbols, reviews_root=None, limit=2: {
            "561380": [
                {
                    "report_type": "scan",
                    "generated_at": "2026-03-29 09:00:00",
                    "markdown": "reports/scans/final/scan_561380_2026-03-29_client_final.md",
                    "manifest": "reports/reviews/scans/final/scan_561380_2026-03-29_client_final__release_manifest.json",
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
        artifact_path="reports/research/internal/research_561380_review_2026-03-29_103000.md",
        summary="复查已完成，但当前正式稿还没更新。",
        recorded_at="2026-03-29 10:30:00",
    )

    monkeypatch.setattr("src.commands.research.load_config", lambda _path=None: {"technical": {}})
    monkeypatch.setattr(
        "src.commands.research._symbol_snapshot",
        lambda symbol, config: {
            "symbol": symbol,
            "answer": "当前更像趋势未坏但还要看兑现。",
            "risks": ["短线波动仍在。"],
            "scenario_lines": ["[场景概率] 主场景仍是震荡上行。"],
            "provenance_lines": ["[行情时点] 561380 日线 as_of `2026-03-29`。"],
            "evidence_lines": ["561380: 最新价 2.230，近20日 +6.00%。"],
        },
    )
    monkeypatch.setattr(
        "src.commands.research._current_asset_event_digest_payload",
        lambda symbol, config: {
            "contract": {
                "status": "已消化",
                "lead_layer": "公告",
                "lead_title": "国电南瑞中标国家电网项目",
                "changed_what": "已经下沉到公司级执行。",
                "next_step": "继续盯兑现节奏。",
            },
            "evidence_lines": ["[事件消化] 当前 `已消化` / `公告`：已经下沉到公司级执行。"],
            "provenance_lines": ["[事件消化] 当前事件快照 as_of `2026-03-29`。"],
            "risk_lines": [],
        },
    )
    monkeypatch.setattr("src.commands.research.resolve_project_path", lambda path="": tmp_path / str(path))

    payload = run_research_review(
        "561380",
        question="这次什么变了",
        thesis_repo=repo,
        recorded_at="2026-03-29 10:40:00",
    )

    assert "最近正式稿状态: 待更新正式稿" in payload["markdown"]
    assert "旧正式稿当前不能直接沿用" in payload["markdown"]


def test_pick_evidence_lines_prioritizes_market_for_market_diagnosis() -> None:
    lines = _pick_evidence_lines(
        ResearchIntent("market_diagnosis", "市场状态 / 风格问答", True, False, True),
        {
            "market": ["[市场主线] 当前主线偏防守。", "[市场宽度] 上涨 2000 家，下跌 2800 家。"],
            "regime": ["[宏观] 当前 macro regime 为 recovery。"],
            "flow": ["[资金/情绪代理] 黄金相对成长更抗跌。"],
            "snapshot": ["[行情/技术] 561380 近20日 +8.0%。"],
        },
        limit=4,
    )

    assert lines[0].startswith("[市场主线]")
    assert lines[1].startswith("[市场宽度]")
    assert lines[2].startswith("[资金/情绪代理]")


def test_flow_and_sentiment_payload_exposes_proxy_confidence(monkeypatch) -> None:
    class _FakeAnalyzer:
        def __init__(self, _history):
            pass

        def generate_scorecard(self, _config):
            return {"ma_system": {"signal": "bullish"}, "volume": {"vol_ratio": 1.3}}

    class _FakeSentimentCollector:
        def __init__(self, _config):
            pass

        def collect(self, symbol, snapshot):
            return {
                "symbol": symbol,
                "aggregate": {
                    "interpretation": "情绪指数 62.0，当前未出现极端一致预期。",
                    "confidence_label": "中",
                    "limitations": ["这是价格和量能行为推导出的情绪代理，不是真实发帖抓取。"],
                    "downgrade_impact": "更适合提示拥挤和冷淡，不适合独立下交易结论。",
                },
            }

    class _FakeFlowCollector:
        def __init__(self, _config):
            pass

        def collect(self, snapshots):
            assert snapshots
            return {
                "lines": ["黄金相对成长更抗跌，资金风格偏防守。"],
                "confidence_label": "中",
                "limitations": ["这是相对强弱代理，不是 ETF 申购赎回原始流向数据。"],
                "downgrade_impact": "当前只适合辅助判断风格切换，不适合单独决定交易。",
            }

    monkeypatch.setattr("src.commands.research.detect_asset_type", lambda symbol, config: "cn_etf")
    monkeypatch.setattr("src.commands.research.fetch_asset_history", lambda symbol, asset_type, config: object())
    monkeypatch.setattr("src.commands.research.normalize_ohlcv_frame", lambda history: history)
    monkeypatch.setattr(
        "src.commands.research.compute_history_metrics",
        lambda history: {"return_1d": 0.01, "return_5d": 0.03, "return_20d": 0.12},
    )
    monkeypatch.setattr("src.commands.research.TechnicalAnalyzer", _FakeAnalyzer)
    monkeypatch.setattr("src.commands.research.SocialSentimentCollector", _FakeSentimentCollector)
    monkeypatch.setattr("src.commands.research.GlobalFlowCollector", _FakeFlowCollector)

    payload = _flow_and_sentiment_payload(["561380"], {})

    assert any("代理置信度 `中`" in item for item in payload["evidence_lines"])
    assert any("情绪代理限制" in item for item in payload["risk_lines"])
    assert any("资金流代理影响" in item for item in payload["risk_lines"])
    assert payload["proxy_contract"]["market_flow"]["confidence_label"] == "中"
    assert payload["proxy_contract"]["social_sentiment"]["covered"] == 1


def test_pulse_stats_counts_dataframe_rows() -> None:
    stats = _pulse_stats(
        {
            "zt_pool": pd.DataFrame([{"a": 1}, {"a": 2}]),
            "dt_pool": pd.DataFrame([{"a": 1}]),
            "strong_pool": pd.DataFrame([{"a": 1}, {"a": 2}, {"a": 3}]),
        }
    )

    assert stats == {"zt_count": 2, "dt_count": 1, "strong_count": 3}


def test_policy_payload_extracts_theme_and_direction() -> None:
    payload = _policy_payload("关于加快新型电力系统建设并推进特高压投资的通知", [])

    assert any("偏支持" in item for item in payload["answer_lines"])
    assert any("电网" in item or "特高压" in item for item in payload["evidence_lines"])
    assert any("政策口径" in item for item in payload["provenance_lines"])
    assert payload["risk_lines"]


def test_policy_payload_keyword_question_uses_keyword_inference_labels() -> None:
    payload = _policy_payload("电网政策接下来怎么看", [])

    assert any("偏支持（关键词推断）" in item for item in payload["answer_lines"])
    assert any("主题跟踪阶段" in item for item in payload["answer_lines"])
    assert any("场景概率" in item for item in payload["evidence_lines"])
    assert any("keyword" in item for item in payload["provenance_lines"])


def test_empty_portfolio_risk_payload_calls_out_missing_holdings() -> None:
    payload = _empty_portfolio_risk_payload("我的组合现在风险大吗")

    assert "没有录入可分析的持仓" in payload["answer_lines"][0]
    assert any("组合为空" in item for item in payload["evidence_lines"])
    assert any("泛泛市场判断" in item for item in payload["risk_lines"])


def test_asset_next_step_lines_for_strong_trend_mentions_ma20() -> None:
    lines = _asset_next_step_lines(
        {
            "symbol": "561380",
            "bias": "偏强",
            "metrics": {"last_close": 2.236},
            "technical": {"ma_system": {"mas": {"MA20": 2.105, "MA60": 1.982}}},
        }
    )

    assert "MA20" in lines[0]
    assert "2.105" in lines[0]


def test_asset_trade_plan_payload_surfaces_position_execution_and_timing(monkeypatch) -> None:
    class _FakeRepo:
        def list_holdings(self):
            return [{"symbol": "QQQM", "market_value": 100000.0}]

    monkeypatch.setattr(
        "src.commands.research.build_trade_plan",
        lambda **kwargs: {
            "symbol": "561380",
            "suggested_max_weight": 0.12,
            "projected_weight": 0.18,
            "horizon": {
                "label": "波段跟踪（2-6周）",
                "fit_reason": "趋势、轮动或风险收益比已经有基础，但更依赖未来几周节奏，而不是长周期基本面完全兑现。",
                "misfit_reason": "现在不适合把它当长期底仓，也不适合只按隔夜消息去赌超短。",
            },
            "execution": {
                "tradability_label": "可成交",
                "estimated_total_cost": 28.0,
                "avg_turnover_20d": 260_000_000.0,
                "participation_rate": 0.02,
                "slippage_bps": 8.6,
                "liquidity_note": "参与率可控。",
                "execution_note": "未含极端行情冲击。",
            },
            "current_risk": {"annual_vol": 0.12, "beta": 0.90},
            "projected_risk": {"annual_vol": 0.15, "beta": 0.98},
            "decision_snapshot": {"market_data_as_of": "2026-03-13", "market_data_source": "561380", "notes": ["只使用当时可见日线。"]},
            "alerts": ["预演后仓位已高于建议上限。"],
        },
    )
    monkeypatch.setattr("src.commands.research.ThesisRepository", lambda: object())

    payload = _asset_trade_plan_payload(
        question="561380 现在适合上多少仓位，做得进去吗",
        snapshot={
            "symbol": "561380",
            "asset_type": "cn_etf",
            "metrics": {"last_close": 2.1},
        },
        repo=_FakeRepo(),
        config={},
    )

    assert any("单票上限" in item or "不适合直接重仓" in item for item in payload["answer_lines"])
    assert any("可成交性" in item or "预估总成本" in item for item in payload["answer_lines"])
    assert any("周期判断" in item for item in payload["evidence_lines"])
    assert any("周期错配风险" in item for item in payload["risk_lines"])
    assert any("仓位预演" in item for item in payload["evidence_lines"])
    assert any("时点快照" in item for item in payload["evidence_lines"])
    assert any("交易时点" in item for item in payload["provenance_lines"])
    assert any("真实金额" in item for item in payload["action_lines"])


def test_fast_market_overview_reads_stale_cache(monkeypatch) -> None:
    class _FakeCollector:
        def __init__(self, _config):
            self.overview_path = "fake.yaml"

        def _load_cache(self, cache_key, ttl_hours=None, allow_stale=False):
            if cache_key == "market_overview:domestic_spot:v1":
                return pd.DataFrame(
                    [
                        {"代码": "000001", "昨收": 3000, "最新价": 3030, "涨跌幅": 1.0},
                    ]
                )
            if cache_key == "market_overview:a_spot_em:v1":
                return pd.DataFrame(
                    [
                        {"涨跌幅": 1.0, "成交额": 100000000},
                        {"涨跌幅": -0.5, "成交额": 80000000},
                    ]
                )
            return None

    monkeypatch.setattr("src.commands.research.MarketOverviewCollector", _FakeCollector)
    monkeypatch.setattr(
        "src.commands.research.load_yaml",
        lambda _path, default=None: {"domestic_indices": [{"symbol": "000001", "name": "上证指数"}]},
    )

    overview = _fast_market_overview({})

    assert overview["source"] == "cache_snapshot"
    assert overview["domestic_indices"][0]["symbol"] == "000001"
    assert overview["breadth"]["up_count"] == 1
    assert overview["breadth"]["down_count"] == 1


def test_portfolio_risk_payload_highlights_concentration() -> None:
    context = SimpleNamespace(
        status={
            "holdings": [
                {"symbol": "561380", "weight": 0.42, "sector": "电网", "region": "CN"},
                {"symbol": "QQQM", "weight": 0.35, "sector": "科技", "region": "US"},
                {"symbol": "GLD", "weight": 0.23, "sector": "黄金", "region": "US"},
            ],
            "region_exposure": {"CN": 0.42, "US": 0.58},
            "sector_exposure": {"电网": 0.42, "科技": 0.35, "黄金": 0.23},
        },
        coverage_notes=[],
    )
    returns = pd.DataFrame(
        {
            "561380": [0.01, -0.02, 0.005, 0.004],
            "QQQM": [0.011, -0.019, 0.004, 0.003],
            "GLD": [-0.002, 0.001, 0.003, 0.002],
        }
    )
    analyzer = RiskAnalyzer(returns, {"561380": 0.42, "QQQM": 0.35, "GLD": 0.23})
    report = {
        "concentration_alerts": [{"warning": "561380 和 QQQM 高度正相关 (0.95)，分散效果有限。"}],
        "beta": {"beta": 1.18, "interpretation": "组合 Beta 为 1.18。"},
        "var_95": {"interpretation": "在 95% 置信水平下，单日损失大致不超过 2.50%。"},
        "max_drawdown": {"interpretation": "历史最大回撤为 -12.40%。"},
    }

    payload = _portfolio_risk_payload(
        question="我的组合现在风险大吗",
        context=context,
        report=report,
        analyzer=analyzer,
        config={},
    )

    assert "相关性偏高" in payload["answer_lines"][0]
    assert any("最大持仓" in item for item in payload["evidence_lines"])
    assert any("场景概率" in item for item in payload["evidence_lines"])
    assert any("组合时点" in item for item in payload["provenance_lines"])
    assert any("Beta" in item for item in payload["risk_lines"])
    assert payload["action_lines"]


def test_market_diagnosis_payload_uses_proxy_flow_and_probability(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.commands.research._light_market_context",
        lambda config, watchlist: {
            "day_theme": {"label": "能源冲击 + 地缘风险"},
            "pulse": {"zt_pool": pd.DataFrame([{"a": 1}]), "dt_pool": pd.DataFrame([]), "strong_pool": pd.DataFrame([{"a": 1}, {"a": 2}])},
            "news_report": {"mode": "proxy"},
            "notes": [],
        },
    )
    monkeypatch.setattr(
        "src.commands.research._fast_market_overview",
        lambda config: {
            "breadth": {"up_count": 2200, "down_count": 3000},
            "domestic_indices": [
                {"name": "上证指数", "symbol": "000001", "change_pct": -0.008},
                {"name": "深证成指", "symbol": "399001", "change_pct": -0.006},
            ],
            "source": "cache_snapshot",
        },
    )
    monkeypatch.setattr(
        "src.commands.research._market_flow_payload",
        lambda config, watchlist: {
            "risk_bias": "risk_off",
            "domestic_bias": "neutral",
            "lines": ["黄金相对成长更抗跌，市场风格偏防守。"],
            "confidence_label": "中",
            "limitations": ["这是相对强弱代理，不是原始资金流。"],
            "downgrade_impact": "更适合辅助判断风格切换，不适合单独下交易结论。",
            "method": "proxy",
        },
    )

    payload = _market_diagnosis_payload({})

    assert any("代理置信度 `中`" in item for item in payload["evidence_lines"])
    assert any("场景概率" in item for item in payload["evidence_lines"])
    assert any("市场时点" in item for item in payload["provenance_lines"])
    assert any("市场风格代理限制" in item for item in payload["risk_lines"])
    assert payload["proxy_contract"]["market_flow"]["confidence_label"] == "中"
