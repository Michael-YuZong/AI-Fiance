from __future__ import annotations

import sys

from src.commands import portfolio as portfolio_module


def test_portfolio_whatif_main_renders_trade_preview(monkeypatch, capsys) -> None:
    monkeypatch.setattr(portfolio_module, "load_config", lambda _path=None: {})
    monkeypatch.setattr(portfolio_module, "PortfolioRepository", lambda: object())
    monkeypatch.setattr(portfolio_module, "ThesisRepository", lambda: object())
    monkeypatch.setattr(
        portfolio_module,
        "build_trade_plan",
        lambda **kwargs: {
            "action": "buy",
            "symbol": "561380",
            "name": "电网ETF",
            "amount": 20_000.0,
            "price": 2.1,
            "headline": "561380 买入后仓位约 `12.0%`，仍在组合和执行约束内。",
            "horizon": {
                "label": "中线配置（1-3月）",
                "fit_reason": "原始 thesis 的预期周期写的是 `1-3月`，当前更适合按 `中线配置（1-3月）` 的框架理解。",
                "misfit_reason": "现在不适合当成纯隔夜交易，也还没强到可以长期不复核地持有一年以上。",
            },
            "current_weight": 0.0,
            "projected_weight": 0.12,
            "suggested_max_weight": 0.18,
            "current_sector": "电网",
            "projected_sector_weight": 0.18,
            "current_region": "CN",
            "projected_region_weight": 0.52,
            "current_risk": {"annual_vol": 0.12, "beta": 0.92},
            "projected_risk": {"annual_vol": 0.14, "beta": 0.98},
            "execution": {
                "execution_mode": "场内成交",
                "tradability_label": "可成交",
                "avg_turnover_20d": 220_000_000.0,
                "participation_rate": 0.02,
                "slippage_bps": 9.0,
                "estimated_slippage_cost": 18.0,
                "fee_rate": 0.0003,
                "estimated_fee_cost": 6.0,
                "estimated_total_cost": 24.0,
                "liquidity_note": "参与率可控。",
                "execution_note": "未含极端行情冲击。",
            },
            "portfolio_overlap": {
                "summary_line": "这条建议和现有组合最重的行业 `电网` 同线，重复度较高，更像同一主线延伸，而不是完全新方向。",
                "overlap_label": "同一行业主线加码",
                "conflict_label": "暂未看到明显主线冲突",
                "style_summary_line": "当前组合风格偏 `进攻偏重`，最重风格是 `进攻` `55.0%`。",
                "style_direction_label": "进攻偏重",
                "candidate_style_bucket": "进攻",
                "style_priority_hint": "如果只是同风格加码，优先级低于补新方向。",
                "style_detail_line": "风格暴露: 进攻 55.0% / 防守 25.0% / 均衡 20.0%",
                "detail_lines": [
                    "当前最重行业: `电网` `28.0%`",
                    "当前最重地区: `CN` `52.0%`",
                    "同主题/行业持仓: 561380 (电网ETF, 28.0%)",
                    "同地区持仓: 510300 (沪深300ETF, 42.0%) / 561380 (电网ETF, 28.0%)",
                ],
            },
            "decision_snapshot": {
                "recorded_at": "2026-03-13T10:00:00",
                "market_data_as_of": "2026-03-13",
                "market_data_source": "561380",
                "history_window": "3y",
                "thesis_snapshot_at": "2026-03-12 20:00:00",
                "notes": ["只使用当时可见日线。"],
            },
            "alerts": [],
        },
    )
    monkeypatch.setattr(sys, "argv", ["portfolio", "whatif", "buy", "561380", "2.1", "20000"])

    portfolio_module.main()

    captured = capsys.readouterr()
    assert "# 交易预演" in captured.out
    assert "## 周期判断" in captured.out
    assert "## 组合联动" in captured.out
    assert "## 风格与方向" in captured.out
    assert "重复度: `同一行业主线加码`" in captured.out
    assert "优先级建议:" in captured.out
    assert "## 执行成本与可成交性" in captured.out
    assert "## 时点与证据快照" in captured.out


def test_portfolio_status_surfaces_priority_hint(monkeypatch, capsys) -> None:
    monkeypatch.setattr(portfolio_module, "load_config", lambda _path=None: {})

    class _FakeRepo:
        def build_status(self, latest_prices):
            assert latest_prices["561380"] == 2.23
            return {
                "base_currency": "CNY",
                "total_value": 100_000.0,
                "holdings": [
                    {
                        "symbol": "561380",
                        "name": "电网ETF",
                        "quantity": 1000.0,
                        "cost_basis": 2.10,
                        "latest_price": 2.23,
                        "market_value": 2230.0,
                        "weight": 0.28,
                        "pnl": 130.0,
                    }
                ],
                "region_exposure": {"CN": 1.0},
                "sector_exposure": {"电网": 1.0},
            }

    monkeypatch.setattr(portfolio_module, "PortfolioRepository", lambda: _FakeRepo())
    monkeypatch.setattr(portfolio_module, "_load_latest_prices", lambda config, repo: {"561380": 2.23})  # noqa: ARG005
    monkeypatch.setattr(
        portfolio_module,
        "build_portfolio_overlap_summary",
        lambda status: {  # noqa: ARG005
            "summary_line": "当前组合行业集中度偏高，更像同主线内部分配。",
            "detail_lines": ["当前最重行业: `电网` `100.0%`"],
            "style_summary_line": "当前组合风格偏 `进攻偏重`。",
            "style_detail_line": "风格暴露: 进攻 60.0% / 防守 20.0% / 均衡 20.0%",
            "style_priority_hint": "如果只选一个，优先补新方向，而不是继续压同一主线。",
        },
    )
    monkeypatch.setattr(sys, "argv", ["portfolio", "status"])

    portfolio_module.main()

    captured = capsys.readouterr()
    assert "# 组合状态" in captured.out
    assert "## 风格与方向" in captured.out
    assert "优先级建议: 如果只选一个，优先补新方向，而不是继续压同一主线。" in captured.out


def test_portfolio_thesis_get_renders_event_digest_memory(monkeypatch, capsys) -> None:
    monkeypatch.setattr(portfolio_module, "load_config", lambda _path=None: {})
    monkeypatch.setattr(portfolio_module, "PortfolioRepository", lambda: object())

    class _FakeThesisRepo:
        def get(self, symbol):
            assert symbol == "561380"
            return {
                "core_assumption": "电网投资提升",
                "validation_metric": "投资完成额同比 > 10%",
                "stop_condition": "估值过高且增速下滑",
                "holding_period": "6-12个月",
                "created_at": "2026-03-20",
                "thesis_state_snapshot": {
                    "state": "升级",
                    "trigger": "事件完成消化",
                    "summary": "当前事件已完成消化并更新主导事件，thesis 可以按更高确定性理解。",
                    "recorded_at": "2026-03-29 10:00:00",
                },
                "event_digest_snapshot": {
                    "status": "已消化",
                    "lead_layer": "公告",
                    "lead_detail": "公告类型：中标/订单",
                    "lead_title": "国电南瑞中标国家电网项目",
                    "importance": "高",
                    "impact_summary": "盈利 / 景气",
                    "thesis_scope": "thesis变化",
                    "importance_reason": "优先前置，因为公司级执行事件已开始改写盈利 / 景气。",
                    "changed_what": "已经下沉到公司级执行。",
                    "recorded_at": "2026-03-29 10:00:00",
                },
                "event_digest_ledger": [{"delta": {"summary": "事件状态已升级。"}}],
            }

        def load_review_queue(self):
            return {
                "active": [
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
                        "recommended_action": "重跑 scan",
                        "active_days": 2,
                    }
                ],
                "history": {
                    "561380": {
                        "last_state": "active",
                        "report_followup": {
                            "status": "需复查",
                            "reason": "当前 thesis 已进入复查队列。",
                            "reports": [
                                {
                                    "report_type": "scan",
                                    "generated_at": "2026-03-29 09:00:00",
                                    "markdown": "reports/scans/final/scan_561380_2026-03-29_client_final.md",
                                }
                            ],
                        },
                        "last_run": {
                            "action": "重跑 scan",
                            "status": "completed",
                            "artifact_path": "reports/scan_561380_2026-03-29.md",
                            "summary": "事件状态已从待复核回到已消化。",
                            "recorded_at": "2026-03-29 10:30:00",
                        },
                    }
                },
            }

    monkeypatch.setattr(portfolio_module, "ThesisRepository", lambda: _FakeThesisRepo())
    monkeypatch.setattr(sys, "argv", ["portfolio", "thesis", "get", "561380"])

    portfolio_module.main()

    captured = capsys.readouterr()
    assert "# Thesis: 561380" in captured.out
    assert "Thesis 状态: 升级" in captured.out
    assert "状态触发: 事件完成消化" in captured.out
    assert "状态解释: 当前事件已完成消化并更新主导事件" in captured.out
    assert "最近事件状态" in captured.out
    assert "最近事件细分: 公告类型：中标/订单" in captured.out
    assert "最近事件优先级: `高`" in captured.out
    assert "最近影响层: `盈利 / 景气`" in captured.out
    assert "最近事件性质: `thesis变化`" in captured.out
    assert "最近优先级判断: 优先前置，因为公司级执行事件已开始改写盈利 / 景气。" in captured.out
    assert "最近前置事件" in captured.out
    assert "Event Ledger 条数" in captured.out
    assert "当前复查状态: 队列中" in captured.out
    assert "建议动作: 重跑 scan" in captured.out
    assert "当前状态解释: 当前事件边界已退回待复核" in captured.out
    assert "复查焦点: 政策影响层：配套细则；事件边界待复核；事件优先级 高；盈利 / 景气；待确认" in captured.out
    assert "最近状态触发: 事件边界待复核" in captured.out
    assert "最近状态解释: 当前事件边界已退回待复核" in captured.out
    assert "python -m src.commands.scan 561380" in captured.out
    assert "最近复查焦点: 政策影响层：配套细则；事件边界待复核；事件优先级 高；盈利 / 景气；待确认" in captured.out
    assert "最近优先级判断: 必须前置复核，因为政策细则可能改写盈利 / 景气。" in captured.out
    assert "最近正式稿状态: 需复查" in captured.out
    assert "最近复查动作: 重跑 scan" in captured.out
    assert "最近复查产物" in captured.out


def test_portfolio_thesis_check_surfaces_event_review_state(monkeypatch, capsys) -> None:
    monkeypatch.setattr(portfolio_module, "load_config", lambda _path=None: {"technical": {}})

    class _FakeRepo:
        def list_holdings(self):
            return [{"symbol": "561380", "asset_type": "cn_etf", "cost_basis": 2.1}]

    class _FakeThesisRepo:
        def get(self, symbol):
            assert symbol == "561380"
            return {
                "core_assumption": "电网投资提升",
                "validation_metric": "投资完成额同比 > 10%",
                "stop_condition": "估值过高且增速下滑",
                "holding_period": "6-12个月",
                "thesis_state_snapshot": {
                    "state": "待复核",
                    "trigger": "事件边界待复核",
                    "summary": "当前事件边界已退回待复核。",
                },
                "event_digest_snapshot": {
                    "status": "待复核",
                    "lead_layer": "政策",
                    "lead_detail": "政策影响层：配套细则",
                    "lead_title": "政策细则待补",
                    "impact_summary": "盈利 / 景气",
                    "thesis_scope": "待确认",
                    "changed_what": "旧结论先退回复核。",
                },
            }

        def list_all(self):
            return [{**self.get("561380"), "symbol": "561380"}]

    monkeypatch.setattr(portfolio_module, "PortfolioRepository", lambda: _FakeRepo())
    monkeypatch.setattr(portfolio_module, "ThesisRepository", lambda: _FakeThesisRepo())
    monkeypatch.setattr(portfolio_module, "detect_asset_type", lambda symbol, config: "cn_etf")
    monkeypatch.setattr(portfolio_module, "fetch_asset_history", lambda symbol, asset_type, config: object())
    monkeypatch.setattr(portfolio_module, "normalize_ohlcv_frame", lambda history: history)
    monkeypatch.setattr(
        portfolio_module,
        "compute_history_metrics",
        lambda history: {"last_close": 2.2, "return_20d": 0.02},
    )

    class _FakeTechnicalAnalyzer:
        def __init__(self, history):
            pass

        def generate_scorecard(self, config):
            return {"ma_system": {"signal": "bullish"}}

    monkeypatch.setattr(portfolio_module, "TechnicalAnalyzer", _FakeTechnicalAnalyzer)
    monkeypatch.setattr(sys, "argv", ["portfolio", "thesis", "check", "561380"])

    portfolio_module.main()

    captured = capsys.readouterr()
    assert "# Thesis 健康检查" in captured.out
    assert "优先复查队列" in captured.out
    assert "待复核" in captured.out
    assert "政策影响层：配套细则" in captured.out
    assert "盈利 / 景气" in captured.out
    assert "thesis 状态已退回待复核" in captured.out
    assert "当前事件边界已退回待复核" in captured.out
    assert "事件层=事件待复核" in captured.out
    assert "thesis 状态=待复核（事件边界待复核）" in captured.out
    assert "状态解释=当前事件边界已退回待复核" in captured.out
    assert "复查优先级=高" in captured.out


def test_portfolio_thesis_check_surfaces_stale_final_followup_action(monkeypatch, capsys) -> None:
    monkeypatch.setattr(portfolio_module, "load_config", lambda _path=None: {"technical": {}})

    class _FakeRepo:
        def list_holdings(self):
            return [{"symbol": "561380", "asset_type": "cn_etf", "cost_basis": 2.1}]

    class _FakeThesisRepo:
        def get(self, symbol):
            assert symbol == "561380"
            return {
                "core_assumption": "电网投资提升",
                "validation_metric": "投资完成额同比 > 10%",
                "stop_condition": "估值过高且增速下滑",
                "holding_period": "6-12个月",
                "event_digest_snapshot": {
                    "status": "待复核",
                    "lead_layer": "政策",
                    "lead_detail": "政策影响层：配套细则",
                    "impact_summary": "盈利 / 景气",
                    "thesis_scope": "待确认",
                },
            }

        def list_all(self):
            return [{**self.get("561380"), "symbol": "561380"}]

        def record_review_queue(self, queue, *, source="", as_of=""):
            return {"new_entries": [], "resolved_entries": [], "stale_high_priority": []}

        def load_review_queue(self):
            return {
                "active": [
                    {
                        "symbol": "561380",
                        "priority": "高",
                        "summary": "事件边界待复核，仓位较重",
                        "event_detail": "政策影响层：配套细则",
                        "impact_summary": "盈利 / 景气",
                        "thesis_scope": "待确认",
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
                ],
                "history": {
                    "561380": {
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
                        }
                    }
                },
            }

    monkeypatch.setattr(portfolio_module, "PortfolioRepository", lambda: _FakeRepo())
    monkeypatch.setattr(portfolio_module, "ThesisRepository", lambda: _FakeThesisRepo())
    monkeypatch.setattr(portfolio_module, "detect_asset_type", lambda symbol, config: "cn_etf")
    monkeypatch.setattr(portfolio_module, "fetch_asset_history", lambda symbol, asset_type, config: object())
    monkeypatch.setattr(portfolio_module, "normalize_ohlcv_frame", lambda history: history)
    monkeypatch.setattr(portfolio_module, "compute_history_metrics", lambda history: {"last_close": 2.2, "return_20d": 0.02})

    class _FakeTechnicalAnalyzer:
        def __init__(self, history):
            pass

        def generate_scorecard(self, config):
            return {"ma_system": {"signal": "bullish"}}

    monkeypatch.setattr(portfolio_module, "TechnicalAnalyzer", _FakeTechnicalAnalyzer)
    monkeypatch.setattr(sys, "argv", ["portfolio", "thesis", "check"])

    portfolio_module.main()

    captured = capsys.readouterr()
    assert "先补正式稿" in captured.out
    assert "正式稿跟进: 561380 当前是 `待更新正式稿`" in captured.out
    assert "python -m src.commands.scan 561380 --client-final" in captured.out


def test_portfolio_thesis_check_records_review_queue_transitions(monkeypatch, capsys) -> None:
    monkeypatch.setattr(portfolio_module, "load_config", lambda _path=None: {"technical": {}})

    class _FakeRepo:
        def list_holdings(self):
            return [{"symbol": "561380", "asset_type": "cn_etf", "cost_basis": 2.1}]

    class _FakeThesisRepo:
        def get(self, symbol):
            assert symbol == "561380"
            return {
                "core_assumption": "电网投资提升",
                "validation_metric": "投资完成额同比 > 10%",
                "stop_condition": "估值过高且增速下滑",
                "holding_period": "6-12个月",
                "event_digest_snapshot": {
                    "status": "待复核",
                    "lead_layer": "政策",
                    "lead_detail": "政策影响层：配套细则",
                    "lead_title": "政策细则待补",
                    "impact_summary": "盈利 / 景气",
                    "thesis_scope": "待确认",
                    "changed_what": "旧结论先退回复核。",
                },
            }

        def list_all(self):
            return [{**self.get("561380"), "symbol": "561380"}]

        def record_review_queue(self, queue, *, source="", as_of=""):
            assert queue
            assert source == "portfolio_thesis_check"
            assert as_of
            return {
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
                    }
                ],
                "resolved_entries": [],
                "stale_high_priority": [],
            }

    monkeypatch.setattr(portfolio_module, "PortfolioRepository", lambda: _FakeRepo())
    monkeypatch.setattr(portfolio_module, "ThesisRepository", lambda: _FakeThesisRepo())
    monkeypatch.setattr(portfolio_module, "detect_asset_type", lambda symbol, config: "cn_etf")
    monkeypatch.setattr(portfolio_module, "fetch_asset_history", lambda symbol, asset_type, config: object())
    monkeypatch.setattr(
        portfolio_module,
        "normalize_ohlcv_frame",
        lambda history: history,
    )
    monkeypatch.setattr(
        portfolio_module,
        "compute_history_metrics",
        lambda history: {"last_close": 2.2, "return_20d": 0.02},
    )

    class _FakeTechnicalAnalyzer:
        def __init__(self, history):
            pass

        def generate_scorecard(self, config):
            return {"ma_system": {"signal": "bullish"}}

    monkeypatch.setattr(portfolio_module, "TechnicalAnalyzer", _FakeTechnicalAnalyzer)
    monkeypatch.setattr(sys, "argv", ["portfolio", "thesis", "check"])

    portfolio_module.main()

    captured = capsys.readouterr()
    assert "优先复查队列" in captured.out
    assert "今日新进复查队列" in captured.out
    assert "重跑 scan" in captured.out
    assert "研究动作 1" in captured.out
    assert "政策影响层：配套细则" in captured.out
    assert "当前事件边界已退回待复核" in captured.out
    assert "python -m src.commands.scan 561380" in captured.out


def test_portfolio_thesis_check_run_executes_review_actions(monkeypatch, capsys) -> None:
    monkeypatch.setattr(portfolio_module, "load_config", lambda _path=None: {"technical": {}})

    class _FakeRepo:
        def list_holdings(self):
            return [{"symbol": "561380", "asset_type": "cn_etf", "cost_basis": 2.1}]

    class _FakeThesisRepo:
        def __init__(self):
            self.run_records = []
            self._record = {
                "core_assumption": "电网投资提升",
                "validation_metric": "投资完成额同比 > 10%",
                "stop_condition": "估值过高且增速下滑",
                "holding_period": "6-12个月",
                "event_digest_snapshot": {
                    "status": "待复核",
                    "lead_layer": "政策",
                    "lead_detail": "政策影响层：配套细则",
                    "lead_title": "政策细则待补",
                    "impact_summary": "盈利 / 景气",
                    "thesis_scope": "待确认",
                    "changed_what": "旧结论先退回复核。",
                },
            }

        def get(self, symbol):
            assert symbol == "561380"
            return self._record

        def list_all(self):
            return [{**self._record, "symbol": "561380"}]

        def record_review_queue(self, queue, *, source="", as_of=""):
            assert source == "portfolio_thesis_check_run"
            assert as_of
            return {
                "new_entries": [],
                "resolved_entries": [],
                "stale_high_priority": [],
            }

        def record_review_run(self, symbol, *, action="", status="", artifact_path="", summary="", recorded_at=""):
            self.run_records.append(
                {
                    "symbol": symbol,
                    "action": action,
                    "status": status,
                    "artifact_path": artifact_path,
                    "summary": summary,
                    "recorded_at": recorded_at,
                }
            )

    fake_thesis_repo = _FakeThesisRepo()
    monkeypatch.setattr(portfolio_module, "PortfolioRepository", lambda: _FakeRepo())
    monkeypatch.setattr(portfolio_module, "ThesisRepository", lambda: fake_thesis_repo)
    monkeypatch.setattr(portfolio_module, "detect_asset_type", lambda symbol, config: "cn_etf")
    monkeypatch.setattr(portfolio_module, "fetch_asset_history", lambda symbol, asset_type, config: object())
    monkeypatch.setattr(portfolio_module, "normalize_ohlcv_frame", lambda history: history)
    monkeypatch.setattr(
        portfolio_module,
        "compute_history_metrics",
        lambda history: {"last_close": 2.2, "return_20d": 0.02},
    )

    class _FakeTechnicalAnalyzer:
        def __init__(self, history):
            pass

        def generate_scorecard(self, config):
            return {"ma_system": {"signal": "bullish"}}

    monkeypatch.setattr(portfolio_module, "TechnicalAnalyzer", _FakeTechnicalAnalyzer)
    monkeypatch.setattr(
        portfolio_module,
        "_run_thesis_review_action",
        lambda item, *, thesis_repo, config_path="": {
            "symbol": "561380",
            "action": "重跑 scan",
            "status": "completed",
            "summary": "事件状态已从待复核回到已消化。",
            "artifact_path": "reports/scan_561380_2026-03-29.md",
            "command": item.get("command", ""),
        },
    )
    monkeypatch.setattr(sys, "argv", ["portfolio", "thesis", "check", "--run", "--limit", "1"])

    portfolio_module.main()

    captured = capsys.readouterr()
    assert "执行复查: 561380 重跑 scan 完成" in captured.out
    assert "产物：`reports/scan_561380_2026-03-29.md`" in captured.out
    assert "复查后队列" in captured.out
