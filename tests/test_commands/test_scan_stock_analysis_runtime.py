from __future__ import annotations

from src.commands import scan as scan_command
from src.commands import stock_analysis as stock_analysis_command
from src.commands.scan import _client_final_runtime_overrides as _scan_runtime_overrides
from src.commands.stock_analysis import _client_final_runtime_overrides as _stock_analysis_runtime_overrides
from src.utils.market import AssetContext


def test_scan_runtime_overrides_apply_lightweight_profile_by_default() -> None:
    config, notes = _scan_runtime_overrides(
        {
            "market_context": {},
            "news_topic_search_enabled": True,
        },
        client_final=True,
    )

    assert config["market_context"]["skip_global_proxy"] is True
    assert config["market_context"]["skip_market_monitor"] is True
    assert config["news_topic_search_enabled"] is True
    assert config["news_feeds_file"] == "config/news_feeds.empty.yaml"
    assert any("跨市场代理" in item for item in notes)
    assert any("主题情报扩搜能力" in item for item in notes)
    assert any("轻量新闻源配置" in item for item in notes)


def test_scan_runtime_overrides_respect_explicit_config_path() -> None:
    config, notes = _scan_runtime_overrides(
        {"news_topic_search_enabled": True},
        client_final=True,
        explicit_config_path="config/custom.yaml",
    )

    assert config["news_topic_search_enabled"] is True
    assert "market_context" not in config
    assert "news_feeds_file" not in config
    assert notes == []


def test_stock_analysis_runtime_overrides_apply_lightweight_profile_by_default() -> None:
    config, notes = _stock_analysis_runtime_overrides(
        {
            "market_context": {},
            "news_topic_search_enabled": True,
        },
        client_final=True,
    )

    assert config["market_context"]["skip_global_proxy"] is True
    assert config["market_context"]["skip_market_monitor"] is True
    assert config["news_topic_search_enabled"] is True
    assert config["news_feeds_file"] == "config/news_feeds.empty.yaml"
    assert any("跨市场代理" in item for item in notes)
    assert any("主题情报扩搜能力" in item for item in notes)
    assert any("轻量新闻源配置" in item for item in notes)


def test_stock_analysis_runtime_overrides_respect_explicit_config_path() -> None:
    config, notes = _stock_analysis_runtime_overrides(
        {"news_topic_search_enabled": True},
        client_final=True,
        explicit_config_path="config/custom.yaml",
    )

    assert config["news_topic_search_enabled"] is True
    assert "market_context" not in config
    assert "news_feeds_file" not in config
    assert notes == []


def test_run_scan_client_final_enables_today_snapshot_for_cn_stock(monkeypatch) -> None:
    monkeypatch.setattr(scan_command, "load_config", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(scan_command, "setup_logger", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scan_command,
        "resolve_asset_context",
        lambda query, config: AssetContext(  # noqa: ARG005
            symbol="600584",
            name="长电科技",
            asset_type="cn_stock",
            source_symbol="600584",
            metadata={"name": "长电科技", "sector": "信息技术"},
        ),
    )
    monkeypatch.setattr(scan_command, "build_market_context", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(scan_command, "_attach_shared_intel_news_report", lambda context, *_args, **_kwargs: dict(context))
    captured: dict[str, bool] = {}

    def _fake_analyze(symbol, asset_type, config, context=None, today_mode=False):  # noqa: ANN001,ARG001
        captured["today_mode"] = bool(today_mode)
        return {"symbol": symbol, "name": "长电科技", "asset_type": asset_type, "notes": []}

    monkeypatch.setattr(scan_command, "analyze_opportunity", _fake_analyze)
    monkeypatch.setattr(scan_command, "_attach_signal_confidence", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scan_command, "build_candidate_portfolio_overlap_summary", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(scan_command, "_persist_scan_report", lambda *_args, **_kwargs: None)

    class _Renderer:
        def render(self, _analysis):  # noqa: ANN001
            return {}

    class _ReportRenderer:
        def render_scan(self, _analysis, visuals=None):  # noqa: ANN001,ARG002
            return "# ok"

    monkeypatch.setattr(scan_command, "AnalysisChartRenderer", lambda **_kwargs: _Renderer())
    monkeypatch.setattr(scan_command, "OpportunityReportRenderer", lambda: _ReportRenderer())

    scan_command.run_scan("600584", client_final=True)

    assert captured["today_mode"] is True


def test_run_stock_analysis_client_final_enables_today_snapshot_for_cn_stock(monkeypatch) -> None:
    monkeypatch.setattr(stock_analysis_command, "load_config", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(stock_analysis_command, "setup_logger", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        stock_analysis_command,
        "resolve_asset_context",
        lambda query, config: AssetContext(  # noqa: ARG005
            symbol="600584",
            name="长电科技",
            asset_type="cn_stock",
            source_symbol="600584",
            metadata={"name": "长电科技", "sector": "信息技术"},
        ),
    )
    monkeypatch.setattr(stock_analysis_command, "build_market_context", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(stock_analysis_command, "_attach_shared_intel_news_report", lambda context, *_args, **_kwargs: dict(context))
    captured: dict[str, bool] = {}

    def _fake_analyze(symbol, asset_type, config, context=None, today_mode=False):  # noqa: ANN001,ARG001
        captured["today_mode"] = bool(today_mode)
        return {"symbol": symbol, "name": "长电科技", "asset_type": asset_type, "notes": []}

    monkeypatch.setattr(stock_analysis_command, "analyze_opportunity", _fake_analyze)
    monkeypatch.setattr(stock_analysis_command, "_attach_signal_confidence", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(stock_analysis_command, "build_candidate_portfolio_overlap_summary", lambda *_args, **_kwargs: {})

    class _Renderer:
        def render(self, _analysis):  # noqa: ANN001
            return {}

    class _ReportRenderer:
        def render_scan(self, _analysis, visuals=None):  # noqa: ANN001,ARG002
            return "# ok"

    monkeypatch.setattr(stock_analysis_command, "AnalysisChartRenderer", lambda **_kwargs: _Renderer())
    monkeypatch.setattr(stock_analysis_command, "OpportunityReportRenderer", lambda: _ReportRenderer())

    stock_analysis_command.run_stock_analysis("600584", client_final=True)

    assert captured["today_mode"] is True


def test_run_scan_resolves_name_input_to_canonical_symbol(monkeypatch) -> None:
    monkeypatch.setattr(scan_command, "load_config", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(scan_command, "setup_logger", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scan_command,
        "resolve_asset_context",
        lambda query, config: AssetContext(  # noqa: ARG005
            symbol="600313",
            name="农发种业",
            asset_type="cn_stock",
            source_symbol="600313",
            metadata={"name": "农发种业", "sector": "农业"},
        ),
    )
    monkeypatch.setattr(scan_command, "build_market_context", lambda *_args, **_kwargs: {})
    captured: dict[str, str] = {}

    def _fake_analyze(symbol, asset_type, config, context=None, today_mode=False):  # noqa: ANN001,ARG001
        captured["symbol"] = symbol
        captured["asset_type"] = asset_type
        return {"symbol": symbol, "name": "农发种业", "asset_type": asset_type, "notes": []}

    monkeypatch.setattr(scan_command, "analyze_opportunity", _fake_analyze)
    monkeypatch.setattr(scan_command, "_attach_signal_confidence", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scan_command, "build_candidate_portfolio_overlap_summary", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(scan_command, "_persist_scan_report", lambda *_args, **_kwargs: None)

    class _Renderer:
        def render(self, _analysis):  # noqa: ANN001
            return {}

    class _ReportRenderer:
        def render_scan(self, _analysis, visuals=None):  # noqa: ANN001,ARG002
            return "# ok"

    monkeypatch.setattr(scan_command, "AnalysisChartRenderer", lambda **_kwargs: _Renderer())
    monkeypatch.setattr(scan_command, "OpportunityReportRenderer", lambda: _ReportRenderer())

    _report, analysis = scan_command.run_scan("农发种业")

    assert captured == {"symbol": "600313", "asset_type": "cn_stock"}
    assert analysis["symbol"] == "600313"


def test_run_scan_attaches_shared_intel_news_report_for_stock(monkeypatch) -> None:
    monkeypatch.setattr(scan_command, "load_config", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(scan_command, "setup_logger", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scan_command,
        "resolve_asset_context",
        lambda query, config: AssetContext(  # noqa: ARG005
            symbol="600519",
            name="贵州茅台",
            asset_type="cn_stock",
            source_symbol="600519",
            metadata={"name": "贵州茅台", "sector": "食品饮料"},
        ),
    )
    monkeypatch.setattr(
        scan_command,
        "build_market_context",
        lambda *_args, **_kwargs: {"news_report": {"mode": "proxy", "items": []}},
    )
    monkeypatch.setattr(
        scan_command,
        "collect_intel_news_report",
        lambda query, **kwargs: {  # noqa: ARG005
            "mode": "live",
            "items": [{"title": "共享 intel 线索", "source": "财联社"}],
            "all_items": [{"title": "共享 intel 线索", "source": "财联社"}],
            "lines": ["共享 intel 线索"],
            "source_list": ["财联社"],
            "note": "intel",
            "disclosure": "—",
        },
    )
    captured: dict[str, str] = {}

    def _fake_analyze(symbol, asset_type, config, context=None, today_mode=False):  # noqa: ANN001,ARG001
        captured["title"] = str(dict(context.get("news_report") or {}).get("items", [{}])[0].get("title", ""))  # type: ignore[union-attr]
        return {"symbol": symbol, "name": "贵州茅台", "asset_type": asset_type, "notes": []}

    monkeypatch.setattr(scan_command, "analyze_opportunity", _fake_analyze)
    monkeypatch.setattr(scan_command, "_attach_signal_confidence", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scan_command, "build_candidate_portfolio_overlap_summary", lambda *_args, **_kwargs: {})

    class _Renderer:
        def render(self, _analysis):  # noqa: ANN001
            return {}

    class _ReportRenderer:
        def render_scan(self, _analysis, visuals=None):  # noqa: ANN001,ARG002
            return "# ok"

    monkeypatch.setattr(scan_command, "AnalysisChartRenderer", lambda **_kwargs: _Renderer())
    monkeypatch.setattr(scan_command, "OpportunityReportRenderer", lambda: _ReportRenderer())
    monkeypatch.setattr(scan_command, "_persist_scan_report", lambda *_args, **_kwargs: None)

    _report, analysis = scan_command.run_scan("600519")

    assert captured["title"] == "共享 intel 线索"
    assert analysis["symbol"] == "600519"


def test_run_scan_skips_shared_intel_news_report_for_etf(monkeypatch) -> None:
    monkeypatch.setattr(scan_command, "load_config", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(scan_command, "setup_logger", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scan_command,
        "resolve_asset_context",
        lambda query, config: AssetContext(  # noqa: ARG005
            symbol="512400",
            name="有色金属ETF",
            asset_type="cn_etf",
            source_symbol="512400",
            metadata={"name": "有色金属ETF", "sector": "资源"},
        ),
    )
    monkeypatch.setattr(
        scan_command,
        "build_market_context",
        lambda *_args, **_kwargs: {"news_report": {"mode": "proxy", "items": []}},
    )
    monkeypatch.setattr(
        scan_command,
        "collect_intel_news_report",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("ETF scan should not use shared intel backfill")),
    )
    captured: dict[str, object] = {}

    def _fake_analyze(symbol, asset_type, config, context=None, today_mode=False):  # noqa: ANN001,ARG001
        captured["items"] = list(dict(context.get("news_report") or {}).get("items") or [])  # type: ignore[union-attr]
        return {"symbol": symbol, "name": "有色金属ETF", "asset_type": asset_type, "notes": []}

    monkeypatch.setattr(scan_command, "analyze_opportunity", _fake_analyze)
    monkeypatch.setattr(scan_command, "_attach_signal_confidence", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scan_command, "build_candidate_portfolio_overlap_summary", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(scan_command, "_backfill_etf_news_report", lambda *_args, **_kwargs: {})

    class _Renderer:
        def render(self, _analysis):  # noqa: ANN001
            return {}

    class _ReportRenderer:
        def render_scan(self, _analysis, visuals=None):  # noqa: ANN001,ARG002
            return "# ok"

    monkeypatch.setattr(scan_command, "AnalysisChartRenderer", lambda **_kwargs: _Renderer())
    monkeypatch.setattr(scan_command, "OpportunityReportRenderer", lambda: _ReportRenderer())
    monkeypatch.setattr(scan_command, "_persist_scan_report", lambda *_args, **_kwargs: None)

    _report, analysis = scan_command.run_scan("512400")

    assert captured["items"] == []
    assert analysis["symbol"] == "512400"


def test_run_scan_client_final_reanalyzes_etf_with_full_profile(monkeypatch) -> None:
    monkeypatch.setattr(
        scan_command,
        "load_config",
        lambda *_args, **_kwargs: {"skip_fund_profile": False, "etf_fund_profile_mode": "light"},
    )
    monkeypatch.setattr(scan_command, "setup_logger", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scan_command,
        "resolve_asset_context",
        lambda query, config: AssetContext(  # noqa: ARG005
            symbol="512400",
            name="有色金属ETF",
            asset_type="cn_etf",
            source_symbol="512400",
            metadata={"name": "有色金属ETF", "sector": "资源"},
        ),
    )
    monkeypatch.setattr(scan_command, "build_market_context", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(scan_command, "_attach_shared_intel_news_report", lambda context, *_args, **_kwargs: dict(context))

    calls: list[dict[str, object]] = []

    def _fake_analyze(symbol, asset_type, config, context=None, today_mode=False):  # noqa: ANN001,ARG001
        calls.append(dict(config))
        if len(calls) == 1:
            return {
                "symbol": symbol,
                "name": "有色金属ETF",
                "asset_type": asset_type,
                "fund_profile": {
                    "profile_mode": "light",
                    "overview": {"基金经理人": ""},
                },
                "notes": [],
            }
        return {
            "symbol": symbol,
            "name": "有色金属ETF",
            "asset_type": asset_type,
            "fund_profile": {
                "profile_mode": "full",
                "overview": {"基金经理人": "崔蕾"},
            },
            "notes": [],
        }

    monkeypatch.setattr(scan_command, "analyze_opportunity", _fake_analyze)
    monkeypatch.setattr(scan_command, "_attach_signal_confidence", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scan_command, "build_candidate_portfolio_overlap_summary", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(scan_command, "_persist_scan_report", lambda *_args, **_kwargs: None)

    class _Renderer:
        def render(self, _analysis):  # noqa: ANN001
            return {}

    class _ReportRenderer:
        def render_scan(self, _analysis, visuals=None):  # noqa: ANN001,ARG002
            return "# ok"

    monkeypatch.setattr(scan_command, "AnalysisChartRenderer", lambda **_kwargs: _Renderer())
    monkeypatch.setattr(scan_command, "OpportunityReportRenderer", lambda: _ReportRenderer())

    _report, analysis = scan_command.run_scan(
        "512400",
        config_path="config/config.etf_pick_fast.yaml",
        client_final=True,
    )

    assert len(calls) == 2
    assert calls[0]["etf_fund_profile_mode"] == "light"
    assert calls[1]["etf_fund_profile_mode"] == "full"
    assert analysis["fund_profile"]["overview"]["基金经理人"] == "崔蕾"
    assert any("full profile" in item for item in analysis["notes"])


def test_run_stock_analysis_resolves_name_input_to_canonical_symbol(monkeypatch) -> None:
    monkeypatch.setattr(stock_analysis_command, "load_config", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(stock_analysis_command, "setup_logger", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        stock_analysis_command,
        "resolve_asset_context",
        lambda query, config: AssetContext(  # noqa: ARG005
            symbol="600313",
            name="农发种业",
            asset_type="cn_stock",
            source_symbol="600313",
            metadata={"name": "农发种业", "sector": "农业"},
        ),
    )
    monkeypatch.setattr(stock_analysis_command, "build_market_context", lambda *_args, **_kwargs: {})
    captured: dict[str, str] = {}

    def _fake_analyze(symbol, asset_type, config, context=None, today_mode=False):  # noqa: ANN001,ARG001
        captured["symbol"] = symbol
        captured["asset_type"] = asset_type
        return {"symbol": symbol, "name": "农发种业", "asset_type": asset_type, "notes": []}

    monkeypatch.setattr(stock_analysis_command, "analyze_opportunity", _fake_analyze)
    monkeypatch.setattr(stock_analysis_command, "_attach_signal_confidence", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(stock_analysis_command, "build_candidate_portfolio_overlap_summary", lambda *_args, **_kwargs: {})

    class _Renderer:
        def render(self, _analysis):  # noqa: ANN001
            return {}

    class _ReportRenderer:
        def render_scan(self, _analysis, visuals=None):  # noqa: ANN001,ARG002
            return "# ok"

    monkeypatch.setattr(stock_analysis_command, "AnalysisChartRenderer", lambda **_kwargs: _Renderer())
    monkeypatch.setattr(stock_analysis_command, "OpportunityReportRenderer", lambda: _ReportRenderer())

    _report, analysis = stock_analysis_command.run_stock_analysis("农发种业")

    assert captured == {"symbol": "600313", "asset_type": "cn_stock"}
    assert analysis["symbol"] == "600313"


def test_run_stock_analysis_attaches_shared_intel_news_report(monkeypatch) -> None:
    monkeypatch.setattr(stock_analysis_command, "load_config", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(stock_analysis_command, "setup_logger", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        stock_analysis_command,
        "resolve_asset_context",
        lambda query, config: AssetContext(  # noqa: ARG005
            symbol="600313",
            name="农发种业",
            asset_type="cn_stock",
            source_symbol="600313",
            metadata={"name": "农发种业", "sector": "农业"},
        ),
    )
    monkeypatch.setattr(
        stock_analysis_command,
        "build_market_context",
        lambda *_args, **_kwargs: {"news_report": {"mode": "proxy", "items": []}},
    )
    monkeypatch.setattr(
        stock_analysis_command,
        "collect_intel_news_report",
        lambda query, **kwargs: {  # noqa: ARG005
            "mode": "live",
            "items": [{"title": "共享 intel 线索", "source": "财联社"}],
            "all_items": [{"title": "共享 intel 线索", "source": "财联社"}],
            "lines": ["共享 intel 线索"],
            "source_list": ["财联社"],
            "note": "intel",
            "disclosure": "—",
        },
    )
    captured: dict[str, str] = {}

    def _fake_analyze(symbol, asset_type, config, context=None, today_mode=False):  # noqa: ANN001,ARG001
        captured["title"] = str(dict(context.get("news_report") or {}).get("items", [{}])[0].get("title", ""))  # type: ignore[union-attr]
        return {"symbol": symbol, "name": "农发种业", "asset_type": asset_type, "notes": []}

    monkeypatch.setattr(stock_analysis_command, "analyze_opportunity", _fake_analyze)
    monkeypatch.setattr(stock_analysis_command, "_attach_signal_confidence", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(stock_analysis_command, "build_candidate_portfolio_overlap_summary", lambda *_args, **_kwargs: {})

    class _Renderer:
        def render(self, _analysis):  # noqa: ANN001
            return {}

    class _ReportRenderer:
        def render_scan(self, _analysis, visuals=None):  # noqa: ANN001,ARG002
            return "# ok"

    monkeypatch.setattr(stock_analysis_command, "AnalysisChartRenderer", lambda **_kwargs: _Renderer())
    monkeypatch.setattr(stock_analysis_command, "OpportunityReportRenderer", lambda: _ReportRenderer())

    _report, analysis = stock_analysis_command.run_stock_analysis("600313")

    assert captured["title"] == "共享 intel 线索"
    assert analysis["symbol"] == "600313"
