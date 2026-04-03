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
    assert config["news_topic_search_enabled"] is False
    assert config["news_feeds_file"] == "config/news_feeds.empty.yaml"
    assert any("跨市场代理" in item for item in notes)
    assert any("主题新闻扩搜" in item for item in notes)
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
    assert config["news_topic_search_enabled"] is False
    assert config["news_feeds_file"] == "config/news_feeds.empty.yaml"
    assert any("跨市场代理" in item for item in notes)
    assert any("主题新闻扩搜" in item for item in notes)
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

    monkeypatch.setattr(scan_command, "AnalysisChartRenderer", lambda: _Renderer())
    monkeypatch.setattr(scan_command, "OpportunityReportRenderer", lambda: _ReportRenderer())

    _report, analysis = scan_command.run_scan("农发种业")

    assert captured == {"symbol": "600313", "asset_type": "cn_stock"}
    assert analysis["symbol"] == "600313"


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

    monkeypatch.setattr(stock_analysis_command, "AnalysisChartRenderer", lambda: _Renderer())
    monkeypatch.setattr(stock_analysis_command, "OpportunityReportRenderer", lambda: _ReportRenderer())

    _report, analysis = stock_analysis_command.run_stock_analysis("农发种业")

    assert captured == {"symbol": "600313", "asset_type": "cn_stock"}
    assert analysis["symbol"] == "600313"
