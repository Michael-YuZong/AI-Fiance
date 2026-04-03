"""Tests for config helpers."""

from __future__ import annotations

from src.utils import config as config_module
from src.utils.config import detect_asset_type, load_config


def test_detect_asset_type_prefers_alias_for_index_symbols() -> None:
    config = load_config()
    assert detect_asset_type("000300", config) == "cn_index"


def test_detect_asset_type_uses_fund_heuristic_for_non_etf_codes() -> None:
    config = load_config()
    assert detect_asset_type("022365", config) == "cn_fund"
    assert detect_asset_type("512660", config) == "cn_etf"


def test_load_config_explicit_profile_inherits_default_config(tmp_path, monkeypatch) -> None:
    project_root = tmp_path
    config_dir = project_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "config.example.yaml").write_text(
        "\n".join(
            [
                'api_keys:',
                '  tushare: "YOUR_TUSHARE_TOKEN"',
                'storage:',
                '  db_path: "data/example.db"',
                '  cache_dir: "data/cache"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    (config_dir / "config.yaml").write_text(
        "\n".join(
            [
                'api_keys:',
                '  tushare: "real_token"',
                'news_topic_search_enabled: true',
                "",
            ]
        ),
        encoding="utf-8",
    )
    (config_dir / "profile.yaml").write_text(
        "\n".join(
            [
                'news_topic_search_enabled: false',
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(config_module, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(config_module, "DEFAULT_CONFIG_PATH", config_dir / "config.yaml")
    monkeypatch.setattr(config_module, "EXAMPLE_CONFIG_PATH", config_dir / "config.example.yaml")

    loaded = load_config("config/profile.yaml")

    assert loaded["api_keys"]["tushare"] == "real_token"
    assert loaded["news_topic_search_enabled"] is False
