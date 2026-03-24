"""Strategy prediction-ledger command."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from src.commands.release_check import check_generic_client_report
from src.commands.report_guard import (
    ReportGuardError,
    ensure_report_task_registered,
    export_reviewed_markdown_bundle,
    exported_bundle_lines,
    review_path_for,
)
from src.output.strategy_report import StrategyReportRenderer
from src.processors.strategy import (
    STRATEGY_V1_ASSET_GAP_DAYS,
    attribute_strategy_rows,
    generate_strategy_experiment,
    generate_strategy_multi_symbol_experiment,
    generate_strategy_multi_symbol_replay_predictions,
    generate_strategy_prediction,
    generate_strategy_replay_predictions,
    validate_strategy_rows,
)
from src.reporting.review_scaffold import ensure_external_review_scaffold
from src.storage.strategy import StrategyRepository
from src.utils.config import detect_asset_type, load_config, resolve_project_path
from src.utils.data import load_strategy_batches, load_watchlist
from src.utils.logger import setup_logger


def _strategy_note_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    return [text] if text else []


def _normalize_strategy_symbols(symbols: Sequence[str]) -> List[str]:
    normalized: List[str] = []
    seen: set[str] = set()
    for item in symbols:
        for token in str(item or "").split(","):
            symbol = token.strip()
            if symbol and symbol not in seen:
                seen.add(symbol)
                normalized.append(symbol)
    return normalized


def _extract_batch_symbols(value: Any) -> List[str]:
    symbols: List[str] = []
    for item in list(value or []):
        if isinstance(item, Mapping):
            symbol = str(item.get("symbol", "")).strip()
        else:
            symbol = str(item).strip()
        if symbol:
            symbols.append(symbol)
    return symbols


def _watchlist_item_matches_filters(item: Mapping[str, Any], filters: Mapping[str, Any]) -> bool:
    asset_types = {str(value).strip() for value in list(filters.get("asset_types") or []) if str(value).strip()}
    if asset_types and str(item.get("asset_type", "")).strip() not in asset_types:
        return False
    regions = {str(value).strip() for value in list(filters.get("regions") or []) if str(value).strip()}
    if regions and str(item.get("region", "")).strip() not in regions:
        return False
    sectors = {str(value).strip() for value in list(filters.get("sectors") or []) if str(value).strip()}
    if sectors and str(item.get("sector", "")).strip() not in sectors:
        return False
    required_nodes = {str(value).strip() for value in list(filters.get("chain_nodes") or []) if str(value).strip()}
    item_nodes = {str(value).strip() for value in list(item.get("chain_nodes") or []) if str(value).strip()}
    if required_nodes and not item_nodes.intersection(required_nodes):
        return False
    symbols = {str(value).strip() for value in list(filters.get("symbols") or []) if str(value).strip()}
    return not symbols or str(item.get("symbol", "")).strip() in symbols


def _load_strategy_batch_registry(config: Mapping[str, Any]) -> Dict[str, Any]:
    batches_path = resolve_project_path(config.get("strategy_batches_file", "config/strategy_batches.yaml"))
    return load_strategy_batches(batches_path)


def _resolve_strategy_batch_context(
    batch_source: str,
    config: Mapping[str, Any],
    registry: Mapping[str, Any],
) -> Dict[str, Any]:
    batch_key = str(batch_source or "").strip()
    if not batch_key:
        return {}
    source_block = dict(dict(registry.get("batch_sources") or {}).get(batch_key) or {})
    if not source_block:
        raise ValueError(f"未知 strategy batch source: {batch_key}")

    explicit_symbols = _extract_batch_symbols(source_block.get("symbols") or [])
    watchlist_filters = dict(source_block.get("watchlist_filters") or {})
    watchlist_symbols: List[str] = []
    if watchlist_filters:
        watchlist_path = resolve_project_path(config.get("watchlist_file", "config/watchlist.yaml"))
        for item in load_watchlist(watchlist_path):
            if _watchlist_item_matches_filters(item, watchlist_filters):
                symbol = str(item.get("symbol", "")).strip()
                if symbol:
                    watchlist_symbols.append(symbol)

    symbols = _normalize_strategy_symbols([*explicit_symbols, *watchlist_symbols])
    if not symbols:
        raise ValueError(f"strategy batch source `{batch_key}` 没有解析出任何标的。")

    unsupported = [symbol for symbol in symbols if detect_asset_type(symbol, config) != "cn_stock"]
    if unsupported:
        raise ValueError(
            "strategy batch source "
            f"`{batch_key}` 解析出了不受支持的标的：{', '.join(unsupported)}；"
            "当前 strategy v1 只允许 A 股普通股票。"
        )

    if explicit_symbols and watchlist_symbols:
        mode = "mixed"
    elif watchlist_symbols:
        mode = "watchlist_filters"
    else:
        mode = "explicit_symbols"

    label = str(source_block.get("label", "")).strip() or batch_key
    summary = (
        f"batch source `{label}` 解析出 `{len(symbols)}` 只 A 股普通股票；"
        f"显式 `{len(explicit_symbols)}` 只，watchlist 命中 `{len(watchlist_symbols)}` 只。"
    )
    return {
        "key": batch_key,
        "label": label,
        "mode": mode,
        "source_symbol_count": len(symbols),
        "explicit_symbol_count": len(explicit_symbols),
        "watchlist_match_count": len(watchlist_symbols),
        "symbols": symbols,
        "summary": summary,
        "notes": _strategy_note_list(source_block.get("notes")),
    }


def _resolve_strategy_cohort_recipe(
    cohort_recipe: str,
    registry: Mapping[str, Any],
    *,
    asset_gap_days: int | None,
    max_samples: int | None,
) -> tuple[int, int, Dict[str, Any]]:
    recipe_key = str(cohort_recipe or "").strip()
    recipe_block = dict(dict(registry.get("cohort_recipes") or {}).get(recipe_key) or {})
    if recipe_key and not recipe_block:
        raise ValueError(f"未知 strategy cohort recipe: {recipe_key}")

    configured_asset_gap_days = max(int(recipe_block.get("asset_gap_days") or STRATEGY_V1_ASSET_GAP_DAYS), 1)
    configured_max_samples = max(int(recipe_block.get("max_samples") or 12), 1)
    resolved_asset_gap_days = max(int(asset_gap_days if asset_gap_days is not None else configured_asset_gap_days), 1)
    resolved_max_samples = max(int(max_samples if max_samples is not None else configured_max_samples), 1)

    if not recipe_key and asset_gap_days is None and max_samples is None:
        return resolved_asset_gap_days, resolved_max_samples, {}

    label = str(recipe_block.get("label", "")).strip() or (recipe_key or "cli_override")
    summary = (
        f"cohort recipe `{label}` 采用资产重入间隔 `{resolved_asset_gap_days}` 个交易日，"
        f"单标的最多 `{resolved_max_samples}` 个样本。"
    )
    if recipe_key:
        applied_via = "config_recipe"
    else:
        applied_via = "cli_override"
    return resolved_asset_gap_days, resolved_max_samples, {
        "key": recipe_key,
        "label": label,
        "applied_via": applied_via,
        "configured_asset_gap_days": configured_asset_gap_days,
        "configured_max_samples": configured_max_samples,
        "asset_gap_days": resolved_asset_gap_days,
        "max_samples": resolved_max_samples,
        "summary": summary,
        "notes": _strategy_note_list(recipe_block.get("notes")),
    }


def _resolve_strategy_symbol_inputs(raw_symbols: Sequence[str], batch_context: Mapping[str, Any]) -> List[str]:
    effective_symbols = _normalize_strategy_symbols([*raw_symbols, *list(batch_context.get("symbols") or [])])
    if not effective_symbols:
        raise ValueError("至少提供一个 symbol 或 `--batch-source`。")
    return effective_symbols


def _finalize_batch_context(batch_context: Mapping[str, Any], effective_symbols: Sequence[str]) -> Dict[str, Any]:
    if not batch_context:
        return {}
    finalized = dict(batch_context)
    finalized["effective_symbol_count"] = len(list(effective_symbols or []))
    return finalized


def _strategy_safe_slug(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "all"
    for old in ("/", "\\", " ", ",", ":", ";"):
        text = text.replace(old, "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_") or "all"


def _strategy_subject_from_symbols(symbols: Sequence[str]) -> str:
    normalized = _normalize_strategy_symbols(symbols)
    if not normalized:
        return "all"
    if len(normalized) == 1:
        return _strategy_safe_slug(normalized[0])
    if len(normalized) <= 3:
        return _strategy_safe_slug("-".join(normalized))
    return f"{len(normalized)}symbols"


def _strategy_validate_subject(symbol: str) -> str:
    return _strategy_safe_slug(symbol or "all")


def _strategy_experiment_subject(payload: Mapping[str, Any], effective_symbols: Sequence[str]) -> str:
    batch_context = dict(payload.get("batch_context") or {})
    batch_key = str(batch_context.get("key", "")).strip()
    if batch_key:
        return _strategy_safe_slug(batch_key)
    payload_symbols = [str(item) for item in list(payload.get("symbols") or []) if str(item).strip()]
    if payload_symbols:
        return _strategy_subject_from_symbols(payload_symbols)
    symbol = str(payload.get("symbol", "")).strip()
    if symbol:
        return _strategy_safe_slug(symbol)
    return _strategy_subject_from_symbols(effective_symbols)


def _strategy_detail_output_path(report_kind: str, subject: str, generated_at: str) -> Path:
    return resolve_project_path(
        f"reports/strategy/{report_kind}/internal/strategy_{report_kind}_{subject}_{generated_at}_internal_detail.md"
    )


def _strategy_client_output_path(report_kind: str, subject: str, generated_at: str) -> Path:
    return resolve_project_path(
        f"reports/strategy/{report_kind}/final/strategy_{report_kind}_{subject}_{generated_at}_client_final.md"
    )


def _export_strategy_client_bundle(
    *,
    report_kind: str,
    subject: str,
    markdown_text: str,
    extra_manifest: Mapping[str, Any] | None = None,
) -> Dict[str, Path]:
    date_str = datetime.now().strftime("%Y-%m-%d")
    detail_path = _strategy_detail_output_path(report_kind, subject, date_str)
    detail_path.parent.mkdir(parents=True, exist_ok=True)
    detail_path.write_text(markdown_text, encoding="utf-8")
    markdown_path = _strategy_client_output_path(report_kind, subject, date_str)
    review_path = review_path_for(markdown_path)
    scaffold_created = False
    if not review_path.exists():
        ensure_external_review_scaffold(
            review_path=review_path,
            markdown_path=markdown_path,
            report_type="strategy",
            report_kind=report_kind,
            detail_source=detail_path,
        )
        scaffold_created = True
    findings = check_generic_client_report(markdown_text, "strategy", source_text=markdown_text)
    manifest_extra = dict(extra_manifest or {})
    manifest_extra["detail_source"] = str(detail_path)
    manifest_extra["report_kind"] = report_kind
    try:
        return export_reviewed_markdown_bundle(
            report_type="strategy",
            markdown_text=markdown_text,
            markdown_path=markdown_path,
            release_findings=findings,
            extra_manifest=manifest_extra,
        )
    except ReportGuardError as exc:
        if scaffold_created:
            raise ReportGuardError(
                f"{exc}\n已生成外审模板：`{review_path}`。先完成 Pass A / Pass B，并把收敛结论更新到 PASS 后，再重跑同一命令。"
            ) from exc
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Strategy research ledger command.")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    predict_parser = subparsers.add_parser("predict", help="Generate and optionally persist a strategy v1 prediction snapshot")
    predict_parser.add_argument("symbol", help="A-share stock symbol")
    predict_parser.add_argument("--note", default="", help="Optional note to persist with the prediction snapshot")
    predict_parser.add_argument("--config", default="", help="Optional path to config YAML")
    predict_parser.add_argument("--preview", action="store_true", help="Render prediction but do not persist it")

    replay_parser = subparsers.add_parser("replay", help="Generate historical replay samples for strategy v1")
    replay_parser.add_argument("symbols", nargs="*", help="Zero or more A-share stock symbols")
    replay_parser.add_argument("--start", default="", help="Replay start date (YYYY-MM-DD)")
    replay_parser.add_argument("--end", default="", help="Replay end date (YYYY-MM-DD)")
    replay_parser.add_argument("--asset-gap-days", type=int, default=None, help="Minimum trading-day gap between replay samples for the same asset")
    replay_parser.add_argument("--max-samples", type=int, default=None, help="Maximum replay samples to generate")
    replay_parser.add_argument("--batch-source", default="", help="Batch source key defined in config/strategy_batches.yaml")
    replay_parser.add_argument("--cohort-recipe", default="", help="Cohort recipe key defined in config/strategy_batches.yaml")
    replay_parser.add_argument("--note", default="", help="Optional note to persist with the replay samples")
    replay_parser.add_argument("--config", default="", help="Optional path to config YAML")
    replay_parser.add_argument("--preview", action="store_true", help="Render replay summary but do not persist samples")

    validate_parser = subparsers.add_parser("validate", help="Validate stored strategy samples against realized forward windows")
    validate_parser.add_argument("--symbol", default="", help="Optional symbol filter")
    validate_parser.add_argument("--limit", type=int, default=100, help="Maximum stored rows to validate")
    validate_parser.add_argument("--config", default="", help="Optional path to config YAML")
    validate_parser.add_argument("--preview", action="store_true", help="Render validation summary but do not persist validation snapshots")
    validate_parser.add_argument("--client-final", action="store_true", help="Render and persist client-facing final markdown/pdf")

    attribute_parser = subparsers.add_parser("attribute", help="Attribute validated strategy samples into structured error buckets")
    attribute_parser.add_argument("--symbol", default="", help="Optional symbol filter")
    attribute_parser.add_argument("--limit", type=int, default=100, help="Maximum stored rows to attribute")
    attribute_parser.add_argument("--preview", action="store_true", help="Render attribution summary but do not persist attribution snapshots")

    experiment_parser = subparsers.add_parser("experiment", help="Compare predefined replay weight variants on the same historical sample set")
    experiment_parser.add_argument("symbols", nargs="*", help="Zero or more A-share stock symbols")
    experiment_parser.add_argument("--start", default="", help="Replay start date (YYYY-MM-DD)")
    experiment_parser.add_argument("--end", default="", help="Replay end date (YYYY-MM-DD)")
    experiment_parser.add_argument("--asset-gap-days", type=int, default=None, help="Minimum trading-day gap between replay samples for the same asset")
    experiment_parser.add_argument("--max-samples", type=int, default=None, help="Maximum replay samples to generate")
    experiment_parser.add_argument("--batch-source", default="", help="Batch source key defined in config/strategy_batches.yaml")
    experiment_parser.add_argument("--cohort-recipe", default="", help="Cohort recipe key defined in config/strategy_batches.yaml")
    experiment_parser.add_argument(
        "--variants",
        default="baseline,momentum_tilt,defensive_tilt,confirmation_tilt",
        help="Comma-separated variant names",
    )
    experiment_parser.add_argument("--config", default="", help="Optional path to config YAML")
    experiment_parser.add_argument("--client-final", action="store_true", help="Render and persist client-facing final markdown/pdf")

    list_parser = subparsers.add_parser("list", help="List recent prediction ledger rows")
    list_parser.add_argument("--symbol", default="", help="Optional symbol filter")
    list_parser.add_argument("--status", default="all", choices=["all", "predicted", "no_prediction"], help="Filter by prediction status")
    list_parser.add_argument("--limit", type=int, default=10, help="Maximum rows to show")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    renderer = StrategyReportRenderer()
    repository = StrategyRepository()

    if args.subcommand in {"validate", "experiment"} and bool(getattr(args, "client_final", False)):
        ensure_report_task_registered("strategy")

    if args.subcommand == "predict":
        setup_logger("ERROR")
        config = load_config(args.config or None)
        payload = generate_strategy_prediction(args.symbol, config, note=args.note)
        persisted = not args.preview
        if persisted:
            repository.upsert_prediction(payload)
        print(renderer.render_prediction(payload, persisted=persisted))
        return

    if args.subcommand == "replay":
        setup_logger("ERROR")
        config = load_config(args.config or None)
        registry = _load_strategy_batch_registry(config)
        try:
            batch_context = _resolve_strategy_batch_context(args.batch_source, config, registry)
            asset_gap_days, max_samples, cohort_recipe = _resolve_strategy_cohort_recipe(
                args.cohort_recipe,
                registry,
                asset_gap_days=args.asset_gap_days,
                max_samples=args.max_samples,
            )
            raw_symbols = [str(item) for item in list(args.symbols or []) if str(item).strip()]
            effective_symbols = _resolve_strategy_symbol_inputs(raw_symbols, batch_context)
        except ValueError as exc:
            parser.error(str(exc))

        finalized_batch_context = _finalize_batch_context(batch_context, effective_symbols)
        if len(effective_symbols) == 1:
            payload = generate_strategy_replay_predictions(
                effective_symbols[0],
                config,
                start=args.start,
                end=args.end,
                note=args.note,
                asset_gap_days=asset_gap_days,
                max_samples=max_samples,
                batch_context=finalized_batch_context,
                cohort_recipe=cohort_recipe,
            )
        else:
            payload = generate_strategy_multi_symbol_replay_predictions(
                effective_symbols,
                config,
                start=args.start,
                end=args.end,
                note=args.note,
                asset_gap_days=asset_gap_days,
                max_samples=max_samples,
                batch_context=finalized_batch_context,
                cohort_recipe=cohort_recipe,
            )
        persisted = not args.preview
        if persisted:
            for row in payload.get("rows", []):
                repository.upsert_prediction(row)
        print(renderer.render_replay_summary(payload, persisted=persisted))
        return

    if args.subcommand == "validate":
        if args.client_final and args.preview:
            parser.error("strategy validate 的 `--client-final` 不能和 `--preview` 一起使用。")
        setup_logger("ERROR")
        config = load_config(args.config or None)
        rows = repository.list_predictions(symbol=args.symbol, status="all", limit=args.limit)
        updated_rows, summary = validate_strategy_rows(rows, config)
        persisted = not args.preview
        if persisted:
            for row in updated_rows:
                repository.upsert_prediction(row)
        markdown = renderer.render_validation_summary(summary, persisted=persisted, client_facing=args.client_final)
        if not args.client_final:
            print(markdown)
            return
        try:
            bundle = _export_strategy_client_bundle(
                report_kind="validate",
                subject=_strategy_validate_subject(args.symbol),
                markdown_text=markdown,
                extra_manifest={
                    "symbol": str(args.symbol or ""),
                    "limit": int(args.limit),
                },
            )
        except ReportGuardError as exc:
            raise SystemExit(str(exc))
        print(markdown)
        for index, line in enumerate(exported_bundle_lines(bundle)):
            print(f"\n{line}" if index == 0 else line)
        return

    if args.subcommand == "attribute":
        rows = repository.list_predictions(symbol=args.symbol, status="all", limit=args.limit)
        updated_rows, summary = attribute_strategy_rows(rows)
        persisted = not args.preview
        if persisted:
            for row in updated_rows:
                repository.upsert_prediction(row)
        print(renderer.render_attribute_summary(summary, persisted=persisted))
        return

    if args.subcommand == "experiment":
        setup_logger("ERROR")
        config = load_config(args.config or None)
        registry = _load_strategy_batch_registry(config)
        try:
            batch_context = _resolve_strategy_batch_context(args.batch_source, config, registry)
            asset_gap_days, max_samples, cohort_recipe = _resolve_strategy_cohort_recipe(
                args.cohort_recipe,
                registry,
                asset_gap_days=args.asset_gap_days,
                max_samples=args.max_samples,
            )
            raw_symbols = [str(item) for item in list(args.symbols or []) if str(item).strip()]
            effective_symbols = _resolve_strategy_symbol_inputs(raw_symbols, batch_context)
        except ValueError as exc:
            parser.error(str(exc))
        variants = [item.strip() for item in str(args.variants or "").split(",") if item.strip()]
        finalized_batch_context = _finalize_batch_context(batch_context, effective_symbols)
        if len(effective_symbols) == 1:
            payload = generate_strategy_experiment(
                effective_symbols[0],
                config,
                start=args.start,
                end=args.end,
                asset_gap_days=asset_gap_days,
                max_samples=max_samples,
                variants=variants,
                batch_context=finalized_batch_context,
                cohort_recipe=cohort_recipe,
            )
        else:
            payload = generate_strategy_multi_symbol_experiment(
                effective_symbols,
                config,
                start=args.start,
                end=args.end,
                asset_gap_days=asset_gap_days,
                max_samples=max_samples,
                variants=variants,
                batch_context=finalized_batch_context,
                cohort_recipe=cohort_recipe,
            )
        markdown = renderer.render_experiment_summary(payload)
        if not args.client_final:
            print(markdown)
            return
        try:
            bundle = _export_strategy_client_bundle(
                report_kind="experiment",
                subject=_strategy_experiment_subject(payload, effective_symbols),
                markdown_text=markdown,
                extra_manifest={
                    "symbols": list(effective_symbols),
                    "symbol_count": len(effective_symbols),
                    "start": str(args.start or ""),
                    "end": str(args.end or ""),
                    "variants": variants,
                    "batch_source": str(finalized_batch_context.get("key", "")),
                    "cohort_recipe": str(cohort_recipe.get("key", "")),
                },
            )
        except ReportGuardError as exc:
            raise SystemExit(str(exc))
        print(markdown)
        for index, line in enumerate(exported_bundle_lines(bundle)):
            print(f"\n{line}" if index == 0 else line)
        return

    rows = repository.list_predictions(symbol=args.symbol, status=args.status, limit=args.limit)
    print(renderer.render_prediction_list(rows))


if __name__ == "__main__":
    main()
