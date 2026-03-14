"""Strategy prediction-ledger command."""

from __future__ import annotations

import argparse

from src.output.strategy_report import StrategyReportRenderer
from src.processors.strategy import generate_strategy_prediction, generate_strategy_replay_predictions, validate_strategy_rows
from src.storage.strategy import StrategyRepository
from src.utils.config import load_config
from src.utils.logger import setup_logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Strategy research ledger command.")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    predict_parser = subparsers.add_parser("predict", help="Generate and optionally persist a strategy v1 prediction snapshot")
    predict_parser.add_argument("symbol", help="A-share stock symbol")
    predict_parser.add_argument("--note", default="", help="Optional note to persist with the prediction snapshot")
    predict_parser.add_argument("--config", default="", help="Optional path to config YAML")
    predict_parser.add_argument("--preview", action="store_true", help="Render prediction but do not persist it")

    replay_parser = subparsers.add_parser("replay", help="Generate historical replay samples for strategy v1")
    replay_parser.add_argument("symbol", help="A-share stock symbol")
    replay_parser.add_argument("--start", default="", help="Replay start date (YYYY-MM-DD)")
    replay_parser.add_argument("--end", default="", help="Replay end date (YYYY-MM-DD)")
    replay_parser.add_argument("--asset-gap-days", type=int, default=20, help="Minimum trading-day gap between replay samples for the same asset")
    replay_parser.add_argument("--max-samples", type=int, default=12, help="Maximum replay samples to generate")
    replay_parser.add_argument("--note", default="", help="Optional note to persist with the replay samples")
    replay_parser.add_argument("--config", default="", help="Optional path to config YAML")
    replay_parser.add_argument("--preview", action="store_true", help="Render replay summary but do not persist samples")

    validate_parser = subparsers.add_parser("validate", help="Validate stored strategy samples against realized forward windows")
    validate_parser.add_argument("--symbol", default="", help="Optional symbol filter")
    validate_parser.add_argument("--limit", type=int, default=100, help="Maximum stored rows to validate")
    validate_parser.add_argument("--config", default="", help="Optional path to config YAML")
    validate_parser.add_argument("--preview", action="store_true", help="Render validation summary but do not persist validation snapshots")

    list_parser = subparsers.add_parser("list", help="List recent prediction ledger rows")
    list_parser.add_argument("--symbol", default="", help="Optional symbol filter")
    list_parser.add_argument("--status", default="all", choices=["all", "predicted", "no_prediction"], help="Filter by prediction status")
    list_parser.add_argument("--limit", type=int, default=10, help="Maximum rows to show")

    return parser


def main() -> None:
    args = build_parser().parse_args()
    renderer = StrategyReportRenderer()
    repository = StrategyRepository()

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
        payload = generate_strategy_replay_predictions(
            args.symbol,
            config,
            start=args.start,
            end=args.end,
            note=args.note,
            asset_gap_days=args.asset_gap_days,
            max_samples=args.max_samples,
        )
        persisted = not args.preview
        if persisted:
            for row in payload.get("rows", []):
                repository.upsert_prediction(row)
        print(renderer.render_replay_summary(payload, persisted=persisted))
        return

    if args.subcommand == "validate":
        setup_logger("ERROR")
        config = load_config(args.config or None)
        rows = repository.list_predictions(symbol=args.symbol, status="all", limit=args.limit)
        updated_rows, summary = validate_strategy_rows(rows, config)
        persisted = not args.preview
        if persisted:
            for row in updated_rows:
                repository.upsert_prediction(row)
        print(renderer.render_validation_summary(summary, persisted=persisted))
        return

    rows = repository.list_predictions(symbol=args.symbol, status=args.status, limit=args.limit)
    print(renderer.render_prediction_list(rows))


if __name__ == "__main__":
    main()
