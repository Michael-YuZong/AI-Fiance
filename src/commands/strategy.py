"""Strategy prediction-ledger command."""

from __future__ import annotations

import argparse

from src.output.strategy_report import StrategyReportRenderer
from src.processors.strategy import generate_strategy_prediction
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

    rows = repository.list_predictions(symbol=args.symbol, status=args.status, limit=args.limit)
    print(renderer.render_prediction_list(rows))


if __name__ == "__main__":
    main()
