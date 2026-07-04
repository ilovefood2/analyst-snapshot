from __future__ import annotations

import argparse
from pathlib import Path

from analyst_snapshot.config import load_config
from analyst_snapshot.datasets import parse_dataset_codes
from analyst_snapshot.logging_utils import JsonlLogger
from analyst_snapshot.runner import RunSummary, read_universe, run_id, run_snapshot
from analyst_snapshot.storage import compact_rating_events
from analyst_snapshot.trading_calendar import print_should_run_report
from analyst_snapshot.verify import print_coverage_report
from analyst_snapshot.yahoo import YahooAnalystFetcher


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="analyst_snapshot")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--datasets", help="Comma-separated dataset codes: a,b,c,d")
    run_parser.add_argument("--symbols", help="Comma-separated symbols. Defaults to UNIVERSE_FILE.")
    run_parser.add_argument("--resume", action="store_true")

    subparsers.add_parser("verify")
    subparsers.add_parser("compact")

    should_run_parser = subparsers.add_parser("should-run")
    should_run_parser.add_argument("--as-of-date", help="New York calendar date, YYYY-MM-DD.")
    should_run_parser.add_argument(
        "--github-output",
        help="Path from GitHub Actions GITHUB_OUTPUT.",
    )
    should_run_parser.add_argument("--force", action="store_true")

    args = parser.parse_args(argv)

    if args.command == "verify":
        config = load_config()
        print_coverage_report(config.snapshot_dir, config.universe_file, config.logs_dir)
        return 0

    if args.command == "should-run":
        print_should_run_report(args.as_of_date, args.github_output, args.force)
        return 0

    config = load_config()
    logger = JsonlLogger(config.logs_dir, run_id())

    if args.command == "run":
        symbols = _symbols_from_args(args.symbols, config.universe_file)
        dataset_codes = parse_dataset_codes(args.datasets)
        fetcher = YahooAnalystFetcher(config.symbol_delay_seconds)
        summary = run_snapshot(
            snapshot_dir=config.snapshot_dir,
            fetcher=fetcher,
            dataset_codes=dataset_codes,
            symbols=symbols,
            logger=logger,
            resume=args.resume,
        )
        print(_summary_text(summary, logger.path))
        return 0

    if args.command == "compact":
        removed = compact_rating_events(config.snapshot_dir)
        print(f"rating_events_compacted duplicates_removed={removed}")
        return 0

    return 1


def _symbols_from_args(raw_symbols: str | None, universe_file: Path) -> list[str]:
    if raw_symbols:
        return [symbol.strip().upper() for symbol in raw_symbols.split(",") if symbol.strip()]
    return read_universe(universe_file)


def _summary_text(summary: RunSummary, log_path: Path) -> str:
    return (
        f"symbols_attempted={summary.symbols_attempted} "
        f"failures={len(summary.failures)} "
        f"events_added={summary.events_added} "
        f"retry_symbols={len(summary.retry_symbols)} "
        f"log={log_path}"
    )
