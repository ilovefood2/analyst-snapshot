from __future__ import annotations

import argparse
import json
from pathlib import Path

from analyst_snapshot.config import load_config
from analyst_snapshot.datasets import parse_dataset_codes
from analyst_snapshot.dropbox_sync import (
    authorization_url,
    exchange_authorization_code,
    load_dropbox_secrets,
    upload_directory,
)
from analyst_snapshot.logging_utils import JsonlLogger
from analyst_snapshot.reader import archive_summary
from analyst_snapshot.runner import RunSummary, read_universe, run_id, run_snapshot
from analyst_snapshot.storage import compact_rating_events, rebuild_rating_events_index
from analyst_snapshot.trading_calendar import DEFAULT_OFFSET_DAYS, print_should_run_report
from analyst_snapshot.verify import print_coverage_report
from analyst_snapshot.yahoo import YahooAnalystFetcher


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="analyst_snapshot")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Fetch and archive a daily snapshot.")
    run_parser.add_argument("--datasets", help="Comma-separated dataset codes: a,b,c,d")
    run_parser.add_argument("--symbols", help="Comma-separated symbols. Defaults to UNIVERSE_FILE.")
    run_parser.add_argument("--resume", action="store_true")
    run_parser.add_argument("--run-date", help="Snapshot partition date, YYYY-MM-DD.")
    run_parser.add_argument(
        "--flush-every",
        type=int,
        default=50,
        help="Symbols buffered before a partition is rewritten. Lower is safer, slower.",
    )

    verify_parser = subparsers.add_parser("verify", help="Report archive coverage as JSON.")
    verify_parser.add_argument(
        "--run-date",
        help="Partition date to report on. Defaults to the newest partition on disk.",
    )
    verify_parser.add_argument(
        "--fail-under",
        type=float,
        help="Exit non-zero when any dataset covers less than this fraction of the universe.",
    )
    verify_parser.add_argument("--json-out", help="Also write the report to this path.")

    subparsers.add_parser("compact", help="Drop duplicate keys from the rating-event index.")
    subparsers.add_parser(
        "repair-events",
        help="Rebuild the rating-event index from the daily upgrades_downgrades partitions.",
    )
    subparsers.add_parser("info", help="Print an inventory of the archive as JSON.")
    subparsers.add_parser("dropbox-auth-url")

    dropbox_exchange_parser = subparsers.add_parser("dropbox-exchange-code")
    dropbox_exchange_parser.add_argument("--code", required=True)

    upload_dropbox_parser = subparsers.add_parser("upload-dropbox")
    upload_dropbox_parser.add_argument("--local-dir", default=None)
    upload_dropbox_parser.add_argument("--remote-root", default=None)
    upload_dropbox_parser.add_argument(
        "--run-date",
        help="Upload only this partition date. Without it the whole archive is re-uploaded.",
    )

    should_run_parser = subparsers.add_parser("should-run")
    should_run_parser.add_argument("--as-of-date", help="New York calendar date, YYYY-MM-DD.")
    should_run_parser.add_argument(
        "--github-output",
        help="Path from GitHub Actions GITHUB_OUTPUT.",
    )
    should_run_parser.add_argument("--force", action="store_true")
    should_run_parser.add_argument(
        "--offset-days",
        type=int,
        default=DEFAULT_OFFSET_DAYS,
        help="Days before --as-of-date to check. 0 for an after-close run, 1 for a morning run.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "should-run":
        print_should_run_report(args.as_of_date, args.github_output, args.force, args.offset_days)
        return 0

    config = load_config()

    if args.command == "verify":
        return print_coverage_report(
            config.snapshot_dir,
            config.universe_file,
            config.logs_dir,
            run_date=args.run_date,
            fail_under=args.fail_under,
            json_out=Path(args.json_out) if args.json_out else None,
        )

    if args.command == "info":
        print(json.dumps(archive_summary(config.snapshot_dir), indent=2, sort_keys=True))
        return 0

    if args.command == "dropbox-auth-url":
        secrets = load_dropbox_secrets(require_refresh_token=False, require_app_secret=False)
        print(authorization_url(secrets.app_key))
        return 0

    if args.command == "dropbox-exchange-code":
        secrets = load_dropbox_secrets(require_refresh_token=False)
        print(exchange_authorization_code(secrets, args.code))
        return 0

    if args.command == "upload-dropbox":
        local_dir = Path(args.local_dir) if args.local_dir else config.snapshot_dir
        remote_root = args.remote_root or config.dropbox_remote_root
        count = upload_directory(
            local_dir,
            remote_root,
            load_dropbox_secrets(),
            run_date=args.run_date,
        )
        print(f"dropbox_uploaded_files={count}")
        return 0

    if args.command == "repair-events":
        result = rebuild_rating_events_index(config.snapshot_dir, verbose=True)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "compact":
        try:
            removed = compact_rating_events(config.snapshot_dir)
        except RuntimeError as exc:
            print(f"compact refused: {exc}")
            return 1
        print(f"rating_events_compacted duplicates_removed={removed}")
        return 0

    if args.command == "run":
        identifier = run_id()
        logger = JsonlLogger(config.logs_dir, identifier)
        summary = run_snapshot(
            snapshot_dir=config.snapshot_dir,
            fetcher=YahooAnalystFetcher(config.symbol_delay_seconds),
            dataset_codes=parse_dataset_codes(args.datasets),
            symbols=_symbols_from_args(args.symbols, config.universe_file),
            logger=logger,
            resume=args.resume,
            run_date=args.run_date,
            run_identifier=identifier,
            flush_every_symbols=args.flush_every,
        )
        print(_summary_text(summary, logger.path))
        return 0

    return 1


def _symbols_from_args(raw_symbols: str | None, universe_file: Path) -> list[str]:
    if raw_symbols:
        return [symbol.strip().upper() for symbol in raw_symbols.split(",") if symbol.strip()]
    return read_universe(universe_file)


def _summary_text(summary: RunSummary, log_path: Path) -> str:
    return (
        f"run_id={summary.run_id} "
        f"run_date={summary.run_date} "
        f"symbols_attempted={summary.symbols_attempted} "
        f"failures={len(summary.failures)} "
        f"events_added={summary.events_added} "
        f"retry_symbols={len(summary.retry_symbols)} "
        f"log={log_path}"
    )
