from __future__ import annotations

import argparse
import json
from pathlib import Path

from analyst_snapshot.config import load_config
from analyst_snapshot.daily_prices import run_daily_prices, verify_daily_prices
from analyst_snapshot.datasets import parse_dataset_codes
from analyst_snapshot.dropbox_sync import (
    authorization_url,
    exchange_authorization_code,
    load_dropbox_secrets,
    publish_recovery_bundle,
    upload_directory,
)
from analyst_snapshot.logging_utils import JsonlLogger
from analyst_snapshot.market_context import run_market_context, verify_market_context
from analyst_snapshot.price_recovery import (
    finalize_price_recovery_bundle,
    publish_price_recovery_bundle,
)
from analyst_snapshot.reader import archive_summary
from analyst_snapshot.recovery_bundle import finalize_recovery_bundle
from analyst_snapshot.runner import RunSummary, read_universe, run_id, run_snapshot
from analyst_snapshot.storage import compact_rating_events, rebuild_rating_events_index
from analyst_snapshot.trading_calendar import (
    DEFAULT_OFFSET_DAYS,
    print_schedule_gate_report,
    print_should_run_report,
)
from analyst_snapshot.universe import (
    DEFAULT_EXCHANGES,
    DEFAULT_MIN_MARKET_CAP,
    fetch_screener_rows,
    load_rows_from_files,
    read_existing,
    select_symbols,
    write_universe,
)
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

    daily_prices_parser = subparsers.add_parser(
        "daily-prices",
        help="Capture sealed-input Yahoo raw+adjusted prices from a 30-session window.",
    )
    daily_prices_parser.add_argument("--run-date", required=True)
    daily_prices_parser.add_argument("--resume", action="store_true")
    daily_prices_parser.add_argument("--batch-size", type=int, choices=(50,), default=50)

    verify_daily_prices_parser = subparsers.add_parser(
        "verify-daily-prices",
        help="Recompute Daily price schema, PIT, session, coverage, and adjustment checks.",
    )
    verify_daily_prices_parser.add_argument("--run-date", required=True)
    verify_daily_prices_parser.add_argument("--fail-under", type=float, default=0.95)
    verify_daily_prices_parser.add_argument("--json-out")

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

    market_parser = subparsers.add_parser(
        "market-context",
        help="Capture free official CFTC/OCC/FINRA prospective market context.",
    )
    market_parser.add_argument("--run-date", required=True, help="Snapshot partition date.")
    market_parser.add_argument("--symbols", default="QQQ,SPY")
    market_parser.add_argument("--resume", action="store_true")

    verify_market_parser = subparsers.add_parser(
        "verify-market-context",
        help="Verify market-context source freshness, hashes, and required scopes.",
    )
    verify_market_parser.add_argument("--run-date", required=True)
    verify_market_parser.add_argument("--json-out")

    subparsers.add_parser("compact", help="Drop duplicate keys from the rating-event index.")
    subparsers.add_parser(
        "repair-events",
        help="Rebuild the rating-event index from the daily upgrades_downgrades partitions.",
    )
    subparsers.add_parser("info", help="Print an inventory of the archive as JSON.")

    universe_parser = subparsers.add_parser(
        "build-universe",
        help="Rebuild UNIVERSE_FILE from the NASDAQ screener (Nasdaq + NYSE common stock/ADRs).",
    )
    universe_parser.add_argument(
        "--min-market-cap",
        type=float,
        default=DEFAULT_MIN_MARKET_CAP,
        help="Market-cap floor in USD. Symbols already in the universe are kept regardless.",
    )
    universe_parser.add_argument(
        "--exchange",
        action="append",
        dest="exchanges",
        help="Exchange to include; repeatable. Defaults to nasdaq and nyse.",
    )
    universe_parser.add_argument(
        "--from-json",
        action="append",
        dest="from_json",
        help="Read screener rows from local JSON instead of the network; repeatable.",
    )
    universe_parser.add_argument(
        "--replace",
        action="store_true",
        help="Drop symbols already in the universe. Stops their history; rarely the right choice.",
    )
    universe_parser.add_argument("--dry-run", action="store_true")
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

    publish_recovery_parser = subparsers.add_parser(
        "upload-recovery-bundle",
        help="Publish one sealed recovery generation to Dropbox, with READY written last.",
    )
    publish_recovery_parser.add_argument("--local-dir", default=None)
    publish_recovery_parser.add_argument("--remote-root", default=None)
    publish_recovery_parser.add_argument("--run-date", required=True)

    publish_price_recovery_parser = subparsers.add_parser(
        "upload-price-recovery-bundle",
        help="Publish one sealed price-only generation, with PRICE_READY written last.",
    )
    publish_price_recovery_parser.add_argument("--local-dir", default=None)
    publish_price_recovery_parser.add_argument("--remote-root", default=None)
    publish_price_recovery_parser.add_argument("--run-date", required=True)

    seal_parser = subparsers.add_parser(
        "seal-recovery-bundle",
        help="Strictly validate and seal one completed-session recovery bundle.",
    )
    seal_parser.add_argument("--run-date", required=True)
    seal_parser.add_argument("--generation-id")
    seal_parser.add_argument("--min-coverage", type=float, choices=(0.95,), default=0.95)
    seal_parser.add_argument("--repository")
    seal_parser.add_argument("--git-ref")
    seal_parser.add_argument("--git-sha")
    seal_parser.add_argument("--workflow-run-id")
    seal_parser.add_argument("--workflow-run-attempt")
    seal_parser.add_argument("--json-out")

    seal_price_parser = subparsers.add_parser(
        "seal-price-recovery-bundle",
        help="Strictly validate and seal the independent Daily-price file pair.",
    )
    seal_price_parser.add_argument("--run-date", required=True)
    seal_price_parser.add_argument("--generation-id")
    seal_price_parser.add_argument("--min-coverage", type=float, choices=(0.95,), default=0.95)
    seal_price_parser.add_argument("--repository")
    seal_price_parser.add_argument("--git-ref")
    seal_price_parser.add_argument("--git-sha")
    seal_price_parser.add_argument("--workflow-run-id")
    seal_price_parser.add_argument("--workflow-run-attempt")
    seal_price_parser.add_argument("--json-out")

    should_run_parser = subparsers.add_parser("should-run")
    should_run_parser.add_argument("--as-of-date", help="New York calendar date, YYYY-MM-DD.")
    should_run_parser.add_argument(
        "--session-date",
        help="Explicit XNYS session to validate as completed; cannot be forced pre-close.",
    )
    should_run_parser.add_argument(
        "--now-utc",
        help="Testing/diagnostic ISO-8601 clock override. Must include a timezone.",
    )
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

    schedule_gate_parser = subparsers.add_parser(
        "schedule-gate",
        help="Authorize the active 18:30 America/New_York GitHub cron lane.",
    )
    schedule_gate_parser.add_argument("--event-name", required=True)
    schedule_gate_parser.add_argument("--event-schedule", default="")
    schedule_gate_parser.add_argument("--github-output")
    schedule_gate_parser.add_argument(
        "--now-utc",
        help="Testing/diagnostic ISO-8601 clock override. Must include a timezone.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "should-run":
        print_should_run_report(
            args.as_of_date,
            args.github_output,
            args.force,
            args.offset_days,
            session_date_raw=args.session_date,
            now_utc_raw=args.now_utc,
        )
        return 0

    if args.command == "schedule-gate":
        print_schedule_gate_report(
            args.event_name,
            args.event_schedule,
            args.github_output,
            now_utc_raw=args.now_utc,
        )
        return 0

    config = load_config()

    if args.command == "daily-prices":
        result = run_daily_prices(
            config.snapshot_dir,
            config.universe_file,
            session_date=args.run_date,
            resume=args.resume,
            batch_size=args.batch_size,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("status") == "complete" else 1

    if args.command == "verify-daily-prices":
        report = verify_daily_prices(
            config.snapshot_dir,
            config.universe_file,
            session_date=args.run_date,
            min_coverage=args.fail_under,
        )
        rendered = json.dumps(report, indent=2, sort_keys=True)
        print(rendered)
        if args.json_out:
            output = Path(args.json_out)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered + "\n", encoding="utf-8")
        return 0 if report.get("ok") else 1

    if args.command == "market-context":
        symbols = tuple(
            symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()
        )
        result = run_market_context(
            config.snapshot_dir,
            run_date=args.run_date,
            symbols=symbols,
            resume=args.resume,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("status") == "complete" else 1

    if args.command == "verify-market-context":
        report = verify_market_context(config.snapshot_dir, run_date=args.run_date)
        rendered = json.dumps(report, indent=2, sort_keys=True)
        print(rendered)
        if args.json_out:
            Path(args.json_out).write_text(rendered + "\n", encoding="utf-8")
        return 0 if report["ok"] else 1

    if args.command == "verify":
        return print_coverage_report(
            config.snapshot_dir,
            config.universe_file,
            config.logs_dir,
            run_date=args.run_date,
            fail_under=args.fail_under,
            json_out=Path(args.json_out) if args.json_out else None,
        )

    if args.command == "build-universe":
        return _build_universe(args, config.universe_file)

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

    if args.command == "upload-recovery-bundle":
        local_dir = Path(args.local_dir) if args.local_dir else config.snapshot_dir
        remote_root = args.remote_root or config.dropbox_remote_root
        count = publish_recovery_bundle(
            local_dir,
            remote_root,
            load_dropbox_secrets(),
            run_date=args.run_date,
            universe_file=config.universe_file,
        )
        print(f"dropbox_recovery_uploaded_files={count}")
        return 0

    if args.command == "upload-price-recovery-bundle":
        local_dir = Path(args.local_dir) if args.local_dir else config.snapshot_dir
        remote_root = args.remote_root or config.dropbox_remote_root
        count = publish_price_recovery_bundle(
            local_dir,
            remote_root,
            load_dropbox_secrets(),
            run_date=args.run_date,
            universe_file=config.universe_file,
        )
        print(f"dropbox_price_recovery_uploaded_files={count}")
        return 0

    if args.command == "seal-recovery-bundle":
        producer = {
            key: value
            for key, value in {
                "repository": args.repository,
                "git_ref": args.git_ref,
                "git_sha": args.git_sha,
                "workflow_run_id": args.workflow_run_id,
                "workflow_run_attempt": args.workflow_run_attempt,
            }.items()
            if value
        }
        manifest = finalize_recovery_bundle(
            config.snapshot_dir,
            config.universe_file,
            session_date=args.run_date,
            min_coverage=args.min_coverage,
            generation_id=args.generation_id,
            producer_identity=producer,
        )
        rendered = json.dumps(manifest, indent=2, sort_keys=True)
        print(rendered)
        if args.json_out:
            output = Path(args.json_out)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered + "\n", encoding="utf-8")
        return 0

    if args.command == "seal-price-recovery-bundle":
        producer = {
            key: value
            for key, value in {
                "repository": args.repository,
                "git_ref": args.git_ref,
                "git_sha": args.git_sha,
                "workflow_run_id": args.workflow_run_id,
                "workflow_run_attempt": args.workflow_run_attempt,
            }.items()
            if value
        }
        manifest = finalize_price_recovery_bundle(
            config.snapshot_dir,
            config.universe_file,
            session_date=args.run_date,
            min_coverage=args.min_coverage,
            generation_id=args.generation_id,
            producer_identity=producer,
        )
        rendered = json.dumps(manifest, indent=2, sort_keys=True)
        print(rendered)
        if args.json_out:
            output = Path(args.json_out)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered + "\n", encoding="utf-8")
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


def _build_universe(args: argparse.Namespace, universe_file: Path) -> int:
    if args.from_json:
        rows = load_rows_from_files([Path(path) for path in args.from_json])
    else:
        rows = fetch_screener_rows(tuple(args.exchanges or DEFAULT_EXCHANGES))
    carry_over = [] if args.replace else read_existing(universe_file)
    symbols, stats = select_symbols(rows, args.min_market_cap, carry_over=carry_over)
    print(
        json.dumps(
            {
                "listings": stats.listings,
                "after_security_filter": stats.after_security_filter,
                "above_market_cap_floor": stats.after_market_cap_filter,
                "carried_over_from_existing": stats.carried_over,
                "total": stats.total,
                "min_market_cap": args.min_market_cap,
                "path": str(universe_file),
                "written": not args.dry_run,
            },
            indent=2,
            sort_keys=True,
        )
    )
    if not args.dry_run:
        write_universe(universe_file, symbols)
    return 0


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
