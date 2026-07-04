from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol

from analyst_snapshot.datasets import DATASETS, DatasetSpec, no_coverage_row, parse_dataset_payload
from analyst_snapshot.logging_utils import JsonlLogger
from analyst_snapshot.storage import (
    append_rating_events,
    append_rows,
    dataset_path,
    symbols_in_snapshot,
    utc_now_iso,
)


class Fetcher(Protocol):
    def fetch_symbol(self, symbol: str, specs: Iterable[DatasetSpec]) -> dict[str, object]: ...


@dataclass
class RunSummary:
    symbols_attempted: int = 0
    rows_written: dict[str, int] = field(default_factory=dict)
    failures: list[dict[str, str]] = field(default_factory=list)
    skipped: list[dict[str, str]] = field(default_factory=list)
    events_added: int = 0
    retry_symbols: list[str] = field(default_factory=list)
    no_coverage: dict[str, list[str]] = field(default_factory=dict)


def read_universe(path: Path) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        symbol = line.strip().upper()
        if not symbol or symbol.startswith("#") or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def run_snapshot(
    snapshot_dir: Path,
    fetcher: Fetcher,
    dataset_codes: Iterable[str],
    symbols: Iterable[str],
    logger: JsonlLogger,
    resume: bool = False,
    run_date: str | None = None,
) -> RunSummary:
    specs = [DATASETS[code] for code in dataset_codes]
    return _run_symbols(
        snapshot_dir=snapshot_dir,
        fetcher=fetcher,
        specs=specs,
        symbols=list(symbols),
        logger=logger,
        resume=resume,
        run_date=run_date,
    )


def _run_symbols(
    snapshot_dir: Path,
    fetcher: Fetcher,
    specs: list[DatasetSpec],
    symbols: list[str],
    logger: JsonlLogger,
    resume: bool,
    run_date: str | None,
) -> RunSummary:
    summary = RunSummary()
    today = run_date or date.today().isoformat()
    retry_queue: list[str] = []

    for symbol in symbols:
        if resume and _symbol_is_complete(snapshot_dir, specs, symbol, today):
            summary.skipped.append({"symbol": symbol, "reason": "already_snapshotted_today"})
            logger.write("skipped_resume", symbol=symbol)
            continue
        if not _fetch_and_store_symbol(
            snapshot_dir,
            fetcher,
            specs,
            symbol,
            today,
            logger,
            summary,
        ):
            retry_queue.append(symbol)

    if retry_queue:
        summary.retry_symbols = retry_queue.copy()
        logger.write("retry_queue_started", symbols=retry_queue)
    for symbol in retry_queue:
        _fetch_and_store_symbol(
            snapshot_dir,
            fetcher,
            specs,
            symbol,
            today,
            logger,
            summary,
            retry=True,
        )

    logger.write(
        "run_complete",
        symbols_attempted=summary.symbols_attempted,
        failures=len(summary.failures),
        events_added=summary.events_added,
    )
    return summary


def _fetch_and_store_symbol(
    snapshot_dir: Path,
    fetcher: Fetcher,
    specs: list[DatasetSpec],
    symbol: str,
    run_date: str,
    logger: JsonlLogger,
    summary: RunSummary,
    retry: bool = False,
) -> bool:
    summary.symbols_attempted += 1
    try:
        payloads = fetcher.fetch_symbol(symbol, specs)
    except Exception as exc:  # noqa: BLE001
        summary.failures.append({"symbol": symbol, "error": str(exc), "retry": str(retry)})
        logger.write("symbol_failure", symbol=symbol, error=str(exc), retry=retry)
        return False

    for spec in specs:
        snapshot_utc = utc_now_iso()
        rows = parse_dataset_payload(spec.name, payloads.get(spec.name), symbol, snapshot_utc)
        if not rows:
            rows = [no_coverage_row(spec.name, symbol, snapshot_utc)]
            summary.no_coverage.setdefault(spec.name, []).append(symbol)

        written = append_rows(dataset_path(snapshot_dir, spec.name, run_date), rows)
        summary.rows_written[spec.name] = summary.rows_written.get(spec.name, 0) + written
        if spec.name == "upgrades_downgrades":
            summary.events_added += append_rating_events(snapshot_dir, rows, snapshot_utc)
        logger.write(
            "dataset_written",
            symbol=symbol,
            dataset=spec.name,
            rows=written,
            no_coverage=bool(rows and rows[0].get("no_analyst_coverage")),
            retry=retry,
        )
    return True


def _symbol_is_complete(
    snapshot_dir: Path,
    specs: list[DatasetSpec],
    symbol: str,
    run_date: str,
) -> bool:
    return all(symbol in symbols_in_snapshot(snapshot_dir, spec.name, run_date) for spec in specs)


def run_id() -> str:
    return datetime.now(UTC).strftime("run_%Y%m%dT%H%M%SZ")
