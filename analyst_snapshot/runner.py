from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

from analyst_snapshot import __version__
from analyst_snapshot.datasets import DATASETS, DatasetSpec, no_coverage_row, parse_dataset_payload
from analyst_snapshot.logging_utils import JsonlLogger
from analyst_snapshot.storage import (
    DEFAULT_FLUSH_EVERY_SYMBOLS,
    SnapshotWriter,
    dataset_path,
    utc_now_iso,
)
from analyst_snapshot.yahoo import DatasetFetchFailure

MANIFEST_DIR_NAME = "_manifests"


class Fetcher(Protocol):
    def fetch_symbol(self, symbol: str, specs: Iterable[DatasetSpec]) -> dict[str, object]: ...


@dataclass
class RunSummary:
    run_id: str = ""
    run_date: str = ""
    started_utc: str = ""
    finished_utc: str = ""
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
    run_identifier: str | None = None,
    flush_every_symbols: int = DEFAULT_FLUSH_EVERY_SYMBOLS,
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
        run_identifier=run_identifier,
        flush_every_symbols=flush_every_symbols,
    )


def _run_symbols(
    snapshot_dir: Path,
    fetcher: Fetcher,
    specs: list[DatasetSpec],
    symbols: list[str],
    logger: JsonlLogger,
    resume: bool,
    run_date: str | None,
    run_identifier: str | None,
    flush_every_symbols: int,
) -> RunSummary:
    today = run_date or date.today().isoformat()
    identifier = run_identifier or run_id()
    summary = RunSummary(run_id=identifier, run_date=today, started_utc=utc_now_iso())
    writer = SnapshotWriter(
        snapshot_dir,
        today,
        run_id=identifier,
        flush_every_symbols=flush_every_symbols,
    )
    retry_queue: list[str] = []

    try:
        for symbol in symbols:
            if resume and _symbol_is_complete(writer, specs, symbol):
                summary.skipped.append({"symbol": symbol, "reason": "already_snapshotted_today"})
                logger.write("skipped_resume", symbol=symbol)
                continue
            if not _fetch_and_store_symbol(writer, fetcher, specs, symbol, logger, summary):
                retry_queue.append(symbol)

        if retry_queue:
            summary.retry_symbols = retry_queue.copy()
            logger.write("retry_queue_started", symbols=retry_queue)
        for symbol in retry_queue:
            _fetch_and_store_symbol(writer, fetcher, specs, symbol, logger, summary, retry=True)
    finally:
        writer.flush()

    summary.finished_utc = utc_now_iso()
    write_manifest(snapshot_dir, summary, specs)
    logger.write(
        "run_complete",
        run_id=summary.run_id,
        run_date=summary.run_date,
        symbols_attempted=summary.symbols_attempted,
        failures=len(summary.failures),
        events_added=summary.events_added,
    )
    return summary


def _fetch_and_store_symbol(
    writer: SnapshotWriter,
    fetcher: Fetcher,
    specs: list[DatasetSpec],
    symbol: str,
    logger: JsonlLogger,
    summary: RunSummary,
    retry: bool = False,
) -> bool:
    summary.symbols_attempted += 1
    if retry:
        specs = [spec for spec in specs if symbol not in writer.known_symbols(spec.name)]
    try:
        payloads = fetcher.fetch_symbol(symbol, specs)
    except Exception as exc:  # noqa: BLE001
        summary.failures.append({"symbol": symbol, "error": str(exc), "retry": str(retry)})
        logger.write("symbol_failure", symbol=symbol, error=str(exc), retry=retry)
        return False

    snapshot_utc = utc_now_iso()  # availability is after the requests, including any retry
    complete = True
    for spec in specs:
        failure = payloads.get(spec.name)
        if isinstance(failure, DatasetFetchFailure) or spec.name not in payloads:
            error = (
                failure.message
                if isinstance(failure, DatasetFetchFailure)
                else "dataset result missing"
            )
            summary.failures.append(
                {
                    "symbol": symbol,
                    "dataset": spec.name,
                    "error": error,
                    "retry": str(retry),
                }
            )
            logger.write(
                "dataset_failure", symbol=symbol, dataset=spec.name, error=error, retry=retry
            )
            complete = False
            continue
        rows = parse_dataset_payload(spec.name, payloads.get(spec.name), symbol, snapshot_utc)
        no_coverage = not rows
        if no_coverage:
            rows = [no_coverage_row(spec.name, symbol, snapshot_utc)]
            summary.no_coverage.setdefault(spec.name, []).append(symbol)

        written = writer.add_rows(spec.name, rows)
        summary.rows_written[spec.name] = summary.rows_written.get(spec.name, 0) + written
        if spec.name == "upgrades_downgrades":
            summary.events_added += writer.add_rating_events(rows, snapshot_utc)
        logger.write(
            "dataset_written",
            symbol=symbol,
            dataset=spec.name,
            rows=written,
            no_coverage=no_coverage,
            retry=retry,
        )
    writer.symbol_done()
    return complete


def _symbol_is_complete(writer: SnapshotWriter, specs: list[DatasetSpec], symbol: str) -> bool:
    return all(symbol in writer.known_symbols(spec.name) for spec in specs)


def write_manifest(snapshot_dir: Path, summary: RunSummary, specs: list[DatasetSpec]) -> Path:
    """Record run lineage next to the archive.

    Manifests live under a leading-underscore directory so Parquet dataset readers skip them.
    """
    path = snapshot_dir / MANIFEST_DIR_NAME / f"date={summary.run_date}" / f"{summary.run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "run_id": summary.run_id,
        "run_date": summary.run_date,
        "started_utc": summary.started_utc,
        "finished_utc": summary.finished_utc,
        "analyst_snapshot_version": __version__,
        "package_versions": _package_versions(),
        "datasets": {
            spec.name: {
                "rows_written": summary.rows_written.get(spec.name, 0),
                "symbols_no_coverage": len(summary.no_coverage.get(spec.name, [])),
                "path": str(dataset_path(snapshot_dir, spec.name, summary.run_date)),
            }
            for spec in specs
        },
        "symbols_attempted": summary.symbols_attempted,
        "symbols_skipped_resume": len(summary.skipped),
        "symbols_retried": summary.retry_symbols,
        "failures": summary.failures,
        "events_added": summary.events_added,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _package_versions() -> dict[str, str]:
    from importlib.metadata import PackageNotFoundError, version

    versions: dict[str, str] = {}
    for name in ("yfinance", "pandas", "pyarrow", "pandas-market-calendars"):
        try:
            versions[name] = version(name)
        except PackageNotFoundError:  # pragma: no cover - depends on the environment
            versions[name] = "unknown"
    return versions


def run_id() -> str:
    return datetime.now(UTC).strftime("run_%Y%m%dT%H%M%SZ")
