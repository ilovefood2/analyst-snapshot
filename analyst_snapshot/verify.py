from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from analyst_snapshot.datasets import DATASETS, data_bearing_symbols
from analyst_snapshot.runner import read_universe
from analyst_snapshot.storage import dataset_path, partition_paths, read_parquet_or_empty

MISSING_SAMPLE = 50
NO_COVERAGE_SAMPLE = 100


def available_dates(snapshot_dir: Path) -> list[str]:
    dates: set[str] = set()
    for spec in DATASETS.values():
        for path in partition_paths(snapshot_dir, spec.name):
            dates.add(path.parent.name.removeprefix("date="))
    return sorted(dates)


def coverage_report(
    snapshot_dir: Path,
    universe_file: Path,
    logs_dir: Path,
    run_date: str | None = None,
    compare_date: str | None = None,
) -> dict[str, Any]:
    universe = read_universe(universe_file) if universe_file.exists() else []
    expected = set(universe)
    dates = available_dates(snapshot_dir)
    # Default to the newest partition on disk. `date.today()` was wrong in CI, where the run writes
    # to the previous trading date and the report always described an empty partition.
    target = run_date or (dates[-1] if dates else date.today().isoformat())
    previous = compare_date or _previous_date(dates, target)

    report: dict[str, Any] = {
        "expected_symbols": len(expected),
        "run_date": target,
        "compare_date": previous,
        "available_dates": len(dates),
        "datasets": {},
    }
    ratios: list[float] = []

    for spec in DATASETS.values():
        today_df = read_parquet_or_empty(dataset_path(snapshot_dir, spec.name, target))
        previous_df = (
            read_parquet_or_empty(dataset_path(snapshot_dir, spec.name, previous))
            if previous
            else pd.DataFrame()
        )
        today_symbols = _symbols(today_df)
        previous_symbols = _symbols(previous_df)
        no_coverage = _no_coverage_symbols(today_df)
        previous_no_coverage = _no_coverage_symbols(previous_df)
        data_symbols = data_bearing_symbols(today_df)
        missing = expected - data_symbols
        # Yahoo answering "no coverage" for a symbol that had coverage the day before is the
        # signature of a throttled response being archived as real data.
        newly_uncovered = sorted((no_coverage - previous_no_coverage) & previous_symbols)
        ratio = len(data_symbols & expected) / len(expected) if expected else 0.0
        ratios.append(ratio)

        report["datasets"][spec.name] = {
            "symbol_coverage_ratio": round(ratio, 4),
            "symbols_with_data": len(data_symbols & expected),
            "recorded_symbol_ratio": len(today_symbols & expected) / len(expected)
            if expected
            else 0.0,
            "run_date_symbols_snapshotted": len(today_symbols),
            "run_date_rows": int(len(today_df)),
            "compare_date_symbols_snapshotted": len(previous_symbols),
            "compare_date_rows": int(len(previous_df)),
            "missing_symbols": sorted(missing)[:MISSING_SAMPLE],
            "missing_symbols_truncated": max(len(missing) - MISSING_SAMPLE, 0),
            "symbols_with_no_analyst_coverage": sorted(no_coverage)[:NO_COVERAGE_SAMPLE],
            "symbols_with_no_analyst_coverage_truncated": max(
                len(no_coverage) - NO_COVERAGE_SAMPLE, 0
            ),
            "newly_uncovered_symbols": newly_uncovered[:NO_COVERAGE_SAMPLE],
            "newly_uncovered_symbols_count": len(newly_uncovered),
            "path": str(dataset_path(snapshot_dir, spec.name, target)),
        }

    report["min_symbol_coverage_ratio"] = round(min(ratios), 4) if ratios else 0.0
    report["failures"] = _recent_failures(logs_dir)
    return report


def print_coverage_report(
    snapshot_dir: Path,
    universe_file: Path,
    logs_dir: Path,
    run_date: str | None = None,
    fail_under: float | None = None,
    json_out: Path | None = None,
) -> int:
    report = coverage_report(snapshot_dir, universe_file, logs_dir, run_date=run_date)
    text = json.dumps(report, indent=2, sort_keys=True)
    print(text)
    if json_out:
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(text, encoding="utf-8")
    if fail_under is not None and report["min_symbol_coverage_ratio"] < fail_under:
        print(
            f"FAIL: min_symbol_coverage_ratio={report['min_symbol_coverage_ratio']} "
            f"is below --fail-under={fail_under} for date={report['run_date']}"
        )
        return 1
    return 0


def _previous_date(dates: list[str], target: str) -> str | None:
    earlier = [value for value in dates if value < target]
    return earlier[-1] if earlier else None


def _recent_failures(logs_dir: Path) -> list[dict[str, Any]]:
    if not logs_dir.exists():
        return []
    failures: list[dict[str, Any]] = []
    for path in sorted(logs_dir.glob("*.jsonl"), reverse=True)[:5]:
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                # A run killed mid-write leaves a partial final line; it must not break verify.
                continue
            if isinstance(record, dict) and record.get("event") in {
                "failure",
                "symbol_failure",
                "dataset_failure",
            }:
                failures.append(record)
    return failures[-100:]


def _symbols(df: pd.DataFrame) -> set[str]:
    if df.empty or "symbol" not in df:
        return set()
    return set(df["symbol"].dropna().astype(str))


def _no_coverage_symbols(df: pd.DataFrame) -> set[str]:
    if df.empty or "symbol" not in df or "no_analyst_coverage" not in df:
        return set()
    mask = df["no_analyst_coverage"].fillna(False).astype(bool)
    return set(df.loc[mask, "symbol"].dropna().astype(str))
