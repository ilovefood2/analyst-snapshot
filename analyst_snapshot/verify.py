from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from analyst_snapshot.datasets import DATASETS
from analyst_snapshot.runner import read_universe
from analyst_snapshot.storage import read_parquet_or_empty


def coverage_report(snapshot_dir: Path, universe_file: Path, logs_dir: Path) -> dict[str, Any]:
    universe = read_universe(universe_file) if universe_file.exists() else []
    expected = set(universe)
    today = date.today()
    yesterday = today - timedelta(days=1)
    report: dict[str, Any] = {
        "expected_symbols": len(expected),
        "today": today.isoformat(),
        "yesterday": yesterday.isoformat(),
        "datasets": {},
    }

    for spec in DATASETS.values():
        today_path = snapshot_dir / spec.name / f"date={today.isoformat()}" / "data.parquet"
        yesterday_path = snapshot_dir / spec.name / f"date={yesterday.isoformat()}" / "data.parquet"
        today_df = read_parquet_or_empty(today_path)
        yesterday_df = read_parquet_or_empty(yesterday_path)
        today_symbols = _symbols(today_df)
        yesterday_symbols = _symbols(yesterday_df)
        no_coverage_symbols = _no_coverage_symbols(today_df)
        missing = expected - today_symbols
        report["datasets"][spec.name] = {
            "today_symbols_snapshotted": len(today_symbols),
            "today_rows": int(len(today_df)),
            "yesterday_symbols_snapshotted": len(yesterday_symbols),
            "yesterday_rows": int(len(yesterday_df)),
            "missing_today_symbols": sorted(missing)[:50],
            "missing_today_symbols_truncated": max(len(missing) - 50, 0),
            "symbols_with_no_analyst_coverage": sorted(no_coverage_symbols)[:100],
            "symbols_with_no_analyst_coverage_truncated": max(len(no_coverage_symbols) - 100, 0),
            "path": str(today_path),
        }

    report["failures"] = _recent_failures(logs_dir)
    return report


def print_coverage_report(snapshot_dir: Path, universe_file: Path, logs_dir: Path) -> None:
    print(
        json.dumps(
            coverage_report(snapshot_dir, universe_file, logs_dir),
            indent=2,
            sort_keys=True,
        )
    )


def _recent_failures(logs_dir: Path) -> list[dict[str, Any]]:
    if not logs_dir.exists():
        return []
    failures: list[dict[str, Any]] = []
    for path in sorted(logs_dir.glob("*.jsonl"), reverse=True)[:5]:
        for line in path.read_text(encoding="utf-8").splitlines():
            record = json.loads(line)
            if record.get("event") in {"failure", "symbol_failure"}:
                failures.append(record)
    return failures[-100:]


def _symbols(df: Any) -> set[str]:
    if df.empty or "symbol" not in df:
        return set()
    return set(df["symbol"].dropna().astype(str))


def _no_coverage_symbols(df: Any) -> set[str]:
    if df.empty or "symbol" not in df or "no_analyst_coverage" not in df:
        return set()
    mask = df["no_analyst_coverage"].fillna(False).astype(bool)
    return set(df.loc[mask, "symbol"].dropna().astype(str))
