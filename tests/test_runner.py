from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pytest

from analyst_snapshot.datasets import DatasetSpec
from analyst_snapshot.logging_utils import JsonlLogger
from analyst_snapshot.runner import run_snapshot
from analyst_snapshot.storage import read_parquet_or_empty, read_rating_events_index


class FakeFetcher:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fail_once = {"MSFT"}

    def fetch_symbol(self, symbol: str, specs: Iterable[DatasetSpec]) -> dict[str, object]:
        self.calls.append(symbol)
        if symbol in self.fail_once:
            self.fail_once.remove(symbol)
            raise RuntimeError("temporary yahoo throttle")
        return {spec.name: [{"value": len(self.calls)}] for spec in specs}


def test_failed_symbols_are_retried_at_end(tmp_path: Path) -> None:
    fetcher = FakeFetcher()
    logger = JsonlLogger(tmp_path / "logs", "test")

    summary = run_snapshot(
        snapshot_dir=tmp_path / "archive",
        fetcher=fetcher,
        dataset_codes=["a"],
        symbols=["AAPL", "MSFT"],
        logger=logger,
        run_date="2026-07-04",
    )

    assert fetcher.calls == ["AAPL", "MSFT", "MSFT"]
    assert summary.retry_symbols == ["MSFT"]
    assert len(summary.failures) == 1
    df = read_parquet_or_empty(
        tmp_path / "archive" / "recommendations" / "date=2026-07-04" / "data.parquet"
    )
    assert set(df["symbol"]) == {"AAPL", "MSFT"}


def test_no_coverage_marker_enables_resume(tmp_path: Path) -> None:
    class EmptyFetcher:
        def __init__(self) -> None:
            self.calls = 0

        def fetch_symbol(self, symbol: str, specs: Iterable[DatasetSpec]) -> dict[str, object]:
            self.calls += 1
            return {spec.name: [] for spec in specs}

    fetcher = EmptyFetcher()
    logger = JsonlLogger(tmp_path / "logs", "test")
    snapshot_dir = tmp_path / "archive"
    run_snapshot(snapshot_dir, fetcher, ["a"], ["AAPL"], logger, run_date="2026-07-04")
    run_snapshot(
        snapshot_dir,
        fetcher,
        ["a"],
        ["AAPL"],
        logger,
        resume=True,
        run_date="2026-07-04",
    )

    assert fetcher.calls == 1


class YahooShapedFetcher:
    """Returns payloads shaped the way yfinance actually returns them."""

    def fetch_symbol(self, symbol: str, specs: Iterable[DatasetSpec]) -> dict[str, object]:
        payloads: dict[str, object] = {}
        for spec in specs:
            if spec.name == "upgrades_downgrades":
                payloads[spec.name] = [
                    {
                        "GradeDate": "2026-07-01T13:12:44",
                        "Firm": "Example Bank",
                        "FromGrade": "Hold",
                        "ToGrade": "Buy",
                        "Action": "up",
                    },
                    {
                        "GradeDate": "2026-06-01T09:30:00",
                        "Firm": "Example Bank",
                        "FromGrade": "Hold",
                        "ToGrade": "Buy",
                        "Action": "up",
                    },
                ]
            else:
                payloads[spec.name] = [{"value": 1.0}]
        return payloads


def test_run_indexes_every_distinct_event_and_writes_a_manifest(tmp_path: Path) -> None:
    import json

    archive = tmp_path / "archive"
    logger = JsonlLogger(tmp_path / "logs", "test")

    summary = run_snapshot(
        snapshot_dir=archive,
        fetcher=YahooShapedFetcher(),
        dataset_codes=["d"],
        symbols=["AAPL"],
        logger=logger,
        run_date="2026-07-04",
        run_identifier="run_test",
    )

    # Both reiterations by the same firm are distinct events; only their date differs.
    assert summary.events_added == 2
    index = read_rating_events_index(archive)
    assert sorted(index["event_utc"]) == ["2026-06-01T09:30:00", "2026-07-01T13:12:44"]

    manifest = json.loads(
        (archive / "_manifests" / "date=2026-07-04" / "run_test.json").read_text(encoding="utf-8")
    )
    assert manifest["run_date"] == "2026-07-04"
    assert manifest["events_added"] == 2
    assert manifest["datasets"]["upgrades_downgrades"]["rows_written"] == 2


def test_partial_run_is_flushed_when_a_symbol_raises_unexpectedly(tmp_path: Path) -> None:
    class Exploding:
        def fetch_symbol(self, symbol: str, specs: Iterable[DatasetSpec]) -> dict[str, object]:
            if symbol == "BOOM":
                raise KeyboardInterrupt
            return {spec.name: [{"value": 1.0}] for spec in specs}

    archive = tmp_path / "archive"
    logger = JsonlLogger(tmp_path / "logs", "test")

    with pytest.raises(KeyboardInterrupt):
        run_snapshot(
            snapshot_dir=archive,
            fetcher=Exploding(),
            dataset_codes=["a"],
            symbols=["AAPL", "BOOM"],
            logger=logger,
            run_date="2026-07-04",
            flush_every_symbols=100,
        )

    # Buffered rows must survive an interrupt, otherwise --resume refetches work already done.
    df = read_parquet_or_empty(archive / "recommendations" / "date=2026-07-04" / "data.parquet")
    assert set(df["symbol"]) == {"AAPL"}
