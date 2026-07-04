from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from analyst_snapshot.datasets import DatasetSpec
from analyst_snapshot.logging_utils import JsonlLogger
from analyst_snapshot.runner import run_snapshot
from analyst_snapshot.storage import read_parquet_or_empty


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
