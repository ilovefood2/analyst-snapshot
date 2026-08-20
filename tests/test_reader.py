from __future__ import annotations

from pathlib import Path

import pytest

from analyst_snapshot.reader import (
    archive_summary,
    available_dates,
    latest_as_of,
    load_rating_events,
    load_snapshots,
)
from analyst_snapshot.storage import append_rating_events, append_rows, dataset_path


@pytest.fixture
def archive(tmp_path: Path) -> Path:
    root = tmp_path / "archive"
    # Each partition is captured the evening of its own trading date.
    append_rows(
        dataset_path(root, "recommendations", "2026-07-02"),
        [
            {"symbol": "AAPL", "snapshot_utc": "2026-07-03T02:05:00Z", "buy": 10.0},
            {"symbol": "MSFT", "snapshot_utc": "2026-07-03T02:06:00Z", "buy": 20.0},
        ],
        "recommendations",
    )
    append_rows(
        dataset_path(root, "recommendations", "2026-07-03"),
        [
            {"symbol": "AAPL", "snapshot_utc": "2026-07-04T02:05:00Z", "buy": 11.0},
            {"symbol": "MSFT", "snapshot_utc": "2026-07-04T02:06:00Z", "no_analyst_coverage": True},
        ],
        "recommendations",
    )
    return root


def test_available_dates(archive: Path) -> None:
    assert available_dates(archive, "recommendations") == ["2026-07-02", "2026-07-03"]


def test_load_snapshots_exposes_the_partition_date_as_trading_date(archive: Path) -> None:
    frame = load_snapshots(archive, "recommendations")

    assert set(frame["trading_date"]) == {"2026-07-02", "2026-07-03"}
    assert "snapshot_ts" in frame.columns
    # The no-coverage placeholder is dropped by default.
    assert len(frame) == 3


def test_no_coverage_rows_can_be_kept(archive: Path) -> None:
    frame = load_snapshots(archive, "recommendations", drop_no_coverage=False)

    assert len(frame) == 4


def test_as_of_excludes_rows_captured_after_the_cutoff(archive: Path) -> None:
    # This is the lookahead guard: the 2026-07-03 partition was not readable until the evening of
    # 2026-07-03, which lands on 2026-07-04 in UTC.
    frame = load_snapshots(archive, "recommendations", as_of="2026-07-03T20:00:00Z")

    assert set(frame["trading_date"]) == {"2026-07-02"}


def test_date_bounds_filter_partitions(archive: Path) -> None:
    frame = load_snapshots(archive, "recommendations", start="2026-07-03")

    assert set(frame["trading_date"]) == {"2026-07-03"}


def test_symbol_filter(archive: Path) -> None:
    frame = load_snapshots(archive, "recommendations", symbols=["MSFT"])

    assert set(frame["symbol"]) == {"MSFT"}


def test_latest_as_of_returns_one_observable_row_per_symbol(archive: Path) -> None:
    frame = latest_as_of(archive, "recommendations", "2026-07-04T12:00:00Z")

    # One row per symbol, each the newest with real data: a day Yahoo reported no coverage does
    # not erase the last known value, it just is not the latest observation.
    assert sorted(frame["symbol"]) == ["AAPL", "MSFT"]
    by_symbol = frame.set_index("symbol")
    assert by_symbol.loc["AAPL", "buy"] == 11.0
    assert by_symbol.loc["AAPL", "trading_date"] == "2026-07-03"
    assert by_symbol.loc["MSFT", "buy"] == 20.0
    assert by_symbol.loc["MSFT", "trading_date"] == "2026-07-02"


def test_latest_as_of_never_reaches_past_the_cutoff(archive: Path) -> None:
    frame = latest_as_of(archive, "recommendations", "2026-07-03T12:00:00Z")

    assert sorted(frame["symbol"]) == ["AAPL", "MSFT"]
    assert set(frame["trading_date"]) == {"2026-07-02"}
    assert frame.loc[frame["symbol"] == "AAPL", "buy"].item() == 10.0


def test_load_rating_events_filters_on_event_date(archive: Path) -> None:
    append_rating_events(
        archive,
        [
            {
                "symbol": "AAPL",
                "event_utc": "2026-06-01T13:00:00",
                "event_date": "2026-06-01",
                "firm": "F",
                "fromGrade": "Hold",
                "toGrade": "Buy",
                "action": "up",
            },
            {
                "symbol": "AAPL",
                "event_utc": "2026-07-01T13:00:00",
                "event_date": "2026-07-01",
                "firm": "F",
                "fromGrade": "Buy",
                "toGrade": "Hold",
                "action": "down",
            },
        ],
        "2026-07-03T02:00:00Z",
        "2026-07-02",
    )

    everything = load_rating_events(archive)
    recent = load_rating_events(archive, start="2026-06-15")

    assert len(everything) == 2
    assert list(recent["event_date"]) == ["2026-07-01"]


def test_archive_summary_counts_partitions(archive: Path) -> None:
    summary = archive_summary(archive)

    assert summary["datasets"]["recommendations"]["partitions"] == 2
    assert summary["datasets"]["recommendations"]["first_date"] == "2026-07-02"


def test_missing_dataset_returns_empty(tmp_path: Path) -> None:
    assert load_snapshots(tmp_path, "recommendations").empty


def test_reader_canonicalises_pre_0_2_0_event_columns(tmp_path: Path) -> None:
    # A partition written before the canonical names existed carries only Yahoo's spellings.
    archive = tmp_path / "archive"
    append_rows(
        dataset_path(archive, "upgrades_downgrades", "2026-07-02"),
        [
            {
                "symbol": "AAPL",
                "snapshot_utc": "2026-07-03T12:40:00Z",
                "GradeDate": "2026-07-01T13:12:44",
                "Firm": "Example Bank",
                "FromGrade": "Hold",
                "ToGrade": "Buy",
                "Action": "up",
            }
        ],
        "upgrades_downgrades",
    )

    frame = load_snapshots(archive, "upgrades_downgrades")

    assert frame.iloc[0]["event_utc"] == "2026-07-01T13:12:44"
    assert frame.iloc[0]["event_date"] == "2026-07-01"
    assert frame.iloc[0]["firm"] == "Example Bank"


def test_new_partitions_do_not_duplicate_yahoo_column_spellings(tmp_path: Path) -> None:
    from analyst_snapshot.datasets import parse_dataset_payload

    archive = tmp_path / "archive"
    rows = parse_dataset_payload(
        "upgrades_downgrades",
        [{"GradeDate": "2026-07-01T13:12:44", "Firm": "F", "ToGrade": "Buy", "Action": "up"}],
        "AAPL",
        "2026-07-03T02:05:00Z",
    )
    append_rows(dataset_path(archive, "upgrades_downgrades", "2026-07-02"), rows)

    frame = load_snapshots(archive, "upgrades_downgrades")

    assert "GradeDate" not in frame.columns
    assert "Firm" not in frame.columns
    assert frame.iloc[0]["event_utc"] == "2026-07-01T13:12:44"


def test_a_query_spanning_the_schema_change_sees_one_shape(tmp_path: Path) -> None:
    from analyst_snapshot.datasets import parse_dataset_payload

    archive = tmp_path / "archive"
    append_rows(
        dataset_path(archive, "upgrades_downgrades", "2026-07-02"),
        [
            {
                "symbol": "AAPL",
                "snapshot_utc": "2026-07-03T12:40:00Z",
                "GradeDate": "2026-07-01T13:12:44",
                "Firm": "Old Vintage",
                "ToGrade": "Buy",
                "Action": "up",
            }
        ],
        "upgrades_downgrades",
    )
    append_rows(
        dataset_path(archive, "upgrades_downgrades", "2026-07-03"),
        parse_dataset_payload(
            "upgrades_downgrades",
            [{"GradeDate": "2026-07-03T09:00:00", "Firm": "New Vintage", "ToGrade": "Hold"}],
            "AAPL",
            "2026-07-04T02:05:00Z",
        ),
    )

    frame = load_snapshots(archive, "upgrades_downgrades").sort_values("trading_date")

    assert frame["event_utc"].isna().sum() == 0
    assert list(frame["firm"]) == ["Old Vintage", "New Vintage"]
