from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from analyst_snapshot.storage import (
    SnapshotWriter,
    append_rating_events,
    append_rows,
    compact_rating_events,
    dataset_path,
    legacy_rating_events_index_path,
    rating_events_index_path,
    read_parquet_or_empty,
    read_rating_events_index,
    rebuild_rating_events_index,
    write_parquet,
)


def _event(when: str, **overrides: object) -> dict[str, object]:
    row = {
        "symbol": "AAPL",
        "event_utc": when,
        "event_date": when[:10],
        "firm": "Example Bank",
        "fromGrade": "Hold",
        "toGrade": "Buy",
        "action": "up",
        "snapshot_utc": "2026-07-04T12:00:00Z",
    }
    row.update(overrides)
    return row


def test_append_rows_preserves_existing_parquet_rows(tmp_path: Path) -> None:
    path = tmp_path / "archive" / "recommendations" / "date=2026-07-04" / "data.parquet"
    append_rows(path, [{"symbol": "AAPL", "snapshot_utc": "t1", "buy": 1}])
    append_rows(path, [{"symbol": "MSFT", "snapshot_utc": "t2", "buy": 2}])

    df = pd.read_parquet(path)
    assert list(df["symbol"]) == ["AAPL", "MSFT"]
    assert list(df["buy"]) == [1, 2]


def test_partitions_share_one_core_schema(tmp_path: Path) -> None:
    # A partition holding only a no-coverage marker must still declare the same columns and types
    # as a partition with real rows, or the archive stops being readable as one dataset.
    full = tmp_path / "recommendations" / "date=2026-07-04" / "data.parquet"
    sparse = tmp_path / "recommendations" / "date=2026-07-05" / "data.parquet"
    append_rows(full, [{"symbol": "AAPL", "snapshot_utc": "t1", "buy": 1.0, "period": "0m"}])
    append_rows(sparse, [{"symbol": "AAPL", "snapshot_utc": "t2", "no_analyst_coverage": True}])

    assert pq.read_schema(full) == pq.read_schema(sparse)
    assert pq.read_schema(sparse).field("buy").type == "double"


def test_rating_event_dedupe_keeps_first_seen_and_writes_date_partition(tmp_path: Path) -> None:
    rows = [_event("2026-07-01T13:12:44")]

    assert append_rating_events(tmp_path, rows, "first-seen-1", "2026-07-04") == 1
    assert append_rating_events(tmp_path, rows, "first-seen-2", "2026-07-04") == 0

    df = read_rating_events_index(tmp_path)
    assert len(df) == 1
    assert df.iloc[0]["first_seen_utc"] == "first-seen-1"
    assert "snapshot_utc" not in df.columns

    date_df = read_parquet_or_empty(dataset_path(tmp_path, "rating_events", "2026-07-04"))
    assert len(date_df) == 1
    assert date_df.iloc[0]["first_seen_utc"] == "first-seen-1"


def test_index_lives_outside_the_partitioned_directory(tmp_path: Path) -> None:
    # A cumulative file at archive/rating_events/data.parquet is scanned as if it were a partition,
    # which double-counts every event when the directory is read as a hive dataset.
    append_rating_events(tmp_path, [_event("2026-07-01T13:12:44")], "first-seen", "2026-07-04")

    assert rating_events_index_path(tmp_path).exists()
    assert not legacy_rating_events_index_path(tmp_path).exists()
    stray = [
        path
        for path in (tmp_path / "rating_events").glob("*")
        if path.is_file() and path.suffix == ".parquet"
    ]
    assert stray == []


def test_same_firm_and_grade_on_different_dates_are_separate_events(tmp_path: Path) -> None:
    added = append_rating_events(
        tmp_path,
        [_event("2026-07-01T13:12:44"), _event("2026-08-01T13:12:44")],
        "first-seen",
        "2026-08-02",
    )

    assert added == 2
    assert len(read_rating_events_index(tmp_path)) == 2


def test_events_without_a_timestamp_are_not_indexed(tmp_path: Path) -> None:
    assert append_rating_events(tmp_path, [_event("2026-07-01T13:12:44", event_utc=None)], "t") == 0


def test_compact_rating_events_removes_duplicate_keys(tmp_path: Path) -> None:
    append_rating_events(tmp_path, [_event("2026-07-01T13:12:44")], "first-seen-1")
    path = rating_events_index_path(tmp_path)
    df = pd.read_parquet(path)
    pd.concat([df, df], ignore_index=True).to_parquet(path, index=False)

    assert compact_rating_events(tmp_path) == 1
    assert len(pd.read_parquet(path)) == 1


def test_compact_refuses_a_legacy_index_it_would_destroy(tmp_path: Path) -> None:
    legacy = legacy_rating_events_index_path(tmp_path)
    legacy.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"symbol": "AAPL", "event_utc": None, "firm": "F", "toGrade": "Buy", "action": "main"},
            {"symbol": "AAPL", "event_utc": None, "firm": "F", "toGrade": "Buy", "action": "main"},
        ]
    ).to_parquet(legacy, index=False)

    with pytest.raises(RuntimeError, match="repair-events"):
        compact_rating_events(tmp_path)
    assert len(read_rating_events_index(tmp_path)) == 2


def test_rebuild_recovers_events_missing_from_the_index(tmp_path: Path) -> None:
    # Two trading dates; the second one carries an extra event by the same firm and grade.
    first = [
        {
            "symbol": "AAPL",
            "GradeDate": "2026-07-01T13:12:44",
            "Firm": "F",
            "ToGrade": "Buy",
            "FromGrade": "Hold",
            "Action": "main",
            "snapshot_utc": "2026-07-02T12:00:00Z",
        }
    ]
    second = first + [
        {
            "symbol": "AAPL",
            "GradeDate": "2026-08-01T13:12:44",
            "Firm": "F",
            "ToGrade": "Buy",
            "FromGrade": "Hold",
            "Action": "main",
            "snapshot_utc": "2026-08-02T12:00:00Z",
        }
    ]
    append_rows(dataset_path(tmp_path, "upgrades_downgrades", "2026-07-01"), first)
    append_rows(dataset_path(tmp_path, "upgrades_downgrades", "2026-08-01"), second)

    result = rebuild_rating_events_index(tmp_path)

    assert result["partitions_scanned"] == 2
    assert result["events_after"] == 2
    index = read_rating_events_index(tmp_path)
    assert sorted(index["event_utc"]) == ["2026-07-01T13:12:44", "2026-08-01T13:12:44"]
    # first_seen_utc comes from the earliest partition the event appeared in.
    by_event = index.set_index("event_utc")["first_seen_utc"].to_dict()
    assert by_event["2026-07-01T13:12:44"] == "2026-07-02T12:00:00Z"
    assert by_event["2026-08-01T13:12:44"] == "2026-08-02T12:00:00Z"


def test_writer_buffers_until_flush(tmp_path: Path) -> None:
    writer = SnapshotWriter(tmp_path, "2026-07-04", run_id="run_x", flush_every_symbols=2)
    path = dataset_path(tmp_path, "recommendations", "2026-07-04")

    writer.add_rows("recommendations", [{"symbol": "AAPL", "snapshot_utc": "t1"}])
    writer.symbol_done()
    assert not path.exists()
    # Buffered symbols still count as present so --resume does not refetch them.
    assert "AAPL" in writer.known_symbols("recommendations")

    writer.add_rows("recommendations", [{"symbol": "MSFT", "snapshot_utc": "t2"}])
    writer.symbol_done()

    df = pd.read_parquet(path)
    assert list(df["symbol"]) == ["AAPL", "MSFT"]
    assert set(df["run_id"]) == {"run_x"}
    assert set(df["dataset"]) == {"recommendations"}


def test_unexpected_yahoo_types_do_not_lose_the_partition(tmp_path: Path, capsys) -> None:
    # If Yahoo ever returns a string where a number is declared, the day's data must still land.
    path = dataset_path(tmp_path, "recommendations", "2026-07-04")
    append_rows(path, [{"symbol": "AAPL", "snapshot_utc": "t1", "buy": "not-a-number"}])

    df = pd.read_parquet(path)
    assert list(df["symbol"]) == ["AAPL"]
    assert list(df["buy"]) == ["not-a-number"]
    assert "keeping inferred type" in capsys.readouterr().out


def test_non_finite_string_in_numeric_column_does_not_abort_append(tmp_path: Path) -> None:
    # Yahoo has returned the literal string "Infinity" for this ratio. Mixed with an existing
    # float partition, that used to make Arrow fail before its per-column fallback could run.
    path = dataset_path(tmp_path, "profile", "2026-08-20")
    append_rows(
        path,
        [{"symbol": "AAPL", "snapshot_utc": "t1", "priceToSalesTrailing12Months": 8.5}],
    )
    append_rows(
        path,
        [{"symbol": "BXBL", "snapshot_utc": "t2", "priceToSalesTrailing12Months": "Infinity"}],
    )

    df = pd.read_parquet(path)
    assert list(df["symbol"]) == ["AAPL", "BXBL"]
    assert df.loc[df["symbol"] == "AAPL", "priceToSalesTrailing12Months"].item() == 8.5
    assert pd.isna(df.loc[df["symbol"] == "BXBL", "priceToSalesTrailing12Months"].item())
    assert pq.read_schema(path).field("priceToSalesTrailing12Months").type == "double"


def test_atomic_parquet_write_preserves_previous_bytes_on_failure(
    tmp_path: Path, monkeypatch
) -> None:
    from analyst_snapshot import storage

    path = dataset_path(tmp_path, "recommendations", "2026-08-27")
    write_parquet(
        path,
        pd.DataFrame([{"symbol": "AAPL", "snapshot_utc": "t1"}]),
        "recommendations",
    )
    before = path.read_bytes()

    def fail_write(*_args, **_kwargs) -> None:
        raise OSError("simulated interrupted parquet write")

    monkeypatch.setattr(storage.pq, "write_table", fail_write)
    with pytest.raises(OSError, match="interrupted"):
        write_parquet(
            path,
            pd.DataFrame([{"symbol": "MSFT", "snapshot_utc": "t2"}]),
            "recommendations",
        )

    assert path.read_bytes() == before
    assert not list(path.parent.glob(f".{path.name}.*"))
