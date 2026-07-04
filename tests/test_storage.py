from __future__ import annotations

import pandas as pd

from analyst_snapshot.storage import (
    append_rating_events,
    append_rows,
    compact_rating_events,
    read_parquet_or_empty,
)


def test_append_rows_preserves_existing_parquet_rows(tmp_path) -> None:
    path = tmp_path / "archive" / "recommendations" / "date=2026-07-04" / "data.parquet"
    append_rows(path, [{"symbol": "AAPL", "snapshot_utc": "t1", "buy": 1}])
    append_rows(path, [{"symbol": "MSFT", "snapshot_utc": "t2", "buy": 2}])

    df = pd.read_parquet(path)
    assert list(df["symbol"]) == ["AAPL", "MSFT"]
    assert list(df["buy"]) == [1, 2]


def test_rating_event_dedupe_keeps_first_seen(tmp_path) -> None:
    rows = [
        {
            "symbol": "AAPL",
            "date": "2026-07-01",
            "firm": "Example Bank",
            "toGrade": "Buy",
            "action": "up",
            "snapshot_utc": "snapshot-1",
        }
    ]
    assert append_rating_events(tmp_path, rows, "first-seen-1") == 1
    assert append_rating_events(tmp_path, rows, "first-seen-2") == 0

    df = read_parquet_or_empty(tmp_path / "rating_events" / "data.parquet")
    assert len(df) == 1
    assert df.iloc[0]["first_seen_utc"] == "first-seen-1"
    assert "snapshot_utc" not in df.columns


def test_compact_rating_events_removes_duplicate_keys(tmp_path) -> None:
    rows = [
        {
            "symbol": "AAPL",
            "date": "2026-07-01",
            "firm": "Example Bank",
            "toGrade": "Buy",
            "action": "up",
        }
    ]
    append_rating_events(tmp_path, rows, "first-seen-1")
    path = tmp_path / "rating_events" / "data.parquet"
    df = pd.read_parquet(path)
    pd.concat([df, df], ignore_index=True).to_parquet(path, index=False)

    assert compact_rating_events(tmp_path) == 1
    assert len(pd.read_parquet(path)) == 1
