from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from analyst_snapshot.datasets import EVENT_KEY_COLUMNS


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def dataset_path(snapshot_dir: Path, dataset_name: str, run_date: str) -> Path:
    return snapshot_dir / dataset_name / f"date={run_date}" / "data.parquet"


def read_parquet_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def append_rows(path: Path, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(rows)
    old_df = read_parquet_or_empty(path)
    combined = pd.concat([old_df, new_df], ignore_index=True, sort=False)
    combined.to_parquet(path, index=False)
    return len(new_df)


def symbols_in_snapshot(snapshot_dir: Path, dataset_name: str, run_date: str) -> set[str]:
    path = dataset_path(snapshot_dir, dataset_name, run_date)
    df = read_parquet_or_empty(path)
    if df.empty or "symbol" not in df.columns:
        return set()
    return set(df["symbol"].dropna().astype(str))


def append_rating_events(
    snapshot_dir: Path,
    rows: list[dict[str, Any]],
    first_seen_utc: str,
) -> int:
    path = snapshot_dir / "rating_events" / "data.parquet"
    event_rows = [row for row in rows if not row.get("no_analyst_coverage")]
    if not event_rows:
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)
    old_df = read_parquet_or_empty(path)
    new_df = pd.DataFrame(event_rows)
    for column in EVENT_KEY_COLUMNS:
        if column not in new_df.columns:
            new_df[column] = None

    if old_df.empty:
        new_only = new_df.copy()
    else:
        old_keys = set(
            old_df.reindex(columns=EVENT_KEY_COLUMNS)
            .astype("string")
            .itertuples(index=False, name=None)
        )
        new_keys = (
            new_df.reindex(columns=EVENT_KEY_COLUMNS)
            .astype("string")
            .itertuples(index=False, name=None)
        )
        keep_mask = [key not in old_keys for key in new_keys]
        new_only = new_df.loc[keep_mask].copy()

    if new_only.empty:
        return 0

    new_only["first_seen_utc"] = first_seen_utc
    if "snapshot_utc" in new_only.columns:
        new_only = new_only.drop(columns=["snapshot_utc"])
    combined = pd.concat([old_df, new_only], ignore_index=True, sort=False)
    combined.to_parquet(path, index=False)
    return len(new_only)


def compact_rating_events(snapshot_dir: Path) -> int:
    path = snapshot_dir / "rating_events" / "data.parquet"
    df = read_parquet_or_empty(path)
    if df.empty:
        return 0
    for column in EVENT_KEY_COLUMNS:
        if column not in df.columns:
            df[column] = None
    compacted = (
        df.sort_values("first_seen_utc", kind="stable")
        .drop_duplicates(subset=EVENT_KEY_COLUMNS, keep="first")
        .reset_index(drop=True)
    )
    compacted.to_parquet(path, index=False)
    return int(len(df) - len(compacted))
