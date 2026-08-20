from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from analyst_snapshot.datasets import (
    CORE_SCHEMAS,
    EVENT_KEY_COLUMNS,
    RATING_EVENTS_DATASET,
    is_blank,
    normalize_event_row,
)

INDEX_DIR_NAME = "_index"
RATING_EVENTS_INDEX_NAME = "rating_events.parquet"

# zstd keeps the archive materially smaller than the pandas default (snappy) at no cost to
# readers. Only newly written partitions are affected; existing files stay byte-for-byte.
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 9

DEFAULT_FLUSH_EVERY_SYMBOLS = 50


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def dataset_path(snapshot_dir: Path, dataset_name: str, run_date: str) -> Path:
    return snapshot_dir / dataset_name / f"date={run_date}" / "data.parquet"


def partition_paths(snapshot_dir: Path, dataset_name: str) -> list[Path]:
    return sorted((snapshot_dir / dataset_name).glob("date=*/data.parquet"))


def rating_events_index_path(snapshot_dir: Path) -> Path:
    """Cumulative dedupe index.

    It lives outside ``archive/<dataset>/date=.../`` on purpose: a stray file at the root of a
    hive-partitioned directory makes the directory unreadable as a single Parquet dataset.
    """
    return snapshot_dir / INDEX_DIR_NAME / RATING_EVENTS_INDEX_NAME


def legacy_rating_events_index_path(snapshot_dir: Path) -> Path:
    return snapshot_dir / RATING_EVENTS_DATASET / "data.parquet"


def read_parquet_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def read_rating_events_index(snapshot_dir: Path) -> pd.DataFrame:
    path = rating_events_index_path(snapshot_dir)
    if path.exists():
        return read_parquet_or_empty(path)
    return read_parquet_or_empty(legacy_rating_events_index_path(snapshot_dir))


def write_parquet(path: Path, df: pd.DataFrame, dataset_name: str) -> None:
    """Write a partition with the dataset's core columns pinned to declared Parquet types."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = _conform_table(df, dataset_name)
    pq.write_table(
        table,
        path,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
    )


def _conform_table(df: pd.DataFrame, dataset_name: str) -> pa.Table:
    core = CORE_SCHEMAS.get(dataset_name, {})
    frame = df.copy()
    for column in core:
        if column not in frame.columns:
            frame[column] = None
    extras = sorted(column for column in frame.columns if column not in core)
    frame = frame[[*core, *extras]]

    table = pa.Table.from_pandas(frame, preserve_index=False)
    return _cast_known_columns(table, core)


def _cast_known_columns(table: pa.Table, core: dict[str, pa.DataType]) -> pa.Table:
    """Cast the declared columns, falling back per column if Yahoo sent an unexpected type.

    Losing a day of history to a failed cast would be a worse outcome than a partition whose type
    for one column is inferred rather than declared.
    """
    fields = []
    for field in table.schema:
        target = core.get(field.name)
        fields.append(pa.field(field.name, target) if target is not None else field)
    schema = pa.schema(fields)
    try:
        return table.cast(schema)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
        pass

    columns = []
    safe_fields = []
    for index, field in enumerate(table.schema):
        column = table.column(index)
        target = schema.field(index).type
        try:
            columns.append(column.cast(target))
            safe_fields.append(pa.field(field.name, target))
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
            print(
                f"warning: keeping inferred type {field.type} for column {field.name!r}; "
                f"it does not cast to the declared {target}"
            )
            columns.append(column)
            safe_fields.append(field)
    return pa.Table.from_arrays(columns, schema=pa.schema(safe_fields))


def append_rows(path: Path, rows: list[dict[str, Any]], dataset_name: str | None = None) -> int:
    if not rows:
        return 0
    name = dataset_name or _dataset_name_from_path(path)
    new_df = pd.DataFrame(rows)
    old_df = read_parquet_or_empty(path)
    combined = pd.concat([old_df, new_df], ignore_index=True, sort=False) if len(old_df) else new_df
    write_parquet(path, combined, name)
    return len(new_df)


def _dataset_name_from_path(path: Path) -> str:
    # archive/<dataset>/date=YYYY-MM-DD/data.parquet
    parent = path.parent.name
    return path.parent.parent.name if parent.startswith("date=") else parent


def symbols_in_snapshot(snapshot_dir: Path, dataset_name: str, run_date: str) -> set[str]:
    path = dataset_path(snapshot_dir, dataset_name, run_date)
    if not path.exists():
        return set()
    table = pq.read_table(path, columns=["symbol"])
    return {value for value in table.column("symbol").to_pylist() if value}


def event_key(row: dict[str, Any]) -> tuple[str, ...]:
    return tuple(
        "" if is_blank(row.get(column)) else str(row[column]) for column in EVENT_KEY_COLUMNS
    )


def _event_keys_from_frame(df: pd.DataFrame) -> Iterator[tuple[str, ...]]:
    if df.empty:
        return iter(())
    keyed = df.reindex(columns=EVENT_KEY_COLUMNS).astype("string").fillna("")
    return (tuple(row) for row in keyed.itertuples(index=False, name=None))


def is_event_row(row: dict[str, Any]) -> bool:
    return not row.get("no_analyst_coverage") and not is_blank(row.get("event_utc"))


class SnapshotWriter:
    """Buffers snapshot rows so each partition file is rewritten once per flush, not per symbol.

    The previous per-symbol append re-read and re-wrote the whole partition (and the whole
    cumulative event index) for every symbol, which is quadratic in universe size.
    """

    def __init__(
        self,
        snapshot_dir: Path,
        run_date: str,
        run_id: str | None = None,
        flush_every_symbols: int = DEFAULT_FLUSH_EVERY_SYMBOLS,
    ) -> None:
        self.snapshot_dir = snapshot_dir
        self.run_date = run_date
        self.run_id = run_id
        self._flush_every_symbols = max(1, flush_every_symbols)
        self._buffers: dict[str, list[dict[str, Any]]] = {}
        self._known_symbols: dict[str, set[str]] = {}
        self._event_buffer: list[dict[str, Any]] = []
        self._event_keys: set[tuple[str, ...]] | None = None
        self._symbols_since_flush = 0

    def known_symbols(self, dataset_name: str) -> set[str]:
        if dataset_name not in self._known_symbols:
            self._known_symbols[dataset_name] = symbols_in_snapshot(
                self.snapshot_dir, dataset_name, self.run_date
            )
        return self._known_symbols[dataset_name]

    def add_rows(self, dataset_name: str, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        stamped = []
        for row in rows:
            enriched = dict(row)
            enriched.setdefault("dataset", dataset_name)
            if self.run_id:
                enriched.setdefault("run_id", self.run_id)
            stamped.append(enriched)
        self._buffers.setdefault(dataset_name, []).extend(stamped)
        known = self.known_symbols(dataset_name)
        known.update(str(row["symbol"]) for row in stamped if row.get("symbol"))
        return len(stamped)

    def add_rating_events(self, rows: list[dict[str, Any]], first_seen_utc: str) -> int:
        keys = self._known_event_keys()
        added = 0
        for row in rows:
            if not is_event_row(row):
                continue
            key = event_key(row)
            if key in keys:
                continue
            keys.add(key)
            event = {name: value for name, value in row.items() if name != "snapshot_utc"}
            event["first_seen_utc"] = first_seen_utc
            event.setdefault("dataset", RATING_EVENTS_DATASET)
            if self.run_id:
                event.setdefault("run_id", self.run_id)
            self._event_buffer.append(event)
            added += 1
        return added

    def symbol_done(self) -> None:
        self._symbols_since_flush += 1
        if self._symbols_since_flush >= self._flush_every_symbols:
            self.flush()

    def flush(self) -> None:
        # One dataset failing to write must not strand the others' buffered rows.
        first_error: Exception | None = None
        for dataset_name, rows in list(self._buffers.items()):
            if not rows:
                continue
            try:
                append_rows(
                    dataset_path(self.snapshot_dir, dataset_name, self.run_date),
                    rows,
                    dataset_name,
                )
            except Exception as exc:  # noqa: BLE001
                print(f"error: failed to write {dataset_name} for {self.run_date}: {exc}")
                first_error = first_error or exc
            self._buffers[dataset_name] = []
        try:
            self._flush_events()
        except Exception as exc:  # noqa: BLE001
            print(f"error: failed to write the rating-event index: {exc}")
            first_error = first_error or exc
        self._symbols_since_flush = 0
        if first_error is not None:
            raise first_error

    def _flush_events(self) -> None:
        if not self._event_buffer:
            return
        rows, self._event_buffer = self._event_buffer, []
        old_df = read_rating_events_index(self.snapshot_dir)
        new_df = pd.DataFrame(rows)
        if len(old_df):
            new_df = pd.concat([old_df, new_df], ignore_index=True, sort=False)
        write_parquet(rating_events_index_path(self.snapshot_dir), new_df, RATING_EVENTS_DATASET)
        _remove_legacy_index(self.snapshot_dir)
        if self.run_date:
            append_rows(
                dataset_path(self.snapshot_dir, RATING_EVENTS_DATASET, self.run_date),
                rows,
                RATING_EVENTS_DATASET,
            )

    def _known_event_keys(self) -> set[tuple[str, ...]]:
        if self._event_keys is None:
            index = read_rating_events_index(self.snapshot_dir)
            self._event_keys = set(_event_keys_from_frame(index))
        return self._event_keys


def _remove_legacy_index(snapshot_dir: Path) -> None:
    legacy = legacy_rating_events_index_path(snapshot_dir)
    if legacy.exists() and rating_events_index_path(snapshot_dir).exists():
        legacy.unlink()


def append_rating_events(
    snapshot_dir: Path,
    rows: list[dict[str, Any]],
    first_seen_utc: str,
    run_date: str | None = None,
) -> int:
    """Append newly first-seen rating events. Kept for one-off use; runs use SnapshotWriter."""
    writer = SnapshotWriter(snapshot_dir, run_date or "")
    added = writer.add_rating_events(rows, first_seen_utc)
    writer.flush()
    return added


def index_uses_legacy_key(snapshot_dir: Path) -> bool:
    """True when the index predates the GradeDate fix and has no usable event timestamp."""
    index = read_rating_events_index(snapshot_dir)
    if index.empty:
        return False
    if "event_utc" not in index.columns:
        return True
    return bool(index["event_utc"].isna().all())


def compact_rating_events(snapshot_dir: Path) -> int:
    df = read_rating_events_index(snapshot_dir)
    if df.empty:
        return 0
    if index_uses_legacy_key(snapshot_dir):
        raise RuntimeError(
            "The rating-event index has no populated `event_utc` column, so compacting it would "
            "collapse distinct events. Run `python -m analyst_snapshot repair-events` first."
        )
    for column in EVENT_KEY_COLUMNS:
        if column not in df.columns:
            df[column] = None
    sort_column = "first_seen_utc" if "first_seen_utc" in df.columns else EVENT_KEY_COLUMNS[0]
    compacted = (
        df.sort_values(sort_column, kind="stable")
        .drop_duplicates(subset=EVENT_KEY_COLUMNS, keep="first")
        .reset_index(drop=True)
    )
    write_parquet(rating_events_index_path(snapshot_dir), compacted, RATING_EVENTS_DATASET)
    _remove_legacy_index(snapshot_dir)
    return int(len(df) - len(compacted))


def rebuild_rating_events_index(snapshot_dir: Path, verbose: bool = False) -> dict[str, int]:
    """Rebuild the cumulative event index from the daily upgrades_downgrades partitions.

    The daily partitions are the point-in-time source of truth and are never modified. Each event's
    ``first_seen_utc`` is set to the ``snapshot_utc`` of the earliest partition it appeared in.
    """
    before = len(read_rating_events_index(snapshot_dir))
    seen: set[tuple[str, ...]] = set()
    kept: list[pd.DataFrame] = []
    partitions = partition_paths(snapshot_dir, "upgrades_downgrades")

    for path in partitions:
        frame = read_parquet_or_empty(path)
        if frame.empty:
            continue
        frame = _normalize_event_frame(frame)
        if frame.empty:
            continue
        keys = list(_event_keys_from_frame(frame))
        mask = []
        for key in keys:
            fresh = key not in seen
            if fresh:
                seen.add(key)
            mask.append(fresh)
        fresh_frame = frame.loc[mask].copy()
        if fresh_frame.empty:
            continue
        fresh_frame["first_seen_utc"] = fresh_frame.get("snapshot_utc")
        fresh_frame = fresh_frame.drop(columns=["snapshot_utc"], errors="ignore")
        kept.append(fresh_frame)
        if verbose:
            print(f"{path.parent.name}: +{len(fresh_frame)} events (total {len(seen)})")

    if kept:
        rebuilt = pd.concat(kept, ignore_index=True, sort=False)
    else:
        rebuilt = pd.DataFrame(columns=EVENT_KEY_COLUMNS)
    rebuilt["dataset"] = RATING_EVENTS_DATASET
    write_parquet(rating_events_index_path(snapshot_dir), rebuilt, RATING_EVENTS_DATASET)
    _remove_legacy_index(snapshot_dir)
    return {
        "partitions_scanned": len(partitions),
        "events_before": int(before),
        "events_after": int(len(rebuilt)),
        "events_recovered": int(len(rebuilt) - before),
    }


def _normalize_event_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if "no_analyst_coverage" in frame.columns:
        covered = ~frame["no_analyst_coverage"].fillna(False).astype(bool)
        frame = frame.loc[covered]
    if frame.empty:
        return frame
    rows = [normalize_event_row(row) for row in frame.to_dict(orient="records")]
    normalized = pd.DataFrame([row for row in rows if is_event_row(row)])
    return normalized


def iter_event_rows(rows: Iterable[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    return (row for row in rows if is_event_row(row))
