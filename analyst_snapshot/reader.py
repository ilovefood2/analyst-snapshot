"""Read side of the archive.

Downstream research code (swinglabv3) should go through this module rather than globbing Parquet
files, because two details of the layout are easy to get wrong and both cause lookahead bias:

1. ``date=YYYY-MM-DD`` is the **trading date the snapshot describes**, not the moment it was
   captured. The scheduled job runs the next morning, so a partition labelled 2026-08-18 typically
   holds data read from Yahoo around 2026-08-19T12:40Z. Treating the partition date as "known at
   that day's close" leaks roughly twenty hours of future information.
2. ``snapshot_utc`` is the honest capture timestamp. Every as-of filter here uses it.

``archive/_index/`` and ``archive/_manifests/`` start with an underscore so Parquet dataset readers
skip them by default; only ``archive/<dataset>/date=*/`` is snapshot data.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as pa_ds

from analyst_snapshot.datasets import (
    DATASETS,
    LEGACY_EVENT_COLUMNS,
    MARKET_CONTEXT_DATASETS,
    RATING_EVENTS_DATASET,
)
from analyst_snapshot.storage import partition_paths, read_rating_events_index

SNAPSHOT_DATASETS = (*tuple(spec.name for spec in DATASETS.values()), *MARKET_CONTEXT_DATASETS)
EVENT_DATASETS = ("upgrades_downgrades", RATING_EVENTS_DATASET)
TRADING_DATE_COLUMN = "trading_date"


def available_dates(snapshot_dir: Path | str, dataset: str) -> list[str]:
    """Trading dates with a partition on disk for ``dataset``, oldest first."""
    return [
        path.parent.name.removeprefix("date=")
        for path in partition_paths(Path(snapshot_dir), dataset)
    ]


def load_snapshots(
    snapshot_dir: Path | str,
    dataset: str,
    *,
    start: str | None = None,
    end: str | None = None,
    symbols: Iterable[str] | None = None,
    columns: Sequence[str] | None = None,
    as_of: str | pd.Timestamp | None = None,
    drop_no_coverage: bool = True,
) -> pd.DataFrame:
    """Load daily snapshot partitions as one DataFrame.

    Args:
        dataset: one of ``recommendations``, ``analyst_price_targets``, ``estimates``,
            ``upgrades_downgrades``, ``rating_events``.
        start / end: inclusive trading-date bounds, ``YYYY-MM-DD``.
        symbols: restrict to these tickers.
        columns: restrict to these columns (``symbol``, ``snapshot_utc`` and the partition date are
            always included).
        as_of: drop rows captured after this UTC instant. Use it whenever the result feeds a
            backtest; without it the frame contains rows recorded after the trading date.
        drop_no_coverage: drop the placeholder rows written for symbols Yahoo has no analyst data
            for. Set to ``False`` to distinguish "no coverage" from "not fetched".

    Returns:
        A DataFrame with a ``trading_date`` column (the partition date), ``snapshot_utc`` as
        written, and ``snapshot_ts`` as a UTC-aware timestamp.
    """
    root = Path(snapshot_dir) / dataset
    if not root.exists():
        return pd.DataFrame()

    dataset_obj = pa_ds.dataset(root, format="parquet", partitioning="hive")
    scan_columns = _scan_columns(dataset_obj, columns)
    table = dataset_obj.to_table(
        columns=scan_columns,
        filter=_scan_filter(dataset_obj, start, end, symbols),
    )
    frame = table.to_pandas()
    if frame.empty:
        return frame

    frame = frame.rename(columns={"date": TRADING_DATE_COLUMN})
    if dataset in EVENT_DATASETS:
        frame = canonicalize_event_columns(frame)
    frame = _add_snapshot_ts(frame)
    if as_of is not None:
        frame = frame.loc[frame["snapshot_ts"] <= _as_timestamp(as_of)]
    if drop_no_coverage and "no_analyst_coverage" in frame.columns:
        frame = frame.loc[~frame["no_analyst_coverage"].fillna(False).astype(bool)]
    return frame.reset_index(drop=True)


def load_rating_events(
    snapshot_dir: Path | str,
    *,
    start: str | None = None,
    end: str | None = None,
    symbols: Iterable[str] | None = None,
    as_of: str | pd.Timestamp | None = None,
    use_index: bool = True,
) -> pd.DataFrame:
    """Load deduped rating-change events.

    By default this reads the cumulative index at ``archive/_index/rating_events.parquet``, which
    covers Yahoo's full history including the backfill from the first run. ``start``/``end`` filter
    on ``event_date`` (when the rating change happened), not on the partition date.

    Set ``use_index=False`` to read the ``rating_events/date=*/`` partitions instead, which record
    when each event was *first observed* — the point-in-time view.
    """
    if use_index:
        frame = read_rating_events_index(Path(snapshot_dir))
        if frame.empty:
            return frame
        if symbols is not None:
            frame = frame.loc[frame["symbol"].isin(set(symbols))]
        frame = canonicalize_event_columns(frame)
        frame = _add_snapshot_ts(frame, source="first_seen_utc")
    else:
        frame = load_snapshots(
            snapshot_dir,
            RATING_EVENTS_DATASET,
            symbols=symbols,
            drop_no_coverage=False,
        )
        if frame.empty:
            return frame
        frame = _add_snapshot_ts(frame, source="first_seen_utc")

    if as_of is not None:
        frame = frame.loc[frame["snapshot_ts"] <= _as_timestamp(as_of)]
    if "event_date" in frame.columns:
        if start is not None:
            frame = frame.loc[frame["event_date"].fillna("") >= start]
        if end is not None:
            frame = frame.loc[frame["event_date"].fillna("9999") <= end]
    return frame.reset_index(drop=True)


def latest_as_of(
    snapshot_dir: Path | str,
    dataset: str,
    as_of: str | pd.Timestamp,
    *,
    symbols: Iterable[str] | None = None,
    columns: Sequence[str] | None = None,
    lookback_days: int = 10,
    group_extra: Sequence[str] = (),
) -> pd.DataFrame:
    """Most recent row per symbol that was genuinely observable at ``as_of``.

    This is the safe primitive for feature building: it never returns a row whose ``snapshot_utc``
    is later than ``as_of``. ``lookback_days`` bounds how far back to scan for a symbol's last
    observation; widen it if the job missed several days.

    ``group_extra`` adds grouping columns for datasets with several rows per symbol — for example
    ``group_extra=("period",)`` for ``recommendations``, or ``("estimate_table", "period")`` for
    ``estimates``.
    """
    stamp = _as_timestamp(as_of)
    start = (stamp - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    frame = load_snapshots(
        snapshot_dir,
        dataset,
        start=start,
        end=stamp.strftime("%Y-%m-%d"),
        symbols=symbols,
        columns=columns,
        as_of=stamp,
    )
    if frame.empty:
        return frame
    keys = ["symbol", *group_extra]
    ordered = frame.sort_values(["snapshot_ts", TRADING_DATE_COLUMN], kind="stable")
    return ordered.drop_duplicates(subset=keys, keep="last").reset_index(drop=True)


def archive_summary(snapshot_dir: Path | str) -> dict[str, Any]:
    """Small inventory of the archive: partitions per dataset and the date range covered."""
    root = Path(snapshot_dir)
    summary: dict[str, Any] = {"snapshot_dir": str(root), "datasets": {}}
    for dataset in (*SNAPSHOT_DATASETS, RATING_EVENTS_DATASET):
        dates = available_dates(root, dataset)
        summary["datasets"][dataset] = {
            "partitions": len(dates),
            "first_date": dates[0] if dates else None,
            "last_date": dates[-1] if dates else None,
        }
    index = read_rating_events_index(root)
    summary["rating_events_index_rows"] = int(len(index))
    return summary


def canonicalize_event_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Fill the canonical event columns from Yahoo's spellings on pre-0.2.0 partitions.

    Both vintages then look identical to callers, so a query spanning the schema change does not
    silently return nulls for half its rows.
    """
    result = frame.copy()
    for legacy, canonical in LEGACY_EVENT_COLUMNS.items():
        if legacy not in result.columns:
            continue
        if canonical in result.columns:
            result[canonical] = result[canonical].fillna(result[legacy])
        else:
            result[canonical] = result[legacy]
    if "event_utc" in result.columns:
        derived = result["event_utc"].astype("string").str.slice(0, 10)
        if "event_date" in result.columns:
            result["event_date"] = result["event_date"].fillna(derived)
        else:
            result["event_date"] = derived
    return result


def _scan_columns(
    dataset_obj: pa_ds.Dataset,
    columns: Sequence[str] | None,
) -> list[str] | None:
    if columns is None:
        return None
    available = set(dataset_obj.schema.names)
    wanted = [*columns, "symbol", "snapshot_utc", "date", "no_analyst_coverage"]
    if any(name in available for name in LEGACY_EVENT_COLUMNS):
        # Keep the legacy spellings in the scan so canonicalisation has something to fill from.
        wanted.extend(
            legacy
            for legacy, canonical in LEGACY_EVENT_COLUMNS.items()
            if canonical in columns or canonical == "event_utc"
        )
    seen: dict[str, None] = {}
    for name in wanted:
        if name in available:
            seen.setdefault(name, None)
    return list(seen)


def _scan_filter(
    dataset_obj: pa_ds.Dataset,
    start: str | None,
    end: str | None,
    symbols: Iterable[str] | None,
) -> pa_ds.Expression | None:
    expression: pa_ds.Expression | None = None
    if "date" in dataset_obj.schema.names:
        if start is not None:
            expression = _combine(expression, pa_ds.field("date") >= start)
        if end is not None:
            expression = _combine(expression, pa_ds.field("date") <= end)
    if symbols is not None:
        expression = _combine(expression, pa_ds.field("symbol").isin(sorted(set(symbols))))
    return expression


def _combine(left: pa_ds.Expression | None, right: pa_ds.Expression) -> pa_ds.Expression:
    return right if left is None else (left & right)


def _add_snapshot_ts(frame: pd.DataFrame, source: str = "snapshot_utc") -> pd.DataFrame:
    result = frame.copy()
    if source in result.columns:
        result["snapshot_ts"] = pd.to_datetime(result[source], utc=True, errors="coerce")
    else:
        result["snapshot_ts"] = pd.NaT
    return result


def _as_timestamp(value: str | pd.Timestamp) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
