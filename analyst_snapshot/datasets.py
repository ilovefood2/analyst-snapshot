from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd
import pyarrow as pa


@dataclass(frozen=True)
class DatasetSpec:
    code: str
    name: str
    attributes: tuple[str, ...]


DATASETS: dict[str, DatasetSpec] = {
    "a": DatasetSpec("a", "recommendations", ("recommendations",)),
    "b": DatasetSpec("b", "analyst_price_targets", ("analyst_price_targets",)),
    "c": DatasetSpec(
        "c",
        "estimates",
        ("earnings_estimate", "revenue_estimate", "eps_trend", "eps_revisions"),
    ),
    "d": DatasetSpec("d", "upgrades_downgrades", ("upgrades_downgrades",)),
}

DEFAULT_DATASET_CODES = tuple(DATASETS)
RATING_EVENTS_DATASET = "rating_events"

# Dedupe key for the rating-event log. `event_utc` carries Yahoo's GradeDate verbatim; `fromGrade`
# is part of the key so two same-day actions by one firm are never collapsed into one event.
# The key column is deliberately not called `date`: that name is the hive partition key, and a file
# column of the same name is shadowed by the partition value when the archive is read as a dataset.
EVENT_KEY_COLUMNS = ["symbol", "event_utc", "firm", "fromGrade", "toGrade", "action"]

# Columns whose Parquet type is pinned so the archive stays readable as one dataset regardless of
# which pandas/pyarrow version wrote a given partition. Columns not listed here are still written;
# their type is inferred, and consumers should treat them as best-effort.
_STRING = pa.large_string()
_COMMON_COLUMNS: dict[str, pa.DataType] = {
    "symbol": _STRING,
    "snapshot_utc": _STRING,
    "dataset": _STRING,
    "run_id": _STRING,
    "no_analyst_coverage": pa.bool_(),
}
_EVENT_COLUMNS: dict[str, pa.DataType] = {
    "event_utc": _STRING,
    "event_date": _STRING,
    "firm": _STRING,
    "fromGrade": _STRING,
    "toGrade": _STRING,
    "action": _STRING,
    "priceTargetAction": _STRING,
    "currentPriceTarget": pa.float64(),
    "priorPriceTarget": pa.float64(),
}

# Yahoo's own spellings. Verified byte-identical to the canonical columns across 1.07M archived
# rows, so new partitions keep only the canonical ones; storing both doubles the largest dataset.
# Partitions written before 0.2.0 still carry these, and the reader fills the canonical columns
# from them so both vintages look the same to consumers.
LEGACY_EVENT_COLUMNS: dict[str, str] = {
    "GradeDate": "event_utc",
    "Firm": "firm",
    "FromGrade": "fromGrade",
    "ToGrade": "toGrade",
    "Action": "action",
}

CORE_SCHEMAS: dict[str, dict[str, pa.DataType]] = {
    "recommendations": {
        **_COMMON_COLUMNS,
        "period": _STRING,
        "strongBuy": pa.float64(),
        "buy": pa.float64(),
        "hold": pa.float64(),
        "sell": pa.float64(),
        "strongSell": pa.float64(),
    },
    "analyst_price_targets": {
        **_COMMON_COLUMNS,
        "current": pa.float64(),
        "low": pa.float64(),
        "high": pa.float64(),
        "mean": pa.float64(),
        "median": pa.float64(),
    },
    "estimates": {
        **_COMMON_COLUMNS,
        "estimate_table": _STRING,
        "period": _STRING,
        "currency": _STRING,
        "avg": pa.float64(),
        "low": pa.float64(),
        "high": pa.float64(),
        "growth": pa.float64(),
        "numberOfAnalysts": pa.float64(),
        "yearAgoEps": pa.float64(),
        "yearAgoRevenue": pa.float64(),
        "current": pa.float64(),
        "7daysAgo": pa.float64(),
        "30daysAgo": pa.float64(),
        "60daysAgo": pa.float64(),
        "90daysAgo": pa.float64(),
        "upLast7days": pa.float64(),
        "upLast30days": pa.float64(),
        "downLast7Days": pa.float64(),
        "downLast30days": pa.float64(),
    },
    "upgrades_downgrades": {**_COMMON_COLUMNS, **_EVENT_COLUMNS},
    RATING_EVENTS_DATASET: {
        **{name: kind for name, kind in _COMMON_COLUMNS.items() if name != "snapshot_utc"},
        **_EVENT_COLUMNS,
        "first_seen_utc": _STRING,
    },
}

# Aliases mapping Yahoo's column spellings onto the canonical lower-camel event columns. The index
# of `Ticker.upgrades_downgrades` is named GradeDate, which is why `date` was never populated by
# the original alias table.
_EVENT_ALIASES: dict[str, tuple[str, ...]] = {
    "firm": ("firm", "Firm"),
    "fromGrade": ("fromGrade", "FromGrade"),
    "toGrade": ("toGrade", "ToGrade"),
    "action": ("action", "Action"),
    "event_utc": ("event_utc", "GradeDate", "Date", "gradeDate"),
}


def parse_dataset_codes(raw: str | None) -> list[str]:
    if not raw:
        return list(DEFAULT_DATASET_CODES)
    codes = [part.strip().lower() for part in raw.split(",") if part.strip()]
    unknown = [code for code in codes if code not in DATASETS]
    if unknown:
        raise ValueError(f"Unknown dataset code(s): {', '.join(unknown)}")
    return codes


def parse_dataset_payload(
    dataset_name: str,
    payload: object,
    symbol: str,
    snapshot_utc: str,
) -> list[dict[str, Any]]:
    if dataset_name == "estimates" and isinstance(payload, dict):
        rows: list[dict[str, Any]] = []
        for table_name, table_payload in payload.items():
            for row in rows_from_payload(table_payload):
                parsed = _base_row(row, symbol, snapshot_utc)
                parsed["estimate_table"] = table_name
                rows.append(parsed)
        return rows

    rows = [_base_row(row, symbol, snapshot_utc) for row in rows_from_payload(payload)]
    if dataset_name == "upgrades_downgrades":
        return [normalize_event_row(row) for row in rows]
    return rows


def no_coverage_row(dataset_name: str, symbol: str, snapshot_utc: str) -> dict[str, Any]:
    return {
        "snapshot_utc": snapshot_utc,
        "symbol": symbol,
        "dataset": dataset_name,
        "no_analyst_coverage": True,
    }


def rows_from_payload(payload: object) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, pd.DataFrame):
        return _dataframe_rows(payload)
    if isinstance(payload, pd.Series):
        return [{str(key): _clean_value(value) for key, value in payload.items()}]
    if isinstance(payload, list):
        return [_clean_mapping(row) for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        if not payload:
            return []
        return [_clean_mapping(payload)]
    return []


def _dataframe_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    frame = df.copy()
    if not isinstance(frame.index, pd.RangeIndex):
        index_name = frame.index.name or "date"
        frame = frame.reset_index(names=index_name)
    return [_clean_mapping(row) for row in frame.to_dict(orient="records")]


def _base_row(row: dict[str, Any], symbol: str, snapshot_utc: str) -> dict[str, Any]:
    parsed = dict(row)
    parsed["symbol"] = symbol
    parsed["snapshot_utc"] = snapshot_utc
    return parsed


def normalize_event_row(row: dict[str, Any]) -> dict[str, Any]:
    """Fill the canonical event columns from whichever spelling Yahoo returned."""
    normalized = dict(row)
    for target, sources in _EVENT_ALIASES.items():
        if is_blank(normalized.get(target)):
            normalized[target] = None
            for source in sources:
                value = normalized.get(source)
                if not is_blank(value):
                    normalized[target] = value
                    break
    normalized["event_date"] = _date_part(normalized.get("event_utc"))
    for legacy in LEGACY_EVENT_COLUMNS:
        normalized.pop(legacy, None)
    return normalized


def is_blank(value: Any) -> bool:
    """True for None, empty/whitespace strings, and pandas NA/NaN.

    Values read back from Parquet come through as pd.NA rather than None, so an identity check
    against None alone silently treats missing data as populated.
    """
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _date_part(value: Any) -> str | None:
    if is_blank(value):
        return None
    text = str(value)
    head = text.split("T", 1)[0].split(" ", 1)[0]
    try:
        return date.fromisoformat(head).isoformat()
    except ValueError:
        return None


def _clean_mapping(row: dict[Any, Any]) -> dict[str, Any]:
    return {str(key): _clean_value(value) for key, value in row.items()}


def _clean_value(value: Any) -> Any:
    if not isinstance(value, (list, dict, tuple)):
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
    if isinstance(value, pd.Timestamp):
        if value.time().isoformat() == "00:00:00":
            return value.date().isoformat()
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value
