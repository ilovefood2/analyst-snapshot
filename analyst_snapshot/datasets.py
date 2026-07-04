from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd


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
EVENT_KEY_COLUMNS = ["symbol", "date", "firm", "toGrade", "action"]


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
        return [_normalize_event_row(row) for row in rows]
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


def _normalize_event_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    aliases = {
        "Firm": "firm",
        "firm": "firm",
        "FromGrade": "fromGrade",
        "fromGrade": "fromGrade",
        "ToGrade": "toGrade",
        "toGrade": "toGrade",
        "Action": "action",
        "action": "action",
        "Date": "date",
        "date": "date",
    }
    for source, target in aliases.items():
        if source in normalized and target not in normalized:
            normalized[target] = normalized[source]
    return normalized


def _clean_mapping(row: dict[Any, Any]) -> dict[str, Any]:
    return {str(key): _clean_value(value) for key, value in row.items()}


def _clean_value(value: Any) -> Any:
    if not isinstance(value, (list, dict, tuple)):
        try:
            if pd.isna(value):
                return None
        except TypeError:
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
