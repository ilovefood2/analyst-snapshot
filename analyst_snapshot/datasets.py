from __future__ import annotations

import re
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
    # Column that records which Yahoo table a row came from, for datasets built from several.
    table_column: str | None = None
    # Curated projection. Yahoo's `info` carries ~150 keys including live quote fields that would
    # bloat the archive with intraday noise; only the listed keys are archived.
    projection: tuple[str, ...] = ()

    @property
    def is_multi_table(self) -> bool:
        return len(self.attributes) > 1


# Point-in-time fields from Ticker.info. Chosen because each is restated or drifts over time and
# cannot be reconstructed later: market cap and share counts move with buybacks and issuance,
# short interest is reported twice a month and revised, sector/industry classifications change,
# and Yahoo rewrites the fundamental ratios as filings land. Live quote fields (bid, ask, volume,
# day range) are deliberately excluded: they are intraday noise here and available elsewhere.
PROFILE_FIELDS: tuple[str, ...] = (
    "quoteType",
    "exchange",
    "currency",
    "financialCurrency",
    "country",
    "sector",
    "industry",
    "marketCap",
    "enterpriseValue",
    "sharesOutstanding",
    "impliedSharesOutstanding",
    "floatShares",
    "sharesShort",
    "sharesShortPriorMonth",
    "sharesShortPreviousMonthDate",
    "dateShortInterest",
    "shortRatio",
    "shortPercentOfFloat",
    "heldPercentInsiders",
    "heldPercentInstitutions",
    "beta",
    "trailingPE",
    "forwardPE",
    "priceToBook",
    "priceToSalesTrailing12Months",
    "enterpriseToRevenue",
    "enterpriseToEbitda",
    "trailingPegRatio",
    "trailingEps",
    "forwardEps",
    "bookValue",
    "revenuePerShare",
    "profitMargins",
    "grossMargins",
    "operatingMargins",
    "returnOnAssets",
    "returnOnEquity",
    "debtToEquity",
    "currentRatio",
    "quickRatio",
    "totalCash",
    "totalDebt",
    "totalRevenue",
    "freeCashflow",
    "operatingCashflow",
    "revenueGrowth",
    "earningsGrowth",
    "earningsQuarterlyGrowth",
    "dividendYield",
    "payoutRatio",
    "exDividendDate",
    "lastDividendDate",
    "lastSplitDate",
    "lastSplitFactor",
    "mostRecentQuarter",
    "lastFiscalYearEnd",
    "nextFiscalYearEnd",
    "regularMarketPreviousClose",
)

# Everything else in PROFILE_FIELDS is numeric. Date-like fields arrive from Yahoo as epoch
# seconds and stay numeric; consumers convert with pd.to_datetime(col, unit="s").
_PROFILE_STRING_FIELDS = frozenset(
    {
        "quoteType",
        "exchange",
        "currency",
        "financialCurrency",
        "country",
        "sector",
        "industry",
        "lastSplitFactor",
    }
)

DATASETS: dict[str, DatasetSpec] = {
    "a": DatasetSpec("a", "recommendations", ("recommendations",)),
    "b": DatasetSpec("b", "analyst_price_targets", ("analyst_price_targets",)),
    "c": DatasetSpec(
        "c",
        "estimates",
        ("earnings_estimate", "revenue_estimate", "eps_trend", "eps_revisions"),
        table_column="estimate_table",
    ),
    "d": DatasetSpec("d", "upgrades_downgrades", ("upgrades_downgrades",)),
    "e": DatasetSpec(
        "e",
        "profile",
        ("info",),
        projection=PROFILE_FIELDS,
    ),
    "f": DatasetSpec(
        "f",
        "earnings",
        ("calendar", "earnings_dates"),
        table_column="earnings_table",
    ),
    "g": DatasetSpec(
        "g",
        "holders",
        ("major_holders", "institutional_holders", "insider_transactions", "insider_purchases"),
        table_column="holders_table",
    ),
    "h": DatasetSpec("h", "shares_outstanding", ("get_shares_full",)),
}

DEFAULT_DATASET_CODES = tuple(DATASETS)
RATING_EVENTS_DATASET = "rating_events"
MARKET_CONTEXT_DATASETS = (
    "cftc_tff_positioning",
    "finra_short_volume",
    "occ_account_volume",
)

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
    "profile": {
        **_COMMON_COLUMNS,
        **{
            field: (_STRING if field in _PROFILE_STRING_FIELDS else pa.float64())
            for field in PROFILE_FIELDS
        },
    },
    "earnings": {
        **_COMMON_COLUMNS,
        "earnings_table": _STRING,
        "earnings_date": _STRING,
        "eps_estimate": pa.float64(),
        "reported_eps": pa.float64(),
        "surprise_pct": pa.float64(),
        "earnings_average": pa.float64(),
        "earnings_high": pa.float64(),
        "earnings_low": pa.float64(),
        "revenue_average": pa.float64(),
        "revenue_high": pa.float64(),
        "revenue_low": pa.float64(),
        "dividend_date": _STRING,
        "ex_dividend_date": _STRING,
    },
    "holders": {
        **_COMMON_COLUMNS,
        "holders_table": _STRING,
        "Holder": _STRING,
        "pctHeld": pa.float64(),
        "Shares": pa.float64(),
        "Value": pa.float64(),
        "date_reported": _STRING,
        "Insider": _STRING,
        "Position": _STRING,
        "Transaction": _STRING,
        "Ownership": _STRING,
        "start_date": _STRING,
    },
    "shares_outstanding": {
        **_COMMON_COLUMNS,
        "as_of_date": _STRING,
        "shares_outstanding": pa.float64(),
    },
    "cftc_tff_positioning": {
        "symbol": _STRING,
        "snapshot_utc": _STRING,
        "dataset": _STRING,
        "run_id": _STRING,
        "source_date": _STRING,
        "source_url": _STRING,
        "source_sha256": _STRING,
        "source_lag_days": pa.int64(),
        "market": _STRING,
        "market_name": _STRING,
        "cftc_contract_market_code": _STRING,
        "open_interest": pa.float64(),
        "dealer_long": pa.float64(),
        "dealer_short": pa.float64(),
        "dealer_spreading": pa.float64(),
        "asset_manager_long": pa.float64(),
        "asset_manager_short": pa.float64(),
        "asset_manager_spreading": pa.float64(),
        "leveraged_money_long": pa.float64(),
        "leveraged_money_short": pa.float64(),
        "leveraged_money_spreading": pa.float64(),
        "other_reportable_long": pa.float64(),
        "other_reportable_short": pa.float64(),
        "other_reportable_spreading": pa.float64(),
        "nonreportable_long": pa.float64(),
        "nonreportable_short": pa.float64(),
        "dealer_net_share": pa.float64(),
        "asset_manager_net_share": pa.float64(),
        "leveraged_money_net_share": pa.float64(),
    },
    "finra_short_volume": {
        "symbol": _STRING,
        "snapshot_utc": _STRING,
        "dataset": _STRING,
        "run_id": _STRING,
        "source_date": _STRING,
        "source_url": _STRING,
        "source_sha256": _STRING,
        "source_lag_days": pa.int64(),
        "short_volume": pa.float64(),
        "short_exempt_volume": pa.float64(),
        "total_volume": pa.float64(),
        "short_volume_ratio": pa.float64(),
        "market": _STRING,
    },
    "occ_account_volume": {
        "symbol": _STRING,
        "snapshot_utc": _STRING,
        "dataset": _STRING,
        "run_id": _STRING,
        "source_date": _STRING,
        "source_url": _STRING,
        "source_sha256": _STRING,
        "source_lag_days": pa.int64(),
        "option_symbol": _STRING,
        "account_type_code": _STRING,
        "account_type": _STRING,
        "call_put_code": _STRING,
        "call_put": _STRING,
        "exchange": _STRING,
        "quantity": pa.float64(),
    },
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


DATASETS_BY_NAME: dict[str, DatasetSpec] = {spec.name: spec for spec in DATASETS.values()}


def parse_dataset_payload(
    dataset_name: str,
    payload: object,
    symbol: str,
    snapshot_utc: str,
) -> list[dict[str, Any]]:
    spec = DATASETS_BY_NAME.get(dataset_name)
    table_column = spec.table_column if spec else None

    if dataset_name == "shares_outstanding":
        return [_base_row(row, symbol, snapshot_utc) for row in _shares_rows(payload)]

    if table_column and isinstance(payload, dict):
        rows: list[dict[str, Any]] = []
        for table_name, table_payload in payload.items():
            for row in rows_from_payload(table_payload):
                parsed = _base_row(_usable_names(row), symbol, snapshot_utc)
                parsed[table_column] = table_name
                rows.append(parsed)
        return rows

    projected = [
        _project(row, spec.projection if spec else ()) for row in rows_from_payload(payload)
    ]
    rows = [_base_row(_usable_names(row), symbol, snapshot_utc) for row in projected]
    if dataset_name == "upgrades_downgrades":
        return [normalize_event_row(row) for row in rows]
    return rows


def _shares_rows(payload: object) -> list[dict[str, Any]]:
    """Flatten Ticker.get_shares_full() into one row per observation date."""
    if payload is None:
        return []
    if isinstance(payload, pd.Series):
        return [
            {"as_of_date": _clean_value(index), "shares_outstanding": _clean_value(value)}
            for index, value in payload.items()
        ]
    if isinstance(payload, pd.DataFrame):
        return _dataframe_rows(payload)
    return rows_from_payload(payload)


def _project(row: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    if not fields:
        return row
    return {field: row.get(field) for field in fields}


def _usable_names(row: dict[str, Any]) -> dict[str, Any]:
    """Rewrite column names Yahoo returns with spaces or punctuation into plain identifiers.

    Names that are already valid identifiers keep Yahoo's exact spelling, so existing columns such
    as `strongBuy` and `pctHeld` are untouched and only `EPS Estimate` or `Surprise(%)` change.
    """
    return {_usable_name(key): value for key, value in row.items()}


def _usable_name(key: str) -> str:
    if key.isidentifier():
        return key
    cleaned = key.replace("(%)", " pct").replace("%", " pct")
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", cleaned).strip("_").lower()
    return cleaned or "unnamed"


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
        index_name = frame.index.name or "index"
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
    if isinstance(value, (list, tuple)):
        cleaned = [_clean_value(item) for item in value]
        scalars = [str(item) for item in cleaned if item is not None]
        return ", ".join(scalars) if scalars else None
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
