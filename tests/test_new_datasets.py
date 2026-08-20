from __future__ import annotations

import pandas as pd
import pyarrow.parquet as pq
import pytest

from analyst_snapshot.datasets import DATASETS, parse_dataset_payload
from analyst_snapshot.storage import append_rows, dataset_path
from analyst_snapshot.yahoo import YahooAnalystFetcher, dataset_request_estimate

SNAP = "2026-08-19T02:05:00Z"


def test_profile_keeps_only_the_curated_point_in_time_fields() -> None:
    # Ticker.info carries ~150 keys including live quote fields; archiving all of them daily
    # would fill the archive with intraday noise.
    info = {
        "marketCap": 3.2e12,
        "sector": "Technology",
        "sharesShort": 1.1e8,
        "shortPercentOfFloat": 0.0072,
        "floatShares": 1.4e10,
        "bid": 224.10,
        "ask": 224.15,
        "regularMarketVolume": 41_000_000,
        "longBusinessSummary": "x" * 5000,
    }

    rows = parse_dataset_payload("profile", info, "AAPL", SNAP)

    assert len(rows) == 1
    row = rows[0]
    assert row["marketCap"] == 3.2e12
    assert row["shortPercentOfFloat"] == 0.0072
    assert row["sector"] == "Technology"
    for noisy in ("bid", "ask", "regularMarketVolume", "longBusinessSummary"):
        assert noisy not in row


def test_earnings_tables_are_labelled_and_dates_flattened() -> None:
    payload = {
        "calendar": {
            "Earnings Date": [pd.Timestamp("2026-10-29"), pd.Timestamp("2026-11-02")],
            "Earnings Average": 2.41,
            "Ex-Dividend Date": pd.Timestamp("2026-08-08"),
        },
        "earnings_dates": pd.DataFrame(
            {"EPS Estimate": [2.35], "Reported EPS": [2.40], "Surprise(%)": [0.0213]},
            index=pd.DatetimeIndex([pd.Timestamp("2026-07-30")], name="Earnings Date"),
        ),
    }

    rows = parse_dataset_payload("earnings", payload, "AAPL", SNAP)

    by_table = {row["earnings_table"]: row for row in rows}
    assert set(by_table) == {"calendar", "earnings_dates"}
    # A calendar earnings date is a range; it is flattened to a scalar string, not a list column.
    assert by_table["calendar"]["earnings_date"] == "2026-10-29, 2026-11-02"
    assert by_table["calendar"]["ex_dividend_date"] == "2026-08-08"
    assert by_table["earnings_dates"]["eps_estimate"] == 2.35
    assert by_table["earnings_dates"]["surprise_pct"] == 0.0213


def test_holders_tables_are_labelled() -> None:
    payload = {
        "major_holders": pd.DataFrame(
            {"Value": [0.0007, 0.6212]},
            index=pd.Index(["insidersPercentHeld", "institutionsPercentHeld"], name="Breakdown"),
        ),
        "institutional_holders": pd.DataFrame(
            {
                "Date Reported": [pd.Timestamp("2026-06-30")],
                "Holder": ["Vanguard Group Inc"],
                "pctHeld": [0.0891],
                "Shares": [1.3e9],
            }
        ),
    }

    rows = parse_dataset_payload("holders", payload, "AAPL", SNAP)

    tables = {row["holders_table"] for row in rows}
    assert tables == {"major_holders", "institutional_holders"}
    inst = next(row for row in rows if row["holders_table"] == "institutional_holders")
    assert inst["Holder"] == "Vanguard Group Inc"
    assert inst["date_reported"] == "2026-06-30"
    assert inst["pctHeld"] == 0.0891


def test_shares_outstanding_series_becomes_one_row_per_date() -> None:
    # get_shares_full returns a Series indexed by date. Treated as a mapping it would collapse
    # into a single row with one column per date.
    series = pd.Series(
        [15_400_000_000, 15_330_000_000],
        index=pd.DatetimeIndex([pd.Timestamp("2026-06-30"), pd.Timestamp("2026-07-31")]),
    )

    rows = parse_dataset_payload("shares_outstanding", series, "AAPL", SNAP)

    assert len(rows) == 2
    assert rows[0]["as_of_date"] == "2026-06-30"
    assert rows[0]["shares_outstanding"] == 15_400_000_000
    assert all(row["symbol"] == "AAPL" for row in rows)


def test_unnamed_index_does_not_collide_with_the_partition_key() -> None:
    frame = pd.DataFrame({"Value": [1.0]}, index=pd.Index(["insidersPercentHeld"]))

    rows = parse_dataset_payload("holders", {"major_holders": frame}, "AAPL", SNAP)

    assert "date" not in rows[0]
    assert rows[0]["index"] == "insidersPercentHeld"


@pytest.mark.parametrize(
    ("dataset", "column"),
    [
        ("profile", "marketCap"),
        ("earnings", "eps_estimate"),
        ("holders", "pctHeld"),
        ("shares_outstanding", "shares_outstanding"),
    ],
)
def test_new_datasets_write_a_stable_schema(tmp_path, dataset: str, column: str) -> None:
    full = dataset_path(tmp_path, dataset, "2026-08-18")
    sparse = dataset_path(tmp_path, dataset, "2026-08-19")
    append_rows(full, [{"symbol": "AAPL", "snapshot_utc": SNAP, column: 1.0}], dataset)
    append_rows(
        sparse,
        [{"symbol": "AAPL", "snapshot_utc": SNAP, "no_analyst_coverage": True}],
        dataset,
    )

    assert pq.read_schema(full) == pq.read_schema(sparse)


def test_getter_attributes_are_called_not_stored() -> None:
    class StubTicker:
        def __init__(self) -> None:
            self.calls = 0

        def get_shares_full(self):
            self.calls += 1
            return pd.Series([1.0], index=pd.DatetimeIndex([pd.Timestamp("2026-06-30")]))

    ticker = StubTicker()
    payload = YahooAnalystFetcher._payload_for_spec(ticker, DATASETS["h"])

    assert ticker.calls == 1
    assert isinstance(payload, pd.Series)


def test_request_estimate_tracks_the_enabled_datasets() -> None:
    assert dataset_request_estimate([DATASETS["a"]]) == 1
    assert dataset_request_estimate(DATASETS.values()) == 15
