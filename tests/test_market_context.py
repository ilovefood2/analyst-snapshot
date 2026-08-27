from __future__ import annotations

import csv
import io
import urllib.parse
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd

from analyst_snapshot.market_context import (
    CFTC_DATASET,
    CFTC_POSITION_COLUMNS,
    FINRA_DATASET,
    MarketContextError,
    OCC_DATASET,
    SourceUnavailable,
    manifest_path,
    parse_occ_account_volume,
    run_market_context,
    verify_market_context,
)
from analyst_snapshot.storage import dataset_path


def _cftc_zip() -> bytes:
    columns = [
        "Market_and_Exchange_Names",
        "Report_Date_as_YYYY-MM-DD",
        "CFTC_Contract_Market_Code",
        "Open_Interest_All",
        *CFTC_POSITION_COLUMNS.values(),
    ]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    for report_date in ("2026-08-11", "2026-08-18"):
        for code, market_name in (
            ("13874+", "S&P 500 Consolidated - CHICAGO MERCANTILE EXCHANGE"),
            ("20974+", "NASDAQ-100 Consolidated - CHICAGO MERCANTILE EXCHANGE"),
        ):
            row = {
                "Market_and_Exchange_Names": market_name,
                "Report_Date_as_YYYY-MM-DD": report_date,
                "CFTC_Contract_Market_Code": code,
                "Open_Interest_All": "100000",
            }
            row.update(
                {
                    name: str(1000 + index)
                    for index, name in enumerate(CFTC_POSITION_COLUMNS.values())
                }
            )
            writer.writerow(row)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("FinFutYY.txt", output.getvalue())
    return buffer.getvalue()


def _finra_text(source_date: date) -> bytes:
    output = io.StringIO()
    output.write("Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n")
    symbols = ["QQQ", "SPY", *(f"S{index:04d}" for index in range(1000))]
    for index, symbol in enumerate(symbols):
        output.write(
            f"{source_date:%Y%m%d}|{symbol}|{100 + index}.5|1|{1000 + index}.5|B,Q,N\n"
        )
    return output.getvalue().encode()


def _occ_csv(symbol: str, source_date: date) -> bytes:
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=["quantity", "underlying", "symbol", "actype", "porc", "exchange", "actdate"],
    )
    writer.writeheader()
    for account in ("C", "F", "M"):
        for call_put in ("C", "P"):
            writer.writerow(
                {
                    "quantity": 100,
                    "underlying": symbol,
                    "symbol": symbol,
                    "actype": account,
                    "porc": call_put,
                    "exchange": "CBOE",
                    "actdate": source_date.strftime("%m/%d/%Y"),
                }
            )
    return output.getvalue().encode()


class FakeSources:
    def __init__(self, run_date: date) -> None:
        self.run_date = run_date
        self.urls: list[str] = []

    def __call__(self, url: str) -> bytes:
        self.urls.append(url)
        if "cftc.gov" in url:
            return _cftc_zip()
        if "finra.org" in url:
            if self.run_date.strftime("%Y%m%d") not in url:
                raise SourceUnavailable("not this date")
            return _finra_text(self.run_date)
        if "marketdata.theocc.com" in url:
            query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            if query["reportDate"] != [self.run_date.strftime("%Y%m%d")]:
                raise SourceUnavailable("not this date")
            return _occ_csv(query["symbol"][0], self.run_date)
        raise AssertionError(url)


def test_run_verify_and_resume_free_market_context(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    source_date = date(2026, 8, 24)
    sources = FakeSources(source_date)

    result = run_market_context(
        archive,
        run_date=source_date.isoformat(),
        symbols=("QQQ", "SPY"),
        resume=True,
        fetcher=sources,
        snapshot_utc="2026-08-25T02:00:00Z",
        run_identifier="market_test",
    )

    assert result["status"] == "complete"
    assert result["incremental_cash_usd"] == 0.0
    assert len(sources.urls) == 4
    assert manifest_path(archive, source_date.isoformat()).is_file()

    cftc = pd.read_parquet(dataset_path(archive, CFTC_DATASET, source_date.isoformat()))
    finra = pd.read_parquet(dataset_path(archive, FINRA_DATASET, source_date.isoformat()))
    occ = pd.read_parquet(dataset_path(archive, OCC_DATASET, source_date.isoformat()))
    assert set(cftc["symbol"]) == {"NASDAQ100", "SP500"}
    assert set(finra["symbol"]) >= {"QQQ", "SPY"}
    assert set(occ["account_type_code"]) == {"C", "F", "M"}
    assert set(occ["call_put_code"]) == {"C", "P"}

    report = verify_market_context(archive, run_date=source_date.isoformat())
    assert report["ok"] is True
    assert report["datasets"][CFTC_DATASET]["source_date"] == "2026-08-18"

    def no_network(_url: str) -> bytes:
        raise AssertionError("resume made a network request")

    resumed = run_market_context(
        archive,
        run_date=source_date.isoformat(),
        symbols=("QQQ", "SPY"),
        resume=True,
        fetcher=no_network,
    )
    assert resumed["resume_status"] == "REUSED_COMPLETE_PARTITION"


def test_verify_detects_raw_capture_tampering(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    run_date = date(2026, 8, 24)
    result = run_market_context(
        archive,
        run_date=run_date.isoformat(),
        symbols=("QQQ", "SPY"),
        resume=True,
        fetcher=FakeSources(run_date),
        snapshot_utc="2026-08-25T02:00:00Z",
        run_identifier="market_test",
    )
    capture = result["sources"][OCC_DATASET]["captures"][0]
    (archive / capture["path"]).write_bytes(b"tampered")

    report = verify_market_context(archive, run_date=run_date.isoformat())

    assert report["ok"] is False
    assert any("hash mismatch" in error for error in report["errors"])


class UnpublishedOccSources(FakeSources):
    """OCC answers an unpublished date with HTTP 200 and a bare sentence, not CSV."""

    def __init__(self, run_date: date, published_date: date) -> None:
        super().__init__(run_date)
        self.published_date = published_date

    def __call__(self, url: str) -> bytes:
        if "marketdata.theocc.com" not in url:
            return super().__call__(url)
        self.urls.append(url)
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        requested = query["reportDate"][0]
        if requested > self.published_date.strftime("%Y%m%d"):
            return b"Report date cannot be greater than %s" % (
                self.published_date.strftime("%m/%d/%Y").encode()
            )
        if requested < self.published_date.strftime("%Y%m%d"):
            return b"No record(s) found"
        return _occ_csv(query["symbol"][0], self.published_date)


def test_occ_unpublished_date_falls_back_to_the_published_one(tmp_path: Path) -> None:
    archive = tmp_path / "archive"
    run_date = date(2026, 8, 25)
    published = date(2026, 8, 24)
    sources = UnpublishedOccSources(run_date, published)

    result = run_market_context(
        archive,
        run_date=run_date.isoformat(),
        symbols=("QQQ", "SPY"),
        resume=True,
        fetcher=sources,
        snapshot_utc="2026-08-25T04:00:00Z",
        run_identifier="market_unpublished_test",
    )

    assert result["status"] == "complete"
    assert result["sources"][OCC_DATASET]["source_date"] == published.isoformat()

    occ = pd.read_parquet(dataset_path(archive, OCC_DATASET, run_date.isoformat()))
    assert set(occ["symbol"]) == {"QQQ", "SPY"}
    assert set(occ["account_type_code"]) == {"C", "F", "M"}

    report = verify_market_context(archive, run_date=run_date.isoformat())
    assert report["ok"] is True


def test_occ_schema_drift_still_fails_hard() -> None:
    drifted = b"quantity,underlying,symbol,actype,porc,exchange\n1,QQQ,QQQ,C,C,CBOE\n"
    try:
        parse_occ_account_volume(
            drifted,
            symbol="QQQ",
            run_date=date(2026, 8, 25),
            snapshot_utc="2026-08-25T04:00:00Z",
            run_id="drift_test",
            source_url="https://marketdata.theocc.com/volume-query?x=1",
        )
    except MarketContextError as exc:
        assert not isinstance(exc, SourceUnavailable)
        assert "schema changed" in str(exc)
    else:
        raise AssertionError("schema drift must not be treated as unavailable")
