from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import math
import os
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from analyst_snapshot.storage import dataset_path, read_parquet_or_empty, write_parquet

CFTC_DATASET = "cftc_tff_positioning"
FINRA_DATASET = "finra_short_volume"
OCC_DATASET = "occ_account_volume"
MARKET_CONTEXT_DATASETS = (CFTC_DATASET, FINRA_DATASET, OCC_DATASET)

CFTC_URL = "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"
FINRA_URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{ymd}.txt"
OCC_URL = "https://marketdata.theocc.com/volume-query"
USER_AGENT = "analyst-snapshot/0.3 personal prospective market-context research"

CFTC_MARKETS = {
    "13874+": ("SP500", "sp500"),
    "20974+": ("NASDAQ100", "nasdaq100"),
}
CFTC_POSITION_COLUMNS = {
    "dealer_long": "Dealer_Positions_Long_All",
    "dealer_short": "Dealer_Positions_Short_All",
    "dealer_spreading": "Dealer_Positions_Spread_All",
    "asset_manager_long": "Asset_Mgr_Positions_Long_All",
    "asset_manager_short": "Asset_Mgr_Positions_Short_All",
    "asset_manager_spreading": "Asset_Mgr_Positions_Spread_All",
    "leveraged_money_long": "Lev_Money_Positions_Long_All",
    "leveraged_money_short": "Lev_Money_Positions_Short_All",
    "leveraged_money_spreading": "Lev_Money_Positions_Spread_All",
    "other_reportable_long": "Other_Rept_Positions_Long_All",
    "other_reportable_short": "Other_Rept_Positions_Short_All",
    "other_reportable_spreading": "Other_Rept_Positions_Spread_All",
    "nonreportable_long": "NonRept_Positions_Long_All",
    "nonreportable_short": "NonRept_Positions_Short_All",
}
OCC_ACCOUNT_TYPES = {"C": "customer", "F": "firm", "M": "market_maker"}
OCC_CALL_PUT = {"C": "call", "P": "put"}
SOURCE_LAG_LIMITS = {CFTC_DATASET: 14, FINRA_DATASET: 4, OCC_DATASET: 4}

FetchBytes = Callable[[str], bytes]


class MarketContextError(RuntimeError):
    """A free-source response, archive partition, or provenance contract failed."""


class SourceUnavailable(MarketContextError):
    """The requested source/date is not published at capture time."""


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def fetch_bytes(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
            if response.status != 200:
                raise MarketContextError(f"HTTP {response.status} from {url}")
            return response.read()
    except urllib.error.HTTPError as exc:
        if exc.code in {403, 404}:
            raise SourceUnavailable(f"source date unavailable: {url}") from exc
        raise MarketContextError(f"HTTP {exc.code} from {url}") from exc
    except (OSError, TimeoutError) as exc:
        raise MarketContextError(f"network failure from {url}: {type(exc).__name__}") from exc


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    payload = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    _atomic_bytes(path, payload)


def _atomic_partition(path: Path, rows: list[dict[str, Any]], dataset: str) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        write_parquet(temporary, pd.DataFrame(rows), dataset)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return {"path": path.as_posix(), "rows": len(rows), "sha256": sha256_file(path)}


def _capture(
    snapshot_dir: Path,
    run_date: str,
    filename: str,
    payload: bytes,
    *,
    gzip_payload: bool,
) -> dict[str, Any]:
    raw_sha256 = sha256_bytes(payload)
    stored = gzip.compress(payload, compresslevel=9, mtime=0) if gzip_payload else payload
    path = snapshot_dir / "_market_context_sources" / f"date={run_date}" / filename
    _atomic_bytes(path, stored)
    return {
        "path": path.relative_to(snapshot_dir).as_posix(),
        "encoding": "gzip" if gzip_payload else "identity",
        "source_bytes": len(payload),
        "source_sha256": raw_sha256,
        "stored_bytes": len(stored),
        "stored_sha256": sha256_bytes(stored),
    }


def _candidate_dates(run_date: date, lookback_days: int = 7) -> Iterable[date]:
    for offset in range(lookback_days + 1):
        yield run_date - timedelta(days=offset)


def _stamp_rows(
    rows: list[dict[str, Any]],
    *,
    dataset: str,
    snapshot_utc: str,
    run_id: str,
    run_date: date,
    source_date: date,
    source_url: str,
    source_sha256: str,
) -> list[dict[str, Any]]:
    lag = (run_date - source_date).days
    if lag < 0:
        raise MarketContextError(f"future source date for {dataset}: {source_date}")
    for row in rows:
        row.update(
            {
                "snapshot_utc": snapshot_utc,
                "dataset": dataset,
                "run_id": run_id,
                "source_date": source_date.isoformat(),
                "source_url": source_url,
                "source_sha256": source_sha256,
                "source_lag_days": lag,
            }
        )
    return rows


def parse_cftc_tff(
    payload: bytes,
    *,
    run_date: date,
    snapshot_utc: str,
    run_id: str,
    source_url: str,
) -> tuple[list[dict[str, Any]], date]:
    if not payload.startswith(b"PK"):
        raise MarketContextError("CFTC response is not a ZIP archive")
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        members = [member for member in archive.infolist() if not member.is_dir()]
        if len(members) != 1 or not members[0].filename.lower().endswith(".txt"):
            raise MarketContextError("CFTC ZIP member inventory changed")
        text = archive.read(members[0]).decode("utf-8-sig", errors="strict")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or not set(CFTC_POSITION_COLUMNS.values()).issubset(reader.fieldnames):
        raise MarketContextError("CFTC TFF schema changed")
    selected: list[tuple[date, dict[str, str]]] = []
    for record in reader:
        code = (record.get("CFTC_Contract_Market_Code") or "").strip()
        raw_date = (record.get("Report_Date_as_YYYY-MM-DD") or "").strip()
        if code not in CFTC_MARKETS or not raw_date:
            continue
        report_date = date.fromisoformat(raw_date)
        if report_date <= run_date:
            selected.append((report_date, record))
    if not selected:
        raise SourceUnavailable("CFTC archive has no eligible TFF rows")
    source_date = max(report_date for report_date, _record in selected)
    latest = [
        (report_date, record)
        for report_date, record in selected
        if report_date == source_date
    ]
    if {record["CFTC_Contract_Market_Code"].strip() for _day, record in latest} != set(
        CFTC_MARKETS
    ):
        raise MarketContextError("CFTC latest report lacks both market contracts")
    raw_sha256 = sha256_bytes(payload)
    rows: list[dict[str, Any]] = []
    for _day, record in latest:
        code = record["CFTC_Contract_Market_Code"].strip()
        symbol, market = CFTC_MARKETS[code]
        open_interest = _nonnegative_float(record.get("Open_Interest_All"), "open interest")
        if open_interest <= 0:
            raise MarketContextError("CFTC open interest is not positive")
        row: dict[str, Any] = {
            "symbol": symbol,
            "market": market,
            "market_name": (record.get("Market_and_Exchange_Names") or "").strip(),
            "cftc_contract_market_code": code,
            "open_interest": open_interest,
        }
        for output, source in CFTC_POSITION_COLUMNS.items():
            row[output] = _nonnegative_float(record.get(source), source)
        for prefix in ("dealer", "asset_manager", "leveraged_money"):
            row[f"{prefix}_net_share"] = (
                float(row[f"{prefix}_long"]) - float(row[f"{prefix}_short"])
            ) / open_interest
        rows.append(row)
    rows.sort(key=lambda row: str(row["symbol"]))
    return (
        _stamp_rows(
            rows,
            dataset=CFTC_DATASET,
            snapshot_utc=snapshot_utc,
            run_id=run_id,
            run_date=run_date,
            source_date=source_date,
            source_url=source_url,
            source_sha256=raw_sha256,
        ),
        source_date,
    )


def parse_finra_short_volume(
    payload: bytes,
    *,
    run_date: date,
    snapshot_utc: str,
    run_id: str,
    source_url: str,
) -> tuple[list[dict[str, Any]], date]:
    text = payload.decode("utf-8-sig", errors="strict")
    reader = csv.DictReader(io.StringIO(text), delimiter="|")
    expected = {"Date", "Symbol", "ShortVolume", "ShortExemptVolume", "TotalVolume", "Market"}
    if not reader.fieldnames or set(reader.fieldnames) != expected:
        raise MarketContextError("FINRA CNMS schema changed")
    rows: list[dict[str, Any]] = []
    source_dates: set[date] = set()
    for record in reader:
        symbol = (record.get("Symbol") or "").strip()
        raw_date = (record.get("Date") or "").strip()
        if not symbol or len(raw_date) != 8 or not raw_date.isdigit():
            continue
        source_date = date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:8]))
        source_dates.add(source_date)
        short = _nonnegative_float(record.get("ShortVolume"), "short volume")
        exempt = _nonnegative_float(record.get("ShortExemptVolume"), "short exempt volume")
        total = _nonnegative_float(record.get("TotalVolume"), "total volume")
        if short > total + 1e-9 or exempt > total + 1e-9:
            raise MarketContextError(f"FINRA component exceeds total for {symbol}")
        rows.append(
            {
                "symbol": symbol,
                "short_volume": short,
                "short_exempt_volume": exempt,
                "total_volume": total,
                "short_volume_ratio": short / total if total > 0 else None,
                "market": (record.get("Market") or "").strip(),
            }
        )
    if len(source_dates) != 1 or len(rows) < 1000:
        raise MarketContextError("FINRA response date/row inventory invalid")
    source_date = next(iter(source_dates))
    if source_date > run_date or not {"QQQ", "SPY"}.issubset({row["symbol"] for row in rows}):
        raise MarketContextError("FINRA response scope invalid")
    rows.sort(key=lambda row: str(row["symbol"]))
    return (
        _stamp_rows(
            rows,
            dataset=FINRA_DATASET,
            snapshot_utc=snapshot_utc,
            run_id=run_id,
            run_date=run_date,
            source_date=source_date,
            source_url=source_url,
            source_sha256=sha256_bytes(payload),
        ),
        source_date,
    )


def parse_occ_account_volume(
    payload: bytes,
    *,
    symbol: str,
    run_date: date,
    snapshot_utc: str,
    run_id: str,
    source_url: str,
) -> tuple[list[dict[str, Any]], date]:
    text = payload.decode("utf-8-sig", errors="strict")
    reader = csv.DictReader(io.StringIO(text))
    expected = {"quantity", "underlying", "symbol", "actype", "porc", "exchange", "actdate"}
    if not reader.fieldnames or set(reader.fieldnames) != expected:
        raise MarketContextError("OCC volume-query schema changed")
    rows: list[dict[str, Any]] = []
    source_dates: set[date] = set()
    for record in reader:
        underlying = (record.get("underlying") or "").strip().upper()
        if not underlying:
            continue
        if underlying != symbol:
            raise MarketContextError(f"OCC returned wrong underlying: {underlying}")
        account_code = (record.get("actype") or "").strip().upper()
        call_put_code = (record.get("porc") or "").strip().upper()
        if account_code not in OCC_ACCOUNT_TYPES or call_put_code not in OCC_CALL_PUT:
            raise MarketContextError("OCC returned unknown account/call-put code")
        raw_date = (record.get("actdate") or "").strip().rstrip(",")
        source_date = datetime.strptime(raw_date, "%m/%d/%Y").date()
        source_dates.add(source_date)
        rows.append(
            {
                "symbol": symbol,
                "option_symbol": (record.get("symbol") or "").strip().upper(),
                "account_type_code": account_code,
                "account_type": OCC_ACCOUNT_TYPES[account_code],
                "call_put_code": call_put_code,
                "call_put": OCC_CALL_PUT[call_put_code],
                "exchange": (record.get("exchange") or "").strip().upper(),
                "quantity": _nonnegative_float(record.get("quantity"), "OCC quantity"),
            }
        )
    if len(source_dates) != 1 or not rows:
        raise SourceUnavailable(f"OCC has no rows for {symbol}")
    source_date = next(iter(source_dates))
    if source_date > run_date:
        raise MarketContextError("OCC returned a future source date")
    observed_accounts = {row["account_type_code"] for row in rows}
    observed_call_put = {row["call_put_code"] for row in rows}
    if observed_accounts != set(OCC_ACCOUNT_TYPES) or observed_call_put != set(OCC_CALL_PUT):
        raise MarketContextError(f"OCC account/call-put coverage incomplete for {symbol}")
    rows.sort(
        key=lambda row: (
            str(row["option_symbol"]),
            str(row["exchange"]),
            str(row["account_type_code"]),
            str(row["call_put_code"]),
        )
    )
    return (
        _stamp_rows(
            rows,
            dataset=OCC_DATASET,
            snapshot_utc=snapshot_utc,
            run_id=run_id,
            run_date=run_date,
            source_date=source_date,
            source_url=source_url,
            source_sha256=sha256_bytes(payload),
        ),
        source_date,
    )


def _nonnegative_float(value: Any, role: str) -> float:
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise MarketContextError(f"invalid {role}: {value!r}") from exc
    if parsed < 0 or not math.isfinite(parsed):
        raise MarketContextError(f"invalid {role}: {value!r}")
    return parsed


def _occ_url(symbol: str, source_date: date) -> str:
    query = urllib.parse.urlencode(
        {
            "reportDate": source_date.strftime("%Y%m%d"),
            "format": "csv",
            "volumeQueryType": "O",
            "symbolType": "U",
            "symbol": symbol,
            "reportType": "D",
            "accountType": "ALL",
            "productKind": "OSTK",
            "porc": "BOTH",
        }
    )
    return f"{OCC_URL}?{query}"


def collect_cftc(
    snapshot_dir: Path,
    run_date: date,
    snapshot_utc: str,
    run_id: str,
    fetcher: FetchBytes,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    last_error: Exception | None = None
    for year in (run_date.year, run_date.year - 1):
        url = CFTC_URL.format(year=year)
        try:
            payload = fetcher(url)
            rows, source_date = parse_cftc_tff(
                payload,
                run_date=run_date,
                snapshot_utc=snapshot_utc,
                run_id=run_id,
                source_url=url,
            )
            capture = _capture(
                snapshot_dir,
                run_date.isoformat(),
                f"cftc_tff_{year}.zip",
                payload,
                gzip_payload=False,
            )
            return rows, {"source_date": source_date.isoformat(), "url": url, "captures": [capture]}
        except SourceUnavailable as exc:
            last_error = exc
    raise SourceUnavailable(f"CFTC TFF unavailable: {last_error}")


def collect_finra(
    snapshot_dir: Path,
    run_date: date,
    snapshot_utc: str,
    run_id: str,
    fetcher: FetchBytes,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    last_error: Exception | None = None
    for source_date in _candidate_dates(run_date):
        url = FINRA_URL.format(ymd=source_date.strftime("%Y%m%d"))
        try:
            payload = fetcher(url)
            rows, observed_date = parse_finra_short_volume(
                payload,
                run_date=run_date,
                snapshot_utc=snapshot_utc,
                run_id=run_id,
                source_url=url,
            )
            if observed_date != source_date:
                raise MarketContextError("FINRA requested and observed dates differ")
            capture = _capture(
                snapshot_dir,
                run_date.isoformat(),
                f"finra_CNMSshvol{source_date:%Y%m%d}.txt.gz",
                payload,
                gzip_payload=True,
            )
            return rows, {
                "source_date": source_date.isoformat(),
                "url": url,
                "captures": [capture],
            }
        except SourceUnavailable as exc:
            last_error = exc
    raise SourceUnavailable(f"FINRA CNMS unavailable: {last_error}")


def collect_occ(
    snapshot_dir: Path,
    run_date: date,
    snapshot_utc: str,
    run_id: str,
    fetcher: FetchBytes,
    symbols: tuple[str, ...],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    last_error: Exception | None = None
    for source_date in _candidate_dates(run_date):
        rows: list[dict[str, Any]] = []
        captures: list[dict[str, Any]] = []
        urls: list[str] = []
        try:
            for symbol in symbols:
                url = _occ_url(symbol, source_date)
                payload = fetcher(url)
                parsed, observed_date = parse_occ_account_volume(
                    payload,
                    symbol=symbol,
                    run_date=run_date,
                    snapshot_utc=snapshot_utc,
                    run_id=run_id,
                    source_url=url,
                )
                if observed_date != source_date:
                    raise MarketContextError("OCC requested and observed dates differ")
                rows.extend(parsed)
                urls.append(url)
                captures.append(
                    _capture(
                        snapshot_dir,
                        run_date.isoformat(),
                        f"occ_{symbol}_{source_date:%Y%m%d}.csv.gz",
                        payload,
                        gzip_payload=True,
                    )
                )
            return rows, {
                "source_date": source_date.isoformat(),
                "urls": urls,
                "captures": captures,
                "symbols": list(symbols),
            }
        except SourceUnavailable as exc:
            last_error = exc
    raise SourceUnavailable(f"OCC account volume unavailable: {last_error}")


def manifest_path(snapshot_dir: Path, run_date: str) -> Path:
    return snapshot_dir / "_market_context_manifests" / f"date={run_date}" / "manifest.json"


def run_market_context(
    snapshot_dir: Path,
    *,
    run_date: str,
    symbols: tuple[str, ...] = ("QQQ", "SPY"),
    resume: bool = False,
    fetcher: FetchBytes = fetch_bytes,
    snapshot_utc: str | None = None,
    run_identifier: str | None = None,
) -> dict[str, Any]:
    parsed_run_date = date.fromisoformat(run_date)
    normalized_symbols = tuple(
        dict.fromkeys(symbol.strip().upper() for symbol in symbols if symbol)
    )
    if normalized_symbols != ("QQQ", "SPY"):
        raise MarketContextError("market-context symbol contract is exactly QQQ,SPY")
    prior_path = manifest_path(snapshot_dir, run_date)
    if resume and prior_path.is_file():
        prior_report = verify_market_context(snapshot_dir, run_date=run_date)
        if prior_report["ok"]:
            prior = json.loads(prior_path.read_text())
            prior["resume_status"] = "REUSED_COMPLETE_PARTITION"
            return prior
    elif prior_path.exists():
        raise MarketContextError(f"market-context partition already exists: {run_date}")

    captured_at = snapshot_utc or _utc_now_iso()
    identifier = run_identifier or f"market_context_{captured_at.replace(':', '').replace('-', '')}"
    collectors = {
        CFTC_DATASET: lambda: collect_cftc(
            snapshot_dir, parsed_run_date, captured_at, identifier, fetcher
        ),
        FINRA_DATASET: lambda: collect_finra(
            snapshot_dir, parsed_run_date, captured_at, identifier, fetcher
        ),
        OCC_DATASET: lambda: collect_occ(
            snapshot_dir,
            parsed_run_date,
            captured_at,
            identifier,
            fetcher,
            normalized_symbols,
        ),
    }
    sources: dict[str, Any] = {}
    errors: list[dict[str, str]] = []
    for dataset, collector in collectors.items():
        try:
            rows, evidence = collector()
            output = _atomic_partition(dataset_path(snapshot_dir, dataset, run_date), rows, dataset)
            output["path"] = Path(output["path"]).relative_to(snapshot_dir).as_posix()
            sources[dataset] = {"status": "ok", "output": output, **evidence}
            print(
                f"market_context source={dataset} status=ok source_date={evidence['source_date']} "
                f"rows={len(rows)}",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001 - preserve other independent free sources
            error = {"source": dataset, "error_type": type(exc).__name__, "error": str(exc)}
            errors.append(error)
            sources[dataset] = {"status": "failed", **error}
            print(f"market_context source={dataset} status=failed error={exc}", flush=True)
    manifest = {
        "schema": "analyst_snapshot_market_context_manifest_v1",
        "status": "complete" if not errors else "partial",
        "run_date": run_date,
        "run_id": identifier,
        "snapshot_utc": captured_at,
        "sources": sources,
        "errors": errors,
        "network_workers": 1,
        "free_public_sources_only": True,
        "incremental_cash_usd": 0.0,
        "participant_data_scope": {
            "cftc": "actual futures trader categories; weekly and publication-lagged",
            "occ": "actual options clearing account type; no buy/sell or open/close fields",
            "finra": "actual consolidated short-sale volume; not participant identity",
            "cboe_open_close": "not collected because it is paid proprietary data",
        },
    }
    _atomic_json(prior_path, manifest)
    return manifest


def verify_market_context(snapshot_dir: Path, *, run_date: str) -> dict[str, Any]:
    path = manifest_path(snapshot_dir, run_date)
    errors: list[str] = []
    if not path.is_file():
        return {"ok": False, "run_date": run_date, "errors": ["manifest missing"]}
    try:
        manifest = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return {"ok": False, "run_date": run_date, "errors": [f"manifest unreadable: {exc}"]}
    if manifest.get("schema") != "analyst_snapshot_market_context_manifest_v1":
        errors.append("manifest schema invalid")
    if manifest.get("status") != "complete" or manifest.get("errors"):
        errors.append("manifest is not complete")
    if manifest.get("incremental_cash_usd") != 0.0:
        errors.append("nonzero incremental cash recorded")
    parsed_run_date = date.fromisoformat(run_date)
    summaries: dict[str, Any] = {}
    for dataset in MARKET_CONTEXT_DATASETS:
        evidence = manifest.get("sources", {}).get(dataset, {})
        if evidence.get("status") != "ok":
            errors.append(f"{dataset}: source status not ok")
            continue
        output = evidence.get("output", {})
        output_path = snapshot_dir / str(output.get("path", ""))
        if not output_path.is_file() or sha256_file(output_path) != output.get("sha256"):
            errors.append(f"{dataset}: output missing or hash mismatch")
            continue
        frame = read_parquet_or_empty(output_path)
        if len(frame) != output.get("rows") or frame.empty:
            errors.append(f"{dataset}: output row count mismatch")
            continue
        source_dates = set(frame["source_date"].astype(str))
        snapshot_values = set(frame["snapshot_utc"].astype(str))
        if set(frame["dataset"].astype(str)) != {dataset}:
            errors.append(f"{dataset}: dataset identity mismatch")
        if len(source_dates) != 1 or len(snapshot_values) != 1:
            errors.append(f"{dataset}: mixed source/capture timestamps")
            continue
        source_date = date.fromisoformat(next(iter(source_dates)))
        lag = (parsed_run_date - source_date).days
        if lag < 0 or lag > SOURCE_LAG_LIMITS[dataset]:
            errors.append(f"{dataset}: source lag {lag} exceeds {SOURCE_LAG_LIMITS[dataset]}")
        if not frame["source_lag_days"].eq(lag).all():
            errors.append(f"{dataset}: source lag column mismatch")
        captures = evidence.get("captures", [])
        capture_source_hashes = {capture.get("source_sha256") for capture in captures}
        if set(frame["source_sha256"].astype(str)) != capture_source_hashes:
            errors.append(f"{dataset}: normalized source hashes do not match captures")
        for capture in captures:
            capture_path = snapshot_dir / str(capture.get("path", ""))
            if not capture_path.is_file() or sha256_file(capture_path) != capture.get(
                "stored_sha256"
            ):
                errors.append(f"{dataset}: raw capture missing or hash mismatch")
                continue
            stored = capture_path.read_bytes()
            try:
                source = gzip.decompress(stored) if capture.get("encoding") == "gzip" else stored
            except OSError:
                errors.append(f"{dataset}: raw capture decompression failed")
                continue
            if sha256_bytes(source) != capture.get("source_sha256"):
                errors.append(f"{dataset}: source-byte hash mismatch")
        summaries[dataset] = {
            "rows": len(frame),
            "source_date": source_date.isoformat(),
            "lag": lag,
        }

    cftc = read_parquet_or_empty(dataset_path(snapshot_dir, CFTC_DATASET, run_date))
    if not cftc.empty and set(cftc["symbol"].astype(str)) != {"NASDAQ100", "SP500"}:
        errors.append("cftc_tff_positioning: expected markets missing")
    finra = read_parquet_or_empty(dataset_path(snapshot_dir, FINRA_DATASET, run_date))
    if not finra.empty and not {"QQQ", "SPY"}.issubset(set(finra["symbol"].astype(str))):
        errors.append("finra_short_volume: QQQ/SPY missing")
    occ = read_parquet_or_empty(dataset_path(snapshot_dir, OCC_DATASET, run_date))
    if not occ.empty:
        if set(occ["symbol"].astype(str)) != {"QQQ", "SPY"}:
            errors.append("occ_account_volume: QQQ/SPY scope mismatch")
        if set(occ["account_type_code"].astype(str)) != set(OCC_ACCOUNT_TYPES):
            errors.append("occ_account_volume: account-type coverage mismatch")
        if set(occ["call_put_code"].astype(str)) != set(OCC_CALL_PUT):
            errors.append("occ_account_volume: call-put coverage mismatch")
    return {"ok": not errors, "run_date": run_date, "datasets": summaries, "errors": errors}
