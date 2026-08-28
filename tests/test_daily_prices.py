from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analyst_snapshot.daily_prices import (
    ADJUSTED_PRICE_BASIS,
    PRICE_SESSION_COUNT,
    UNADJUSTED_PRICE_BASIS,
    _price_sessions,
    _provider_symbol,
    _requested_symbols,
    _rows_from_download,
    _schema_sha256,
    _sha256_file,
    _valid_ohlc,
    daily_price_manifest_path,
    run_daily_prices,
    verify_daily_prices,
)
from analyst_snapshot.datasets import (
    DAILY_PRICE_MANIFEST_SCHEMA,
    DAILY_PRICE_SCHEMA,
    DAILY_PRICES_DATASET,
    TREND_PRICE_ANCHORS,
)
from analyst_snapshot.storage import dataset_path, write_parquet

TARGET = "2026-08-27"
AFTER_CLOSE = datetime(2026, 8, 28, 1, 0, tzinfo=UTC)
SWINGLAB_PRICE_SUPPLEMENT = {
    "BETR",
    "BOT",
    "BXSL",
    "BYND",
    "CCXI",
    "CVCO",
    "DFNS",
    "DXYZ",
    "EQX",
    "FCUV",
    "GOF",
    "GTE",
    "IE",
    "IMO",
    "MPTI",
    "NG",
    "NHC",
    "PAGP",
    "PRK",
    "PTY",
    "RVII",
    "SEB",
    "SVM",
    "TGB",
    "UEC",
    "UMAC",
    "UTG",
    "UUUU",
    "VCX",
    "WETO",
}


class FakeDailyDownload:
    def __init__(
        self,
        *,
        omit_first_target: str | None = None,
        always_missing: set[str] | None = None,
    ):
        self.omit_first_target = omit_first_target
        self.always_missing = always_missing or set()
        self.calls: list[tuple[list[str], dict[str, Any]]] = []
        self.sessions = _price_sessions(datetime.fromisoformat(TARGET).date())

    def __call__(self, tickers: list[str], **kwargs: Any) -> pd.DataFrame:
        symbols = list(tickers)
        self.calls.append((symbols, kwargs))
        assert kwargs == {
            "start": self.sessions[0].isoformat(),
            "end": "2026-08-28",
            "interval": "1d",
            "auto_adjust": False,
            "actions": True,
            "threads": False,
            "progress": False,
            "repair": False,
            "group_by": "ticker",
        }
        fields = (
            "Open",
            "High",
            "Low",
            "Close",
            "Adj Close",
            "Volume",
            "Dividends",
            "Stock Splits",
        )
        values: dict[tuple[str, str], list[float]] = {}
        first_multi_call = len(self.calls) == 1 and len(symbols) > 1
        for symbol_number, symbol in enumerate(symbols):
            missing_all = symbol in self.always_missing
            omit_target = missing_all or (first_multi_call and symbol == self.omit_first_target)
            for field in fields:
                field_values: list[float] = []
                for session_number, _session in enumerate(self.sessions):
                    if missing_all or (omit_target and session_number == len(self.sessions) - 1):
                        field_values.append(float("nan"))
                        continue
                    base = 100.0 + symbol_number + session_number
                    field_values.append(
                        {
                            "Open": base,
                            "High": base + 2.0,
                            "Low": base - 2.0,
                            "Close": base + 1.0,
                            "Adj Close": (base + 1.0) * 0.5,
                            "Volume": 1_000_000.0 + session_number,
                            "Dividends": 0.0,
                            "Stock Splits": 0.0,
                        }[field]
                    )
                values[(symbol, field)] = field_values
        frame = pd.DataFrame(values, index=pd.to_datetime(self.sessions))
        frame.columns = pd.MultiIndex.from_tuples(frame.columns)
        return frame


def _universe(tmp_path: Path, symbols: list[str]) -> Path:
    path = tmp_path / "universe.txt"
    path.write_text("\n".join(symbols) + "\n", encoding="utf-8")
    return path


def _run(
    tmp_path: Path,
    universe: Path,
    fetcher: FakeDailyDownload,
    *,
    resume: bool = False,
) -> dict[str, Any]:
    return run_daily_prices(
        tmp_path / "archive",
        universe,
        TARGET,
        resume=resume,
        batch_size=50,
        fetcher=fetcher,
        clock=lambda: AFTER_CLOSE,
        progress=lambda _payload: None,
        sleep_fn=lambda _seconds: None,
    )


def test_tracked_universe_covers_swinglab_price_supplement_without_alias_collision() -> None:
    universe_path = Path(__file__).resolve().parents[1] / "universe.txt"
    universe = [
        line.strip().upper()
        for line in universe_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert universe == sorted(set(universe))
    assert SWINGLAB_PRICE_SUPPLEMENT <= set(universe)
    # SwingLab uses dot-class canonical names locally. Yahoo already has the exact economic
    # securities under its hyphen aliases, so the producer must not request each provider code
    # twice. The consumer owns the explicit BF.B/BRK.B recovery alias bridge.
    assert {"BF-B", "BRK-B"} <= set(universe)
    assert {"BF.B", "BRK.B"}.isdisjoint(universe)
    requested = _requested_symbols(universe)
    provider_symbols = [_provider_symbol(symbol) for symbol in requested]
    assert len(provider_symbols) == len(set(provider_symbols))


def test_schema_and_trend_anchor_contract_is_exact() -> None:
    assert DAILY_PRICES_DATASET == "daily_prices"
    assert DAILY_PRICE_MANIFEST_SCHEMA == "analyst_snapshot_daily_prices_manifest_v1"
    assert len(DAILY_PRICE_SCHEMA) == 31
    assert list(DAILY_PRICE_SCHEMA) == [
        "dataset",
        "run_id",
        "target_session",
        "bar_session",
        "symbol",
        "canonical_symbol",
        "provider_symbol",
        "provider_name",
        "transport",
        "provider_version",
        "currency",
        "unadjusted_price_basis",
        "adjusted_price_basis",
        "unadjusted_open",
        "unadjusted_high",
        "unadjusted_low",
        "unadjusted_close",
        "adjusted_open",
        "adjusted_high",
        "adjusted_low",
        "adjusted_close",
        "volume",
        "adjustment_factor",
        "dividend_cash",
        "stock_split_ratio",
        "capture_started_utc",
        "capture_finished_utc",
        "available_at_utc",
        "is_target_session",
        "batch_id",
        "raw_record_sha256",
    ]
    assert DAILY_PRICE_SCHEMA["target_session"] == pa.date32()
    assert DAILY_PRICE_SCHEMA["bar_session"] == pa.date32()
    assert TREND_PRICE_ANCHORS == (
        "QQQ",
        "SPY",
        "IWM",
        "HYG",
        "LQD",
        "TLT",
        "IEF",
        "RSP",
        "SOXX",
        "XLK",
        "XLP",
        "XLU",
        "VIXY",
        "VXZ",
    )


def test_adjusted_ohlc_roundoff_preserves_exact_envelope() -> None:
    target = date(2026, 8, 27)
    raw_close = 1.01
    adjusted_close = 0.8585
    assert raw_close * (adjusted_close / raw_close) < adjusted_close
    fields = {
        "Open": 1.00,
        "High": raw_close,
        "Low": 0.99,
        "Close": raw_close,
        "Adj Close": adjusted_close,
        "Volume": 1000.0,
    }
    payload = pd.DataFrame(
        {("AAPL", field): [value] for field, value in fields.items()},
        index=pd.to_datetime([target]),
    )
    payload.columns = pd.MultiIndex.from_tuples(payload.columns)

    rows, errors = _rows_from_download(
        payload,
        canonical_batch=["AAPL"],
        provider_batch=["AAPL"],
        canonical_by_provider={"AAPL": "AAPL"},
        sessions=[target],
        target_session=target,
        run_id="roundoff-test",
        batch_id="batch_0001",
        provider_version="test",
        capture_started_utc="2026-08-27T22:00:00Z",
        capture_finished_utc="2026-08-27T22:00:01Z",
    )

    assert errors == []
    assert len(rows) == 1
    row = rows[0]
    assert row["adjusted_close"] == adjusted_close
    assert row["adjusted_high"] == adjusted_close
    assert _valid_ohlc(
        row["adjusted_open"],
        row["adjusted_high"],
        row["adjusted_low"],
        row["adjusted_close"],
    )


def test_adjusted_low_roundoff_does_not_cross_exact_close() -> None:
    target = date(2026, 8, 27)
    raw_close = 1.05
    adjusted_close = 0.8505
    assert raw_close * (adjusted_close / raw_close) > adjusted_close
    payload = pd.DataFrame(
        {
            ("AAPL", "Open"): [1.055],
            ("AAPL", "High"): [1.06],
            ("AAPL", "Low"): [raw_close],
            ("AAPL", "Close"): [raw_close],
            ("AAPL", "Adj Close"): [adjusted_close],
            ("AAPL", "Volume"): [1000.0],
        },
        index=pd.to_datetime([target]),
    )
    payload.columns = pd.MultiIndex.from_tuples(payload.columns)

    rows, errors = _rows_from_download(
        payload,
        canonical_batch=["AAPL"],
        provider_batch=["AAPL"],
        canonical_by_provider={"AAPL": "AAPL"},
        sessions=[target],
        target_session=target,
        run_id="roundoff-test",
        batch_id="batch_0001",
        provider_version="test",
        capture_started_utc="2026-08-27T22:00:00Z",
        capture_finished_utc="2026-08-27T22:00:01Z",
    )

    assert errors == []
    assert rows[0]["adjusted_low"] == adjusted_close
    assert _valid_ohlc(
        rows[0]["adjusted_open"],
        rows[0]["adjusted_high"],
        rows[0]["adjusted_low"],
        rows[0]["adjusted_close"],
    )


def test_roundoff_rows_survive_full_capture_and_strict_verify(tmp_path: Path) -> None:
    universe = _universe(tmp_path, ["AAPL"])

    class RoundoffDownload(FakeDailyDownload):
        def __call__(self, tickers: list[str], **kwargs: Any) -> pd.DataFrame:
            frame = super().__call__(tickers, **kwargs)
            for symbol in tickers:
                frame[(symbol, "Open")] = 1.00
                frame[(symbol, "High")] = 1.01
                frame[(symbol, "Low")] = 0.99
                frame[(symbol, "Close")] = 1.01
                frame[(symbol, "Adj Close")] = 0.8585
            return frame

    summary = _run(tmp_path, universe, RoundoffDownload())
    report = verify_daily_prices(
        tmp_path / "archive",
        universe,
        TARGET,
        now_utc=AFTER_CLOSE,
    )

    assert summary["ok"] is True
    assert report["ok"] is True, report["errors"]
    assert report["coverage"]["ratio"] == 1.0


def test_capture_retries_a_missing_target_serially_and_verifies(tmp_path: Path) -> None:
    universe = _universe(tmp_path, ["MSFT", "AAPL"])
    fetcher = FakeDailyDownload(omit_first_target="AAPL")

    summary = _run(tmp_path, universe, fetcher)

    assert summary["ok"] is True
    assert len(fetcher.calls) == 2
    assert fetcher.calls[0][0] == sorted({"AAPL", "MSFT", *TREND_PRICE_ANCHORS})
    assert fetcher.calls[1][0] == ["AAPL"]
    output = dataset_path(tmp_path / "archive", DAILY_PRICES_DATASET, TARGET)
    schema = pq.read_schema(output)
    assert schema == pa.schema([pa.field(name, kind) for name, kind in DAILY_PRICE_SCHEMA.items()])
    frame = pq.ParquetFile(output).read().to_pandas()
    assert len(frame) == (len(TREND_PRICE_ANCHORS) + 2) * PRICE_SESSION_COUNT
    assert frame[["symbol", "bar_session"]].values.tolist() == sorted(
        frame[["symbol", "bar_session"]].values.tolist()
    )
    assert frame["unadjusted_price_basis"].eq(UNADJUSTED_PRICE_BASIS).all()
    assert frame["adjusted_price_basis"].eq(ADJUSTED_PRICE_BASIS).all()
    assert (frame["adjusted_close"] == frame["unadjusted_close"] * 0.5).all()
    assert frame["raw_record_sha256"].str.fullmatch(r"[0-9a-f]{64}").all()

    manifest = json.loads(
        daily_price_manifest_path(tmp_path / "archive", TARGET).read_text(encoding="utf-8")
    )
    assert set(manifest) == {
        "schema",
        "status",
        "dataset",
        "run_id",
        "session_date",
        "session_market_close_utc",
        "capture_started_utc",
        "capture_finished_utc",
        "provider",
        "request",
        "universe",
        "sessions",
        "coverage",
        "failures",
        "output",
        "manifest_identity_sha256",
    }
    assert manifest["provider"]["intended_use"] == "historical_gap_recovery"
    assert manifest["provider"]["license_verified"] is False
    assert manifest["provider"]["capabilities"]["has_adjusted_prices"] is True
    assert manifest["provider"]["capabilities"]["data_is_survivor_only"] is True
    assert manifest["provider"]["capabilities"]["promotion_eligible_provider"] is False
    assert manifest["universe"]["requested_symbols"] == sorted(
        {"AAPL", "MSFT", *TREND_PRICE_ANCHORS}
    )
    assert manifest["sessions"]["count"] == PRICE_SESSION_COUNT
    assert len(manifest["output"]["logical_sha256"]) == 64
    checkpoint_metadata = next(
        (tmp_path / "archive" / "_daily_price_checkpoints").rglob("batch_*.json")
    )
    checkpoint_report = json.loads(checkpoint_metadata.read_text(encoding="utf-8"))
    assert checkpoint_report["retry_cooldown_seconds"] >= 2.0
    assert checkpoint_report["individual_retry_symbols"] == 1

    verified = verify_daily_prices(tmp_path / "archive", universe, TARGET, now_utc=AFTER_CLOSE)
    assert verified["ok"] is True, verified["errors"]
    assert verified["coverage"]["anchor_usable_tail_symbols"] == len(TREND_PRICE_ANCHORS)


def test_resume_reuses_hash_valid_atomic_batch_checkpoint(tmp_path: Path) -> None:
    universe = _universe(tmp_path, ["AAPL"])
    initial = FakeDailyDownload()
    first = _run(tmp_path, universe, initial)

    class NoNetwork:
        def __call__(self, *_args: Any, **_kwargs: Any) -> pd.DataFrame:
            raise AssertionError("resume unexpectedly contacted Yahoo")

    resumed = run_daily_prices(
        tmp_path / "archive",
        universe,
        TARGET,
        resume=True,
        batch_size=50,
        fetcher=NoNetwork(),
        clock=lambda: AFTER_CLOSE,
        progress=lambda _payload: None,
        sleep_fn=lambda _seconds: None,
    )

    assert len(initial.calls) == 1
    assert resumed["ok"] is True
    assert resumed["run_id"] == first["run_id"]
    assert resumed["resumed_batches"] == 1
    assert resumed["failed_batches"] == 0


def test_missing_non_anchor_within_threshold_keeps_complete_manifest(tmp_path: Path) -> None:
    broad = [f"TEST{number:02d}" for number in range(30)]
    universe = _universe(tmp_path, broad)
    fetcher = FakeDailyDownload(always_missing={"TEST00"})

    summary = _run(tmp_path, universe, fetcher)
    manifest = json.loads(
        daily_price_manifest_path(tmp_path / "archive", TARGET).read_text(encoding="utf-8")
    )
    verified = verify_daily_prices(
        tmp_path / "archive", universe, TARGET, min_coverage=0.95, now_utc=AFTER_CLOSE
    )

    assert summary["status"] == "complete"
    assert manifest["status"] == "complete"
    assert manifest["coverage"]["ratio"] > 0.95
    assert manifest["coverage"]["failed_symbols"] == ["TEST00"]
    output = dataset_path(tmp_path / "archive", DAILY_PRICES_DATASET, TARGET)
    frame = pq.ParquetFile(output).read().to_pandas()
    assert "TEST00" not in set(frame["symbol"])
    assert manifest["failures"] == [
        {
            "attempts": 2,
            "error_message": "exact target or 30-session tail is incomplete",
            "error_type": "IncompletePriceTail",
            "symbol": "TEST00",
        }
    ]
    assert verified["ok"] is True, verified["errors"]


def test_verify_rejects_adjusted_price_tampering(tmp_path: Path) -> None:
    universe = _universe(tmp_path, ["AAPL"])
    _run(tmp_path, universe, FakeDailyDownload())
    output = dataset_path(tmp_path / "archive", DAILY_PRICES_DATASET, TARGET)
    frame = pq.ParquetFile(output).read().to_pandas()
    frame.loc[0, "adjusted_close"] = float(frame.loc[0, "adjusted_close"]) + 10.0
    write_parquet(output, frame, DAILY_PRICES_DATASET)

    report = verify_daily_prices(tmp_path / "archive", universe, TARGET, now_utc=AFTER_CLOSE)

    assert report["ok"] is False
    assert any("output" in error and "mismatch" in error for error in report["errors"])
    assert any("invalid adjusted OHLC envelope" in error for error in report["errors"])
    assert any("adjustment-factor mismatch" in error for error in report["errors"])


def test_session_axis_is_exact_xnys_and_includes_half_day() -> None:
    sessions = _price_sessions(date(2026, 11, 27))

    assert len(sessions) == PRICE_SESSION_COUNT
    assert sessions[-1] == date(2026, 11, 27)
    assert date(2026, 11, 26) not in sessions
    assert all(session.weekday() < 5 for session in sessions)


def test_verify_rejects_preclose_observation(tmp_path: Path) -> None:
    universe = _universe(tmp_path, ["AAPL"])
    _run(tmp_path, universe, FakeDailyDownload())

    report = verify_daily_prices(
        tmp_path / "archive",
        universe,
        TARGET,
        now_utc=datetime(2026, 8, 27, 19, 59, tzinfo=UTC),
    )

    assert report["ok"] is False
    assert any("has not closed" in error for error in report["errors"])


def test_capture_refuses_before_target_close_without_network(tmp_path: Path) -> None:
    universe = _universe(tmp_path, ["AAPL"])

    class NoNetwork:
        def __call__(self, *_args: Any, **_kwargs: Any) -> pd.DataFrame:
            raise AssertionError("preclose admission unexpectedly contacted Yahoo")

    with pytest.raises(ValueError, match="refused before the target XNYS close"):
        run_daily_prices(
            tmp_path / "archive",
            universe,
            TARGET,
            fetcher=NoNetwork(),
            clock=lambda: datetime(2026, 8, 27, 19, 59, tzinfo=UTC),
            progress=lambda _payload: None,
        )

    assert not (tmp_path / "archive" / "_daily_price_checkpoints").exists()


def test_verify_rejects_missing_trend_anchor(tmp_path: Path) -> None:
    universe = _universe(tmp_path, ["AAPL"])
    _run(tmp_path, universe, FakeDailyDownload(always_missing={"QQQ"}))

    report = verify_daily_prices(tmp_path / "archive", universe, TARGET, now_utc=AFTER_CLOSE)

    assert report["ok"] is False
    assert any("Trend anchors" in error for error in report["errors"])


def test_verify_rejects_raw_record_hash_tampering(tmp_path: Path) -> None:
    universe = _universe(tmp_path, ["AAPL"])
    _run(tmp_path, universe, FakeDailyDownload())
    output = dataset_path(tmp_path / "archive", DAILY_PRICES_DATASET, TARGET)
    frame = pq.ParquetFile(output).read().to_pandas()
    frame.loc[0, "raw_record_sha256"] = "0" * 64
    write_parquet(output, frame, DAILY_PRICES_DATASET)

    report = verify_daily_prices(tmp_path / "archive", universe, TARGET, now_utc=AFTER_CLOSE)

    assert report["ok"] is False
    assert any("raw-record hash mismatch" in error for error in report["errors"])


def test_resume_refetches_a_tampered_checkpoint(tmp_path: Path) -> None:
    universe = _universe(tmp_path, ["AAPL"])
    first_fetcher = FakeDailyDownload()
    _run(tmp_path, universe, first_fetcher)
    checkpoint = next((tmp_path / "archive" / "_daily_price_checkpoints").rglob("batch_*.parquet"))
    checkpoint.write_bytes(checkpoint.read_bytes() + b"tamper")
    retry_fetcher = FakeDailyDownload()

    resumed = _run(tmp_path, universe, retry_fetcher, resume=True)

    assert resumed["ok"] is True
    assert len(retry_fetcher.calls) == 1
    assert resumed["resumed_batches"] == 0


def test_resume_refetches_an_incomplete_batch(tmp_path: Path) -> None:
    universe = _universe(tmp_path, ["AAPL"])
    _run(tmp_path, universe, FakeDailyDownload(always_missing={"AAPL"}))
    retry_fetcher = FakeDailyDownload()

    resumed = _run(tmp_path, universe, retry_fetcher, resume=True)
    verified = verify_daily_prices(tmp_path / "archive", universe, TARGET, now_utc=AFTER_CLOSE)

    assert resumed["ok"] is True
    assert len(retry_fetcher.calls) == 1
    assert resumed["resumed_batches"] == 0
    assert verified["ok"] is True, verified["errors"]


def test_resume_refetches_hash_valid_but_semantically_invalid_checkpoint(
    tmp_path: Path,
) -> None:
    universe = _universe(tmp_path, ["AAPL"])
    _run(tmp_path, universe, FakeDailyDownload())
    checkpoint_root = tmp_path / "archive" / "_daily_price_checkpoints"
    checkpoint = next(checkpoint_root.rglob("batch_*.parquet"))
    metadata_path = checkpoint.with_suffix(".json")
    frame = pq.ParquetFile(checkpoint).read().to_pandas()
    frame.loc[0, "unadjusted_high"] = 1.0
    write_parquet(checkpoint, frame, DAILY_PRICES_DATASET)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["parquet_sha256"] = _sha256_file(checkpoint)
    metadata["schema_sha256"] = _schema_sha256(pq.read_schema(checkpoint))
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    retry_fetcher = FakeDailyDownload()

    resumed = _run(tmp_path, universe, retry_fetcher, resume=True)

    assert resumed["ok"] is True
    assert len(retry_fetcher.calls) == 1
    assert resumed["resumed_batches"] == 0
