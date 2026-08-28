from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
import time
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, timedelta
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
import pandas_market_calendars as mcal
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf

from analyst_snapshot.datasets import (
    DAILY_PRICE_MANIFEST_SCHEMA,
    DAILY_PRICE_SCHEMA,
    DAILY_PRICES_DATASET,
    TREND_PRICE_ANCHORS,
)
from analyst_snapshot.runner import read_universe
from analyst_snapshot.storage import dataset_path, write_parquet
from analyst_snapshot.trading_calendar import session_market_close_utc

PRICE_SESSION_COUNT = 30
DEFAULT_PRICE_BATCH_SIZE = 50
DAILY_PRICE_CHECKPOINT_SCHEMA = "analyst_snapshot_daily_price_checkpoint_v1"
DAILY_PRICE_RUN_STATE_SCHEMA = "analyst_snapshot_daily_price_run_state_v1"

PROVIDER_NAME = "yahoo"
TRANSPORT = "yfinance"
UNADJUSTED_PRICE_BASIS = "raw_unadjusted"
ADJUSTED_PRICE_BASIS = "fully_adjusted"
PROVIDER_CAPABILITIES: dict[str, bool] = {
    "has_delisted_symbols": False,
    "has_historical_constituents": False,
    "has_point_in_time_universe": False,
    "has_adjusted_prices": True,
    "has_unadjusted_prices": True,
    "has_corporate_actions": True,
    "has_delisting_returns": False,
    "supports_full_market_universe": False,
    "supports_symbol_history": True,
    "supports_permanent_ids": False,
    "data_is_survivor_only": True,
    "promotion_eligible_provider": False,
}

_REQUIRED_YAHOO_FIELDS = ("Open", "High", "Low", "Close", "Adj Close", "Volume")
_MAX_REPORTED_ERRORS = 500


class PriceFetcher(Protocol):
    def __call__(self, tickers: list[str], **kwargs: Any) -> pd.DataFrame: ...


def daily_price_manifest_path(snapshot_dir: Path, session_date: str) -> Path:
    return Path(snapshot_dir) / "_daily_price_manifests" / f"date={session_date}" / "manifest.json"


def run_daily_prices(
    snapshot_dir: Path,
    universe_file: Path,
    session_date: str,
    resume: bool = False,
    batch_size: int = DEFAULT_PRICE_BATCH_SIZE,
    *,
    fetcher: PriceFetcher | Any | None = None,
    clock: Callable[[], datetime] | None = None,
    progress: Callable[[dict[str, Any]], None] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Capture authentic Yahoo rows from a 30-XNYS-session recovery window.

    Network work is deliberately serial.  Each batch is atomically checkpointed before the final
    canonical Parquet is assembled, so a rerun with ``resume=True`` never repeats a hash-valid
    completed batch.  Ordinary symbols need an exact target-session row; the fixed Trend anchors
    retain the stricter complete-window contract.  ``fetcher`` is injectable for an entirely
    offline contract test.
    """

    root = Path(snapshot_dir)
    universe_path = Path(universe_file)
    if batch_size != DEFAULT_PRICE_BATCH_SIZE:
        raise ValueError(f"daily-price batch_size must be exactly {DEFAULT_PRICE_BATCH_SIZE}")

    target = _parse_session_date(session_date)
    sessions = _price_sessions(target)
    session_text = [item.isoformat() for item in sessions]
    base_symbols = read_universe(universe_path)
    if not base_symbols:
        raise ValueError(f"universe is empty: {universe_path}")
    requested_symbols = _requested_symbols(base_symbols)
    provider_symbols = [_provider_symbol(symbol) for symbol in requested_symbols]
    if len(set(provider_symbols)) != len(provider_symbols):
        raise ValueError("canonical symbols collide after Yahoo provider-symbol normalization")

    now = clock or _utc_now
    admission_time = _as_utc(now(), "clock")
    target_close = session_market_close_utc(target)
    if admission_time < target_close:
        raise ValueError(
            "daily-price capture refused before the target XNYS close: "
            f"target={target.isoformat()} close={_iso_utc(target_close)} "
            f"now={_iso_utc(admission_time)}"
        )
    provider_version = _package_version("yfinance")
    exact_arrow_schema = pa.schema(
        [pa.field(name, kind) for name, kind in DAILY_PRICE_SCHEMA.items()]
    )
    state_identity = {
        "target_session": target.isoformat(),
        "session_axis": session_text,
        "requested_symbols": requested_symbols,
        "provider_symbols": provider_symbols,
        "batch_size": batch_size,
        "collector_source_sha256": _sha256_file(Path(__file__)),
        "daily_price_schema_sha256": _schema_sha256(exact_arrow_schema),
        "provider_version": provider_version,
        "request": {
            "auto_adjust": False,
            "actions": True,
            "threads": False,
            "repair": False,
            "group_by": "ticker",
            "batch_attempts": 2,
            "batch_backoff_seconds": [2.0],
            "incomplete_retry_cooldown": "min_30_max_2_0.1_seconds_per_symbol",
            "individual_retry_spacing_seconds": 0.5,
        },
    }
    state_identity_sha256 = _json_sha256(state_identity)
    state_path = _run_state_path(root, target.isoformat())
    state = _load_run_state(state_path) if resume else None
    if state is not None:
        try:
            state_started = _parse_utc(state.get("capture_started_utc"), "capture_started_utc")
        except ValueError:
            state = None
        else:
            if state_started < target_close:
                state = None
    if state is not None:
        if state.get("identity_sha256") != state_identity_sha256:
            raise ValueError("daily-price resume state does not match this request")
        run_id = _required_text(state, "run_id")
        capture_started_utc = _required_text(state, "capture_started_utc")
    else:
        started = admission_time
        capture_started_utc = _iso_utc(started)
        run_id = (
            f"daily_price_{target.strftime('%Y%m%d')}_"
            f"{started.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
        )
        state = {
            "schema": DAILY_PRICE_RUN_STATE_SCHEMA,
            "run_id": run_id,
            "capture_started_utc": capture_started_utc,
            "identity_sha256": state_identity_sha256,
            **state_identity,
        }
        _atomic_json(state_path, state)

    canonical_by_provider = dict(zip(provider_symbols, requested_symbols, strict=True))
    total_batches = math.ceil(len(requested_symbols) / batch_size)
    completed_batches = 0
    failed_batches = 0
    resumed_batches = 0
    rows_written = 0
    failures: list[dict[str, Any]] = []
    batch_reports: list[dict[str, Any]] = []
    batch_frames: list[pd.DataFrame] = []
    monotonic_started = time.monotonic()
    estimate_best = 0.2 * len(requested_symbols)
    estimate_likely = 0.8 * len(requested_symbols)
    # Worst case includes one single-symbol retry for every requested symbol, retry spacing and
    # the bounded per-batch cooldown. GitHub's step timeout may stop that pathological case; the
    # workflow remains red and no READY is published.
    estimate_worst = 6.5 * len(requested_symbols) + 30.0 * total_batches
    _emit_progress(
        progress,
        {
            "phase": "runtime_estimate",
            "workers": 1,
            "requested_symbols": len(requested_symbols),
            "batches": total_batches,
            "best_seconds": estimate_best,
            "likely_seconds": estimate_likely,
            "worst_seconds": estimate_worst,
            "basis": "serial_yfinance_chart_requests",
            "confidence": "medium",
        },
    )

    for batch_number, offset in enumerate(range(0, len(requested_symbols), batch_size), start=1):
        canonical_batch = requested_symbols[offset : offset + batch_size]
        provider_batch = provider_symbols[offset : offset + batch_size]
        batch_id = f"batch_{batch_number:04d}"
        parquet_path, metadata_path = _checkpoint_paths(root, target.isoformat(), run_id, batch_id)
        checkpoint = None
        if resume:
            checkpoint = _read_valid_checkpoint(
                parquet_path,
                metadata_path,
                run_id=run_id,
                batch_id=batch_id,
                target_session=target,
                expected_symbols=canonical_batch,
                expected_sessions=sessions,
                expected_provider_version=provider_version,
            )
        if checkpoint is not None:
            frame, batch_report = checkpoint
            batch_frames.append(frame)
            batch_reports.append(batch_report)
            completed_batches += 1
            resumed_batches += 1
            rows_written += len(frame)
            _emit_progress(
                progress,
                _progress_payload(
                    phase="resumed_batch",
                    batch_id=batch_id,
                    completed_batches=completed_batches,
                    total_batches=total_batches,
                    failed_batches=failed_batches,
                    resumed_batches=resumed_batches,
                    rows=rows_written,
                    started=monotonic_started,
                ),
            )
            continue

        _emit_progress(
            progress,
            {
                "phase": "fetching_batch",
                "batch_id": batch_id,
                "workers": 1,
                "completed_batches": completed_batches,
                "total_batches": total_batches,
                "symbols_in_batch": len(provider_batch),
            },
        )
        capture_batch_started = _as_utc(now(), "clock")
        payload: pd.DataFrame | None = None
        last_error: Exception | None = None
        attempts = 0
        for attempt_number in range(1, 3):
            attempts = attempt_number
            try:
                payload = _download_batch(
                    fetcher,
                    provider_batch,
                    first_session=sessions[0],
                    target_session=target,
                )
                last_error = None
                break
            except Exception as exc:  # noqa: BLE001 - one batch must not strand later batches
                last_error = exc
                if attempt_number < 2:
                    backoff_seconds = float(2**attempt_number)
                    _emit_progress(
                        progress,
                        {
                            "phase": "batch_retry_backoff",
                            "batch_id": batch_id,
                            "attempt": attempt_number,
                            "seconds": backoff_seconds,
                            "error_type": type(exc).__name__,
                        },
                    )
                    sleep_fn(backoff_seconds)
        capture_batch_finished = _as_utc(now(), "clock")
        if capture_batch_finished < capture_batch_started:
            raise ValueError("clock moved backwards while capturing a daily-price batch")

        if payload is None or last_error is not None:
            failed_batches += 1
            error = last_error or RuntimeError("download returned no DataFrame")
            failures.extend(
                {
                    "symbol": symbol,
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                    "attempts": attempts,
                }
                for symbol in canonical_batch
            )
            batch_reports.append(
                {
                    "status": "failed",
                    "batch_id": batch_id,
                    "symbols": canonical_batch,
                    "attempts": attempts,
                    "error": str(error),
                }
            )
            _emit_progress(
                progress,
                _progress_payload(
                    phase="failed_batch",
                    batch_id=batch_id,
                    completed_batches=completed_batches,
                    total_batches=total_batches,
                    failed_batches=failed_batches,
                    resumed_batches=resumed_batches,
                    rows=rows_written,
                    started=monotonic_started,
                ),
            )
            continue

        latest_capture_finished = capture_batch_finished
        rows, parse_errors = _rows_from_download(
            payload,
            canonical_batch=canonical_batch,
            provider_batch=provider_batch,
            canonical_by_provider=canonical_by_provider,
            sessions=sessions,
            target_session=target,
            run_id=run_id,
            batch_id=batch_id,
            provider_version=provider_version,
            capture_started_utc=_iso_utc(capture_batch_started),
            capture_finished_utc=_iso_utc(capture_batch_finished),
        )
        expected_axis = set(sessions)
        observed_axes = {
            canonical: {row["bar_session"] for row in rows if row["canonical_symbol"] == canonical}
            for canonical in canonical_batch
        }
        incomplete_symbols = [
            canonical for canonical in canonical_batch if observed_axes[canonical] != expected_axis
        ]
        retry_cooldown_seconds = 0.0
        if incomplete_symbols:
            retry_cooldown_seconds = min(30.0, max(2.0, 0.1 * len(incomplete_symbols)))
            _emit_progress(
                progress,
                {
                    "phase": "incomplete_tail_retry_cooldown",
                    "batch_id": batch_id,
                    "symbols": len(incomplete_symbols),
                    "seconds": retry_cooldown_seconds,
                },
            )
            sleep_fn(retry_cooldown_seconds)
        # A multi-ticker Yahoo response can succeed while silently omitting one ticker or one bar.
        # Retry incomplete windows one symbol at a time to retain as much authentic history as
        # Yahoo exposes.  Missing history is not fabricated and does not disqualify an ordinary
        # symbol when its exact target row exists.
        retried_symbols = 0
        for canonical, provider_symbol in zip(canonical_batch, provider_batch, strict=True):
            if observed_axes[canonical] == expected_axis:
                continue
            if retried_symbols:
                sleep_fn(0.5)
            retried_symbols += 1
            retry_started = _as_utc(now(), "clock")
            retry_error: Exception | None = None
            try:
                retry_payload = _download_batch(
                    fetcher,
                    [provider_symbol],
                    first_session=sessions[0],
                    target_session=target,
                )
                retry_finished = _as_utc(now(), "clock")
                latest_capture_finished = max(latest_capture_finished, retry_finished)
                retry_rows, retry_parse_errors = _rows_from_download(
                    retry_payload,
                    canonical_batch=[canonical],
                    provider_batch=[provider_symbol],
                    canonical_by_provider=canonical_by_provider,
                    sessions=sessions,
                    target_session=target,
                    run_id=run_id,
                    batch_id=batch_id,
                    provider_version=provider_version,
                    capture_started_utc=_iso_utc(retry_started),
                    capture_finished_utc=_iso_utc(retry_finished),
                )
                parse_errors.extend(retry_parse_errors)
            except Exception as exc:  # noqa: BLE001 - continue with the remaining batch symbols
                retry_error = exc
                retry_rows = []
            if retry_rows:
                merged = {
                    row["bar_session"]: row for row in rows if row["canonical_symbol"] == canonical
                }
                merged.update({row["bar_session"]: row for row in retry_rows})
                rows = [row for row in rows if row["canonical_symbol"] != canonical]
                rows.extend(merged[session] for session in sorted(merged))
                observed_axes[canonical] = set(merged)
            if retry_error is not None and not _price_symbol_is_eligible(
                canonical,
                observed_axes[canonical],
                target_session=target,
                expected_axis=expected_axis,
            ):
                failures.append(
                    {
                        "symbol": canonical,
                        "error_type": type(retry_error).__name__,
                        "error_message": str(retry_error),
                        "attempts": 2,
                    }
                )
        eligible_symbols = {
            canonical
            for canonical, axis in observed_axes.items()
            if _price_symbol_is_eligible(
                canonical,
                axis,
                target_session=target,
                expected_axis=expected_axis,
            )
        }
        # Ordinary symbols publish every authentic row in the capture window when the exact target
        # row exists.  Trend anchors remain complete 30-session matrices.  Rows for ineligible
        # symbols are omitted so the manifest failure inventory remains unambiguous.
        rows = [row for row in rows if row["canonical_symbol"] in eligible_symbols]
        rows.sort(key=lambda row: (str(row["canonical_symbol"]), row["bar_session"]))
        frame = pd.DataFrame(rows, columns=list(DAILY_PRICE_SCHEMA))
        write_parquet(parquet_path, frame, DAILY_PRICES_DATASET)
        succeeded = sorted(eligible_symbols)
        missing = [symbol for symbol in canonical_batch if symbol not in eligible_symbols]
        batch_report = {
            "schema": DAILY_PRICE_CHECKPOINT_SCHEMA,
            "status": "complete",
            "run_id": run_id,
            "batch_id": batch_id,
            "target_session": target.isoformat(),
            "expected_symbols": canonical_batch,
            "expected_symbols_sha256": _json_sha256(canonical_batch),
            "succeeded_symbols": succeeded,
            "missing_symbols": missing,
            "attempts": attempts,
            "rows": len(frame),
            "capture_started_utc": _iso_utc(capture_batch_started),
            "capture_finished_utc": _iso_utc(latest_capture_finished),
            "available_at_utc": _iso_utc(latest_capture_finished),
            "parse_errors": parse_errors,
            "retry_cooldown_seconds": retry_cooldown_seconds,
            "individual_retry_symbols": retried_symbols,
            "parquet_sha256": _sha256_file(parquet_path),
            "schema_sha256": _schema_sha256(pq.read_schema(parquet_path)),
        }
        _atomic_json(metadata_path, batch_report)
        batch_frames.append(frame)
        batch_reports.append(batch_report)
        completed_batches += 1
        rows_written += len(frame)
        _emit_progress(
            progress,
            _progress_payload(
                phase="completed_batch",
                batch_id=batch_id,
                completed_batches=completed_batches,
                total_batches=total_batches,
                failed_batches=failed_batches,
                resumed_batches=resumed_batches,
                rows=rows_written,
                started=monotonic_started,
            ),
        )

    if batch_frames:
        combined = pd.concat(batch_frames, ignore_index=True)
        combined = combined.sort_values(
            ["canonical_symbol", "bar_session"], kind="mergesort"
        ).reset_index(drop=True)
    else:
        combined = pd.DataFrame(columns=list(DAILY_PRICE_SCHEMA))
    output_path = dataset_path(root, DAILY_PRICES_DATASET, target.isoformat())
    write_parquet(output_path, combined, DAILY_PRICES_DATASET)
    finished = _as_utc(now(), "clock")
    output_schema = pq.read_schema(output_path)
    output_frame = pq.ParquetFile(output_path).read().to_pandas()

    coverage = _coverage_report(
        output_frame,
        requested_symbols=requested_symbols,
        base_symbols=base_symbols,
        sessions=sessions,
        invalid_symbols=set(),
    )
    already_failed = {str(item["symbol"]) for item in failures}
    for symbol in coverage["failed_symbols"]:
        if symbol in already_failed:
            continue
        if symbol in TREND_PRICE_ANCHORS:
            error_type = "IncompleteTrendAnchorTail"
            error_message = "Trend anchor requires an exact target and complete 30-session tail"
        else:
            error_type = "MissingTargetPriceRow"
            error_message = "exact target-session price row is missing"
        failures.append(
            {
                "symbol": symbol,
                "error_type": error_type,
                "error_message": error_message,
                "attempts": 2,
            }
        )
    failures.sort(
        key=lambda item: (
            str(item["symbol"]),
            str(item["error_type"]),
            str(item["error_message"]),
        )
    )
    complete = completed_batches == total_batches and failed_batches == 0
    session_values = [item.isoformat() for item in sessions]
    manifest: dict[str, Any] = {
        "schema": DAILY_PRICE_MANIFEST_SCHEMA,
        "status": "complete" if complete else "incomplete",
        "dataset": DAILY_PRICES_DATASET,
        "run_id": run_id,
        "session_date": target.isoformat(),
        "session_market_close_utc": _iso_utc(session_market_close_utc(target)),
        "capture_started_utc": capture_started_utc,
        "capture_finished_utc": _iso_utc(finished),
        "provider": {
            "provider_name": PROVIDER_NAME,
            "transport": TRANSPORT,
            "provider_version": provider_version,
            "price_role": "conditional_recovery_input",
            "intended_use": "historical_gap_recovery",
            "price_bases": [UNADJUSTED_PRICE_BASIS, ADJUSTED_PRICE_BASIS],
            "lookback_sessions": PRICE_SESSION_COUNT,
            "response_identity_kind": "canonical_provider_records",
            "raw_provider_bytes_claimed": False,
            "license_summary": "Unofficial Yahoo Finance via yfinance; rights not verified",
            "license_verified": False,
            "license_allows_local_cache": "unknown",
            "license_allows_model_training": "unknown",
            "license_allows_redistribution": "unknown",
            "capabilities": dict(PROVIDER_CAPABILITIES),
        },
        "request": {
            "interval": "1d",
            "auto_adjust": False,
            "actions": True,
            "threads": False,
            "repair": False,
            "batch_size": batch_size,
            "lookback_sessions": PRICE_SESSION_COUNT,
            "end_exclusive": (target + timedelta(days=1)).isoformat(),
            "group_by": "ticker",
        },
        "universe": {
            "universe_file_sha256": _sha256_file(universe_path),
            "universe_symbols": base_symbols,
            "trend_anchor_symbols": list(TREND_PRICE_ANCHORS),
            "requested_symbols": requested_symbols,
            "requested_symbols_sha256": _json_sha256(requested_symbols),
        },
        "sessions": {
            "count": len(sessions),
            "first": sessions[0].isoformat(),
            "last": sessions[-1].isoformat(),
            "values": session_values,
            "sha256": _json_sha256(session_values),
        },
        "coverage": coverage,
        "failures": failures,
        "output": {
            "path": output_path.relative_to(root).as_posix(),
            "rows": len(output_frame),
            "bytes": output_path.stat().st_size,
            "sha256": _sha256_file(output_path),
            "schema_sha256": _schema_sha256(output_schema),
            "logical_sha256": _frame_identity_sha256(output_frame),
        },
    }
    manifest["manifest_identity_sha256"] = _manifest_identity_sha256(manifest)
    manifest_path = daily_price_manifest_path(root, target.isoformat())
    _atomic_json(manifest_path, manifest)
    actual_elapsed = time.monotonic() - monotonic_started
    _emit_progress(
        progress,
        {
            "phase": "run_complete",
            "workers": 1,
            "requested_symbols": len(requested_symbols),
            "batches": total_batches,
            "actual_elapsed_seconds": actual_elapsed,
            "estimate_match": estimate_best <= actual_elapsed <= estimate_worst,
            "status": manifest["status"],
            "rows": len(output_frame),
        },
    )
    return {
        "ok": manifest["status"] == "complete",
        "status": manifest["status"],
        "run_id": run_id,
        "target_session": target.isoformat(),
        "rows": len(output_frame),
        "completed_batches": completed_batches,
        "total_batches": total_batches,
        "failed_batches": failed_batches,
        "resumed_batches": resumed_batches,
        "failures": failures,
        "coverage": coverage,
        "provider": manifest["provider"],
        "manifest": {"path": str(manifest_path), "schema": DAILY_PRICE_MANIFEST_SCHEMA},
        "output": manifest["output"],
    }


def verify_daily_prices(
    snapshot_dir: Path,
    universe_file: Path,
    session_date: str,
    min_coverage: float = 0.95,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Strictly re-prove a sealed daily-price tail without contacting Yahoo."""

    errors: list[str] = []
    root = Path(snapshot_dir)
    output_path = dataset_path(root, DAILY_PRICES_DATASET, session_date)
    manifest_path = daily_price_manifest_path(root, session_date)
    report: dict[str, Any] = {
        "ok": False,
        "errors": errors,
        "coverage": {},
        "provider": {},
        "manifest": {"path": str(manifest_path)},
        "output": {"path": str(output_path)},
    }
    if not 0.0 <= min_coverage <= 1.0:
        errors.append("min_coverage must be between 0 and 1")
        return report
    try:
        target = _parse_session_date(session_date)
        sessions = _price_sessions(target)
    except ValueError as exc:
        errors.append(str(exc))
        return report

    observed = _as_utc(now_utc or _utc_now(), "now_utc")
    close = session_market_close_utc(target)
    if observed < close:
        errors.append("target XNYS session has not closed")

    base_symbols = read_universe(Path(universe_file))
    requested_symbols = _requested_symbols(base_symbols)
    expected_provider_by_symbol = {symbol: _provider_symbol(symbol) for symbol in requested_symbols}
    if not manifest_path.is_file():
        errors.append(f"daily-price manifest is missing: {manifest_path}")
        return report
    try:
        manifest = _read_json(manifest_path)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        errors.append(f"daily-price manifest is unreadable: {exc}")
        return report

    report["manifest"].update(
        {
            "schema": manifest.get("schema"),
            "sha256": _sha256_file(manifest_path),
            "identity_sha256": manifest.get("manifest_identity_sha256"),
        }
    )
    report["provider"] = manifest.get("provider", {})
    expected_manifest_keys = {
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
    if set(manifest) != expected_manifest_keys:
        errors.append("daily-price manifest top-level keys drifted")
    if manifest.get("schema") != DAILY_PRICE_MANIFEST_SCHEMA:
        errors.append("daily-price manifest schema mismatch")
    if manifest.get("status") != "complete":
        errors.append("daily-price manifest is not complete")
    if manifest.get("dataset") != DAILY_PRICES_DATASET:
        errors.append("daily-price manifest dataset mismatch")
    if manifest.get("session_date") != target.isoformat():
        errors.append("daily-price manifest session_date mismatch")
    if manifest.get("session_market_close_utc") != _iso_utc(close):
        errors.append("daily-price manifest XNYS close mismatch")
    if manifest.get("manifest_identity_sha256") != _manifest_identity_sha256(manifest):
        errors.append("daily-price manifest identity hash mismatch")

    provider = manifest.get("provider")
    if not isinstance(provider, dict):
        errors.append("daily-price provider declaration is missing")
        provider = {}
    expected_provider: dict[str, Any] = {
        "provider_name": PROVIDER_NAME,
        "transport": TRANSPORT,
        "price_role": "conditional_recovery_input",
        "intended_use": "historical_gap_recovery",
        "price_bases": [UNADJUSTED_PRICE_BASIS, ADJUSTED_PRICE_BASIS],
        "lookback_sessions": PRICE_SESSION_COUNT,
        "response_identity_kind": "canonical_provider_records",
        "raw_provider_bytes_claimed": False,
        "license_summary": "Unofficial Yahoo Finance via yfinance; rights not verified",
        "license_verified": False,
        "license_allows_local_cache": "unknown",
        "license_allows_model_training": "unknown",
        "license_allows_redistribution": "unknown",
        "capabilities": PROVIDER_CAPABILITIES,
    }
    if set(provider) != {*expected_provider, "provider_version"}:
        errors.append("daily-price provider keys drifted")
    for field, expected in expected_provider.items():
        if provider.get(field) != expected:
            errors.append(f"daily-price provider declaration mismatch: {field}")
    provider_version = provider.get("provider_version")
    if not isinstance(provider_version, str) or not provider_version.strip():
        errors.append("daily-price provider_version is missing")

    request = manifest.get("request")
    expected_request: dict[str, Any] = {
        "interval": "1d",
        "auto_adjust": False,
        "actions": True,
        "threads": False,
        "repair": False,
        "lookback_sessions": PRICE_SESSION_COUNT,
        "end_exclusive": (target + timedelta(days=1)).isoformat(),
        "group_by": "ticker",
    }
    if not isinstance(request, dict):
        errors.append("daily-price request declaration is missing")
        request = {}
    if set(request) != {*expected_request, "batch_size"}:
        errors.append("daily-price request keys drifted")
    for field, expected in expected_request.items():
        if request.get(field) != expected:
            errors.append(f"daily-price request declaration mismatch: {field}")
    if request.get("batch_size") != DEFAULT_PRICE_BATCH_SIZE:
        errors.append("daily-price request batch_size is invalid")

    universe = manifest.get("universe")
    expected_universe_keys = {
        "universe_file_sha256",
        "universe_symbols",
        "trend_anchor_symbols",
        "requested_symbols",
        "requested_symbols_sha256",
    }
    if not isinstance(universe, dict):
        errors.append("daily-price universe declaration is missing")
        universe = {}
    if set(universe) != expected_universe_keys:
        errors.append("daily-price universe keys drifted")
    if universe.get("universe_file_sha256") != _sha256_file(Path(universe_file)):
        errors.append("daily-price universe file hash mismatch")
    if universe.get("universe_symbols") != base_symbols:
        errors.append("daily-price universe inventory mismatch")
    if universe.get("trend_anchor_symbols") != list(TREND_PRICE_ANCHORS):
        errors.append("daily-price Trend-anchor inventory mismatch")
    if universe.get("requested_symbols") != requested_symbols:
        errors.append("daily-price requested-symbol inventory mismatch")
    if universe.get("requested_symbols_sha256") != _json_sha256(requested_symbols):
        errors.append("daily-price requested-symbol hash mismatch")

    session_values = [item.isoformat() for item in sessions]
    expected_sessions = {
        "count": len(sessions),
        "first": sessions[0].isoformat(),
        "last": sessions[-1].isoformat(),
        "values": session_values,
        "sha256": _json_sha256(session_values),
    }
    session_evidence = manifest.get("sessions")
    if not isinstance(session_evidence, dict) or session_evidence != expected_sessions:
        errors.append("daily-price exact XNYS session evidence mismatch")

    failure_evidence = manifest.get("failures")
    failure_keys = {"symbol", "error_type", "error_message", "attempts"}
    if not isinstance(failure_evidence, list) or any(
        not isinstance(item, dict) or set(item) != failure_keys for item in failure_evidence or []
    ):
        errors.append("daily-price failure evidence is malformed")
        failure_evidence = []
    if failure_evidence != sorted(
        failure_evidence,
        key=lambda item: (
            str(item["symbol"]),
            str(item["error_type"]),
            str(item["error_message"]),
        ),
    ):
        errors.append("daily-price failures are not canonically sorted")

    _verify_manifest_times(manifest, close=close, observed=observed, errors=errors)
    try:
        manifest_started = _parse_utc(manifest.get("capture_started_utc"), "capture_started_utc")
        manifest_finished = _parse_utc(manifest.get("capture_finished_utc"), "capture_finished_utc")
    except ValueError:
        manifest_started = None
        manifest_finished = None
    if not output_path.is_file():
        errors.append(f"daily-price Parquet is missing: {output_path}")
        return report

    try:
        schema = pq.read_schema(output_path)
        frame = pq.ParquetFile(output_path).read().to_pandas()
    except (OSError, pa.ArrowException) as exc:
        errors.append(f"daily-price Parquet is unreadable: {exc}")
        return report
    expected_schema = pa.schema([pa.field(name, kind) for name, kind in DAILY_PRICE_SCHEMA.items()])
    if schema != expected_schema:
        errors.append("daily-price Parquet does not have the exact 31-column schema")

    actual_output = {
        "path": output_path.relative_to(root).as_posix(),
        "rows": len(frame),
        "bytes": output_path.stat().st_size,
        "sha256": _sha256_file(output_path),
        "schema_sha256": _schema_sha256(schema),
        "logical_sha256": _frame_identity_sha256(frame),
    }
    report["output"].update(actual_output)
    declared_output = manifest.get("output")
    if not isinstance(declared_output, dict):
        errors.append("daily-price manifest output declaration is missing")
    else:
        if set(declared_output) != set(actual_output):
            errors.append("daily-price output keys drifted")
        for field, actual in actual_output.items():
            if declared_output.get(field) != actual:
                errors.append(f"daily-price output {field} mismatch")

    required_columns = list(DAILY_PRICE_SCHEMA)
    if list(frame.columns) != required_columns:
        errors.append("daily-price frame column order mismatch")
        missing = [column for column in required_columns if column not in frame]
        if missing:
            errors.append(f"daily-price frame missing columns: {', '.join(missing)}")
        return report

    invalid_symbols: set[str] = set()
    session_set = set(sessions)
    keys: list[tuple[str, date]] = []
    seen_keys: set[tuple[str, date]] = set()
    run_ids = set(frame["run_id"].dropna().astype(str))
    manifest_run_id = manifest.get("run_id")
    if run_ids != {manifest_run_id}:
        errors.append("daily-price run_id is not uniformly bound to the manifest")
    row_provider_versions = set(frame["provider_version"].dropna().astype(str))
    if row_provider_versions != {provider_version}:
        errors.append("daily-price row provider_version mismatch")

    for row in frame.itertuples(index=False):
        row_data = row._asdict()
        canonical = str(row_data["canonical_symbol"])
        symbol = str(row_data["symbol"])
        bar_session = _coerce_date(row_data["bar_session"])
        target_session = _coerce_date(row_data["target_session"])
        if canonical not in expected_provider_by_symbol:
            _append_error(errors, f"unexpected daily-price symbol: {canonical}")
            invalid_symbols.add(canonical)
        if symbol != canonical:
            _append_error(errors, f"symbol/canonical_symbol mismatch for {canonical}")
            invalid_symbols.add(canonical)
        if row_data["provider_symbol"] != expected_provider_by_symbol.get(canonical):
            _append_error(errors, f"provider_symbol mismatch for {canonical}")
            invalid_symbols.add(canonical)
        if target_session != target:
            _append_error(errors, f"target_session mismatch for {canonical}")
            invalid_symbols.add(canonical)
        if bar_session not in session_set:
            _append_error(errors, f"bar_session outside exact 30-session axis for {canonical}")
            invalid_symbols.add(canonical)
        key = (canonical, bar_session)
        keys.append(key)
        if key in seen_keys:
            _append_error(errors, f"duplicate symbol/session row: {canonical} {bar_session}")
            invalid_symbols.add(canonical)
        seen_keys.add(key)
        _verify_row_contract(
            row_data,
            target=target,
            close=close,
            observed=observed,
            manifest_started=manifest_started,
            manifest_finished=manifest_finished,
            canonical=canonical,
            errors=errors,
            invalid_symbols=invalid_symbols,
        )

    if keys != sorted(keys):
        errors.append("daily-price rows are not in canonical symbol/session order")

    coverage = _coverage_report(
        frame,
        requested_symbols=requested_symbols,
        base_symbols=base_symbols,
        sessions=sessions,
        invalid_symbols=invalid_symbols,
    )
    report["coverage"] = coverage
    if coverage["ratio"] < min_coverage:
        errors.append(
            f"daily-price target-row coverage {coverage['ratio']:.6f} is below {min_coverage:.6f}"
        )
    if coverage["anchor_exact_target_symbols"] != coverage["anchor_expected_symbols"]:
        errors.append("Trend anchors do not all have an exact target row")
    if coverage["anchor_usable_tail_symbols"] != coverage["anchor_expected_symbols"]:
        errors.append("Trend anchors do not have complete valid 30-session tails")
    if {str(item["symbol"]) for item in failure_evidence} != set(coverage["failed_symbols"]):
        errors.append("daily-price failures do not bind the failed-symbol inventory")
    if manifest.get("coverage") != coverage:
        errors.append("daily-price manifest coverage declaration mismatch")

    report["ok"] = not errors
    report["error_count"] = len(errors)
    return report


def _download_batch(
    fetcher: PriceFetcher | Any | None,
    provider_symbols: list[str],
    *,
    first_session: date,
    target_session: date,
) -> pd.DataFrame:
    downloader = yf.download if fetcher is None else getattr(fetcher, "download", fetcher)
    if not callable(downloader):
        raise TypeError("fetcher must be callable or expose download()")
    payload = downloader(
        provider_symbols,
        start=first_session.isoformat(),
        end=(target_session + timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        actions=True,
        threads=False,
        progress=False,
        repair=False,
        group_by="ticker",
    )
    if not isinstance(payload, pd.DataFrame):
        raise TypeError("Yahoo daily-price download did not return a pandas DataFrame")
    return payload


def _rows_from_download(
    payload: pd.DataFrame,
    *,
    canonical_batch: list[str],
    provider_batch: list[str],
    canonical_by_provider: Mapping[str, str],
    sessions: list[date],
    target_session: date,
    run_id: str,
    batch_id: str,
    provider_version: str,
    capture_started_utc: str,
    capture_finished_utc: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    allowed = set(sessions)
    for canonical, provider_symbol in zip(canonical_batch, provider_batch, strict=True):
        symbol_frame = _symbol_frame(payload, provider_symbol, len(provider_batch))
        if symbol_frame.empty:
            continue
        columns = {_normal_column(column): column for column in symbol_frame.columns}
        missing_fields = [field for field in _REQUIRED_YAHOO_FIELDS if field not in columns]
        if missing_fields:
            errors.append(f"{canonical}: missing Yahoo fields {', '.join(missing_fields)}")
            continue
        for index, values in symbol_frame.iterrows():
            bar_session = _index_date(index)
            if bar_session not in allowed:
                continue
            raw_values = {
                field: _finite_float(values.get(columns[field])) for field in _REQUIRED_YAHOO_FIELDS
            }
            if any(value is None for value in raw_values.values()):
                continue
            raw_open = float(raw_values["Open"])
            raw_high = float(raw_values["High"])
            raw_low = float(raw_values["Low"])
            raw_close = float(raw_values["Close"])
            adjusted_close = float(raw_values["Adj Close"])
            volume = float(raw_values["Volume"])
            if raw_close == 0.0:
                errors.append(f"{canonical} {bar_session}: zero unadjusted close")
                continue
            factor = adjusted_close / raw_close
            adjusted_open = raw_open * factor
            adjusted_high = raw_high * factor
            adjusted_low = raw_low * factor
            # Yahoo's Adj Close is retained exactly. When raw High/Low equals Close,
            # division followed by multiplication can land one float64 ULP inside the envelope
            # (for example 1.01 * (0.8585 / 1.01) < 0.8585). Clamp only the derived extremes to
            # the exact derived/open-close envelope; the raw envelope is still independently
            # validated and the factor-parity check remains tight.
            adjusted_high = max(adjusted_high, adjusted_open, adjusted_close)
            adjusted_low = min(adjusted_low, adjusted_open, adjusted_close)
            dividend = _optional_float(values.get(columns.get("Dividends")))
            split = _optional_float(values.get(columns.get("Stock Splits")))
            if split == 0.0:
                split = None
            raw_identity = {
                "provider": PROVIDER_NAME,
                "symbol": canonical,
                "date": bar_session.isoformat(),
                "open": raw_open,
                "high": raw_high,
                "low": raw_low,
                "close": raw_close,
                "volume": volume,
                "price_basis": UNADJUSTED_PRICE_BASIS,
            }
            row = {
                "dataset": DAILY_PRICES_DATASET,
                "run_id": run_id,
                "target_session": target_session,
                "bar_session": bar_session,
                "symbol": canonical_by_provider[provider_symbol],
                "canonical_symbol": canonical,
                "provider_symbol": provider_symbol,
                "provider_name": PROVIDER_NAME,
                "transport": TRANSPORT,
                "provider_version": provider_version,
                "currency": "USD",
                "unadjusted_price_basis": UNADJUSTED_PRICE_BASIS,
                "adjusted_price_basis": ADJUSTED_PRICE_BASIS,
                "unadjusted_open": raw_open,
                "unadjusted_high": raw_high,
                "unadjusted_low": raw_low,
                "unadjusted_close": raw_close,
                "adjusted_open": adjusted_open,
                "adjusted_high": adjusted_high,
                "adjusted_low": adjusted_low,
                "adjusted_close": adjusted_close,
                "volume": volume,
                "adjustment_factor": factor,
                "dividend_cash": dividend,
                "stock_split_ratio": split,
                "capture_started_utc": capture_started_utc,
                "capture_finished_utc": capture_finished_utc,
                "available_at_utc": capture_finished_utc,
                "is_target_session": bar_session == target_session,
                "batch_id": batch_id,
                "raw_record_sha256": _json_sha256(raw_identity),
            }
            rows.append(row)
    rows.sort(key=lambda row: (str(row["canonical_symbol"]), row["bar_session"]))
    return rows, errors


def _symbol_frame(payload: pd.DataFrame, provider_symbol: str, batch_count: int) -> pd.DataFrame:
    if payload.empty:
        return pd.DataFrame()
    if not isinstance(payload.columns, pd.MultiIndex):
        return payload.copy() if batch_count == 1 else pd.DataFrame()
    for level in range(payload.columns.nlevels):
        values = payload.columns.get_level_values(level)
        match = next(
            (
                value
                for value in values.unique()
                if str(value).strip().upper() == provider_symbol.upper()
            ),
            None,
        )
        if match is not None:
            frame = payload.xs(match, axis=1, level=level, drop_level=True)
            while isinstance(frame.columns, pd.MultiIndex) and frame.columns.nlevels > 1:
                frame.columns = frame.columns.droplevel(-1)
            return frame
    return pd.DataFrame()


def _normal_column(value: Any) -> str:
    text = str(value).strip()
    aliases = {
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "adj close": "Adj Close",
        "adjclose": "Adj Close",
        "volume": "Volume",
        "dividends": "Dividends",
        "stock splits": "Stock Splits",
        "stocksplits": "Stock Splits",
    }
    return aliases.get(text.casefold(), text)


def _verify_row_contract(
    row: Mapping[str, Any],
    *,
    target: date,
    close: datetime,
    observed: datetime,
    manifest_started: datetime | None,
    manifest_finished: datetime | None,
    canonical: str,
    errors: list[str],
    invalid_symbols: set[str],
) -> None:
    exact_values = {
        "dataset": DAILY_PRICES_DATASET,
        "provider_name": PROVIDER_NAME,
        "transport": TRANSPORT,
        "currency": "USD",
        "unadjusted_price_basis": UNADJUSTED_PRICE_BASIS,
        "adjusted_price_basis": ADJUSTED_PRICE_BASIS,
    }
    for field, expected in exact_values.items():
        if row.get(field) != expected:
            _append_error(errors, f"{canonical}: {field} contract mismatch")
            invalid_symbols.add(canonical)

    bar_session = _coerce_date(row.get("bar_session"))
    if bool(row.get("is_target_session")) != (bar_session == target):
        _append_error(errors, f"{canonical} {bar_session}: is_target_session mismatch")
        invalid_symbols.add(canonical)

    try:
        started = _parse_utc(row.get("capture_started_utc"), "capture_started_utc")
        finished = _parse_utc(row.get("capture_finished_utc"), "capture_finished_utc")
        available = _parse_utc(row.get("available_at_utc"), "available_at_utc")
    except ValueError as exc:
        _append_error(errors, f"{canonical} {bar_session}: {exc}")
        invalid_symbols.add(canonical)
    else:
        invalid_timeline = (
            started < close
            or finished < started
            or available != finished
            or available > observed
            or (manifest_started is not None and started < manifest_started)
            or (manifest_finished is not None and finished > manifest_finished)
        )
        if invalid_timeline:
            _append_error(errors, f"{canonical} {bar_session}: invalid PIT capture timeline")
            invalid_symbols.add(canonical)

    numeric_fields = (
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
    )
    numeric: dict[str, float] = {}
    for field in numeric_fields:
        value = _finite_float(row.get(field))
        if value is None:
            _append_error(errors, f"{canonical} {bar_session}: non-finite {field}")
            invalid_symbols.add(canonical)
        else:
            numeric[field] = value
    if len(numeric) == len(numeric_fields):
        price_fields = [
            field for field in numeric if "adjust" in field and field != "adjustment_factor"
        ]
        if any(numeric[field] <= 0.0 for field in price_fields):
            _append_error(errors, f"{canonical} {bar_session}: non-positive OHLC")
            invalid_symbols.add(canonical)
        if numeric["volume"] < 0.0 or numeric["adjustment_factor"] <= 0.0:
            _append_error(errors, f"{canonical} {bar_session}: invalid volume/factor")
            invalid_symbols.add(canonical)
        if not _valid_ohlc(
            numeric["unadjusted_open"],
            numeric["unadjusted_high"],
            numeric["unadjusted_low"],
            numeric["unadjusted_close"],
        ):
            _append_error(errors, f"{canonical} {bar_session}: invalid unadjusted OHLC envelope")
            invalid_symbols.add(canonical)
        if not _valid_ohlc(
            numeric["adjusted_open"],
            numeric["adjusted_high"],
            numeric["adjusted_low"],
            numeric["adjusted_close"],
        ):
            _append_error(errors, f"{canonical} {bar_session}: invalid adjusted OHLC envelope")
            invalid_symbols.add(canonical)
        factor = numeric["adjustment_factor"]
        pairs = (
            ("unadjusted_open", "adjusted_open"),
            ("unadjusted_high", "adjusted_high"),
            ("unadjusted_low", "adjusted_low"),
            ("unadjusted_close", "adjusted_close"),
        )
        if any(
            not math.isclose(numeric[raw] * factor, numeric[adjusted], rel_tol=1e-10, abs_tol=1e-10)
            for raw, adjusted in pairs
        ):
            _append_error(errors, f"{canonical} {bar_session}: adjustment-factor mismatch")
            invalid_symbols.add(canonical)

    dividend = _optional_float(row.get("dividend_cash"))
    split = _optional_float(row.get("stock_split_ratio"))
    if dividend is not None and dividend < 0.0:
        _append_error(errors, f"{canonical} {bar_session}: negative dividend")
        invalid_symbols.add(canonical)
    if split is not None and split <= 0.0:
        _append_error(errors, f"{canonical} {bar_session}: invalid stock split ratio")
        invalid_symbols.add(canonical)

    if len(numeric) == len(numeric_fields):
        raw_identity = {
            "provider": PROVIDER_NAME,
            "symbol": canonical,
            "date": bar_session.isoformat(),
            "open": numeric["unadjusted_open"],
            "high": numeric["unadjusted_high"],
            "low": numeric["unadjusted_low"],
            "close": numeric["unadjusted_close"],
            "volume": numeric["volume"],
            "price_basis": UNADJUSTED_PRICE_BASIS,
        }
        if row.get("raw_record_sha256") != _json_sha256(raw_identity):
            _append_error(errors, f"{canonical} {bar_session}: raw-record hash mismatch")
            invalid_symbols.add(canonical)


def _coverage_report(
    frame: pd.DataFrame,
    *,
    requested_symbols: list[str],
    base_symbols: list[str],
    sessions: list[date],
    invalid_symbols: set[str],
) -> dict[str, Any]:
    target = sessions[-1]
    session_set = set(sessions)
    by_symbol: dict[str, set[date]] = {}
    if not frame.empty and "canonical_symbol" in frame and "bar_session" in frame:
        for canonical, bar_session in zip(
            frame["canonical_symbol"], frame["bar_session"], strict=True
        ):
            if pd.isna(canonical) or pd.isna(bar_session):
                continue
            by_symbol.setdefault(str(canonical), set()).add(_coerce_date(bar_session))
    expected = set(requested_symbols)
    observed = set(by_symbol)
    target_present = {
        symbol
        for symbol in requested_symbols
        if target in by_symbol.get(symbol, set()) and symbol not in invalid_symbols
    }
    usable_tail = {
        symbol
        for symbol in requested_symbols
        if by_symbol.get(symbol, set()) == session_set and symbol not in invalid_symbols
    }
    anchors = set(TREND_PRICE_ANCHORS)
    eligible = (target_present - anchors) | (usable_tail & anchors)
    universe = set(base_symbols)
    requested_count = len(requested_symbols)
    observed_universe = observed & universe
    eligible_universe = eligible & universe
    duplicate_pairs = 0
    target_rows = 0
    history_rows = 0
    if not frame.empty:
        duplicate_pairs = int(frame.duplicated(["symbol", "bar_session"]).sum())
        target_rows = sum(
            _coerce_date(value) == target for value in frame["bar_session"] if not pd.isna(value)
        )
        history_rows = len(frame) - target_rows
    failed = sorted(expected - eligible)
    session_axis_sha256 = _json_sha256([item.isoformat() for item in sessions])
    return {
        "expected_symbols": len(universe),
        "observed_symbols": len(observed_universe),
        "matched_symbols": len(eligible_universe),
        "ratio": len(eligible_universe) / len(universe) if universe else 0.0,
        "universe_expected_symbols": len(universe),
        "requested_symbols": requested_count,
        "exact_target_symbols": len(target_present),
        "usable_tail_symbols": len(usable_tail),
        "anchor_expected_symbols": len(anchors),
        "anchor_exact_target_symbols": len(target_present & anchors),
        "anchor_usable_tail_symbols": len(usable_tail & anchors),
        "anchor_ratio": len(target_present & anchors) / len(anchors) if anchors else 0.0,
        "target_rows": target_rows,
        "history_rows": history_rows,
        "missing_target_symbols": sorted(expected - target_present),
        "failed_symbols": failed,
        "duplicate_pairs": duplicate_pairs,
        "target_session": target.isoformat(),
        "lookback_first_session": sessions[0].isoformat(),
        "lookback_sessions": len(sessions),
        "session_axis_sha256": session_axis_sha256,
    }


def _price_symbol_is_eligible(
    symbol: str,
    observed_axis: set[date],
    *,
    target_session: date,
    expected_axis: set[date],
) -> bool:
    if symbol in TREND_PRICE_ANCHORS:
        return observed_axis == expected_axis
    return target_session in observed_axis


def _verify_manifest_times(
    manifest: Mapping[str, Any],
    *,
    close: datetime,
    observed: datetime,
    errors: list[str],
) -> None:
    try:
        started = _parse_utc(manifest.get("capture_started_utc"), "capture_started_utc")
        finished = _parse_utc(manifest.get("capture_finished_utc"), "capture_finished_utc")
    except ValueError as exc:
        errors.append(f"daily-price manifest {exc}")
        return
    if started < close:
        errors.append("daily-price capture began before the target XNYS close")
    if finished < started:
        errors.append("daily-price manifest capture timeline is not monotonic")
    if finished > observed:
        errors.append("daily-price manifest claims future PIT availability")


def _read_valid_checkpoint(
    parquet_path: Path,
    metadata_path: Path,
    *,
    run_id: str,
    batch_id: str,
    target_session: date,
    expected_symbols: list[str],
    expected_sessions: list[date],
    expected_provider_version: str,
) -> tuple[pd.DataFrame, dict[str, Any]] | None:
    if not parquet_path.is_file() or not metadata_path.is_file():
        return None
    try:
        metadata = _read_json(metadata_path)
        schema = pq.read_schema(parquet_path)
        frame = pq.ParquetFile(parquet_path).read().to_pandas()
    except (OSError, json.JSONDecodeError, ValueError, pa.ArrowException):
        return None
    expected_schema = pa.schema([pa.field(name, kind) for name, kind in DAILY_PRICE_SCHEMA.items()])
    checks = (
        metadata.get("schema") == DAILY_PRICE_CHECKPOINT_SCHEMA,
        metadata.get("status") == "complete",
        metadata.get("run_id") == run_id,
        metadata.get("batch_id") == batch_id,
        metadata.get("target_session") == target_session.isoformat(),
        metadata.get("expected_symbols") == expected_symbols,
        metadata.get("expected_symbols_sha256") == _json_sha256(expected_symbols),
        metadata.get("rows") == len(frame),
        metadata.get("parquet_sha256") == _sha256_file(parquet_path),
        metadata.get("schema_sha256") == _schema_sha256(schema),
        schema == expected_schema,
        list(frame.columns) == list(DAILY_PRICE_SCHEMA),
    )
    if not all(checks):
        return None
    if not frame.empty:
        if set(frame["run_id"].dropna().astype(str)) != {run_id}:
            return None
        if set(frame["batch_id"].dropna().astype(str)) != {batch_id}:
            return None
        if set(frame["canonical_symbol"].dropna().astype(str)) - set(expected_symbols):
            return None
    expected_axis = set(expected_sessions)
    observed_axes: dict[str, set[date]] = {}
    for symbol in expected_symbols:
        observed_axes[symbol] = {
            _coerce_date(value)
            for value in frame.loc[frame["canonical_symbol"] == symbol, "bar_session"]
            if not pd.isna(value)
        }
    eligible_symbols = {
        symbol
        for symbol, observed_axis in observed_axes.items()
        if _price_symbol_is_eligible(
            symbol,
            observed_axis,
            target_session=target_session,
            expected_axis=expected_axis,
        )
    }
    # Preserve the existing recovery behavior: a checkpoint with any genuinely missing target
    # input is deliberately refetched on resume.  A short-history ordinary symbol is not missing
    # and therefore does not invalidate an otherwise complete checkpoint.
    if eligible_symbols != set(expected_symbols):
        return None
    succeeded = metadata.get("succeeded_symbols")
    missing = metadata.get("missing_symbols")
    if succeeded != sorted(eligible_symbols):
        return None
    if missing != [symbol for symbol in expected_symbols if symbol not in eligible_symbols]:
        return None
    observed_symbols = set(frame["canonical_symbol"].dropna().astype(str))
    if observed_symbols != eligible_symbols:
        return None
    try:
        checkpoint_started = _parse_utc(
            metadata.get("capture_started_utc"), "checkpoint capture_started_utc"
        )
        checkpoint_finished = _parse_utc(
            metadata.get("capture_finished_utc"), "checkpoint capture_finished_utc"
        )
    except ValueError:
        return None
    semantic_errors: list[str] = []
    invalid_symbols: set[str] = set()
    keys: list[tuple[str, date]] = []
    expected_symbol_set = set(expected_symbols)
    checkpoint_close = session_market_close_utc(target_session)
    for row in frame.itertuples(index=False):
        record = row._asdict()
        canonical = str(record.get("canonical_symbol"))
        try:
            bar_session = _coerce_date(record.get("bar_session"))
            row_target = _coerce_date(record.get("target_session"))
        except (TypeError, ValueError):
            return None
        if (
            canonical not in expected_symbol_set
            or record.get("symbol") != canonical
            or record.get("provider_symbol") != _provider_symbol(canonical)
            or record.get("provider_version") != expected_provider_version
            or record.get("run_id") != run_id
            or record.get("batch_id") != batch_id
            or row_target != target_session
            or bar_session not in expected_axis
        ):
            return None
        keys.append((canonical, bar_session))
        _verify_row_contract(
            record,
            target=target_session,
            close=checkpoint_close,
            observed=checkpoint_finished,
            manifest_started=checkpoint_started,
            manifest_finished=checkpoint_finished,
            canonical=canonical,
            errors=semantic_errors,
            invalid_symbols=invalid_symbols,
        )
    if semantic_errors or invalid_symbols or keys != sorted(keys) or len(keys) != len(set(keys)):
        return None
    return frame, metadata


def _progress_payload(
    *,
    phase: str,
    batch_id: str,
    completed_batches: int,
    total_batches: int,
    failed_batches: int,
    resumed_batches: int,
    rows: int,
    started: float,
) -> dict[str, Any]:
    elapsed = max(time.monotonic() - started, 1e-9)
    processed = completed_batches + failed_batches
    rate = processed / elapsed
    remaining = max(total_batches - processed, 0)
    return {
        "phase": phase,
        "batch_id": batch_id,
        "workers": 1,
        "completed_batches": completed_batches,
        "total_batches": total_batches,
        "failed_batches": failed_batches,
        "resumed_batches": resumed_batches,
        "rows": rows,
        "batches_per_second": rate,
        "eta_seconds": remaining / rate if rate > 0 else None,
    }


def _emit_progress(sink: Callable[[dict[str, Any]], None] | None, payload: dict[str, Any]) -> None:
    if sink is not None:
        sink(payload)
    else:
        print(json.dumps(payload, allow_nan=False, sort_keys=True), flush=True)


def _price_sessions(target: date) -> list[date]:
    calendar = mcal.get_calendar("NYSE")
    schedule = calendar.schedule(
        start_date=(target - timedelta(days=90)).isoformat(), end_date=target.isoformat()
    )
    sessions = [timestamp.date() for timestamp in schedule.index]
    if target not in sessions:
        raise ValueError(f"{target.isoformat()} is not an XNYS trading session")
    sessions = sessions[-PRICE_SESSION_COUNT:]
    if len(sessions) != PRICE_SESSION_COUNT:
        raise ValueError(f"fewer than {PRICE_SESSION_COUNT} XNYS sessions precede target")
    return sessions


def _requested_symbols(base_symbols: list[str]) -> list[str]:
    return sorted(
        {symbol.strip().upper() for symbol in base_symbols if symbol.strip()}
        | set(TREND_PRICE_ANCHORS)
    )


def _provider_symbol(canonical_symbol: str) -> str:
    return canonical_symbol.replace(".", "-")


def _checkpoint_paths(
    snapshot_dir: Path, session_date: str, run_id: str, batch_id: str
) -> tuple[Path, Path]:
    root = snapshot_dir / "_daily_price_checkpoints" / f"date={session_date}" / f"run={run_id}"
    return root / f"{batch_id}.parquet", root / f"{batch_id}.json"


def _run_state_path(snapshot_dir: Path, session_date: str) -> Path:
    return snapshot_dir / "_daily_price_checkpoints" / f"date={session_date}" / "state.json"


def _load_run_state(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        state = _read_json(path)
    except (OSError, json.JSONDecodeError, ValueError):
        return None
    if state.get("schema") != DAILY_PRICE_RUN_STATE_SCHEMA:
        return None
    return state


def _frame_identity_sha256(frame: pd.DataFrame) -> str:
    rows = []
    for row in frame.reindex(columns=list(DAILY_PRICE_SCHEMA)).to_dict(orient="records"):
        rows.append({key: _json_value(value) for key, value in row.items()})
    rows.sort(key=lambda row: (str(row["symbol"]), str(row["bar_session"])))
    return _json_sha256(rows)


def _manifest_identity_sha256(manifest: Mapping[str, Any]) -> str:
    payload = {key: value for key, value in manifest.items() if key != "manifest_identity_sha256"}
    return _json_sha256(payload)


def _schema_sha256(schema: pa.Schema) -> str:
    canonical = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in schema
    ]
    return _json_sha256(canonical)


def _json_sha256(value: Any) -> str:
    payload = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    payload = (json.dumps(value, allow_nan=False, indent=2, sort_keys=True) + "\n").encode()
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


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON object expected: {path}")
    return value


def _required_text(value: Mapping[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item.strip():
        raise ValueError(f"daily-price run state is missing {key}")
    return item


def _parse_session_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid daily-price session date: {value!r}") from exc


def _index_date(value: Any) -> date:
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError("Yahoo returned a missing daily-price index")
    return timestamp.date()


def _coerce_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _finite_float(value: Any) -> float | None:
    converted = _optional_float(value)
    return converted if converted is not None and math.isfinite(converted) else None


def _valid_ohlc(open_: float, high: float, low: float, close: float) -> bool:
    return high >= max(open_, low, close) and low <= min(open_, high, close)


def _parse_utc(value: Any, role: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{role} is missing")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{role} is not ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{role} is timezone-naive")
    return parsed.astimezone(UTC)


def _as_utc(value: datetime, role: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{role} must return a timezone-aware datetime")
    return value.astimezone(UTC)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _package_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:  # pragma: no cover - yfinance is a runtime dependency
        return "unknown"


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        value = value.item()
    return value


def _append_error(errors: list[str], message: str) -> None:
    if len(errors) < _MAX_REPORTED_ERRORS:
        errors.append(message)
