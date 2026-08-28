from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from analyst_snapshot import __version__
from analyst_snapshot.datasets import CORE_SCHEMAS, DATASETS, MARKET_CONTEXT_DATASETS
from analyst_snapshot.market_context import manifest_path as market_context_manifest_path
from analyst_snapshot.market_context import verify_market_context
from analyst_snapshot.runner import MANIFEST_DIR_NAME, read_universe
from analyst_snapshot.storage import dataset_path
from analyst_snapshot.trading_calendar import session_market_close_utc

RECOVERY_BUNDLE_SCHEMA = "swinglab_recovery_bundle_v1"
RECOVERY_MANIFEST_DIR_NAME = "_recovery_manifests"
RATING_EVENTS_DATASET = "rating_events"


class RecoveryBundleError(RuntimeError):
    """The candidate date cannot be sealed as a trustworthy recovery bundle."""


def recovery_manifest_path(snapshot_dir: Path, session_date: str) -> Path:
    return snapshot_dir / RECOVERY_MANIFEST_DIR_NAME / f"date={session_date}" / "manifest.json"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def finalize_recovery_bundle(
    snapshot_dir: Path,
    universe_file: Path,
    *,
    session_date: str,
    min_coverage: float = 0.95,
    generation_id: str | None = None,
    now_utc: datetime | None = None,
    producer_identity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Strictly validate and atomically seal one completed-session recovery generation.

    The output manifest inventories the exact bytes that may be published to Dropbox. It is not a
    run log: every count, schema and hash is recomputed from the final files on disk.
    """

    try:
        parsed_session = date.fromisoformat(session_date)
        market_close = session_market_close_utc(parsed_session)
    except (TypeError, ValueError) as exc:
        raise RecoveryBundleError(f"invalid XNYS session date {session_date!r}: {exc}") from exc

    observed_now = _as_utc(now_utc or datetime.now(UTC))
    if observed_now < market_close:
        raise RecoveryBundleError(
            f"session {session_date} has not closed: close={_iso_utc(market_close)} "
            f"now={_iso_utc(observed_now)}"
        )
    if not 0.0 < min_coverage <= 1.0:
        raise RecoveryBundleError("min_coverage must be in (0, 1]")

    expected_symbols = set(read_universe(universe_file))
    if not expected_symbols:
        raise RecoveryBundleError(f"universe is empty or unreadable: {universe_file}")

    files: list[dict[str, Any]] = []
    coverage: dict[str, Any] = {}
    for spec in DATASETS.values():
        path = dataset_path(snapshot_dir, spec.name, session_date)
        evidence, symbols = _validate_parquet(
            snapshot_dir,
            path,
            dataset=spec.name,
            pit_column="snapshot_utc",
            market_close=market_close,
            sealed_at=observed_now,
        )
        ratio = len(symbols & expected_symbols) / len(expected_symbols)
        coverage[spec.name] = {
            "expected_symbols": len(expected_symbols),
            "observed_symbols": len(symbols),
            "matched_symbols": len(symbols & expected_symbols),
            "ratio": round(ratio, 6),
        }
        if ratio < min_coverage:
            raise RecoveryBundleError(
                f"{spec.name} coverage {ratio:.6f} is below {min_coverage:.6f}"
            )
        files.append(evidence)

    rating_path = dataset_path(snapshot_dir, RATING_EVENTS_DATASET, session_date)
    if rating_path.is_file():
        evidence, _symbols = _validate_parquet(
            snapshot_dir,
            rating_path,
            dataset=RATING_EVENTS_DATASET,
            pit_column="first_seen_utc",
            market_close=market_close,
            sealed_at=observed_now,
        )
        files.append(evidence)

    market_report = verify_market_context(snapshot_dir, run_date=session_date)
    if not market_report.get("ok"):
        raise RecoveryBundleError(
            "market-context verification failed: "
            + "; ".join(str(error) for error in market_report.get("errors", []))
        )
    for dataset in MARKET_CONTEXT_DATASETS:
        evidence, _symbols = _validate_parquet(
            snapshot_dir,
            dataset_path(snapshot_dir, dataset, session_date),
            dataset=dataset,
            pit_column="snapshot_utc",
            market_close=market_close,
            sealed_at=observed_now,
        )
        files.append(evidence)

    analyst_manifests = sorted(
        (snapshot_dir / MANIFEST_DIR_NAME / f"date={session_date}").glob("*.json")
    )
    if not analyst_manifests:
        raise RecoveryBundleError(f"analyst run manifest missing for {session_date}")
    run_ids: list[str] = []
    for path in analyst_manifests:
        payload = _read_json(path, role="analyst run manifest")
        if payload.get("run_date") != session_date:
            raise RecoveryBundleError(f"analyst manifest date mismatch: {path}")
        run_id = payload.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise RecoveryBundleError(f"analyst manifest run_id missing: {path}")
        run_ids.append(run_id)
        files.append(_file_evidence(snapshot_dir, path, kind="analyst_run_manifest"))

    context_manifest = market_context_manifest_path(snapshot_dir, session_date)
    context_payload = _read_json(context_manifest, role="market-context manifest")
    if context_payload.get("run_date") != session_date:
        raise RecoveryBundleError("market-context manifest date mismatch")
    context_snapshot = _parse_utc(
        context_payload.get("snapshot_utc"), "market-context snapshot_utc"
    )
    if context_snapshot < market_close:
        raise RecoveryBundleError("market-context snapshot was captured before the session close")
    if context_snapshot > observed_now:
        raise RecoveryBundleError("market-context snapshot is later than the seal time")
    files.append(_file_evidence(snapshot_dir, context_manifest, kind="market_context_manifest"))

    source_dir = snapshot_dir / "_market_context_sources" / f"date={session_date}"
    source_files = sorted(path for path in source_dir.rglob("*") if path.is_file())
    if not source_files:
        raise RecoveryBundleError(f"market-context raw sources missing for {session_date}")
    files.extend(
        _file_evidence(snapshot_dir, path, kind="market_context_source") for path in source_files
    )

    relative_paths = [str(item["path"]) for item in files]
    if len(relative_paths) != len(set(relative_paths)):
        raise RecoveryBundleError("recovery inventory contains duplicate paths")
    files.sort(key=lambda item: str(item["path"]))

    sealed_at = _iso_utc(observed_now)
    resolved_generation = generation_id or (
        f"recovery_{session_date}_{observed_now.strftime('%Y%m%dT%H%M%SZ')}"
    )
    if not _safe_identifier(resolved_generation):
        raise RecoveryBundleError(
            "generation_id may contain only letters, digits, '.', '_' and '-'"
        )

    manifest: dict[str, Any] = {
        "schema": RECOVERY_BUNDLE_SCHEMA,
        "status": "complete",
        "session_date": session_date,
        "generation_id": resolved_generation,
        "session_market_close_utc": _iso_utc(market_close),
        "sealed_at_utc": sealed_at,
        "analyst_snapshot_version": __version__,
        "providers": {
            "analyst": "yahoo_via_yfinance",
            "market_context": ["cftc", "finra", "occ"],
        },
        "universe": {
            "symbols": len(expected_symbols),
            "sha256": sha256_file(universe_file),
        },
        "coverage_threshold": min_coverage,
        "coverage": coverage,
        "market_context": market_report,
        "analyst_run_ids": sorted(run_ids),
        "producer": dict(producer_identity or {}),
        "files": files,
    }
    manifest["manifest_identity_sha256"] = manifest_identity_sha256(manifest)
    _atomic_json(recovery_manifest_path(snapshot_dir, session_date), manifest)
    return manifest


def _validate_parquet(
    snapshot_dir: Path,
    path: Path,
    *,
    dataset: str,
    pit_column: str,
    market_close: datetime,
    sealed_at: datetime,
) -> tuple[dict[str, Any], set[str]]:
    if not path.is_file():
        raise RecoveryBundleError(f"required parquet missing: {path}")
    try:
        parquet = pq.ParquetFile(path)
        schema = parquet.schema_arrow
    except (OSError, pa.ArrowException) as exc:
        raise RecoveryBundleError(f"unreadable parquet {path}: {exc}") from exc
    rows = int(parquet.metadata.num_rows)
    if rows <= 0:
        raise RecoveryBundleError(f"required parquet is empty: {path}")

    expected_schema = CORE_SCHEMAS.get(dataset, {})
    for name, expected_type in expected_schema.items():
        field_index = schema.get_field_index(name)
        if field_index < 0:
            raise RecoveryBundleError(f"{dataset} missing required column {name}")
        observed_type = schema.field(field_index).type
        if observed_type != expected_type:
            raise RecoveryBundleError(
                f"{dataset}.{name} type mismatch: expected {expected_type}, got {observed_type}"
            )
    for required in ("symbol", "dataset", pit_column):
        if schema.get_field_index(required) < 0:
            raise RecoveryBundleError(f"{dataset} missing recovery column {required}")

    try:
        table = pq.read_table(path, columns=["symbol", "dataset", pit_column])
        observed_datasets = set(table.column("dataset").drop_null().to_pylist())
        symbols = set(table.column("symbol").drop_null().to_pylist())
        minimum = pc.min(table.column(pit_column)).as_py()
        maximum = pc.max(table.column(pit_column)).as_py()
    except (OSError, KeyError, pa.ArrowException) as exc:
        raise RecoveryBundleError(f"failed to inspect {dataset}: {exc}") from exc
    if observed_datasets != {dataset}:
        raise RecoveryBundleError(
            f"{dataset} identity mismatch: observed={sorted(map(str, observed_datasets))}"
        )
    minimum_utc = _parse_utc(minimum, f"{dataset}.{pit_column} min")
    maximum_utc = _parse_utc(maximum, f"{dataset}.{pit_column} max")
    if minimum_utc < market_close:
        raise RecoveryBundleError(
            f"{dataset} contains pre-close data: min={_iso_utc(minimum_utc)} "
            f"close={_iso_utc(market_close)}"
        )
    if maximum_utc > sealed_at:
        raise RecoveryBundleError(
            f"{dataset} contains future PIT data: max={_iso_utc(maximum_utc)} "
            f"seal={_iso_utc(sealed_at)}"
        )

    evidence = _file_evidence(snapshot_dir, path, kind="parquet")
    evidence.update(
        {
            "dataset": dataset,
            "rows": rows,
            "schema_sha256": _schema_sha256(schema),
            "pit_column": pit_column,
            "pit_min_utc": _iso_utc(minimum_utc),
            "pit_max_utc": _iso_utc(maximum_utc),
        }
    )
    return evidence, {str(symbol) for symbol in symbols}


def _file_evidence(snapshot_dir: Path, path: Path, *, kind: str) -> dict[str, Any]:
    try:
        relative = path.relative_to(snapshot_dir).as_posix()
    except ValueError as exc:
        raise RecoveryBundleError(f"file escapes snapshot root: {path}") from exc
    return {
        "path": relative,
        "kind": kind,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _schema_sha256(schema: pa.Schema) -> str:
    canonical = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in schema
    ]
    payload = json.dumps(canonical, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(payload).hexdigest()


def manifest_identity_sha256(manifest: dict[str, Any]) -> str:
    """Hash the semantic manifest while excluding its self-hash field."""

    identity = {key: value for key, value in manifest.items() if key != "manifest_identity_sha256"}
    payload = json.dumps(identity, allow_nan=False, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    return hashlib.sha256(payload).hexdigest()


def _read_json(path: Path, *, role: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RecoveryBundleError(f"{role} unreadable: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RecoveryBundleError(f"{role} must be a JSON object: {path}")
    return payload


def _parse_utc(value: Any, role: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise RecoveryBundleError(f"{role} is missing")
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise RecoveryBundleError(f"{role} is not ISO-8601: {value!r}") from exc
    if parsed.tzinfo is None:
        raise RecoveryBundleError(f"{role} is timezone-naive: {value!r}")
    return parsed.astimezone(UTC)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise RecoveryBundleError("now_utc must be timezone-aware")
    return value.astimezone(UTC)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_identifier(value: str) -> bool:
    return re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", value) is not None


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    payload = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
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
