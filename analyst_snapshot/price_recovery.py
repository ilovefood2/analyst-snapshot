from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from analyst_snapshot import __version__
from analyst_snapshot.daily_prices import daily_price_manifest_path, verify_daily_prices
from analyst_snapshot.datasets import DAILY_PRICES_DATASET
from analyst_snapshot.dropbox_sync import (
    DropboxSecrets,
    _dropbox_content_hash_bytes,
    _identity_sha256,
    _remote_join,
    _upload_bytes,
    _upload_immutable_file,
    _verify_dropbox_commit,
    refresh_access_token,
)
from analyst_snapshot.storage import dataset_path
from analyst_snapshot.trading_calendar import session_market_close_utc

PRICE_RECOVERY_BUNDLE_SCHEMA = "swinglab_price_recovery_bundle_v1"
PRICE_RECOVERY_READY_SCHEMA = "swinglab_price_recovery_ready_v1"
PRICE_RECOVERY_MANIFEST_DIR_NAME = "_price_recovery_manifests"
PRICE_GENERATIONS_DIR_NAME = "price_generations"
PRICE_READY_FILE_NAME = "_PRICE_READY.json"

_BUNDLE_KEYS = {
    "schema",
    "status",
    "session_date",
    "generation_id",
    "session_market_close_utc",
    "sealed_at_utc",
    "producer_version",
    "provider",
    "coverage",
    "producer",
    "files",
    "manifest_identity_sha256",
}
_PARQUET_ENTRY_KEYS = {
    "path",
    "kind",
    "bytes",
    "sha256",
    "dataset",
    "rows",
    "schema_sha256",
    "pit_column",
    "pit_min_utc",
    "pit_max_utc",
}
_ROLE_ENTRY_KEYS = {"path", "kind", "bytes", "sha256"}
_PRODUCER_KEYS = {
    "repository",
    "git_ref",
    "git_sha",
    "workflow_run_id",
    "workflow_run_attempt",
}
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_GIT_SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
_GENERATION_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_REPOSITORY_PATTERN = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")


class PriceRecoveryBundleError(RuntimeError):
    """A Daily-price pair cannot be sealed as an independent recovery generation."""


def price_recovery_manifest_path(snapshot_dir: Path, session_date: str) -> Path:
    return (
        Path(snapshot_dir)
        / PRICE_RECOVERY_MANIFEST_DIR_NAME
        / f"date={session_date}"
        / "manifest.json"
    )


def manifest_identity_sha256(manifest: dict[str, Any]) -> str:
    identity = {key: value for key, value in manifest.items() if key != "manifest_identity_sha256"}
    try:
        payload = json.dumps(
            identity,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise PriceRecoveryBundleError(
            f"price-recovery identity is not canonical JSON: {exc}"
        ) from exc
    return hashlib.sha256(payload).hexdigest()


def finalize_price_recovery_bundle(
    snapshot_dir: Path,
    universe_file: Path,
    *,
    session_date: str,
    min_coverage: float = 0.95,
    generation_id: str | None = None,
    now_utc: datetime | None = None,
    producer_identity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Seal only the strict Daily-price Parquet and its source manifest.

    This is deliberately separate from the full analyst/market recovery family. It can publish
    immediately after price verification without granting analyst or market-context readiness.
    """

    parsed_session, market_close = _session_contract(session_date)
    observed_now = _as_utc(now_utc or datetime.now(UTC), "now_utc")
    if observed_now < market_close:
        raise PriceRecoveryBundleError(
            f"session {session_date} has not closed: close={_iso_utc(market_close)} "
            f"now={_iso_utc(observed_now)}"
        )
    if min_coverage != 0.95:
        raise PriceRecoveryBundleError("price-recovery v1 min_coverage must be exactly 0.95")

    price_report = verify_daily_prices(
        Path(snapshot_dir),
        Path(universe_file),
        session_date=parsed_session.isoformat(),
        min_coverage=min_coverage,
        now_utc=observed_now,
    )
    if not price_report.get("ok"):
        raise PriceRecoveryBundleError(
            "daily-price verification failed: "
            + "; ".join(str(error) for error in price_report.get("errors", []))
        )

    price_path = dataset_path(Path(snapshot_dir), DAILY_PRICES_DATASET, session_date)
    price_evidence = _price_parquet_evidence(
        Path(snapshot_dir),
        price_path,
        market_close=market_close,
        sealed_at=observed_now,
    )
    verified_output = price_report.get("output")
    if not isinstance(verified_output, dict) or any(
        verified_output.get(field) != price_evidence.get(field)
        for field in ("path", "rows", "bytes", "sha256", "schema_sha256")
    ):
        raise PriceRecoveryBundleError("daily-price Parquet changed after strict verification")

    source_manifest_path = daily_price_manifest_path(Path(snapshot_dir), session_date)
    source_manifest = _read_json(source_manifest_path, "daily-price manifest")
    if source_manifest.get("session_date") != session_date:
        raise PriceRecoveryBundleError("daily-price manifest session_date mismatch")
    if source_manifest.get("status") != "complete":
        raise PriceRecoveryBundleError("daily-price manifest is not complete")
    source_manifest_evidence = _role_evidence(
        Path(snapshot_dir),
        source_manifest_path,
        kind="daily_price_manifest",
    )
    verified_manifest = price_report.get("manifest")
    if (
        not isinstance(verified_manifest, dict)
        or verified_manifest.get("sha256") != source_manifest_evidence["sha256"]
        or verified_manifest.get("identity_sha256")
        != source_manifest.get("manifest_identity_sha256")
    ):
        raise PriceRecoveryBundleError("daily-price manifest changed after strict verification")

    files = sorted(
        [price_evidence, source_manifest_evidence],
        key=lambda item: str(item["path"]),
    )
    if len(files) != 2 or len({str(item["path"]) for item in files}) != 2:
        raise PriceRecoveryBundleError("price-recovery inventory must contain exactly two files")

    resolved_generation = generation_id or (
        f"price_{session_date}_{observed_now.strftime('%Y%m%dT%H%M%SZ')}"
    )
    if not _GENERATION_ID_PATTERN.fullmatch(resolved_generation):
        raise PriceRecoveryBundleError(
            "generation_id may contain only letters, digits, '.', '_' and '-'"
        )
    producer = dict(producer_identity or {})
    try:
        _validate_producer(producer)
    except ValueError as exc:
        raise PriceRecoveryBundleError(str(exc)) from exc

    manifest: dict[str, Any] = {
        "schema": PRICE_RECOVERY_BUNDLE_SCHEMA,
        "status": "complete",
        "session_date": session_date,
        "generation_id": resolved_generation,
        "session_market_close_utc": _iso_utc(market_close),
        "sealed_at_utc": _iso_utc(observed_now),
        "producer_version": __version__,
        "provider": dict(price_report.get("provider") or {}),
        "coverage": dict(price_report.get("coverage") or {}),
        "producer": producer,
        "files": files,
    }
    manifest["manifest_identity_sha256"] = manifest_identity_sha256(manifest)
    if set(manifest) != _BUNDLE_KEYS:
        raise PriceRecoveryBundleError("price-recovery manifest top-level keys drifted")
    _atomic_json(price_recovery_manifest_path(Path(snapshot_dir), session_date), manifest)
    return manifest


def publish_price_recovery_bundle(
    local_archive: Path,
    remote_root: str,
    secrets: DropboxSecrets,
    *,
    run_date: str,
    universe_file: Path | None = None,
) -> int:
    """Publish an immutable price generation and write ``_PRICE_READY.json`` last."""

    _validate_canonical_date(run_date)
    archive_root = Path(local_archive).resolve()
    manifest_path = price_recovery_manifest_path(Path(local_archive), run_date).resolve()
    if not manifest_path.is_relative_to(archive_root):
        raise ValueError(f"Price-recovery manifest escapes local archive: {manifest_path}")
    publication_now = datetime.now(UTC)
    manifest, manifest_bytes = _load_manifest(manifest_path)
    generation_id, files = _validate_price_recovery_manifest(
        manifest,
        local_archive=Path(local_archive),
        run_date=run_date,
        publication_now=publication_now,
    )

    price_report = verify_daily_prices(
        Path(local_archive),
        universe_file or Path(os.getenv("UNIVERSE_FILE", "./universe.txt")),
        session_date=run_date,
        min_coverage=0.95,
        now_utc=publication_now,
    )
    if not price_report.get("ok"):
        raise ValueError(
            "Price-recovery daily_prices semantic verification failed: "
            + "; ".join(str(error) for error in price_report.get("errors", []))
        )
    _bind_verified_price_evidence(manifest, price_report, local_archive=Path(local_archive))

    # Complete local validation precedes both token acquisition and remote side effects.
    for file_path, expected_bytes, expected_sha256, relative_path in files:
        _verify_file(file_path, expected_bytes, expected_sha256, relative_path)

    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    manifest_identity = str(manifest["manifest_identity_sha256"])
    access_token = refresh_access_token(secrets)
    generation_root = _remote_join(
        remote_root,
        f"date={run_date}",
        PRICE_GENERATIONS_DIR_NAME,
        generation_id,
    )

    uploaded = 0
    for file_path, expected_bytes, expected_sha256, relative_path in files:
        expected_content_hash = _verify_file(
            file_path,
            expected_bytes,
            expected_sha256,
            relative_path,
        )
        remote_path = _remote_join(generation_root, relative_path)
        metadata = _upload_immutable_file(file_path, remote_path, access_token)
        _verify_dropbox_commit(
            metadata,
            remote_path=remote_path,
            expected_bytes=expected_bytes,
            expected_content_hash=expected_content_hash,
        )
        _verify_file(file_path, expected_bytes, expected_sha256, relative_path)
        uploaded += 1
        print(f"uploaded {file_path} -> dropbox:{remote_path}")

    if manifest_path.read_bytes() != manifest_bytes:
        raise ValueError(f"Price-recovery manifest changed during publication: {manifest_path}")
    remote_manifest_path = _remote_join(generation_root, "manifest.json")
    manifest_metadata = _upload_immutable_file(
        manifest_path,
        remote_manifest_path,
        access_token,
    )
    _verify_dropbox_commit(
        manifest_metadata,
        remote_path=remote_manifest_path,
        expected_bytes=len(manifest_bytes),
        expected_content_hash=_dropbox_content_hash_bytes(manifest_bytes),
    )
    if manifest_path.read_bytes() != manifest_bytes:
        raise ValueError(f"Price-recovery manifest changed during publication: {manifest_path}")
    uploaded += 1
    print(f"uploaded {manifest_path} -> dropbox:{remote_manifest_path}")

    ready_payload: dict[str, Any] = {
        "schema": PRICE_RECOVERY_READY_SCHEMA,
        "status": "ready",
        "session_date": run_date,
        "generation_id": generation_id,
        "manifest_path": (f"{PRICE_GENERATIONS_DIR_NAME}/{generation_id}/manifest.json"),
        "manifest_sha256": manifest_sha256,
        "manifest_identity_sha256": manifest_identity,
        "files_count": 2,
        "published_at_utc": _iso_utc(publication_now),
    }
    ready_payload["ready_identity_sha256"] = _identity_sha256(
        ready_payload,
        self_hash_field="ready_identity_sha256",
    )
    ready_bytes = (
        json.dumps(ready_payload, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode("utf-8")
    remote_ready_path = _remote_join(remote_root, f"date={run_date}", PRICE_READY_FILE_NAME)
    ready_metadata = _upload_bytes(ready_bytes, remote_ready_path, access_token)
    _verify_dropbox_commit(
        ready_metadata,
        remote_path=remote_ready_path,
        expected_bytes=len(ready_bytes),
        expected_content_hash=_dropbox_content_hash_bytes(ready_bytes),
    )
    uploaded += 1
    print(f"uploaded price recovery READY -> dropbox:{remote_ready_path}")
    return uploaded


def _validate_price_recovery_manifest(
    manifest: dict[str, Any],
    *,
    local_archive: Path,
    run_date: str,
    publication_now: datetime | None = None,
) -> tuple[str, list[tuple[Path, int, str, str]]]:
    if set(manifest) != _BUNDLE_KEYS:
        raise ValueError("Price-recovery manifest top-level keys drifted")
    if manifest.get("schema") != PRICE_RECOVERY_BUNDLE_SCHEMA:
        raise ValueError(f"Price-recovery manifest schema must be {PRICE_RECOVERY_BUNDLE_SCHEMA}")
    if manifest.get("status") != "complete":
        raise ValueError("Price-recovery manifest status must be complete")
    if manifest.get("session_date") != run_date:
        raise ValueError("Price-recovery manifest session_date does not match requested date")
    _parsed_session, expected_close = _session_contract_value_error(run_date)
    close = _parse_utc(manifest.get("session_market_close_utc"), "session_market_close_utc")
    if close != expected_close:
        raise ValueError("Price-recovery manifest XNYS close mismatch")
    sealed = _parse_utc(manifest.get("sealed_at_utc"), "sealed_at_utc")
    if sealed < close:
        raise ValueError("Price-recovery manifest was sealed before the session close")
    if sealed > _as_utc_value_error(publication_now or datetime.now(UTC), "publication_now"):
        raise ValueError("Price-recovery manifest sealed_at_utc is in the future")
    if manifest.get("producer_version") != __version__:
        raise ValueError("Price-recovery producer_version does not match this publisher")

    provider = manifest.get("provider")
    if (
        not isinstance(provider, dict)
        or provider.get("provider_name") != "yahoo"
        or provider.get("transport") != "yfinance"
        or provider.get("price_role") != "conditional_recovery_input"
        or provider.get("intended_use") != "historical_gap_recovery"
    ):
        raise ValueError("Price-recovery provider contract is invalid")
    coverage = manifest.get("coverage")
    if not isinstance(coverage, dict):
        raise ValueError("Price-recovery coverage must be an object")
    ratio = coverage.get("ratio")
    if not isinstance(ratio, (int, float)) or isinstance(ratio, bool) or ratio < 0.95:
        raise ValueError("Price-recovery usable-tail coverage is below 0.95")
    if coverage.get("anchor_exact_target_symbols") != coverage.get(
        "anchor_expected_symbols"
    ) or coverage.get("anchor_usable_tail_symbols") != coverage.get("anchor_expected_symbols"):
        raise ValueError("Price-recovery Trend-anchor coverage is incomplete")
    _validate_producer(manifest.get("producer"))

    identity = manifest.get("manifest_identity_sha256")
    if not isinstance(identity, str) or not _SHA256_PATTERN.fullmatch(identity):
        raise ValueError("Price-recovery manifest identity is invalid")
    try:
        actual_identity = manifest_identity_sha256(manifest)
    except PriceRecoveryBundleError as exc:
        raise ValueError(str(exc)) from exc
    if identity != actual_identity:
        raise ValueError("Price-recovery manifest identity mismatch")

    generation_id = manifest.get("generation_id")
    if not isinstance(generation_id, str) or not _GENERATION_ID_PATTERN.fullmatch(generation_id):
        raise ValueError("Price-recovery generation_id is missing or path-unsafe")

    inventory = manifest.get("files")
    if not isinstance(inventory, list) or len(inventory) != 2:
        raise ValueError("Price-recovery manifest files must contain exactly two entries")
    inventory_paths = [item.get("path") if isinstance(item, dict) else None for item in inventory]
    if inventory_paths != sorted(inventory_paths, key=lambda value: str(value)):
        raise ValueError("Price-recovery file inventory is not path-sorted")

    expected_date_part = f"date={run_date}"
    expected_price_path = f"{DAILY_PRICES_DATASET}/{expected_date_part}/data.parquet"
    expected_source_manifest_path = f"_daily_price_manifests/{expected_date_part}/manifest.json"
    expected_paths = {expected_price_path, expected_source_manifest_path}
    archive_root = Path(local_archive).resolve()
    seen_paths: set[str] = set()
    files: list[tuple[Path, int, str, str]] = []
    for index, item in enumerate(inventory):
        if not isinstance(item, dict):
            raise ValueError(f"Price-recovery files[{index}] must be an object")
        relative_path = _validate_inventory_path(item.get("path"), expected_date_part, index)
        if relative_path in seen_paths:
            raise ValueError(f"Price-recovery contains duplicate path: {relative_path}")
        seen_paths.add(relative_path)
        if relative_path not in expected_paths:
            raise ValueError(f"Price-recovery contains an unexpected path: {relative_path}")

        expected_bytes = item.get("bytes")
        if (
            not isinstance(expected_bytes, int)
            or isinstance(expected_bytes, bool)
            or expected_bytes <= 0
        ):
            raise ValueError(f"Price-recovery files[{index}].bytes is invalid")
        expected_sha256 = item.get("sha256")
        if not isinstance(expected_sha256, str) or not _SHA256_PATTERN.fullmatch(expected_sha256):
            raise ValueError(f"Price-recovery files[{index}].sha256 is invalid")

        if relative_path == expected_price_path:
            if set(item) != _PARQUET_ENTRY_KEYS:
                raise ValueError("Price-recovery Parquet entry keys drifted")
            if item.get("kind") != "parquet" or item.get("dataset") != DAILY_PRICES_DATASET:
                raise ValueError("Price-recovery Parquet role is invalid")
            rows = item.get("rows")
            if not isinstance(rows, int) or isinstance(rows, bool) or rows <= 0:
                raise ValueError("Price-recovery Parquet rows must be positive")
            schema_sha256 = item.get("schema_sha256")
            if not isinstance(schema_sha256, str) or not _SHA256_PATTERN.fullmatch(schema_sha256):
                raise ValueError("Price-recovery Parquet schema hash is invalid")
            if item.get("pit_column") != "available_at_utc":
                raise ValueError("Price-recovery PIT column must be available_at_utc")
            pit_min = _parse_utc(item.get("pit_min_utc"), "files[].pit_min_utc")
            pit_max = _parse_utc(item.get("pit_max_utc"), "files[].pit_max_utc")
            if pit_min < close or pit_max < pit_min or pit_max > sealed:
                raise ValueError("Price-recovery Parquet PIT interval is invalid")
        else:
            if set(item) != _ROLE_ENTRY_KEYS or item.get("kind") != "daily_price_manifest":
                raise ValueError("Price-recovery daily-price manifest entry keys drifted")

        file_path = (archive_root / Path(*PurePosixPath(relative_path).parts)).resolve()
        if not file_path.is_relative_to(archive_root):
            raise ValueError(f"Price-recovery file escapes local archive: {relative_path}")
        if not file_path.is_file():
            raise ValueError(f"Price-recovery file is missing: {relative_path}")
        files.append((file_path, expected_bytes, expected_sha256, relative_path))

    if seen_paths != expected_paths:
        raise ValueError("Price-recovery manifest does not bind the exact price file pair")
    files.sort(key=lambda item: item[3])
    return generation_id, files


def _bind_verified_price_evidence(
    manifest: dict[str, Any],
    price_report: dict[str, Any],
    *,
    local_archive: Path,
) -> None:
    if manifest.get("provider") != price_report.get("provider"):
        raise ValueError("Price-recovery provider differs from strict verification")
    if manifest.get("coverage") != price_report.get("coverage"):
        raise ValueError("Price-recovery coverage differs from strict verification")
    inventory = manifest["files"]
    price_entry = next(item for item in inventory if item.get("dataset") == DAILY_PRICES_DATASET)
    source_entry = next(item for item in inventory if item.get("kind") == "daily_price_manifest")
    verified_output = price_report.get("output")
    verified_manifest = price_report.get("manifest")
    if not isinstance(verified_output, dict) or not isinstance(verified_manifest, dict):
        raise ValueError("Price-recovery verifier omitted physical evidence")
    if any(
        price_entry.get(field) != verified_output.get(field)
        for field in ("path", "rows", "bytes", "sha256", "schema_sha256")
    ):
        raise ValueError("Price-recovery Parquet differs from strict verification")
    if source_entry.get("sha256") != verified_manifest.get("sha256"):
        raise ValueError("Price-recovery source manifest differs from strict verification")
    source_path = Path(local_archive) / str(source_entry["path"])
    source_payload = _read_json_value_error(source_path, "daily-price manifest")
    if source_payload.get("manifest_identity_sha256") != verified_manifest.get("identity_sha256"):
        raise ValueError("Price-recovery source manifest identity differs from verification")
    close = _parse_utc(manifest.get("session_market_close_utc"), "session_market_close_utc")
    sealed = _parse_utc(manifest.get("sealed_at_utc"), "sealed_at_utc")
    capture_started = _parse_utc(
        source_payload.get("capture_started_utc"),
        "daily-price manifest capture_started_utc",
    )
    capture_finished = _parse_utc(
        source_payload.get("capture_finished_utc"),
        "daily-price manifest capture_finished_utc",
    )
    if not close <= capture_started <= capture_finished <= sealed:
        raise ValueError("Price-recovery source capture timeline is outside close/seal bounds")


def _price_parquet_evidence(
    snapshot_dir: Path,
    path: Path,
    *,
    market_close: datetime,
    sealed_at: datetime,
) -> dict[str, Any]:
    if not path.is_file():
        raise PriceRecoveryBundleError(f"required daily-price Parquet missing: {path}")
    try:
        parquet = pq.ParquetFile(path)
        schema = parquet.schema_arrow
        available = pq.read_table(path, columns=["available_at_utc"]).column("available_at_utc")
    except (OSError, KeyError, pa.ArrowException) as exc:
        raise PriceRecoveryBundleError(f"daily-price Parquet is unreadable: {path}: {exc}") from exc
    rows = int(parquet.metadata.num_rows)
    if rows <= 0:
        raise PriceRecoveryBundleError("daily-price Parquet is empty")
    minimum = pc.min(available).as_py()
    maximum = pc.max(available).as_py()
    pit_min = _parse_utc_error(minimum, "daily_prices.available_at_utc min")
    pit_max = _parse_utc_error(maximum, "daily_prices.available_at_utc max")
    if pit_min < market_close:
        raise PriceRecoveryBundleError("daily-price Parquet contains pre-close PIT data")
    if pit_max < pit_min or pit_max > sealed_at:
        raise PriceRecoveryBundleError("daily-price Parquet PIT interval is invalid")
    evidence = _role_evidence(snapshot_dir, path, kind="parquet")
    evidence.update(
        {
            "dataset": DAILY_PRICES_DATASET,
            "rows": rows,
            "schema_sha256": _schema_sha256(schema),
            "pit_column": "available_at_utc",
            "pit_min_utc": _iso_utc(pit_min),
            "pit_max_utc": _iso_utc(pit_max),
        }
    )
    return evidence


def _role_evidence(snapshot_dir: Path, path: Path, *, kind: str) -> dict[str, Any]:
    try:
        relative = path.relative_to(snapshot_dir).as_posix()
    except ValueError as exc:
        raise PriceRecoveryBundleError(f"file escapes snapshot root: {path}") from exc
    return {
        "path": relative,
        "kind": kind,
        "bytes": path.stat().st_size,
        "sha256": _sha256_file(path),
    }


def _schema_sha256(schema: pa.Schema) -> str:
    canonical = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in schema
    ]
    return hashlib.sha256(
        json.dumps(canonical, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify_file(
    path: Path,
    expected_bytes: int,
    expected_sha256: str,
    relative_path: str,
) -> str:
    if path.stat().st_size != expected_bytes:
        raise ValueError(f"Price-recovery file byte count mismatch: {relative_path}")
    sha256_digest = hashlib.sha256()
    dropbox_digest = hashlib.sha256()
    block_size = 4 * 1024 * 1024
    with path.open("rb") as handle:
        while block := handle.read(block_size):
            sha256_digest.update(block)
            dropbox_digest.update(hashlib.sha256(block).digest())
    if sha256_digest.hexdigest() != expected_sha256:
        raise ValueError(f"Price-recovery file SHA-256 mismatch: {relative_path}")
    return dropbox_digest.hexdigest()


def _validate_inventory_path(value: Any, expected_date_part: str, index: int) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError(f"Price-recovery files[{index}].path is invalid")
    raw_parts = value.split("/")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or any(part in {"", ".", ".."} for part in raw_parts)
        or path.as_posix() != value
    ):
        raise ValueError(f"Price-recovery file path is not normalized: {value}")
    if [part for part in path.parts if part.startswith("date=")] != [expected_date_part]:
        raise ValueError(f"Price-recovery file does not belong only to {expected_date_part}")
    return value


def _load_manifest(path: Path) -> tuple[dict[str, Any], bytes]:
    try:
        raw = path.read_bytes()
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"Price-recovery manifest is unreadable: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError("Price-recovery manifest must be a JSON object")
    return payload, raw


def _read_json(path: Path, role: str) -> dict[str, Any]:
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
        )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise PriceRecoveryBundleError(f"{role} is unreadable: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise PriceRecoveryBundleError(f"{role} must be a JSON object: {path}")
    return payload


def _read_json_value_error(path: Path, role: str) -> dict[str, Any]:
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
        )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"{role} is unreadable: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{role} must be a JSON object: {path}")
    return payload


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def _validate_producer(value: Any) -> None:
    if not isinstance(value, dict) or set(value) != _PRODUCER_KEYS:
        raise ValueError("Price-recovery producer keys drifted")
    if not all(isinstance(item, str) and item.strip() for item in value.values()):
        raise ValueError("Price-recovery producer values must be non-empty strings")
    if (
        not _REPOSITORY_PATTERN.fullmatch(value["repository"])
        or value["repository"] != "ilovefood2/analyst-snapshot"
    ):
        raise ValueError("Price-recovery producer repository is not the authorized repository")
    if value["git_ref"] != "refs/heads/main":
        raise ValueError("Price-recovery producer git_ref is not refs/heads/main")
    if not _GIT_SHA_PATTERN.fullmatch(value["git_sha"]):
        raise ValueError("Price-recovery producer git_sha must be a 40-character lowercase hash")
    for field in ("workflow_run_id", "workflow_run_attempt"):
        if not value[field].isdigit() or int(value[field]) <= 0:
            raise ValueError(f"Price-recovery producer {field} must be a positive integer string")


def _session_contract(value: str) -> tuple[date, datetime]:
    try:
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value:
            raise ValueError("date is not canonical")
        close = session_market_close_utc(parsed)
    except (TypeError, ValueError) as exc:
        raise PriceRecoveryBundleError(f"invalid XNYS session date {value!r}: {exc}") from exc
    return parsed, close


def _session_contract_value_error(value: str) -> tuple[date, datetime]:
    try:
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value:
            raise ValueError("date is not canonical")
        return parsed, session_market_close_utc(parsed)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Price-recovery run_date is not an XNYS session: {value!r}") from exc


def _validate_canonical_date(value: str) -> None:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Price-recovery run_date is not a valid ISO date: {value!r}") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"Price-recovery run_date is not canonical YYYY-MM-DD: {value!r}")


def _parse_utc(value: Any, role: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Price-recovery {role} is missing")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Price-recovery {role} is not ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"Price-recovery {role} is timezone-naive")
    return parsed.astimezone(UTC)


def _parse_utc_error(value: Any, role: str) -> datetime:
    try:
        return _parse_utc(value, role)
    except ValueError as exc:
        raise PriceRecoveryBundleError(str(exc)) from exc


def _as_utc(value: datetime, role: str) -> datetime:
    if value.tzinfo is None:
        raise PriceRecoveryBundleError(f"{role} must be timezone-aware")
    return value.astimezone(UTC)


def _as_utc_value_error(value: datetime, role: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"Price-recovery {role} must be timezone-aware")
    return value.astimezone(UTC)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    data = (json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
