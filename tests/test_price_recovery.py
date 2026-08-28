from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from analyst_snapshot.cli import build_parser
from analyst_snapshot.daily_prices import daily_price_manifest_path
from analyst_snapshot.datasets import DAILY_PRICES_DATASET
from analyst_snapshot.dropbox_sync import DropboxSecrets, _dropbox_content_hash_bytes
from analyst_snapshot.price_recovery import (
    PRICE_READY_FILE_NAME,
    PRICE_RECOVERY_BUNDLE_SCHEMA,
    PRICE_RECOVERY_READY_SCHEMA,
    PriceRecoveryBundleError,
    _identity_sha256,
    _schema_sha256,
    finalize_price_recovery_bundle,
    manifest_identity_sha256,
    price_recovery_manifest_path,
    publish_price_recovery_bundle,
)
from analyst_snapshot.storage import dataset_path, write_parquet

SESSION_DATE = "2026-08-27"
POST_CLOSE = "2026-08-27T21:00:00.941609Z"
SEALED_AT = datetime(2026, 8, 28, 1, tzinfo=UTC)

EXPECTED_BUNDLE_KEYS = {
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
EXPECTED_READY_KEYS = {
    "schema",
    "status",
    "session_date",
    "generation_id",
    "manifest_path",
    "manifest_sha256",
    "manifest_identity_sha256",
    "files_count",
    "published_at_utc",
    "ready_identity_sha256",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _candidate(tmp_path: Path, monkeypatch) -> tuple[Path, Path, dict]:
    archive = tmp_path / "archive"
    universe = tmp_path / "universe.txt"
    universe.write_text("AAPL\n", encoding="utf-8")
    price_path = dataset_path(archive, DAILY_PRICES_DATASET, SESSION_DATE)
    write_parquet(
        price_path,
        pd.DataFrame(
            [
                {
                    "dataset": DAILY_PRICES_DATASET,
                    "run_id": "price_test",
                    "symbol": "AAPL",
                    "canonical_symbol": "AAPL",
                    "available_at_utc": POST_CLOSE,
                }
            ]
        ),
        DAILY_PRICES_DATASET,
    )
    source_manifest_path = daily_price_manifest_path(archive, SESSION_DATE)
    source_manifest_path.parent.mkdir(parents=True)
    source_manifest_path.write_text(
        json.dumps(
            {
                "schema": "analyst_snapshot_daily_prices_manifest_v1",
                "status": "complete",
                "session_date": SESSION_DATE,
                "capture_started_utc": POST_CLOSE,
                "capture_finished_utc": POST_CLOSE,
                "manifest_identity_sha256": "source-price-identity",
            }
        ),
        encoding="utf-8",
    )
    provider = {
        "provider_name": "yahoo",
        "transport": "yfinance",
        "price_role": "conditional_recovery_input",
        "intended_use": "historical_gap_recovery",
    }
    coverage = {
        "ratio": 1.0,
        "anchor_expected_symbols": 14,
        "anchor_exact_target_symbols": 14,
        "anchor_usable_tail_symbols": 14,
    }
    report = {
        "ok": True,
        "errors": [],
        "provider": provider,
        "coverage": coverage,
        "output": {
            "path": price_path.relative_to(archive).as_posix(),
            "rows": pq.ParquetFile(price_path).metadata.num_rows,
            "bytes": price_path.stat().st_size,
            "sha256": _sha256(price_path),
            "schema_sha256": _schema_sha256(pq.read_schema(price_path)),
        },
        "manifest": {
            "sha256": _sha256(source_manifest_path),
            "identity_sha256": "source-price-identity",
        },
    }
    monkeypatch.setattr(
        "analyst_snapshot.price_recovery.verify_daily_prices",
        lambda *_args, **_kwargs: report,
    )
    return archive, universe, report


def _seal(archive: Path, universe: Path) -> dict:
    return finalize_price_recovery_bundle(
        archive,
        universe,
        session_date=SESSION_DATE,
        generation_id="price_generation_test",
        now_utc=SEALED_AT,
        producer_identity={
            "repository": "ilovefood2/analyst-snapshot",
            "git_ref": "refs/heads/main",
            "git_sha": "a" * 40,
            "workflow_run_id": "42",
            "workflow_run_attempt": "1",
        },
    )


def _dropbox_metadata(remote_path: str, data: bytes) -> dict:
    return {
        "size": len(data),
        "content_hash": _dropbox_content_hash_bytes(data),
        "path_display": remote_path,
    }


def test_price_finalizer_seals_exact_independent_two_file_contract(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive, universe, report = _candidate(tmp_path, monkeypatch)
    full_sentinel = archive / "_recovery_manifests" / f"date={SESSION_DATE}" / "manifest.json"
    full_sentinel.parent.mkdir(parents=True)
    full_sentinel.write_text("full-sentinel", encoding="utf-8")

    manifest = _seal(archive, universe)

    assert set(manifest) == EXPECTED_BUNDLE_KEYS
    assert manifest["schema"] == PRICE_RECOVERY_BUNDLE_SCHEMA
    assert manifest["status"] == "complete"
    assert manifest["session_date"] == SESSION_DATE
    assert manifest["session_market_close_utc"] == "2026-08-27T20:00:00Z"
    assert manifest["provider"] == report["provider"]
    assert manifest["coverage"] == report["coverage"]
    assert manifest["producer"] == {
        "repository": "ilovefood2/analyst-snapshot",
        "git_ref": "refs/heads/main",
        "git_sha": "a" * 40,
        "workflow_run_id": "42",
        "workflow_run_attempt": "1",
    }
    assert manifest["manifest_identity_sha256"] == manifest_identity_sha256(manifest)
    assert [item["path"] for item in manifest["files"]] == [
        f"_daily_price_manifests/date={SESSION_DATE}/manifest.json",
        f"daily_prices/date={SESSION_DATE}/data.parquet",
    ]
    assert set(manifest["files"][0]) == {"path", "kind", "bytes", "sha256"}
    assert manifest["files"][0]["kind"] == "daily_price_manifest"
    assert set(manifest["files"][1]) == {
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
    assert manifest["files"][1]["dataset"] == DAILY_PRICES_DATASET
    assert manifest["files"][1]["pit_column"] == "available_at_utc"
    assert manifest["files"][1]["pit_min_utc"] == POST_CLOSE
    assert manifest["files"][1]["pit_max_utc"] == POST_CLOSE
    assert full_sentinel.read_text(encoding="utf-8") == "full-sentinel"
    assert price_recovery_manifest_path(archive, SESSION_DATE).is_file()


def test_price_finalizer_fails_closed_on_semantic_or_physical_drift(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive, universe, report = _candidate(tmp_path, monkeypatch)
    report["output"]["sha256"] = "0" * 64

    with pytest.raises(PriceRecoveryBundleError, match="changed after strict verification"):
        _seal(archive, universe)

    assert not price_recovery_manifest_path(archive, SESSION_DATE).exists()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"generation_id": "../unsafe"}, "generation_id"),
        ({"min_coverage": 0.90}, "exactly 0.95"),
        (
            {"now_utc": datetime(2026, 8, 27, 19, 59, tzinfo=UTC)},
            "has not closed",
        ),
    ],
)
def test_price_finalizer_rejects_unsafe_seal_contract(
    tmp_path: Path,
    monkeypatch,
    kwargs: dict,
    message: str,
) -> None:
    archive, universe, _report = _candidate(tmp_path, monkeypatch)
    call = {
        "session_date": SESSION_DATE,
        "generation_id": "price_generation_test",
        "now_utc": SEALED_AT,
    }
    call.update(kwargs)

    with pytest.raises(PriceRecoveryBundleError, match=message):
        finalize_price_recovery_bundle(archive, universe, **call)


@pytest.mark.parametrize(
    ("producer_update", "message"),
    [
        ({"repository": "attacker/analyst-snapshot"}, "authorized repository"),
        ({"git_ref": "refs/heads/feature"}, "refs/heads/main"),
    ],
)
def test_price_finalizer_rejects_unauthorized_repository_or_ref(
    tmp_path: Path,
    monkeypatch,
    producer_update: dict[str, str],
    message: str,
) -> None:
    archive, universe, _report = _candidate(tmp_path, monkeypatch)
    producer = {
        "repository": "ilovefood2/analyst-snapshot",
        "git_ref": "refs/heads/main",
        "git_sha": "a" * 40,
        "workflow_run_id": "42",
        "workflow_run_attempt": "1",
    }
    producer.update(producer_update)

    with pytest.raises(PriceRecoveryBundleError, match=message):
        finalize_price_recovery_bundle(
            archive,
            universe,
            session_date=SESSION_DATE,
            generation_id="price_generation_test",
            now_utc=SEALED_AT,
            producer_identity=producer,
        )


def test_publish_price_bundle_uses_distinct_generation_and_price_ready_last(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive, universe, _report = _candidate(tmp_path, monkeypatch)
    manifest = _seal(archive, universe)
    manifest_path = price_recovery_manifest_path(archive, SESSION_DATE)
    attempted: list[str] = []
    ready_bytes: bytes | None = None
    monkeypatch.setattr(
        "analyst_snapshot.price_recovery.refresh_access_token",
        lambda _secrets: "token",
    )

    def fake_upload_file(path: Path, remote: str, _token: str) -> dict:
        attempted.append(remote)
        return _dropbox_metadata(remote, path.read_bytes())

    def fake_upload_bytes(data: bytes, remote: str, _token: str) -> dict:
        nonlocal ready_bytes
        attempted.append(remote)
        ready_bytes = data
        return _dropbox_metadata(remote, data)

    monkeypatch.setattr(
        "analyst_snapshot.price_recovery._upload_immutable_file",
        fake_upload_file,
    )
    monkeypatch.setattr("analyst_snapshot.price_recovery._upload_bytes", fake_upload_bytes)

    count = publish_price_recovery_bundle(
        archive,
        "/root",
        DropboxSecrets("key", "secret", "refresh"),
        run_date=SESSION_DATE,
        universe_file=universe,
    )

    generation_root = "/root/date=2026-08-27/price_generations/price_generation_test"
    assert count == 4
    assert attempted == [
        f"{generation_root}/_daily_price_manifests/date={SESSION_DATE}/manifest.json",
        f"{generation_root}/daily_prices/date={SESSION_DATE}/data.parquet",
        f"{generation_root}/manifest.json",
        f"/root/date={SESSION_DATE}/{PRICE_READY_FILE_NAME}",
    ]
    assert all(not path.endswith("/_READY.json") for path in attempted)
    assert ready_bytes is not None
    ready = json.loads(ready_bytes)
    assert set(ready) == EXPECTED_READY_KEYS
    assert ready["schema"] == PRICE_RECOVERY_READY_SCHEMA
    assert ready["status"] == "ready"
    assert ready["manifest_path"] == "price_generations/price_generation_test/manifest.json"
    assert ready["manifest_sha256"] == hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    assert ready["manifest_identity_sha256"] == manifest["manifest_identity_sha256"]
    assert ready["files_count"] == 2
    assert "." not in ready["published_at_utc"]
    assert ready["ready_identity_sha256"] == _identity_sha256(
        ready,
        self_hash_field="ready_identity_sha256",
    )


def test_publish_price_bundle_rejects_tamper_before_token(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive, universe, _report = _candidate(tmp_path, monkeypatch)
    _seal(archive, universe)
    price_path = dataset_path(archive, DAILY_PRICES_DATASET, SESSION_DATE)
    price_path.write_bytes(price_path.read_bytes() + b"tamper")
    token_requested = False

    def fake_token(_secrets) -> str:
        nonlocal token_requested
        token_requested = True
        return "token"

    monkeypatch.setattr("analyst_snapshot.price_recovery.refresh_access_token", fake_token)
    with pytest.raises(ValueError, match="byte count mismatch"):
        publish_price_recovery_bundle(
            archive,
            "/root",
            DropboxSecrets("key", "secret", "refresh"),
            run_date=SESSION_DATE,
            universe_file=universe,
        )
    assert token_requested is False


def test_publish_price_bundle_rejects_extra_inventory_before_token(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive, universe, _report = _candidate(tmp_path, monkeypatch)
    _seal(archive, universe)
    manifest_path = price_recovery_manifest_path(archive, SESSION_DATE)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"].append(dict(manifest["files"][0]))
    manifest["manifest_identity_sha256"] = manifest_identity_sha256(manifest)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setattr(
        "analyst_snapshot.price_recovery.refresh_access_token",
        lambda _secrets: pytest.fail("invalid inventory must not request a token"),
    )

    with pytest.raises(ValueError, match="exactly two"):
        publish_price_recovery_bundle(
            archive,
            "/root",
            DropboxSecrets("key", "secret", "refresh"),
            run_date=SESSION_DATE,
            universe_file=universe,
        )


def test_publish_price_bundle_rejects_non_main_manifest_before_token(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive, universe, _report = _candidate(tmp_path, monkeypatch)
    _seal(archive, universe)
    manifest_path = price_recovery_manifest_path(archive, SESSION_DATE)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["producer"]["git_ref"] = "refs/heads/feature"
    manifest["manifest_identity_sha256"] = manifest_identity_sha256(manifest)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    token_requested = False

    def fake_token(_secrets) -> str:
        nonlocal token_requested
        token_requested = True
        return "token"

    monkeypatch.setattr("analyst_snapshot.price_recovery.refresh_access_token", fake_token)

    with pytest.raises(ValueError, match="refs/heads/main"):
        publish_price_recovery_bundle(
            archive,
            "/root",
            DropboxSecrets("key", "secret", "refresh"),
            run_date=SESSION_DATE,
            universe_file=universe,
        )

    assert token_requested is False


@pytest.mark.parametrize("failure_stage", ["data", "manifest"])
def test_price_upload_failure_never_writes_price_ready(
    tmp_path: Path,
    monkeypatch,
    failure_stage: str,
) -> None:
    archive, universe, _report = _candidate(tmp_path, monkeypatch)
    _seal(archive, universe)
    manifest_path = price_recovery_manifest_path(archive, SESSION_DATE)
    attempted: list[str] = []
    monkeypatch.setattr(
        "analyst_snapshot.price_recovery.refresh_access_token",
        lambda _secrets: "token",
    )

    def fake_upload_file(path: Path, remote: str, _token: str) -> dict:
        attempted.append(remote)
        is_bundle_manifest = path == manifest_path
        if (failure_stage == "data" and not is_bundle_manifest) or (
            failure_stage == "manifest" and is_bundle_manifest
        ):
            raise RuntimeError("simulated Dropbox failure")
        return _dropbox_metadata(remote, path.read_bytes())

    monkeypatch.setattr(
        "analyst_snapshot.price_recovery._upload_immutable_file",
        fake_upload_file,
    )
    monkeypatch.setattr(
        "analyst_snapshot.price_recovery._upload_bytes",
        lambda _data, remote, _token: pytest.fail(f"READY must not be attempted: {remote}"),
    )

    with pytest.raises(RuntimeError, match="simulated Dropbox failure"):
        publish_price_recovery_bundle(
            archive,
            "/root",
            DropboxSecrets("key", "secret", "refresh"),
            run_date=SESSION_DATE,
            universe_file=universe,
        )
    assert all(not path.endswith(f"/{PRICE_READY_FILE_NAME}") for path in attempted)


def test_price_recovery_cli_commands_are_distinct_from_full_bundle() -> None:
    parser = build_parser()

    seal = parser.parse_args(
        [
            "seal-price-recovery-bundle",
            "--run-date",
            SESSION_DATE,
            "--generation-id",
            "price_test",
        ]
    )
    upload = parser.parse_args(["upload-price-recovery-bundle", "--run-date", SESSION_DATE])

    assert seal.command == "seal-price-recovery-bundle"
    assert upload.command == "upload-price-recovery-bundle"
