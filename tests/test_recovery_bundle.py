from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from analyst_snapshot import dropbox_sync
from analyst_snapshot.datasets import DATASETS, MARKET_CONTEXT_DATASETS
from analyst_snapshot.recovery_bundle import (
    RECOVERY_BUNDLE_SCHEMA,
    RecoveryBundleError,
    finalize_recovery_bundle,
    manifest_identity_sha256,
    recovery_manifest_path,
)
from analyst_snapshot.storage import dataset_path, write_parquet

SESSION_DATE = "2026-08-27"
POST_CLOSE = "2026-08-27T21:00:00Z"


def _candidate(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    archive = tmp_path / "archive"
    universe = tmp_path / "universe.txt"
    universe.write_text("AAPL\n", encoding="utf-8")

    for spec in DATASETS.values():
        write_parquet(
            dataset_path(archive, spec.name, SESSION_DATE),
            pd.DataFrame(
                [
                    {
                        "symbol": "AAPL",
                        "snapshot_utc": POST_CLOSE,
                        "dataset": spec.name,
                        "run_id": "run_test",
                        "no_analyst_coverage": False,
                    }
                ]
            ),
            spec.name,
        )
    for name in MARKET_CONTEXT_DATASETS:
        write_parquet(
            dataset_path(archive, name, SESSION_DATE),
            pd.DataFrame(
                [
                    {
                        "symbol": "AAPL",
                        "snapshot_utc": POST_CLOSE,
                        "dataset": name,
                        "run_id": "market_test",
                    }
                ]
            ),
            name,
        )

    run_manifest = archive / "_manifests" / f"date={SESSION_DATE}" / "run_test.json"
    run_manifest.parent.mkdir(parents=True)
    run_manifest.write_text(
        json.dumps({"run_date": SESSION_DATE, "run_id": "run_test"}), encoding="utf-8"
    )
    context_manifest = (
        archive / "_market_context_manifests" / f"date={SESSION_DATE}" / "manifest.json"
    )
    context_manifest.parent.mkdir(parents=True)
    context_manifest.write_text(
        json.dumps(
            {
                "schema": "analyst_snapshot_market_context_manifest_v1",
                "status": "complete",
                "run_date": SESSION_DATE,
                "run_id": "market_test",
                "snapshot_utc": POST_CLOSE,
            }
        ),
        encoding="utf-8",
    )
    source = archive / "_market_context_sources" / f"date={SESSION_DATE}" / "source.bin"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"source")
    monkeypatch.setattr(
        "analyst_snapshot.recovery_bundle.verify_market_context",
        lambda *_args, **_kwargs: {"ok": True, "run_date": SESSION_DATE, "errors": []},
    )
    return archive, universe


def _seal(archive: Path, universe: Path):
    return finalize_recovery_bundle(
        archive,
        universe,
        session_date=SESSION_DATE,
        generation_id="generation_test",
        now_utc=datetime(2026, 8, 28, 1, tzinfo=UTC),
        producer_identity={"git_sha": "abc123", "workflow_run_id": "42"},
    )


def test_finalizer_seals_complete_hash_addressed_inventory(tmp_path: Path, monkeypatch) -> None:
    archive, universe = _candidate(tmp_path, monkeypatch)

    manifest = _seal(archive, universe)

    assert manifest["schema"] == RECOVERY_BUNDLE_SCHEMA
    assert manifest["status"] == "complete"
    assert manifest["session_date"] == SESSION_DATE
    assert manifest["generation_id"] == "generation_test"
    assert manifest["session_market_close_utc"] == "2026-08-27T20:00:00Z"
    assert manifest["coverage"]["recommendations"]["ratio"] == 1.0
    assert manifest["producer"]["git_sha"] == "abc123"
    assert manifest["manifest_identity_sha256"] == manifest_identity_sha256(manifest)
    paths = [entry["path"] for entry in manifest["files"]]
    assert paths == sorted(paths)
    assert len(paths) == len(set(paths))
    parquet = [entry for entry in manifest["files"] if entry["kind"] == "parquet"]
    assert all(entry["rows"] == 1 for entry in parquet)
    assert all(len(entry["sha256"]) == 64 for entry in manifest["files"])
    assert all(len(entry["schema_sha256"]) == 64 for entry in parquet)
    assert recovery_manifest_path(archive, SESSION_DATE).is_file()
    generation, publish_files = dropbox_sync._validate_recovery_manifest(
        manifest,
        local_archive=archive,
        run_date=SESSION_DATE,
    )
    assert generation == "generation_test"
    assert len(publish_files) == len(manifest["files"])


def test_finalizer_rejects_preclose_rows(tmp_path: Path, monkeypatch) -> None:
    archive, universe = _candidate(tmp_path, monkeypatch)
    name = "recommendations"
    write_parquet(
        dataset_path(archive, name, SESSION_DATE),
        pd.DataFrame(
            [
                {
                    "symbol": "AAPL",
                    "snapshot_utc": "2026-08-27T19:59:59Z",
                    "dataset": name,
                    "run_id": "bad",
                    "no_analyst_coverage": False,
                }
            ]
        ),
        name,
    )

    with pytest.raises(RecoveryBundleError, match="pre-close"):
        _seal(archive, universe)
    assert not recovery_manifest_path(archive, SESSION_DATE).exists()


def test_finalizer_rejects_insufficient_coverage(tmp_path: Path, monkeypatch) -> None:
    archive, universe = _candidate(tmp_path, monkeypatch)
    universe.write_text("AAPL\nMSFT\n", encoding="utf-8")

    with pytest.raises(RecoveryBundleError, match="coverage"):
        _seal(archive, universe)


def test_finalizer_rejects_future_pit_rows(tmp_path: Path, monkeypatch) -> None:
    archive, universe = _candidate(tmp_path, monkeypatch)
    name = "recommendations"
    write_parquet(
        dataset_path(archive, name, SESSION_DATE),
        pd.DataFrame(
            [
                {
                    "symbol": "AAPL",
                    "snapshot_utc": "2026-08-28T02:00:00Z",
                    "dataset": name,
                    "run_id": "future",
                    "no_analyst_coverage": False,
                }
            ]
        ),
        name,
    )

    with pytest.raises(RecoveryBundleError, match="future PIT"):
        _seal(archive, universe)


def test_finalizer_rejects_market_context_failure(tmp_path: Path, monkeypatch) -> None:
    archive, universe = _candidate(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "analyst_snapshot.recovery_bundle.verify_market_context",
        lambda *_args, **_kwargs: {"ok": False, "errors": ["tampered"]},
    )

    with pytest.raises(RecoveryBundleError, match="tampered"):
        _seal(archive, universe)


def test_finalizer_rejects_a_session_that_has_not_closed(tmp_path: Path, monkeypatch) -> None:
    archive, universe = _candidate(tmp_path, monkeypatch)

    with pytest.raises(RecoveryBundleError, match="has not closed"):
        finalize_recovery_bundle(
            archive,
            universe,
            session_date=SESSION_DATE,
            now_utc=datetime(2026, 8, 27, 19, 59, tzinfo=UTC),
        )
