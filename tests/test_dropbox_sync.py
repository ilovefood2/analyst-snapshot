from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

import pytest

from analyst_snapshot import dropbox_sync
from analyst_snapshot.dropbox_sync import (
    authorization_url,
    load_dropbox_secrets,
    remote_path_for_file,
)


@pytest.fixture(autouse=True)
def _offline_price_semantic_verifier(monkeypatch):
    def fake_verify(archive: Path, _universe: Path, *, session_date: str, **_kwargs):
        manifest_path = archive / "_recovery_manifests" / f"date={session_date}" / "manifest.json"
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        price = next(item for item in payload["files"] if item.get("dataset") == "daily_prices")
        price_manifest = next(
            item for item in payload["files"] if item.get("kind") == "daily_price_manifest"
        )
        return {
            "ok": True,
            "errors": [],
            "provider": payload["providers"]["daily_prices"],
            "coverage": payload["coverage"]["daily_prices"],
            "output": {
                field: price[field]
                for field in ("path", "rows", "bytes", "sha256", "schema_sha256")
            },
            "manifest": {"sha256": price_manifest["sha256"]},
        }

    monkeypatch.setattr(dropbox_sync, "verify_daily_prices", fake_verify)


def _identity_sha256(payload: dict[str, object], self_hash_field: str) -> str:
    identity = {key: value for key, value in payload.items() if key != self_hash_field}
    canonical = json.dumps(
        identity,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _dropbox_metadata(remote_path: str, payload: bytes) -> dict[str, object]:
    return {
        "path_display": remote_path,
        "size": len(payload),
        "content_hash": dropbox_sync._dropbox_content_hash_bytes(payload),
    }


def _write_recovery_bundle(
    tmp_path: Path,
    *,
    run_date: str = "2026-07-06",
    manifest_session_date: str | None = None,
    files: dict[str, bytes] | None = None,
) -> tuple[Path, Path, dict[str, Path]]:
    archive = tmp_path / "archive"
    if files is None:
        files = {
            f"{dataset}/date={run_date}/data.parquet": dataset.encode()
            for dataset in sorted(dropbox_sync._REQUIRED_PARQUET_DATASETS)
        }
        files[f"_manifests/date={run_date}/run.json"] = b"analyst-manifest"
        files[f"_daily_price_manifests/date={run_date}/manifest.json"] = b"prices"
        files[f"_market_context_manifests/date={run_date}/manifest.json"] = b"context"
        files[f"_market_context_sources/date={run_date}/source.bin"] = b"source"

    local_files: dict[str, Path] = {}
    inventory: list[dict[str, object]] = []
    for relative_path, payload in files.items():
        local_path = archive.joinpath(*relative_path.split("/"))
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(payload)
        local_files[relative_path] = local_path
        entry: dict[str, object] = {
            "path": relative_path,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "bytes": len(payload),
        }
        top = relative_path.split("/", 1)[0]
        if top in dropbox_sync._REQUIRED_PARQUET_DATASETS:
            entry.update(
                kind="parquet",
                dataset=top,
                rows=1,
                schema_sha256="0" * 64,
                pit_column=(
                    "available_at_utc"
                    if top == dropbox_sync.DAILY_PRICES_DATASET
                    else "snapshot_utc"
                ),
                pit_min_utc="2026-07-06T20:00:00Z",
                pit_max_utc="2026-07-06T20:00:00Z",
            )
        elif top == "_manifests":
            entry["kind"] = "analyst_run_manifest"
        elif top == "_daily_price_manifests":
            entry["kind"] = "daily_price_manifest"
        elif top == "_market_context_manifests":
            entry["kind"] = "market_context_manifest"
        elif top == "_market_context_sources":
            entry["kind"] = "market_context_source"
        inventory.append(entry)
    inventory.sort(key=lambda item: str(item["path"]))

    manifest = {
        "schema": "swinglab_recovery_bundle_v2",
        "status": "complete",
        "session_date": manifest_session_date or run_date,
        "generation_id": "generation-test-001",
        "session_market_close_utc": "2026-07-06T20:00:00Z",
        "sealed_at_utc": "2026-07-06T21:00:00Z",
        "analyst_snapshot_version": "0.4.0",
        "providers": {
            "daily_prices": {
                "provider_name": "yahoo",
                "transport": "yfinance",
                "intended_use": "historical_gap_recovery",
            }
        },
        "universe": {"symbols": 1, "sha256": "0" * 64},
        "coverage_threshold": 0.95,
        "coverage": {"daily_prices": {"ratio": 1.0}},
        "market_context": {"ok": True},
        "analyst_run_ids": ["run-test"],
        "producer": {"workflow_run_id": "42"},
        "files": inventory,
    }
    manifest["manifest_identity_sha256"] = _identity_sha256(
        manifest,
        "manifest_identity_sha256",
    )
    manifest_path = archive / "_recovery_manifests" / f"date={run_date}" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return archive, manifest_path, local_files


def test_authorization_url_requests_offline_token() -> None:
    url = authorization_url("app-key")

    assert "client_id=app-key" in url
    assert "response_type=code" in url
    assert "token_access_type=offline" in url


def test_remote_path_for_file_sorts_snapshots_by_date(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    file_path = archive_dir / "recommendations" / "date=2026-07-06" / "data.parquet"
    file_path.parent.mkdir(parents=True)
    file_path.write_text("x", encoding="utf-8")

    remote_path = remote_path_for_file(archive_dir, file_path, "/DailyStockSnapshots")

    assert remote_path == "/DailyStockSnapshots/date=2026-07-06/recommendations/data.parquet"


def test_remote_path_for_file_skips_non_date_derived_files(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    file_path = archive_dir / "rating_events" / "data.parquet"
    file_path.parent.mkdir(parents=True)
    file_path.write_text("x", encoding="utf-8")

    remote_path = remote_path_for_file(archive_dir, file_path, "/DailyStockSnapshots")

    assert remote_path is None


def test_remote_path_for_file_sorts_first_seen_events_by_date(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    file_path = archive_dir / "rating_events" / "date=2026-07-06" / "data.parquet"
    file_path.parent.mkdir(parents=True)
    file_path.write_text("x", encoding="utf-8")

    remote_path = remote_path_for_file(archive_dir, file_path, "/DailyStockSnapshots")

    assert remote_path == "/DailyStockSnapshots/date=2026-07-06/rating_events/data.parquet"


def test_auth_url_secret_loading_only_requires_app_key(monkeypatch) -> None:
    monkeypatch.setenv("DROPBOX_APP_KEY", "app-key")
    monkeypatch.delenv("DROPBOX_APP_SECRET", raising=False)

    secrets = load_dropbox_secrets(require_refresh_token=False, require_app_secret=False)

    assert secrets.app_key == "app-key"


def test_upload_directory_can_be_scoped_to_one_run_date(tmp_path: Path, monkeypatch) -> None:
    from analyst_snapshot import dropbox_sync

    archive = tmp_path / "archive"
    for date_str in ("2026-07-03", "2026-07-06"):
        path = archive / "recommendations" / f"date={date_str}" / "data.parquet"
        path.parent.mkdir(parents=True)
        path.write_text("x", encoding="utf-8")

    uploaded: list[str] = []
    monkeypatch.setattr(dropbox_sync, "refresh_access_token", lambda _secrets: "token")
    monkeypatch.setattr(
        dropbox_sync,
        "upload_file",
        lambda local, remote, token: uploaded.append(remote),
    )
    secrets = dropbox_sync.DropboxSecrets("key", "secret", "refresh")

    count = dropbox_sync.upload_directory(archive, "/root", secrets, run_date="2026-07-06")

    assert count == 1
    assert uploaded == ["/root/date=2026-07-06/recommendations/data.parquet"]


def test_upload_directory_counts_uploads_not_candidates(tmp_path: Path, monkeypatch) -> None:
    from analyst_snapshot import dropbox_sync

    archive = tmp_path / "archive"
    (archive / "_index").mkdir(parents=True)
    (archive / "_index" / "rating_events.parquet").write_text("x", encoding="utf-8")
    partition = archive / "recommendations" / "date=2026-07-06" / "data.parquet"
    partition.parent.mkdir(parents=True)
    partition.write_text("x", encoding="utf-8")

    monkeypatch.setattr(dropbox_sync, "refresh_access_token", lambda _secrets: "token")
    monkeypatch.setattr(dropbox_sync, "upload_file", lambda local, remote, token: None)
    secrets = dropbox_sync.DropboxSecrets("key", "secret", "refresh")

    # The derived index is skipped, so the returned count must not include it.
    assert dropbox_sync.upload_directory(archive, "/root", secrets) == 1


@pytest.mark.parametrize(
    ("uploader_name", "expected_mode", "expected_strict_conflict"),
    [
        ("upload_file", "overwrite", False),
        ("_upload_immutable_file", "add", True),
    ],
)
def test_upload_modes_keep_generations_immutable(
    tmp_path: Path,
    monkeypatch,
    uploader_name: str,
    expected_mode: str,
    expected_strict_conflict: bool,
) -> None:
    local_path = tmp_path / "payload.bin"
    local_path.write_bytes(b"payload")
    observed_args: list[dict[str, object]] = []

    def fake_post_content(url: str, token: str, args: dict[str, object], data: bytes):
        observed_args.append(args)
        return {}

    monkeypatch.setattr(dropbox_sync, "_post_content", fake_post_content)

    getattr(dropbox_sync, uploader_name)(local_path, "/root/payload.bin", "token")

    assert observed_args[0]["mode"] == expected_mode
    assert observed_args[0]["strict_conflict"] is expected_strict_conflict


def test_publish_recovery_bundle_uploads_ready_last(tmp_path: Path, monkeypatch) -> None:
    archive, manifest_path, local_files = _write_recovery_bundle(tmp_path)
    uploads: list[tuple[str, bytes]] = []

    monkeypatch.setattr(dropbox_sync, "refresh_access_token", lambda _secrets: "token")

    def fake_immutable(local: Path, remote: str, token: str):
        payload = local.read_bytes()
        uploads.append((remote, payload))
        return _dropbox_metadata(remote, payload)

    def fake_bytes(data: bytes, remote: str, token: str):
        uploads.append((remote, data))
        return _dropbox_metadata(remote, data)

    monkeypatch.setattr(dropbox_sync, "_upload_immutable_file", fake_immutable)
    monkeypatch.setattr(dropbox_sync, "_upload_bytes", fake_bytes)
    secrets = dropbox_sync.DropboxSecrets("key", "secret", "refresh")

    count = dropbox_sync.publish_recovery_bundle(
        archive,
        "/root",
        secrets,
        run_date="2026-07-06",
    )

    generation_root = "/root/date=2026-07-06/generations/generation-test-001"
    assert count == len(local_files) + 2
    assert [remote for remote, _ in uploads] == [
        *(f"{generation_root}/{path}" for path in sorted(local_files)),
        f"{generation_root}/manifest.json",
        "/root/date=2026-07-06/_READY.json",
    ]
    ready = json.loads(uploads[-1][1])
    assert ready["schema"] == "swinglab_recovery_ready_v2"
    assert ready["status"] == "ready"
    assert ready["session_date"] == "2026-07-06"
    assert ready["generation_id"] == "generation-test-001"
    assert ready["manifest_path"] == "generations/generation-test-001/manifest.json"
    assert ready["manifest_sha256"] == hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert ready["manifest_identity_sha256"] == manifest["manifest_identity_sha256"]
    assert ready["files_count"] == len(local_files)
    published_at = str(ready["published_at_utc"])
    assert published_at.endswith("Z")
    assert datetime.fromisoformat(published_at.replace("Z", "+00:00")).microsecond == 0
    assert ready["ready_identity_sha256"] == _identity_sha256(
        ready,
        "ready_identity_sha256",
    )


def test_publish_recovery_bundle_refuses_tampered_data(tmp_path: Path, monkeypatch) -> None:
    relative_path = "recommendations/date=2026-07-06/data.parquet"
    archive, _, local_files = _write_recovery_bundle(tmp_path)
    local_files[relative_path].write_bytes(b"X" * len(b"recommendations"))
    token_requested = False

    def fake_refresh(_secrets) -> str:
        nonlocal token_requested
        token_requested = True
        return "token"

    monkeypatch.setattr(dropbox_sync, "refresh_access_token", fake_refresh)
    secrets = dropbox_sync.DropboxSecrets("key", "secret", "refresh")

    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        dropbox_sync.publish_recovery_bundle(
            archive,
            "/root",
            secrets,
            run_date="2026-07-06",
        )

    assert token_requested is False


def test_publish_recovery_bundle_requires_real_price_semantics(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive, _, _ = _write_recovery_bundle(tmp_path)
    token_requested = False

    def fake_refresh(_secrets) -> str:
        nonlocal token_requested
        token_requested = True
        return "token"

    monkeypatch.setattr(
        dropbox_sync,
        "verify_daily_prices",
        lambda *_args, **_kwargs: {"ok": False, "errors": ["invalid OHLC envelope"]},
    )
    monkeypatch.setattr(dropbox_sync, "refresh_access_token", fake_refresh)

    with pytest.raises(ValueError, match="invalid OHLC envelope"):
        dropbox_sync.publish_recovery_bundle(
            archive,
            "/root",
            dropbox_sync.DropboxSecrets("key", "secret", "refresh"),
            run_date="2026-07-06",
        )

    assert token_requested is False


def test_publish_recovery_bundle_refuses_empty_inventory(tmp_path: Path, monkeypatch) -> None:
    archive, _, _ = _write_recovery_bundle(tmp_path, files={})
    token_requested = False

    def fake_refresh(_secrets) -> str:
        nonlocal token_requested
        token_requested = True
        return "token"

    monkeypatch.setattr(dropbox_sync, "refresh_access_token", fake_refresh)
    secrets = dropbox_sync.DropboxSecrets("key", "secret", "refresh")

    with pytest.raises(ValueError, match="at least one data file"):
        dropbox_sync.publish_recovery_bundle(
            archive,
            "/root",
            secrets,
            run_date="2026-07-06",
        )

    assert token_requested is False


def test_publish_recovery_bundle_refuses_manifest_date_mismatch(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive, _, _ = _write_recovery_bundle(
        tmp_path,
        manifest_session_date="2026-07-03",
    )
    token_requested = False

    def fake_refresh(_secrets) -> str:
        nonlocal token_requested
        token_requested = True
        return "token"

    monkeypatch.setattr(dropbox_sync, "refresh_access_token", fake_refresh)
    secrets = dropbox_sync.DropboxSecrets("key", "secret", "refresh")

    with pytest.raises(ValueError, match="session_date does not match"):
        dropbox_sync.publish_recovery_bundle(
            archive,
            "/root",
            secrets,
            run_date="2026-07-06",
        )

    assert token_requested is False


def test_publish_recovery_bundle_refuses_manifest_identity_mismatch(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive, manifest_path, _ = _write_recovery_bundle(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["producer"] = {"workflow_run_id": "tampered"}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    token_requested = False

    def fake_refresh(_secrets) -> str:
        nonlocal token_requested
        token_requested = True
        return "token"

    monkeypatch.setattr(dropbox_sync, "refresh_access_token", fake_refresh)
    secrets = dropbox_sync.DropboxSecrets("key", "secret", "refresh")

    with pytest.raises(ValueError, match="identity mismatch"):
        dropbox_sync.publish_recovery_bundle(
            archive,
            "/root",
            secrets,
            run_date="2026-07-06",
        )

    assert token_requested is False


def test_publish_recovery_bundle_requires_complete_v2_roles(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive, manifest_path, _ = _write_recovery_bundle(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"] = [
        item for item in manifest["files"] if item.get("dataset") != "recommendations"
    ]
    manifest["manifest_identity_sha256"] = _identity_sha256(
        manifest,
        "manifest_identity_sha256",
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setattr(
        dropbox_sync,
        "refresh_access_token",
        lambda _secrets: pytest.fail("invalid bundle must not request a token"),
    )

    with pytest.raises(ValueError, match="missing required parquet datasets"):
        dropbox_sync.publish_recovery_bundle(
            archive,
            "/root",
            dropbox_sync.DropboxSecrets("key", "secret", "refresh"),
            run_date="2026-07-06",
        )


def test_publish_recovery_bundle_requires_daily_price_manifest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive, manifest_path, _ = _write_recovery_bundle(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"] = [
        item for item in manifest["files"] if item.get("kind") != "daily_price_manifest"
    ]
    manifest["manifest_identity_sha256"] = _identity_sha256(
        manifest,
        "manifest_identity_sha256",
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setattr(
        dropbox_sync,
        "refresh_access_token",
        lambda _secrets: pytest.fail("invalid bundle must not request a token"),
    )

    with pytest.raises(ValueError, match="exactly one daily_price_manifest"):
        dropbox_sync.publish_recovery_bundle(
            archive,
            "/root",
            dropbox_sync.DropboxSecrets("key", "secret", "refresh"),
            run_date="2026-07-06",
        )


def test_publish_recovery_bundle_rejects_legacy_v1_for_active_publication(
    tmp_path: Path,
    monkeypatch,
) -> None:
    archive, manifest_path, _ = _write_recovery_bundle(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["schema"] = "swinglab_recovery_bundle_v1"
    manifest["manifest_identity_sha256"] = _identity_sha256(
        manifest,
        "manifest_identity_sha256",
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setattr(
        dropbox_sync,
        "refresh_access_token",
        lambda _secrets: pytest.fail("legacy bundle must not request a token"),
    )

    with pytest.raises(ValueError, match="swinglab_recovery_bundle_v2"):
        dropbox_sync.publish_recovery_bundle(
            archive,
            "/root",
            dropbox_sync.DropboxSecrets("key", "secret", "refresh"),
            run_date="2026-07-06",
        )


@pytest.mark.parametrize("failure_stage", ["data", "manifest"])
def test_publish_recovery_bundle_upload_failure_never_writes_ready(
    tmp_path: Path,
    monkeypatch,
    failure_stage: str,
) -> None:
    archive, manifest_path, _ = _write_recovery_bundle(tmp_path)
    attempted_remote_paths: list[str] = []

    monkeypatch.setattr(dropbox_sync, "refresh_access_token", lambda _secrets: "token")

    def fake_upload_file(local: Path, remote: str, token: str) -> None:
        attempted_remote_paths.append(remote)
        is_manifest = local == manifest_path
        if (failure_stage == "data" and not is_manifest) or (
            failure_stage == "manifest" and is_manifest
        ):
            raise RuntimeError("simulated Dropbox failure")
        return _dropbox_metadata(remote, local.read_bytes())

    def fake_upload_bytes(data: bytes, remote: str, token: str) -> None:
        attempted_remote_paths.append(remote)
        return _dropbox_metadata(remote, data)

    monkeypatch.setattr(dropbox_sync, "_upload_immutable_file", fake_upload_file)
    monkeypatch.setattr(dropbox_sync, "_upload_bytes", fake_upload_bytes)
    secrets = dropbox_sync.DropboxSecrets("key", "secret", "refresh")

    with pytest.raises(RuntimeError, match="simulated Dropbox failure"):
        dropbox_sync.publish_recovery_bundle(
            archive,
            "/root",
            secrets,
            run_date="2026-07-06",
        )

    assert "/root/date=2026-07-06/_READY.json" not in attempted_remote_paths
