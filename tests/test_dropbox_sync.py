from __future__ import annotations

from pathlib import Path

from analyst_snapshot.dropbox_sync import (
    authorization_url,
    load_dropbox_secrets,
    remote_path_for_file,
)


def test_authorization_url_requests_offline_token() -> None:
    url = authorization_url("app-key")

    assert "client_id=app-key" in url
    assert "response_type=code" in url
    assert "token_access_type=offline" in url


def test_remote_path_for_file_sorts_snapshots_by_date(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    file_path = archive_dir / "recommendations" / "date=2026-07-04" / "data.parquet"
    file_path.parent.mkdir(parents=True)
    file_path.write_text("x", encoding="utf-8")

    remote_path = remote_path_for_file(archive_dir, file_path, "/DailyStockSnapshots")

    assert remote_path == "/DailyStockSnapshots/date=2026-07-04/recommendations/data.parquet"


def test_remote_path_for_file_skips_non_date_derived_files(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    file_path = archive_dir / "rating_events" / "data.parquet"
    file_path.parent.mkdir(parents=True)
    file_path.write_text("x", encoding="utf-8")

    remote_path = remote_path_for_file(archive_dir, file_path, "/DailyStockSnapshots")

    assert remote_path is None


def test_remote_path_for_file_sorts_first_seen_events_by_date(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    file_path = archive_dir / "rating_events" / "date=2026-07-04" / "data.parquet"
    file_path.parent.mkdir(parents=True)
    file_path.write_text("x", encoding="utf-8")

    remote_path = remote_path_for_file(archive_dir, file_path, "/DailyStockSnapshots")

    assert remote_path == "/DailyStockSnapshots/date=2026-07-04/rating_events/data.parquet"


def test_auth_url_secret_loading_only_requires_app_key(monkeypatch) -> None:
    monkeypatch.setenv("DROPBOX_APP_KEY", "app-key")
    monkeypatch.delenv("DROPBOX_APP_SECRET", raising=False)

    secrets = load_dropbox_secrets(require_refresh_token=False, require_app_secret=False)

    assert secrets.app_key == "app-key"


def test_upload_directory_can_be_scoped_to_one_run_date(tmp_path: Path, monkeypatch) -> None:
    from analyst_snapshot import dropbox_sync

    archive = tmp_path / "archive"
    for date_str in ("2026-07-03", "2026-07-04"):
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

    count = dropbox_sync.upload_directory(archive, "/root", secrets, run_date="2026-07-04")

    assert count == 1
    assert uploaded == ["/root/date=2026-07-04/recommendations/data.parquet"]


def test_upload_directory_counts_uploads_not_candidates(tmp_path: Path, monkeypatch) -> None:
    from analyst_snapshot import dropbox_sync

    archive = tmp_path / "archive"
    (archive / "_index").mkdir(parents=True)
    (archive / "_index" / "rating_events.parquet").write_text("x", encoding="utf-8")
    partition = archive / "recommendations" / "date=2026-07-04" / "data.parquet"
    partition.parent.mkdir(parents=True)
    partition.write_text("x", encoding="utf-8")

    monkeypatch.setattr(dropbox_sync, "refresh_access_token", lambda _secrets: "token")
    monkeypatch.setattr(dropbox_sync, "upload_file", lambda local, remote, token: None)
    secrets = dropbox_sync.DropboxSecrets("key", "secret", "refresh")

    # The derived index is skipped, so the returned count must not include it.
    assert dropbox_sync.upload_directory(archive, "/root", secrets) == 1
