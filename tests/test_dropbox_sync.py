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

    remote_path = remote_path_for_file(archive_dir, file_path, "/Claude/DailyStockSnapshots")

    assert remote_path == "/Claude/DailyStockSnapshots/date=2026-07-04/recommendations/data.parquet"


def test_remote_path_for_file_skips_non_date_derived_files(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    file_path = archive_dir / "rating_events" / "data.parquet"
    file_path.parent.mkdir(parents=True)
    file_path.write_text("x", encoding="utf-8")

    remote_path = remote_path_for_file(archive_dir, file_path, "/Claude/DailyStockSnapshots")

    assert remote_path is None


def test_remote_path_for_file_sorts_first_seen_events_by_date(tmp_path: Path) -> None:
    archive_dir = tmp_path / "archive"
    file_path = archive_dir / "rating_events" / "date=2026-07-04" / "data.parquet"
    file_path.parent.mkdir(parents=True)
    file_path.write_text("x", encoding="utf-8")

    remote_path = remote_path_for_file(archive_dir, file_path, "/Claude/DailyStockSnapshots")

    assert remote_path == "/Claude/DailyStockSnapshots/date=2026-07-04/rating_events/data.parquet"


def test_auth_url_secret_loading_only_requires_app_key(monkeypatch) -> None:
    monkeypatch.setenv("DROPBOX_APP_KEY", "app-key")
    monkeypatch.delenv("DROPBOX_APP_SECRET", raising=False)

    secrets = load_dropbox_secrets(require_refresh_token=False, require_app_secret=False)

    assert secrets.app_key == "app-key"
