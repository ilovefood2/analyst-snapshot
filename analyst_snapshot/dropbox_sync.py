from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"
AUTH_URL = "https://www.dropbox.com/oauth2/authorize"
CONTENT_API_URL = "https://content.dropboxapi.com/2"
MAX_SINGLE_UPLOAD_BYTES = 150 * 1024 * 1024
CHUNK_SIZE_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class DropboxSecrets:
    app_key: str
    app_secret: str
    refresh_token: str | None = None


def load_dropbox_secrets(
    require_refresh_token: bool = True,
    require_app_secret: bool = True,
) -> DropboxSecrets:
    app_key = os.getenv("DROPBOX_APP_KEY", "")
    app_secret = os.getenv("DROPBOX_APP_SECRET", "")
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
    missing = []
    if not app_key:
        missing.append("DROPBOX_APP_KEY")
    if require_app_secret and not app_secret:
        missing.append("DROPBOX_APP_SECRET")
    if require_refresh_token and not refresh_token:
        missing.append("DROPBOX_REFRESH_TOKEN")
    if missing:
        raise ValueError(f"Missing Dropbox environment variable(s): {', '.join(missing)}")
    return DropboxSecrets(app_key=app_key, app_secret=app_secret, refresh_token=refresh_token)


def authorization_url(app_key: str) -> str:
    query = urllib.parse.urlencode(
        {
            "client_id": app_key,
            "response_type": "code",
            "token_access_type": "offline",
        }
    )
    return f"{AUTH_URL}?{query}"


def exchange_authorization_code(secrets: DropboxSecrets, code: str) -> str:
    payload = _post_form(
        TOKEN_URL,
        {
            "code": code,
            "grant_type": "authorization_code",
            "client_id": secrets.app_key,
            "client_secret": secrets.app_secret,
        },
    )
    refresh_token = payload.get("refresh_token")
    if not isinstance(refresh_token, str) or not refresh_token:
        raise RuntimeError(
            "Dropbox did not return a refresh_token. Re-authorize with offline access."
        )
    return refresh_token


def refresh_access_token(secrets: DropboxSecrets) -> str:
    if not secrets.refresh_token:
        raise ValueError("DROPBOX_REFRESH_TOKEN is required")
    payload = _post_form(
        TOKEN_URL,
        {
            "refresh_token": secrets.refresh_token,
            "grant_type": "refresh_token",
            "client_id": secrets.app_key,
            "client_secret": secrets.app_secret,
        },
    )
    access_token = payload.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        raise RuntimeError("Dropbox did not return an access_token")
    return access_token


def upload_directory(
    local_dir: Path,
    remote_root: str,
    secrets: DropboxSecrets,
    run_date: str | None = None,
) -> int:
    """Upload archive partitions to Dropbox and return the number of files actually uploaded.

    ``run_date`` restricts the upload to a single partition date. The daily job passes it so a run
    uploads that day's files instead of re-uploading the entire (permanently growing) archive.
    """
    if not local_dir.exists():
        print(f"Dropbox upload skipped: {local_dir} does not exist.")
        return 0
    candidates = sorted(path for path in local_dir.rglob("*") if path.is_file())
    if run_date is not None:
        wanted = f"date={run_date}"
        candidates = [path for path in candidates if wanted in path.parts]
        if not candidates:
            print(f"Dropbox upload skipped: no files under date={run_date}.")
            return 0

    access_token = refresh_access_token(secrets)
    uploaded = 0
    for path in candidates:
        remote_path = remote_path_for_file(local_dir, path, remote_root)
        if remote_path is None:
            print(f"skipped non-date archive file {path}")
            continue
        upload_file(path, remote_path, access_token)
        uploaded += 1
        print(f"uploaded {path} -> dropbox:{remote_path}")
    return uploaded


def remote_path_for_file(local_dir: Path, file_path: Path, remote_root: str) -> str | None:
    root = "/" + remote_root.strip("/")
    relative_parts = file_path.relative_to(local_dir).parts
    if len(relative_parts) >= 3 and relative_parts[1].startswith("date="):
        dataset, date_part, *remaining = relative_parts
        filename = "/".join(remaining)
        return f"{root}/{date_part}/{dataset}/{filename}".replace("//", "/")
    return None


def upload_file(local_path: Path, remote_path: str, access_token: str) -> None:
    size = local_path.stat().st_size
    if size <= MAX_SINGLE_UPLOAD_BYTES:
        _upload_small_file(local_path, remote_path, access_token)
    else:
        _upload_large_file(local_path, remote_path, access_token)


def _upload_small_file(local_path: Path, remote_path: str, access_token: str) -> None:
    args = {
        "path": remote_path,
        "mode": "overwrite",
        "autorename": False,
        "mute": True,
        "strict_conflict": False,
    }
    _post_content(
        f"{CONTENT_API_URL}/files/upload",
        access_token,
        args,
        local_path.read_bytes(),
    )


def _upload_large_file(local_path: Path, remote_path: str, access_token: str) -> None:
    with local_path.open("rb") as handle:
        first_chunk = handle.read(CHUNK_SIZE_BYTES)
        response = _post_content(
            f"{CONTENT_API_URL}/files/upload_session/start",
            access_token,
            {"close": False},
            first_chunk,
        )
        session_id = response["session_id"]
        offset = len(first_chunk)

        while True:
            chunk = handle.read(CHUNK_SIZE_BYTES)
            if not chunk:
                break
            next_offset = offset + len(chunk)
            is_last_chunk = next_offset == local_path.stat().st_size
            if is_last_chunk:
                _post_content(
                    f"{CONTENT_API_URL}/files/upload_session/finish",
                    access_token,
                    {
                        "cursor": {"session_id": session_id, "offset": offset},
                        "commit": {
                            "path": remote_path,
                            "mode": "overwrite",
                            "autorename": False,
                            "mute": True,
                            "strict_conflict": False,
                        },
                    },
                    chunk,
                )
            else:
                _post_content(
                    f"{CONTENT_API_URL}/files/upload_session/append_v2",
                    access_token,
                    {"cursor": {"session_id": session_id, "offset": offset}, "close": False},
                    chunk,
                )
            offset = next_offset


def _post_form(url: str, fields: dict[str, str]) -> dict[str, Any]:
    data = urllib.parse.urlencode(fields).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    return _json_response(request)


def _post_content(
    url: str,
    access_token: str,
    args: dict[str, Any],
    data: bytes,
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Dropbox-API-Arg": json.dumps(args, separators=(",", ":")),
            "Content-Type": "application/octet-stream",
        },
        method="POST",
    )
    return _json_response(request)


def _json_response(request: urllib.request.Request) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Dropbox API error {exc.code}: {detail}") from exc
    if not body:
        return {}
    payload = json.loads(body.decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("Dropbox API returned an unexpected non-object response")
    return payload
