from __future__ import annotations

import hashlib
import json
import os
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from analyst_snapshot.datasets import DATASETS, MARKET_CONTEXT_DATASETS, RATING_EVENTS_DATASET

TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"
AUTH_URL = "https://www.dropbox.com/oauth2/authorize"
CONTENT_API_URL = "https://content.dropboxapi.com/2"
MAX_SINGLE_UPLOAD_BYTES = 150 * 1024 * 1024
CHUNK_SIZE_BYTES = 8 * 1024 * 1024
DROPBOX_CONTENT_HASH_BLOCK_BYTES = 4 * 1024 * 1024
RECOVERY_BUNDLE_SCHEMA = "swinglab_recovery_bundle_v1"
RECOVERY_READY_SCHEMA = "swinglab_recovery_ready_v1"
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_GENERATION_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_REQUIRED_PARQUET_DATASETS = {spec.name for spec in DATASETS.values()} | set(
    MARKET_CONTEXT_DATASETS
)


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


def publish_recovery_bundle(
    local_archive: Path,
    remote_root: str,
    secrets: DropboxSecrets,
    run_date: str,
) -> int:
    """Validate and publish one immutable recovery generation to Dropbox.

    The date-level ``_READY.json`` pointer is deliberately uploaded last. An incomplete or
    corrupted generation can therefore remain on Dropbox after a failed upload, but it cannot be
    selected as the ready generation.
    """
    _validate_run_date(run_date)
    archive_root = local_archive.resolve()
    manifest_path = local_archive / "_recovery_manifests" / f"date={run_date}" / "manifest.json"
    resolved_manifest_path = manifest_path.resolve()
    if not resolved_manifest_path.is_relative_to(archive_root):
        raise ValueError(f"Recovery manifest escapes local archive: {manifest_path}")
    manifest_path = resolved_manifest_path
    manifest, manifest_bytes = _load_recovery_manifest(manifest_path)
    generation_id, files = _validate_recovery_manifest(
        manifest,
        local_archive=local_archive,
        run_date=run_date,
    )
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    manifest_identity_sha256 = str(manifest["manifest_identity_sha256"])

    # Do not acquire a token or create any remote generation until the complete local inventory
    # has passed its size and digest checks.
    for file_path, expected_bytes, expected_sha256, relative_path in files:
        _verify_recovery_file(file_path, expected_bytes, expected_sha256, relative_path)

    access_token = refresh_access_token(secrets)
    generation_root = _remote_join(
        remote_root,
        f"date={run_date}",
        "generations",
        generation_id,
    )
    uploaded = 0
    for file_path, expected_bytes, expected_sha256, relative_path in files:
        expected_content_hash = _verify_recovery_file(
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
        _verify_recovery_file(file_path, expected_bytes, expected_sha256, relative_path)
        uploaded += 1
        print(f"uploaded {file_path} -> dropbox:{remote_path}")

    # Publish the exact manifest bytes that were validated. A concurrent local manifest change
    # fails closed rather than allowing READY to bind to bytes other than those uploaded.
    if manifest_path.read_bytes() != manifest_bytes:
        raise ValueError(f"Recovery manifest changed during publication: {manifest_path}")
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
        raise ValueError(f"Recovery manifest changed during publication: {manifest_path}")
    uploaded += 1
    print(f"uploaded {manifest_path} -> dropbox:{remote_manifest_path}")

    ready_payload = {
        "schema": RECOVERY_READY_SCHEMA,
        "status": "ready",
        "session_date": run_date,
        "generation_id": generation_id,
        "manifest_path": f"generations/{generation_id}/manifest.json",
        "manifest_sha256": manifest_sha256,
        "manifest_identity_sha256": manifest_identity_sha256,
        "files_count": len(files),
        "published_at_utc": _utc_now(),
    }
    ready_payload["ready_identity_sha256"] = _identity_sha256(
        ready_payload,
        self_hash_field="ready_identity_sha256",
    )
    ready_bytes = (json.dumps(ready_payload, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )
    remote_ready_path = _remote_join(remote_root, f"date={run_date}", "_READY.json")
    ready_metadata = _upload_bytes(ready_bytes, remote_ready_path, access_token)
    _verify_dropbox_commit(
        ready_metadata,
        remote_path=remote_ready_path,
        expected_bytes=len(ready_bytes),
        expected_content_hash=_dropbox_content_hash_bytes(ready_bytes),
    )
    uploaded += 1
    print(f"uploaded recovery READY -> dropbox:{remote_ready_path}")
    return uploaded


def _load_recovery_manifest(manifest_path: Path) -> tuple[dict[str, Any], bytes]:
    try:
        raw = manifest_path.read_bytes()
    except OSError as exc:
        raise ValueError(f"Recovery manifest is not readable: {manifest_path}") from exc
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Recovery manifest is not valid UTF-8 JSON: {manifest_path}") from exc
    if not isinstance(payload, dict):
        raise ValueError("Recovery manifest must be a JSON object")
    return payload, raw


def _validate_recovery_manifest(
    manifest: dict[str, Any],
    *,
    local_archive: Path,
    run_date: str,
) -> tuple[str, list[tuple[Path, int, str, str]]]:
    if manifest.get("schema") != RECOVERY_BUNDLE_SCHEMA:
        raise ValueError(f"Recovery manifest schema must be {RECOVERY_BUNDLE_SCHEMA}")
    if manifest.get("status") != "complete":
        raise ValueError("Recovery manifest status must be complete")
    if manifest.get("session_date") != run_date:
        raise ValueError(
            "Recovery manifest session_date does not match requested run date: "
            f"{manifest.get('session_date')!r} != {run_date!r}"
        )

    expected_manifest_identity = manifest.get("manifest_identity_sha256")
    if not isinstance(expected_manifest_identity, str) or not _SHA256_PATTERN.fullmatch(
        expected_manifest_identity
    ):
        raise ValueError("Recovery manifest manifest_identity_sha256 is invalid")
    actual_manifest_identity = _identity_sha256(
        manifest,
        self_hash_field="manifest_identity_sha256",
    )
    if actual_manifest_identity != expected_manifest_identity:
        raise ValueError(
            "Recovery manifest identity mismatch: "
            f"{actual_manifest_identity} != {expected_manifest_identity}"
        )

    generation_id = manifest.get("generation_id")
    if not isinstance(generation_id, str) or not _GENERATION_ID_PATTERN.fullmatch(generation_id):
        raise ValueError("Recovery manifest generation_id is missing or path-unsafe")

    inventory = manifest.get("files")
    if not isinstance(inventory, list) or not inventory:
        raise ValueError("Recovery manifest must contain at least one data file")

    archive_root = local_archive.resolve()
    expected_date_part = f"date={run_date}"
    seen_paths: set[str] = set()
    parquet_datasets: set[str] = set()
    analyst_manifest_count = 0
    market_manifest_count = 0
    market_source_count = 0
    files: list[tuple[Path, int, str, str]] = []
    for index, item in enumerate(inventory):
        if not isinstance(item, dict):
            raise ValueError(f"Recovery manifest files[{index}] must be an object")
        relative_path = item.get("path")
        if not isinstance(relative_path, str) or not relative_path:
            raise ValueError(f"Recovery manifest files[{index}].path must be a non-empty string")
        if "\\" in relative_path:
            raise ValueError(
                f"Recovery file path must use archive-relative POSIX syntax: {relative_path}"
            )
        raw_parts = relative_path.split("/")
        posix_path = PurePosixPath(relative_path)
        if (
            posix_path.is_absolute()
            or any(part in {"", ".", ".."} for part in raw_parts)
            or posix_path.as_posix() != relative_path
        ):
            raise ValueError(
                f"Recovery file path is not a normalized relative path: {relative_path}"
            )
        if relative_path in seen_paths:
            raise ValueError(f"Recovery manifest contains duplicate path: {relative_path}")
        seen_paths.add(relative_path)
        if posix_path.parts[0] == "_recovery_manifests":
            raise ValueError(f"Recovery manifest cannot list itself as data: {relative_path}")
        date_parts = [part for part in posix_path.parts if part.startswith("date=")]
        if date_parts != [expected_date_part]:
            raise ValueError(
                f"Recovery file path must belong only to {expected_date_part}: {relative_path}"
            )

        expected_sha256 = item.get("sha256")
        if not isinstance(expected_sha256, str) or not _SHA256_PATTERN.fullmatch(expected_sha256):
            raise ValueError(f"Recovery manifest files[{index}].sha256 is invalid")
        expected_bytes = item.get("bytes")
        if (
            not isinstance(expected_bytes, int)
            or isinstance(expected_bytes, bool)
            or expected_bytes < 0
        ):
            raise ValueError(f"Recovery manifest files[{index}].bytes is invalid")

        kind = item.get("kind")
        if kind == "parquet":
            dataset = item.get("dataset")
            allowed = _REQUIRED_PARQUET_DATASETS | {RATING_EVENTS_DATASET}
            if not isinstance(dataset, str) or dataset not in allowed:
                raise ValueError(f"Recovery manifest files[{index}].dataset is invalid")
            if dataset in parquet_datasets:
                raise ValueError(f"Recovery manifest contains duplicate parquet dataset: {dataset}")
            parquet_datasets.add(dataset)
            rows = item.get("rows")
            if not isinstance(rows, int) or isinstance(rows, bool) or rows <= 0:
                raise ValueError(f"Recovery manifest files[{index}].rows must be positive")
            schema_sha256 = item.get("schema_sha256")
            if not isinstance(schema_sha256, str) or not _SHA256_PATTERN.fullmatch(schema_sha256):
                raise ValueError(f"Recovery manifest files[{index}].schema_sha256 is invalid")
            for field in ("pit_column", "pit_min_utc", "pit_max_utc"):
                value = item.get(field)
                if not isinstance(value, str) or not value:
                    raise ValueError(f"Recovery manifest files[{index}].{field} is missing")
        elif kind == "analyst_run_manifest":
            analyst_manifest_count += 1
        elif kind == "market_context_manifest":
            market_manifest_count += 1
        elif kind == "market_context_source":
            market_source_count += 1
        else:
            raise ValueError(f"Recovery manifest files[{index}].kind is invalid")

        file_path = (archive_root / Path(*posix_path.parts)).resolve()
        if not file_path.is_relative_to(archive_root):
            raise ValueError(f"Recovery file escapes local archive: {relative_path}")
        if not file_path.is_file():
            raise ValueError(f"Recovery file is missing or not a regular file: {relative_path}")
        files.append((file_path, expected_bytes, expected_sha256, relative_path))

    missing_datasets = _REQUIRED_PARQUET_DATASETS - parquet_datasets
    if missing_datasets:
        raise ValueError(
            f"Recovery manifest is missing required parquet datasets: {sorted(missing_datasets)}"
        )
    if analyst_manifest_count < 1:
        raise ValueError("Recovery manifest requires at least one analyst_run_manifest")
    if market_manifest_count != 1:
        raise ValueError("Recovery manifest requires exactly one market_context_manifest")
    if market_source_count < 1:
        raise ValueError("Recovery manifest requires at least one market_context_source")

    files.sort(key=lambda item: item[3])
    return generation_id, files


def _verify_recovery_file(
    file_path: Path,
    expected_bytes: int,
    expected_sha256: str,
    relative_path: str,
) -> str:
    actual_bytes = file_path.stat().st_size
    if actual_bytes != expected_bytes:
        raise ValueError(
            f"Recovery file byte count mismatch for {relative_path}: "
            f"{actual_bytes} != {expected_bytes}"
        )
    actual_sha256, content_hash = _file_hashes(file_path)
    if actual_sha256 != expected_sha256:
        raise ValueError(
            f"Recovery file SHA-256 mismatch for {relative_path}: "
            f"{actual_sha256} != {expected_sha256}"
        )
    return content_hash


def _file_hashes(path: Path) -> tuple[str, str]:
    sha256_digest = hashlib.sha256()
    dropbox_digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(DROPBOX_CONTENT_HASH_BLOCK_BYTES):
            sha256_digest.update(block)
            dropbox_digest.update(hashlib.sha256(block).digest())
    return sha256_digest.hexdigest(), dropbox_digest.hexdigest()


def _dropbox_content_hash_bytes(data: bytes) -> str:
    digest = hashlib.sha256()
    for offset in range(0, len(data), DROPBOX_CONTENT_HASH_BLOCK_BYTES):
        block = data[offset : offset + DROPBOX_CONTENT_HASH_BLOCK_BYTES]
        digest.update(hashlib.sha256(block).digest())
    return digest.hexdigest()


def _verify_dropbox_commit(
    metadata: dict[str, Any],
    *,
    remote_path: str,
    expected_bytes: int,
    expected_content_hash: str,
) -> None:
    if not isinstance(metadata, dict):
        raise RuntimeError(f"Dropbox upload returned no file metadata for {remote_path}")
    if metadata.get("size") != expected_bytes:
        raise RuntimeError(
            f"Dropbox size mismatch for {remote_path}: {metadata.get('size')!r} != {expected_bytes}"
        )
    if metadata.get("content_hash") != expected_content_hash:
        raise RuntimeError(
            f"Dropbox content hash mismatch for {remote_path}: "
            f"{metadata.get('content_hash')!r} != {expected_content_hash}"
        )
    path_display = metadata.get("path_display")
    path_lower = metadata.get("path_lower")
    if path_display != remote_path and path_lower != remote_path.lower():
        raise RuntimeError(
            f"Dropbox path mismatch for {remote_path}: "
            f"path_display={path_display!r} path_lower={path_lower!r}"
        )


def _identity_sha256(payload: dict[str, Any], *, self_hash_field: str) -> str:
    identity = {key: value for key, value in payload.items() if key != self_hash_field}
    try:
        canonical = json.dumps(
            identity,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Recovery identity payload is not canonical JSON: {exc}") from exc
    return hashlib.sha256(canonical).hexdigest()


def _validate_run_date(run_date: str) -> None:
    try:
        parsed = date.fromisoformat(run_date)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Recovery run_date is not a valid ISO date: {run_date!r}") from exc
    if parsed.isoformat() != run_date:
        raise ValueError(f"Recovery run_date is not canonical YYYY-MM-DD: {run_date!r}")


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _remote_join(root: str, *parts: str) -> str:
    root_part = root.strip("/")
    joined = "/".join(
        [part for part in (root_part, *(value.strip("/") for value in parts)) if part]
    )
    return f"/{joined}"


def _upload_bytes(data: bytes, remote_path: str, access_token: str) -> dict[str, Any]:
    args = {
        "path": remote_path,
        "mode": "overwrite",
        "autorename": False,
        "mute": True,
        "strict_conflict": False,
    }
    return _post_content(
        f"{CONTENT_API_URL}/files/upload",
        access_token,
        args,
        data,
    )


def remote_path_for_file(local_dir: Path, file_path: Path, remote_root: str) -> str | None:
    root = "/" + remote_root.strip("/")
    relative_parts = file_path.relative_to(local_dir).parts
    if len(relative_parts) >= 3 and relative_parts[1].startswith("date="):
        dataset, date_part, *remaining = relative_parts
        filename = "/".join(remaining)
        return f"{root}/{date_part}/{dataset}/{filename}".replace("//", "/")
    return None


def upload_file(local_path: Path, remote_path: str, access_token: str) -> dict[str, Any]:
    return _upload_file_with_mode(
        local_path,
        remote_path,
        access_token,
        mode="overwrite",
        strict_conflict=False,
    )


def _upload_immutable_file(
    local_path: Path,
    remote_path: str,
    access_token: str,
) -> dict[str, Any]:
    return _upload_file_with_mode(
        local_path,
        remote_path,
        access_token,
        mode="add",
        strict_conflict=True,
    )


def _upload_file_with_mode(
    local_path: Path,
    remote_path: str,
    access_token: str,
    *,
    mode: str,
    strict_conflict: bool,
) -> dict[str, Any]:
    size = local_path.stat().st_size
    if size <= MAX_SINGLE_UPLOAD_BYTES:
        return _upload_small_file(
            local_path,
            remote_path,
            access_token,
            mode=mode,
            strict_conflict=strict_conflict,
        )
    return _upload_large_file(
        local_path,
        remote_path,
        access_token,
        mode=mode,
        strict_conflict=strict_conflict,
    )


def _upload_small_file(
    local_path: Path,
    remote_path: str,
    access_token: str,
    *,
    mode: str,
    strict_conflict: bool,
) -> dict[str, Any]:
    args = {
        "path": remote_path,
        "mode": mode,
        "autorename": False,
        "mute": True,
        "strict_conflict": strict_conflict,
    }
    return _post_content(
        f"{CONTENT_API_URL}/files/upload",
        access_token,
        args,
        local_path.read_bytes(),
    )


def _upload_large_file(
    local_path: Path,
    remote_path: str,
    access_token: str,
    *,
    mode: str,
    strict_conflict: bool,
) -> dict[str, Any]:
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
                return _post_content(
                    f"{CONTENT_API_URL}/files/upload_session/finish",
                    access_token,
                    {
                        "cursor": {"session_id": session_id, "offset": offset},
                        "commit": {
                            "path": remote_path,
                            "mode": mode,
                            "autorename": False,
                            "mute": True,
                            "strict_conflict": strict_conflict,
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
    raise RuntimeError(f"Dropbox upload session ended without committing {remote_path}")


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
