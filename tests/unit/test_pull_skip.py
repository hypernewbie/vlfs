import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import vlfs


def _write_index(vlfs_dir: Path, index: dict) -> None:
    vlfs_dir.mkdir(parents=True, exist_ok=True)
    with open(vlfs_dir / "index.json", "w", encoding="utf-8") as f:
        json.dump(index, f)


def _write_config_with_r2_http(vlfs_dir: Path, public_url: str) -> None:
    vlfs_dir.mkdir(parents=True, exist_ok=True)
    content = f"""[remotes.r2]
public_base_url = "{public_url}"

[remotes.gdrive]
bucket = "vlfs"
"""
    (vlfs_dir / "config.toml").write_text(content)


def _write_config_no_public_url(vlfs_dir: Path, r2_bucket: str = "vlfs") -> None:
    vlfs_dir.mkdir(parents=True, exist_ok=True)
    content = f"""[remotes.r2]
bucket = "{r2_bucket}"

[remotes.gdrive]
bucket = "vlfs"
"""
    (vlfs_dir / "config.toml").write_text(content)


def _mock_download_r2_http_write(cache_dir: Path, data: bytes):
    """
    Return a mock function to mimic download_from_r2_http:
    It writes compressed data for each missing key into cache and returns count.
    """

    def _fn(missing_keys, cache_dir_arg, r2_public_url, dry_run):
        # Write compressed bytes for each missing key
        for key in missing_keys:
            obj_path = cache_dir_arg / "objects" / key
            obj_path.parent.mkdir(parents=True, exist_ok=True)
            compressed = vlfs.compress_bytes(data)
            obj_path.write_bytes(compressed)
        return len(missing_keys)

    return _fn


def _mock_download_drive_write(cache_dir: Path, data: bytes):
    """Return a mock function to mimic download_from_drive writing objects to cache."""

    def _fn(missing_keys, cache_dir_arg, bucket="vlfs", dry_run=False):
        for key in missing_keys:
            obj_path = cache_dir_arg / "objects" / key
            obj_path.parent.mkdir(parents=True, exist_ok=True)
            compressed = vlfs.compress_bytes(data)
            obj_path.write_bytes(compressed)
        return len(missing_keys)

    return _fn


def _obj_exists_in_workspace(repo_root: Path, rel_path: str) -> bool:
    return (repo_root / rel_path).exists()


def _read_stdout(capsys):
    cap = capsys.readouterr()
    return cap.out + cap.err


def _ensure_cache_dirs(cache_dir: Path):
    (cache_dir / "objects").mkdir(parents=True, exist_ok=True)


@pytest.mark.unit
def test_pull_skips_gdrive_without_auth(repo_root, monkeypatch, capsys, tmp_path):
    """
    If Google Drive auth is missing, gdrive objects should be skipped while R2 objects
    are downloaded and materialized. Exit code should be 0.
    """
    vlfs_dir = repo_root / ".vlfs"
    cache_dir = repo_root / ".vlfs-cache"

    # Index: one R2 file and one GDrive file
    index = {
        "version": 1,
        "entries": {
            "file_r2.bin": {
                "object_key": "ab/cd/r2_hash",
                "remote": "r2",
                "hash": "h1",
                "compressed_size": 100,
            },
            "file_gdrive.bin": {
                "object_key": "ef/gh/gdrive_hash",
                "remote": "gdrive",
                "hash": "h2",
                "compressed_size": 200,
            },
        },
    }
    _write_index(vlfs_dir, index)
    _write_config_with_r2_http(vlfs_dir, "https://example.com/vlfs")

    # Ensure cache dirs exist
    _ensure_cache_dirs(cache_dir)

    # No Drive token
    monkeypatch.setattr(vlfs, "has_drive_token", lambda: False)

    # Mock R2 HTTP download to write a valid compressed object into the cache
    r2_data = b"r2-contents"
    monkeypatch.setattr(
        vlfs,
        "download_from_r2_http",
        _mock_download_r2_http_write(cache_dir, r2_data),
    )

    # Ensure rclone invocations (if any) don't run external commands
    monkeypatch.setattr(vlfs, "run_rclone", lambda *a, **k: (0, "", ""))

    # Run pull
    rc = vlfs.cmd_pull(repo_root=repo_root, vlfs_dir=vlfs_dir, cache_dir=cache_dir)
    assert rc == 0

    out = _read_stdout(capsys)
    assert "Skipped 1 private files (Google Drive auth required)" in out
    assert "Wrote 1 files" in out

    # R2 file should be materialized into workspace
    assert _obj_exists_in_workspace(repo_root, "file_r2.bin")
    # GDrive file should NOT be materialized
    assert not _obj_exists_in_workspace(repo_root, "file_gdrive.bin")


@pytest.mark.unit
def test_pull_handles_ci_no_drive(repo_root, monkeypatch, capsys, tmp_path):
    """
    If has_drive_token raises RuntimeError (CI mode), cmd_pull should catch and skip
    gdrive files gracefully, not crash.
    """
    vlfs_dir = repo_root / ".vlfs"
    cache_dir = repo_root / ".vlfs-cache"

    index = {
        "version": 1,
        "entries": {
            "file_r2.bin": {
                "object_key": "ab/cd/r2_hash",
                "remote": "r2",
                "hash": "h1",
                "compressed_size": 100,
            },
            "file_gdrive.bin": {
                "object_key": "ef/gh/gdrive_hash",
                "remote": "gdrive",
                "hash": "h2",
                "compressed_size": 200,
            },
        },
    }
    _write_index(vlfs_dir, index)
    _write_config_with_r2_http(vlfs_dir, "https://example.com/vlfs")

    _ensure_cache_dirs(cache_dir)

    # Simulate CI: has_drive_token raises RuntimeError
    def _raise_ci():
        raise RuntimeError("Google Drive is not available in CI")

    monkeypatch.setattr(vlfs, "has_drive_token", _raise_ci)

    # Mock R2 HTTP download to write compressed object
    r2_data = b"r2-contents-ci"
    monkeypatch.setattr(
        vlfs,
        "download_from_r2_http",
        _mock_download_r2_http_write(cache_dir, r2_data),
    )

    monkeypatch.setattr(vlfs, "run_rclone", lambda *a, **k: (0, "", ""))

    # Run pull - should not raise, should return 0 and skip gdrive
    rc = vlfs.cmd_pull(repo_root=repo_root, vlfs_dir=vlfs_dir, cache_dir=cache_dir)
    assert rc == 0

    out = _read_stdout(capsys)
    assert "Skipped 1 private files (Google Drive auth required)" in out
    assert "Wrote 1 files" in out
    assert _obj_exists_in_workspace(repo_root, "file_r2.bin")
    assert not _obj_exists_in_workspace(repo_root, "file_gdrive.bin")


@pytest.mark.unit
def test_pull_with_token_downloads_all(repo_root, monkeypatch, capsys, tmp_path):
    """
    When has_drive_token returns True, both R2 and Drive objects should be downloaded
    and materialized.
    """
    vlfs_dir = repo_root / ".vlfs"
    cache_dir = repo_root / ".vlfs-cache"

    index = {
        "version": 1,
        "entries": {
            "file_r2.bin": {
                "object_key": "ab/cd/r2_hash",
                "remote": "r2",
                "hash": "h1",
                "compressed_size": 100,
            },
            "file_gdrive.bin": {
                "object_key": "ef/gh/gdrive_hash",
                "remote": "gdrive",
                "hash": "h2",
                "compressed_size": 200,
            },
        },
    }
    _write_index(vlfs_dir, index)
    # Use HTTP for R2 in this test as well
    _write_config_with_r2_http(vlfs_dir, "https://example.com/vlfs")

    _ensure_cache_dirs(cache_dir)

    # Drive is available
    monkeypatch.setattr(vlfs, "has_drive_token", lambda: True)

    # Mock R2 HTTP download and Drive download to write compressed objects into cache
    r2_data = b"r2-all"
    gdrive_data = b"gdrive-all"
    monkeypatch.setattr(
        vlfs,
        "download_from_r2_http",
        _mock_download_r2_http_write(cache_dir, r2_data),
    )
    monkeypatch.setattr(
        vlfs,
        "download_from_drive",
        _mock_download_drive_write(cache_dir, gdrive_data),
    )

    monkeypatch.setattr(vlfs, "run_rclone", lambda *a, **k: (0, "", ""))

    rc = vlfs.cmd_pull(repo_root=repo_root, vlfs_dir=vlfs_dir, cache_dir=cache_dir)
    assert rc == 0

    out = _read_stdout(capsys)
    # No skipped message expected
    assert "Skipped" not in out
    # Both files should be written
    assert "Wrote 2 files" in out
    assert _obj_exists_in_workspace(repo_root, "file_r2.bin")
    assert _obj_exists_in_workspace(repo_root, "file_gdrive.bin")